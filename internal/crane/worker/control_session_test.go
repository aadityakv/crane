package worker

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestControlRejectsOperationBeforeHandshakeAndBindsHandshakeSlots(t *testing.T) {
	fixture := newControlFixture(t)
	session := fixture.session(t, 2)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 1, protocol.FenceRequest{CoordinatorEpoch: fixture.epoch})); !errors.Is(err, ErrControlHandshakeRequired) {
		t.Fatalf("pre-handshake fence error = %v", err)
	}
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, fixture.handshake(2)))
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := response.(protocol.WorkerHandshakeAck)
	if !ok || ack.NodeID != fixture.repository.localNode || ack.WorkerEpoch != fixture.repository.localEpoch || ack.SlotCapacity != fixture.configuration.Crane.WorkerSlots || ack.ConsensusFingerprint != model.ConsensusFingerprint() || ack.RegistryFingerprint != model.RegistryFingerprint() {
		t.Fatalf("handshake ACK = %#v", response)
	}
	if fixture.repository.fenceCalls != 0 {
		t.Fatal("pre-handshake request mutated durable state")
	}
}

func TestControlHandshakeFailsClosedForFingerprintMembershipReplayAndChange(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*controlFixture, *protocol.WorkerHandshake)
	}{
		{name: "unknown member", mutate: func(f *controlFixture, _ *protocol.WorkerHandshake) { f.members.view.Members = nil }},
		{name: "wrong ip", mutate: func(f *controlFixture, _ *protocol.WorkerHandshake) {
			f.members.authorizeErr = membership.ErrUnauthorized
		}},
		{name: "consensus", mutate: func(_ *controlFixture, h *protocol.WorkerHandshake) { h.ConsensusFingerprint[0] ^= 1 }},
		{name: "registry", mutate: func(_ *controlFixture, h *protocol.WorkerHandshake) { h.RegistryFingerprint[0] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newControlFixture(t)
			handshake := fixture.handshake(2)
			test.mutate(fixture, &handshake)
			if _, err := fixture.session(t, 2).Handle(context.Background(), fixture.frameUnchecked(2, 1, handshake)); err == nil {
				t.Fatal("invalid handshake accepted")
			}
		})
	}

	fixture := newControlFixture(t)
	session := fixture.session(t, 2)
	handshakeFrame := fixture.frame(t, 2, 1, fixture.handshake(2))
	if _, err := session.Handle(context.Background(), handshakeFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Handle(context.Background(), handshakeFrame); !errors.Is(err, wire.ErrReplay) {
		t.Fatalf("replayed handshake error = %v", err)
	}
	fixture.members.view.Members[1].Incarnation++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.FenceRequest{CoordinatorEpoch: fixture.epoch})); !errors.Is(err, ErrControlUnauthorized) {
		t.Fatalf("changed membership error = %v", err)
	}
}

func TestControlFenceDrainsGatePersistsThenClosesOlderSessions(t *testing.T) {
	fixture := newControlFixture(t)
	old := fixture.session(t, 3)
	oldClosed := make(chan struct{})
	old.close = func() error { close(oldClosed); return nil }
	fixture.authenticate(t, old, 3, 1)
	current := fixture.session(t, 2)
	fixture.authenticate(t, current, 2, 2)

	prior := model.CoordinatorEpoch{Term: 3, BeginIndex: 8, Coordinator: 3, Nonce: [16]byte{6}}
	fixture.repository.work.Fence = prior
	if err := fixture.gate.Open(prior); err != nil {
		t.Fatal(err)
	}
	release, err := fixture.gate.Enter()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, handleErr := current.Handle(context.Background(), fixture.frame(t, 2, 3, protocol.FenceRequest{CoordinatorEpoch: fixture.epoch}))
		done <- handleErr
	}()
	select {
	case <-fixture.repository.fenceStarted:
		t.Fatal("fence persisted before admitted work drained")
	case <-time.After(10 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fixture.repository.log[0] != "fence" {
		t.Fatalf("durable order = %v", fixture.repository.log)
	}
	select {
	case <-oldClosed:
	default:
		t.Fatal("older coordinator session remained open")
	}
}

func TestControlFenceRetryDoesNotRecloseCurrentRunningGateAndRejectsCollisions(t *testing.T) {
	fixture := newControlFixture(t)
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	fixture.repository.work.Fence = fixture.epoch
	if err := fixture.gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.FenceRequest{CoordinatorEpoch: fixture.epoch})); err != nil {
		t.Fatal(err)
	}
	if release, err := fixture.gate.Enter(); err != nil {
		t.Fatalf("same-fence retry closed current Running gate: %v", err)
	} else {
		release()
	}
	if fixture.repository.fenceCalls != 0 {
		t.Fatal("same-fence retry rewrote durable fence")
	}
	collision := fixture.epoch
	collision.Nonce[0]++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, protocol.FenceRequest{CoordinatorEpoch: collision})); !errors.Is(err, ErrControlStaleEpoch) {
		t.Fatalf("same-order collision error = %v", err)
	}
	nonvoter := fixture.session(t, 4)
	fixture.authenticate(t, nonvoter, 4, 4)
	if _, err := nonvoter.Handle(context.Background(), fixture.frame(t, 4, 5, protocol.FenceRequest{CoordinatorEpoch: model.CoordinatorEpoch{Term: 5, BeginIndex: 10, Coordinator: 4, Nonce: [16]byte{9}}})); !errors.Is(err, ErrControlUnauthorized) {
		t.Fatalf("nonvoter fence error = %v", err)
	}
}

func TestControlAssignmentIsDurableBeforeReconcileAndOpensExactCurrentRunningGate(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	install := protocol.AssignmentSetInstall{
		Assignment: fixture.assignment.Assignment, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(),
		JobControlRevision: fixture.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch,
	}
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, install))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := response.(protocol.AssignmentSetInstallAck); !ok {
		t.Fatalf("response = %#v", response)
	}
	if !reflect.DeepEqual(fixture.repository.log, []string{"assignment", "reconcile"}) {
		t.Fatalf("assignment/reconcile order = %v", fixture.repository.log)
	}
	if release, err := fixture.gate.Enter(); err != nil {
		t.Fatalf("exact-current Running install did not open gate: %v", err)
	} else {
		release()
	}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, install)); err != nil {
		t.Fatalf("exact install retry = %v", err)
	}
	if fixture.repository.installCalls != 2 {
		t.Fatalf("install calls = %d", fixture.repository.installCalls)
	}
}

func TestControlStatusPagesDeepOwnedDataAndRejectsUncommittedInventoryVector(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	event := workerFixture(t).failureEvent(1)
	fixture.repository.work.PendingEvents = []model.WorkerEvent{event}
	fixture.repository.work.NextTransactionID = 2
	fixture.repository.transactionID = 7
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1}))
	if err != nil {
		t.Fatal(err)
	}
	status := response.(protocol.WorkerStatus)
	if status.StoreTransactionID != 7 || len(status.Assignments) != 1 || len(status.Events) != 1 || status.LastTransactionID != 1 {
		t.Fatalf("status = %#v", status)
	}
	fixture.repository.work.PendingEvents[0].Failure.DetailDigest[0]++
	fixture.repository.work.Assignments[0].SpecificationBytes[0]++
	if status.Events[0].Failure.DetailDigest == fixture.repository.work.PendingEvents[0].Failure.DetailDigest {
		t.Fatal("status shared event storage with repository")
	}

	query := fixture.inventoryQuery(t)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query})); !errors.Is(err, ErrControlStaleAssignment) {
		t.Fatalf("uncommitted inventory vector error = %v", err)
	}
}

func TestControlRepairPersistsBeforeStatusAndResultChunkBindsExactSessionPeer(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	query := fixture.inventoryQuery(t)
	fixture.repository.work.Sources = []store.SourceCursor{{Source: query.Checkpoints[0].Source, Watermark: query.Checkpoints[0].Watermark, RaftIndex: 11}}
	grant := fixture.repairGrant(t, query)
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Repair: &grant}))
	if err != nil {
		t.Fatal(err)
	}
	status := response.(protocol.WorkerStatus)
	if status.Repair == nil || fixture.repository.log[0] != "repair" {
		t.Fatalf("repair status/log = %#v/%v", status.Repair, fixture.repository.log)
	}

	chunk := protocol.ResultRecordChunk{}
	fixture.transfer.ack = protocol.ResultRecordAck{NodeID: fixture.repository.localNode, WorkerEpoch: fixture.repository.localEpoch, CoordinatorEpoch: fixture.epoch}
	if _, err := session.Handle(context.Background(), fixture.frameUnchecked(2, 3, chunk)); err == nil {
		t.Fatal("malformed chunk reached transfer owner")
	}
	if fixture.transfer.calls != 0 {
		t.Fatal("malformed chunk mutated transfer owner")
	}
}

func TestControlCheckpointUsesSerializedEngineBeforeAck(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: fixture.assignment.Assignment.Tasks[0].Task, Watermark: 1, RaftIndex: 7, Epoch: fixture.epoch}, JobControlRevision: 1, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, notice))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := response.(protocol.CheckpointAck); !ok || !reflect.DeepEqual(fixture.repository.log, []string{"checkpoint"}) {
		t.Fatalf("checkpoint response/log = %#v/%v", response, fixture.repository.log)
	}
}

func TestControlBoundsInvalidReplaySendersAndCanceledWorkNeverMutates(t *testing.T) {
	fixture := newControlFixture(t)
	for node := uint16(10); node < 100; node++ {
		session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.9"), Port: 40000}, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		_, _ = session.Handle(context.Background(), fixture.frameUnchecked(node, byte(node), fixture.handshake(node)))
		_ = session.Close()
	}
	if got := len(fixture.owner.replay.perSender); got > 32 {
		t.Fatalf("invalid sender replay owners = %d, want at most 32", got)
	}

	fixture = newControlFixture(t)
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	for attempt := byte(2); attempt < 32; attempt++ {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		epoch := model.CoordinatorEpoch{Term: uint64(attempt + 10), BeginIndex: uint64(attempt + 20), Coordinator: 2, Nonce: [16]byte{attempt}}
		if _, err := session.Handle(canceled, fixture.frame(t, 2, attempt, protocol.FenceRequest{CoordinatorEpoch: epoch})); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled request %d error = %v", attempt, err)
		}
	}
	if fixture.repository.fenceCalls != 0 {
		t.Fatalf("canceled work persisted %d fences", fixture.repository.fenceCalls)
	}
}

type controlFixture struct {
	configuration config.NodeConfig
	repository    *controlTestRepository
	engine        *controlTestEngine
	transfer      *controlTestTransfer
	members       *controlTestMembership
	gate          *admission.Gate
	owner         *ControlOwner
	assignment    store.InstalledAssignment
	epoch         model.CoordinatorEpoch
	cluster       [16]byte
}

func newControlFixture(t *testing.T) *controlFixture {
	t.Helper()
	base := workerFixture(t)
	configuration := config.NodeConfig{NodeID: base.localNode, Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig(), RaftVoters: []config.RaftVoter{{NodeID: 2}, {NodeID: 3}, {NodeID: 5}}}
	repository := &controlTestRepository{localNode: base.localNode, localEpoch: base.localEpoch, work: store.RecoveredWork{NextTransactionID: 1}, transactionID: 1, fenceStarted: make(chan struct{})}
	engine := &controlTestEngine{repository: repository}
	members := &controlTestMembership{view: membership.View{Revision: 1, Members: []swim.Member{
		{NodeID: 1, Host: "127.0.0.1", BasePort: 9100, Incarnation: 1, Status: swim.Alive},
		{NodeID: 2, Host: "127.0.0.2", BasePort: 9200, Incarnation: 1, Status: swim.Alive},
		{NodeID: 3, Host: "127.0.0.3", BasePort: 9300, Incarnation: 1, Status: swim.Alive},
		{NodeID: 4, Host: "127.0.0.4", BasePort: 9400, Incarnation: 1, Status: swim.Alive},
	}}}
	fixture := &controlFixture{configuration: configuration, repository: repository, engine: engine, transfer: &controlTestTransfer{}, members: members, gate: admission.NewGate(), assignment: base.assignment, epoch: base.epoch, cluster: [16]byte{1}}
	owner, err := NewControlOwner(ControlOptions{Config: configuration, ClusterID: fixture.cluster, Repository: repository, Engine: engine, Transfer: fixture.transfer, Gate: fixture.gate, Membership: members, Clock: clock.NewManual(time.Unix(100, 0)), MaxSessions: 8, MaxSessionsPerPeer: 2, MaxQueuedWork: 4, MaxReplayEntries: 32, MaxReplayEntriesPerPeer: 8})
	if err != nil {
		t.Fatal(err)
	}
	fixture.owner = owner
	return fixture
}

func (fixture *controlFixture) session(t *testing.T, node uint16) *ControlSession {
	t.Helper()
	session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0." + string(rune('0'+node))), Port: 40000}, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func (fixture *controlFixture) authenticate(t *testing.T, session *ControlSession, node uint16, request byte) {
	t.Helper()
	if _, err := session.Handle(context.Background(), fixture.frame(t, node, request, fixture.handshake(node))); err != nil {
		t.Fatal(err)
	}
}

func (fixture *controlFixture) handshake(node uint16) protocol.WorkerHandshake {
	return protocol.WorkerHandshake{NodeID: node, WorkerEpoch: model.WorkerEpoch{byte(node)}, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
}

func (fixture *controlFixture) frame(t *testing.T, node uint16, request byte, message protocol.WorkerMessage) wire.Frame {
	t.Helper()
	payload, err := protocol.MarshalWorkerMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Frame{Header: wire.Header{Version: wire.Version1, Message: message.MessageType(), ClusterID: fixture.cluster, SenderID: node, RequestID: wire.RequestID{request}, TimestampMillis: 100_000, Codec: wire.CodecBinary}, Payload: payload}
}

func (fixture *controlFixture) frameUnchecked(node uint16, request byte, message protocol.WorkerMessage) wire.Frame {
	payload, _ := protocol.MarshalWorkerMessage(message)
	return wire.Frame{Header: wire.Header{Version: wire.Version1, Message: message.MessageType(), ClusterID: fixture.cluster, SenderID: node, RequestID: wire.RequestID{request}, TimestampMillis: 100_000, Codec: wire.CodecBinary}, Payload: payload}
}

func (fixture *controlFixture) inventoryQuery(t *testing.T) protocol.ResultInventoryQuery {
	t.Helper()
	var sink model.TaskID
	for _, replica := range fixture.assignment.Assignment.ResultReplicas {
		if replica.PrimaryNodeID == fixture.repository.localNode || replica.SecondaryNodeID == fixture.repository.localNode {
			sink = replica.SinkTask
			break
		}
	}
	checkpoint := protocol.SourceCheckpoint{Source: fixture.assignment.Assignment.Tasks[0].Task, Watermark: 1}
	query := protocol.ResultInventoryQuery{JobID: fixture.assignment.Assignment.JobID, SinkTask: sink, SpecificationHash: fixture.assignment.Topology.Digest(), AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, Checkpoints: []protocol.SourceCheckpoint{checkpoint}}
	query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	return query
}

func (fixture *controlFixture) repairGrant(t *testing.T, query protocol.ResultInventoryQuery) protocol.RepairGrant {
	t.Helper()
	var replica model.ResultReplicaSet
	for _, candidate := range fixture.assignment.Assignment.ResultReplicas {
		if candidate.SinkTask == query.SinkTask {
			replica = candidate
		}
	}
	instruction := protocol.RepairResultPartition{CoordinatorEpoch: fixture.epoch, JobID: query.JobID, AssignmentRevision: query.AssignmentRevision, AssignmentDigest: query.AssignmentDigest, SourceNodeID: replica.SecondaryNodeID, SourceWorkerEpoch: replica.SecondaryEpoch, DestinationNodeID: replica.PrimaryNodeID, DestinationWorkerEpoch: replica.PrimaryEpoch, SinkTask: query.SinkTask, SpecificationHash: query.SpecificationHash, Checkpoints: append([]protocol.SourceCheckpoint(nil), query.Checkpoints...), CheckpointDigest: query.CheckpointDigest, InventoryQueryDigest: query.QueryDigest, ExpectedContentDigest: model.EmptyResultInventoryDigest(query.QueryDigest)}
	instruction.RepairID = protocol.DeriveRepairID(instruction)
	instruction.InstructionDigest = protocol.RepairInstructionDigest(instruction)
	role := protocol.RepairDestination
	if instruction.SourceNodeID == fixture.repository.localNode {
		role = protocol.RepairSource
	}
	return protocol.RepairGrant{Instruction: instruction, Role: role}
}

type controlTestRepository struct {
	mu            sync.Mutex
	localNode     uint16
	localEpoch    model.WorkerEpoch
	work          store.RecoveredWork
	transactionID uint64
	fenceCalls    int
	installCalls  int
	fenceStarted  chan struct{}
	log           []string
}

func (repository *controlTestRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return repository.localNode, repository.localEpoch
}
func (repository *controlTestRepository) RecoverWork() (store.RecoveredWork, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.work.Clone(), nil
}
func (repository *controlTestRepository) DurableTransactionID() (uint64, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.transactionID, nil
}
func (repository *controlTestRepository) Fence(epoch model.CoordinatorEpoch) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.fenceCalls++
	select {
	case <-repository.fenceStarted:
	default:
		close(repository.fenceStarted)
	}
	repository.work.Fence = epoch
	repository.transactionID++
	repository.log = append(repository.log, "fence")
	return nil
}
func (repository *controlTestRepository) InstallAssignment(set model.AssignmentSet, topology model.TopologySpec, revision uint64, scheduling model.SchedulingState, epoch model.CoordinatorEpoch) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.installCalls++
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		return err
	}
	installed := store.InstalledAssignment{Assignment: set, SpecificationBytes: validated.CanonicalBytes(), Topology: validated, JobControlRevision: revision, SchedulingState: scheduling, CoordinatorEpoch: epoch}
	repository.work.Assignments = []store.InstalledAssignment{installed}
	repository.transactionID++
	repository.log = append(repository.log, "assignment")
	return nil
}
func (repository *controlTestRepository) PendingEvents(after uint64, max uint16) ([]model.WorkerEvent, uint64, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	result := make([]model.WorkerEvent, 0, max)
	for _, event := range repository.work.PendingEvents {
		if event.TransactionID > after && len(result) < int(max) {
			result = append(result, cloneWorkerEvent(event))
		}
	}
	last := after
	if len(result) > 0 {
		last = result[len(result)-1].TransactionID
	}
	return result, last, len(result) < len(repository.work.PendingEvents), nil
}
func (repository *controlTestRepository) UpsertRepair(repair store.ResultRepairRecord) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.work.Repairs = []store.ResultRepairRecord{repair}
	repository.transactionID++
	repository.log = append(repository.log, "repair")
	return nil
}

type controlTestEngine struct{ repository *controlTestRepository }

func (engine *controlTestEngine) ReconcileAssignment(_ context.Context, _ model.JobID) error {
	engine.repository.mu.Lock()
	defer engine.repository.mu.Unlock()
	engine.repository.log = append(engine.repository.log, "reconcile")
	return nil
}
func (engine *controlTestEngine) ApplyCheckpoint(_ context.Context, _ protocol.CheckpointNotice) error {
	engine.repository.mu.Lock()
	defer engine.repository.mu.Unlock()
	engine.repository.log = append(engine.repository.log, "checkpoint")
	return nil
}
func (engine *controlTestEngine) AcknowledgeEvents(context.Context, uint64) error { return nil }

type controlTestTransfer struct {
	calls int
	peer  TransferPeer
	ack   protocol.ResultRecordAck
}

func (transfer *controlTestTransfer) ReceiveResultRecord(_ context.Context, peer TransferPeer, _ protocol.ResultRecordChunk) (protocol.ResultRecordAck, error) {
	transfer.calls++
	transfer.peer = peer
	return transfer.ack, nil
}
func (transfer *controlTestTransfer) ReceiveResultArtifact(context.Context, TransferPeer, protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error) {
	return protocol.ResultArtifactAck{}, ErrResultArtifactUnavailable
}
func (transfer *controlTestTransfer) OpenResultFetch(context.Context, TransferPeer, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error) {
	return protocol.ResultFetchChunk{}, ErrResultFetchUnavailable
}

type controlTestMembership struct {
	view         membership.View
	authorizeErr error
}

func (members *controlTestMembership) View() membership.View {
	return membership.View{Revision: members.view.Revision, Members: append([]swim.Member(nil), members.view.Members...)}
}
func (members *controlTestMembership) AuthorizeTCP(uint16, net.Addr) error {
	return members.authorizeErr
}
