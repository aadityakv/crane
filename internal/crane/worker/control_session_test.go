package worker

import (
	"context"
	"errors"
	"net"
	"path/filepath"
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
	fixture.repository.work.Sources = []store.SourceCursor{fixture.sourceCursor(t, query.Checkpoints[0], 11)}
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
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
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

func TestControlReplicaCheckpointPersistsObservationBeforeAckAndInventoryUsesIt(t *testing.T) {
	fixture := newControlFixture(t)
	tasks := append([]model.AssignmentToken(nil), fixture.assignment.Assignment.Tasks...)
	var source model.TaskID
	for index := range tasks {
		stage, _ := controlStage(fixture.assignment.Topology.Spec(), tasks[index].Task.StageID)
		if stage.Role == model.StageSource {
			source = tasks[index].Task
			tasks[index].WorkerID = 4
			tasks[index].WorkerEpoch = model.WorkerEpoch{4}
			break
		}
	}
	set, err := model.NewAssignmentSet(fixture.assignment.Assignment.JobID, fixture.assignment.Assignment.Revision, tasks, fixture.assignment.Assignment.ResultReplicas, fixture.assignment.Topology)
	if err != nil {
		t.Fatal(err)
	}
	fixture.assignment.Assignment = set
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: set.JobID, Source: source, Watermark: 1, RaftIndex: 7, Epoch: fixture.epoch}, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: set.Revision, AssignmentDigest: set.Digest}
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, notice))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := response.(protocol.CheckpointAck); !ok || !reflect.DeepEqual(fixture.repository.log, []string{"observation"}) {
		t.Fatalf("checkpoint response/log = %#v/%v", response, fixture.repository.log)
	}
	query := fixture.inventoryQuery(t)
	query.Checkpoints = []protocol.SourceCheckpoint{{Source: source, Watermark: notice.Notice.Watermark}}
	query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query})); err != nil {
		t.Fatalf("inventory from observation: %v", err)
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

func TestControlQueuedCommandRevalidatesClosedSessionAndMembershipUnderMutationLock(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*controlFixture, *ControlSession)
	}{
		{name: "session closed", change: func(_ *controlFixture, session *ControlSession) { _ = session.Close() }},
		{name: "membership incarnation changed", change: func(fixture *controlFixture, _ *ControlSession) { fixture.members.incrementIncarnation(1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newControlFixture(t)
			fixture.repository.work.Fence = fixture.epoch
			session := fixture.session(t, 2)
			fixture.authenticate(t, session, 2, 1)
			newer := fixture.epoch
			newer.Term++
			newer.BeginIndex++
			newer.Nonce[0]++
			fixture.owner.mutations.Lock()
			done := make(chan error, 1)
			go func() {
				_, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.FenceRequest{CoordinatorEpoch: newer}))
				done <- err
			}()
			time.Sleep(10 * time.Millisecond)
			test.change(fixture, session)
			fixture.owner.mutations.Unlock()
			if err := <-done; !errors.Is(err, ErrControlClosed) && !errors.Is(err, ErrControlUnauthorized) {
				t.Fatalf("blocked command error = %v", err)
			}
			if fixture.repository.fenceCalls != 0 || fixture.repository.work.Fence != fixture.epoch {
				t.Fatalf("closed/stale session persisted fence: calls=%d fence=%#v", fixture.repository.fenceCalls, fixture.repository.work.Fence)
			}
		})
	}
}

func TestControlAssignmentRejectsAggregateLocalTaskSlotsBeforeMutation(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.configuration.Crane.WorkerSlots = 1
	owner, err := NewControlOwner(ControlOptions{Config: fixture.configuration, ClusterID: fixture.cluster, Repository: fixture.repository, Engine: fixture.engine, Transfer: fixture.transfer, Gate: fixture.gate, Membership: fixture.members, Clock: clock.NewManual(time.Unix(100, 0))})
	if err != nil {
		t.Fatal(err)
	}
	fixture.owner = owner
	fixture.repository.work.Fence = fixture.epoch
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	install := protocol.AssignmentSetInstall{Assignment: fixture.assignment.Assignment, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(), JobControlRevision: fixture.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, install)); !errors.Is(err, ErrControlCapacity) {
		t.Fatalf("over-slot assignment error = %v", err)
	}
	if fixture.repository.installCalls != 0 || len(fixture.repository.work.Assignments) != 0 {
		t.Fatalf("over-slot assignment mutated repository: calls=%d assignments=%d", fixture.repository.installCalls, len(fixture.repository.work.Assignments))
	}
}

func TestControlAssignmentCapacityIsAtomicAndExactReplacementRetriesWithRealStore(t *testing.T) {
	fixture := newControlFixture(t)
	path := filepath.Join(t.TempDir(), "worker")
	durable, err := store.Open(path, store.Identity{ClusterID: fixture.cluster, NodeID: fixture.repository.localNode}, store.Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.repository.localEpoch, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if err := durable.Fence(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if err := durable.InstallAssignment(fixture.assignment.Assignment, fixture.assignment.Topology.Spec(), fixture.assignment.JobControlRevision, model.Running, fixture.epoch); err != nil {
		t.Fatal(err)
	}
	localSlots := localTaskCount(fixture.assignment.Assignment, fixture.repository.localNode, fixture.repository.localEpoch)
	if localSlots == 0 {
		t.Fatal("fixture has no local task slots")
	}
	fixture.configuration.Crane.WorkerSlots = uint16(localSlots)
	repository := &serviceRepository{Store: durable, node: fixture.repository.localNode, fatal: make(chan error, 1)}
	engine := &controlNoopEngine{}
	owner, err := NewControlOwner(ControlOptions{Config: fixture.configuration, ClusterID: fixture.cluster, Repository: repository, Engine: engine, Transfer: fixture.transfer, Gate: fixture.gate, Membership: fixture.members, Clock: clock.NewManual(time.Unix(100, 0))})
	if err != nil {
		t.Fatal(err)
	}
	fixture.owner = owner
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	replacement := protocol.AssignmentSetInstall{Assignment: fixture.assignment.Assignment, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(), JobControlRevision: fixture.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, replacement)); err != nil {
		t.Fatalf("exact replacement retry: %v", err)
	}
	workers := []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: fixture.repository.localEpoch, SlotCapacity: 8}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{2}, SlotCapacity: 8}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{3}, SlotCapacity: 8}}
	var candidate model.AssignmentSet
	for value := uint16(2); value < 512; value++ {
		job := model.JobID{byte(value), byte(value >> 8)}
		candidate, err = model.BuildAssignmentSet(job, fixture.assignment.Topology.Digest(), 1, fixture.assignment.Topology, workers)
		if err != nil {
			t.Fatal(err)
		}
		if localTaskCount(candidate, fixture.repository.localNode, fixture.repository.localEpoch) != 0 {
			break
		}
	}
	over := protocol.AssignmentSetInstall{Assignment: candidate, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(), JobControlRevision: 1, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch}
	before, _ := durable.RecoverWork()
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, over)); !errors.Is(err, ErrControlCapacity) {
		t.Fatalf("aggregate capacity error = %v", err)
	}
	after, _ := durable.RecoverWork()
	if !reflect.DeepEqual(after, before) || len(after.Assignments) != 1 {
		t.Fatal("capacity rejection mutated durable Store")
	}
}

func TestControlInventoryAndLocalStatusRejectStaleOrUnrequestedDurableState(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	stale := fixture.assignment
	stale.CoordinatorEpoch.Term--
	fixture.repository.work.Assignments = []store.InstalledAssignment{stale}
	query := fixture.inventoryQuery(t)
	fixture.repository.work.Sources = []store.SourceCursor{fixture.sourceCursor(t, query.Checkpoints[0], 11)}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query})); !errors.Is(err, ErrControlStaleAssignment) {
		t.Fatalf("old-fence inventory error = %v", err)
	}

	fixture.repository.work.Assignments[0] = fixture.assignment
	grant := fixture.repairGrant(t, query)
	oldRepair := store.ResultRepairRecord{Instruction: protocolRepairDefinition(grant.Instruction), InstructionDigest: grant.Instruction.InstructionDigest, Role: grant.Role, State: store.RepairPending, ContentDigest: model.EmptyResultInventoryDigest(grant.Instruction.InstructionDigest)}
	oldRepair.Instruction.CoordinatorEpoch.Term--
	fixture.repository.work.Repairs = []store.ResultRepairRecord{oldRepair}
	status, err := fixture.owner.localStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Repair != nil {
		t.Fatalf("unrequested old repair leaked into local status: %#v", status.Repair)
	}
}

func TestControlResultRecordRequiresExactSharedGatePermit(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	chunk := validControlResultChunk(t, fixture)
	fixture.transfer.ack = protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: chunk.DestinationNodeID, WorkerEpoch: chunk.DestinationWorkerEpoch, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, chunk)); !errors.Is(err, admission.ErrClosed) {
		t.Fatalf("closed-gate 212 error = %v", err)
	}
	if fixture.transfer.calls != 0 {
		t.Fatalf("closed-gate 212 reached transfer owner %d times", fixture.transfer.calls)
	}
	if err := fixture.gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if response, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, chunk)); err != nil {
		t.Fatalf("open-gate 212: %v", err)
	} else if _, ok := response.(protocol.ResultRecordAck); !ok {
		t.Fatalf("open-gate 212 response = %#v", response)
	}
}

func TestControlOwnerDeepClonesConfiguredVoters(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.configuration.RaftVoters[0].NodeID = 99
	fixture.repository.work.Fence = fixture.epoch
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.FenceRequest{CoordinatorEpoch: fixture.epoch})); err != nil {
		t.Fatalf("caller mutation changed voter authorization: %v", err)
	}
}

func validControlResultChunk(t *testing.T, fixture *controlFixture) protocol.ResultRecordChunk {
	t.Helper()
	base, sink, replica := workerFixtureWithLocalPrimarySink(t)
	fixture.assignment = base.assignment
	fixture.epoch = base.epoch
	fixture.repository.work.Fence = base.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{base.assignment}
	role := model.PrimaryReplica
	if replica.PrimaryNodeID != fixture.repository.localNode || replica.PrimaryEpoch != fixture.repository.localEpoch {
		role = model.SecondaryReplica
	}
	record, provenance := base.result(t, sink, replica, 1, role)
	chunk, err := buildResultRecordChunk(TransferNormalReplication, record, provenance, fixture.repository.localNode, fixture.repository.localEpoch, [16]byte{}, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	return chunk
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

func (fixture *controlFixture) sourceCursor(t *testing.T, checkpoint protocol.SourceCheckpoint, raftIndex uint64) store.SourceCursor {
	t.Helper()
	var source model.AssignmentToken
	for _, token := range fixture.assignment.Assignment.Tasks {
		if token.Task == checkpoint.Source {
			source = token
			break
		}
	}
	if source == (model.AssignmentToken{}) {
		t.Fatal("missing source token")
	}
	authority := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest, SourceToken: source, CoordinatorEpoch: fixture.epoch}
	return store.SourceCursor{Source: checkpoint.Source, Watermark: checkpoint.Watermark, RaftIndex: raftIndex, CheckpointAuthority: authority}
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
	observeCalls  int
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
func (repository *controlTestRepository) ObserveCheckpoint(notice protocol.CheckpointNotice) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.observeCalls++
	repository.work.Checkpoints = []store.CommittedCheckpoint{{Notice: notice.Notice, JobControlRevision: notice.JobControlRevision, AssignmentRevision: notice.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest}}
	repository.transactionID++
	repository.log = append(repository.log, "observation")
	return nil
}

type controlTestEngine struct{ repository *controlTestRepository }

type controlNoopEngine struct{}

func (*controlNoopEngine) ReconcileAssignment(context.Context, model.JobID) error { return nil }
func (*controlNoopEngine) ApplyCheckpoint(context.Context, protocol.CheckpointNotice) error {
	return nil
}
func (*controlNoopEngine) AcknowledgeEvents(context.Context, uint64) error { return nil }

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
	calls         int
	artifactCalls int
	fetchCalls    int
	peer          TransferPeer
	ack           protocol.ResultRecordAck
}

func (transfer *controlTestTransfer) ReceiveResultRecord(_ context.Context, peer TransferPeer, _ protocol.ResultRecordChunk) (protocol.ResultRecordAck, error) {
	transfer.calls++
	transfer.peer = peer
	return transfer.ack, nil
}
func (transfer *controlTestTransfer) ReceiveResultArtifact(context.Context, TransferPeer, protocol.ResultArtifactChunk) (protocol.ResultArtifactAck, error) {
	transfer.artifactCalls++
	return protocol.ResultArtifactAck{}, ErrResultArtifactUnavailable
}
func (transfer *controlTestTransfer) OpenResultFetch(context.Context, TransferPeer, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error) {
	transfer.fetchCalls++
	return protocol.ResultFetchChunk{}, ErrResultFetchUnavailable
}

type controlTestMembership struct {
	mu           sync.RWMutex
	view         membership.View
	authorizeErr error
}

func (members *controlTestMembership) View() membership.View {
	members.mu.RLock()
	defer members.mu.RUnlock()
	return membership.View{Revision: members.view.Revision, Members: append([]swim.Member(nil), members.view.Members...)}
}
func (members *controlTestMembership) incrementIncarnation(index int) {
	members.mu.Lock()
	defer members.mu.Unlock()
	members.view.Members[index].Incarnation++
}
func (members *controlTestMembership) AuthorizeTCP(uint16, net.Addr) error {
	return members.authorizeErr
}
