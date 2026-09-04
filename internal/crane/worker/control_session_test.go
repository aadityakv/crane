package worker

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
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
		{name: "payload identity", mutate: func(_ *controlFixture, h *protocol.WorkerHandshake) { h.NodeID++ }},
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

func TestControlSessionBoundsArePerPeerAndGlobalAndReleaseOnClose(t *testing.T) {
	fixture := newControlFixture(t)
	var sessions []*ControlSession
	for index := 0; index < 2; index++ {
		session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000 + index}, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := session.authenticate(2, fixture.handshake(2)); err != nil {
			t.Fatal(err)
		}
		sessions = append(sessions, session)
	}
	overPeer, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40003}, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := overPeer.authenticate(2, fixture.handshake(2)); !errors.Is(err, ErrControlCapacity) {
		t.Fatalf("per-peer session bound error=%v", err)
	}
	if err := overPeer.Close(); err != nil {
		t.Fatal(err)
	}
	for node := 3; len(sessions) < 8; node++ {
		session, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0." + string(rune('0'+node))), Port: 40000}, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		sessions = append(sessions, session)
	}
	if _, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.9"), Port: 40000}, func() error { return nil }); !errors.Is(err, ErrControlCapacity) {
		t.Fatalf("global session bound error=%v", err)
	}
	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := fixture.owner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.9"), Port: 40000}, func() error { return nil })
	if err != nil {
		t.Fatalf("closed session did not release capacity: %v", err)
	}
	sessions = append(sessions[1:], replacement)
	for _, session := range sessions {
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
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

func TestControlStatusReportsProcessAdmissionEpoch(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	request := protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1}

	// The process gate starts closed (a fresh worker process): the durable
	// status reports no admission epoch even though the Running assignment is
	// durably installed.
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, request))
	if err != nil {
		t.Fatal(err)
	}
	if status := response.(protocol.WorkerStatus); status.AdmissionEpoch != (model.CoordinatorEpoch{}) {
		t.Fatalf("closed process gate reported admission epoch %#v", status.AdmissionEpoch)
	}

	install := protocol.AssignmentSetInstall{
		Assignment: fixture.assignment.Assignment, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(),
		JobControlRevision: fixture.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch,
	}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, install)); err != nil {
		t.Fatal(err)
	}
	response, err = session.Handle(context.Background(), fixture.frame(t, 2, 4, request))
	if err != nil {
		t.Fatal(err)
	}
	if status := response.(protocol.WorkerStatus); status.AdmissionEpoch != fixture.epoch {
		t.Fatalf("Running install admission epoch = %#v want %#v", status.AdmissionEpoch, fixture.epoch)
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

func TestControlStatusHasMoreIsRelativeToRequestedCursor(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	base := workerFixture(t)
	fixture.repository.work.PendingEvents = []model.WorkerEvent{base.failureEvent(1), base.failureEvent(2)}
	fixture.repository.work.NextTransactionID = 3
	fixture.repository.transactionID = 3
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, AfterTransactionID: 1, MaxEvents: 1}))
	if err != nil {
		t.Fatal(err)
	}
	status := response.(protocol.WorkerStatus)
	if len(status.Events) != 1 || status.Events[0].TransactionID != 2 || status.LastTransactionID != 2 || status.HasMore {
		t.Fatalf("cursor-relative page = %#v", status)
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

func TestControlClosePublishesBeforeEveryFinalMutationBoundaryAndJoins(t *testing.T) {
	for _, kind := range []string{"fence", "assignment", "checkpoint", "repair", "result-record"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newControlFixture(t)
			session := fixture.session(t, 2)
			fixture.authenticate(t, session, 2, 1)
			entered := make(chan struct{})
			release := make(chan struct{})
			fixture.owner.beforeMutation = func(candidate string) {
				if candidate == kind {
					close(entered)
					<-release
				}
			}
			var message protocol.WorkerMessage
			switch kind {
			case "fence":
				prior := fixture.epoch
				prior.Term--
				prior.BeginIndex--
				fixture.repository.work.Fence = prior
				message = protocol.FenceRequest{CoordinatorEpoch: fixture.epoch}
			case "assignment":
				fixture.repository.work.Fence = fixture.epoch
				message = protocol.AssignmentSetInstall{Assignment: fixture.assignment.Assignment, Specification: fixture.assignment.Topology.Spec(), SpecificationDigest: fixture.assignment.Topology.Digest(), JobControlRevision: fixture.assignment.JobControlRevision, SchedulingState: model.Running, CoordinatorEpoch: fixture.epoch}
			case "checkpoint":
				fixture.repository.work.Fence = fixture.epoch
				fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
				message = protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: fixture.assignment.Assignment.Tasks[0].Task, Watermark: 1, RaftIndex: 7, Epoch: fixture.epoch}, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
			case "repair":
				fixture.repository.work.Fence = fixture.epoch
				fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
				query := fixture.inventoryQuery(t)
				fixture.repository.work.Sources = []store.SourceCursor{fixture.sourceCursor(t, query.Checkpoints[0], 11)}
				grant := fixture.repairGrant(t, query)
				message = protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Repair: &grant}
			case "result-record":
				chunk := validControlResultChunk(t, fixture)
				if err := fixture.gate.Open(fixture.epoch); err != nil {
					t.Fatal(err)
				}
				message = chunk
			}
			type outcome struct {
				response protocol.WorkerMessage
				err      error
			}
			handled := make(chan outcome, 1)
			go func() {
				response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, message))
				handled <- outcome{response: response, err: err}
			}()
			<-entered
			closed := make(chan error, 1)
			go func() { closed <- session.Close() }()
			<-session.done.Done()
			select {
			case err := <-closed:
				t.Fatalf("Close returned before handler joined: %v", err)
			default:
			}
			close(release)
			result := <-handled
			if result.response != nil || result.err == nil {
				t.Fatalf("closing handler response/error = %#v/%v", result.response, result.err)
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if fixture.repository.fenceCalls != 0 || fixture.repository.installCalls != 0 || fixture.repository.observeCalls != 0 || fixture.transfer.calls != 0 || len(fixture.repository.log) != 0 {
				t.Fatalf("closing handler mutated state: repo=%v fence=%d install=%d observe=%d transfer=%d", fixture.repository.log, fixture.repository.fenceCalls, fixture.repository.installCalls, fixture.repository.observeCalls, fixture.transfer.calls)
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

func TestControlAssignmentAdmitsOnlyClosedHistoricalHolderAndRejectsRevisionGap(t *testing.T) {
	fixture := newControlFixture(t)
	base := workerFixture(t)
	var sink model.AssignmentToken
	var replica model.ResultReplicaSet
	for _, candidate := range base.assignment.Assignment.ResultReplicas {
		if candidate.PrimaryNodeID != base.localNode && candidate.SecondaryNodeID != base.localNode {
			continue
		}
		replica = candidate
		for _, token := range base.assignment.Assignment.Tasks {
			if token.Task == candidate.SinkTask {
				sink = token
				break
			}
		}
		break
	}
	if sink == (model.AssignmentToken{}) {
		t.Fatal("fixture has no locally retained result replica")
	}
	fixture.assignment = base.assignment
	fixture.epoch = base.epoch
	fixture.repository.work.Fence = base.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{base.assignment}
	role := model.PrimaryReplica
	if replica.PrimaryNodeID != base.localNode || replica.PrimaryEpoch != base.localEpoch {
		role = model.SecondaryReplica
	}
	record, provenance := base.result(t, sink, replica, 1, role)
	fixture.repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}

	next := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+1)
	install := protocol.AssignmentSetInstall{Assignment: next, Specification: base.assignment.Topology.Spec(), SpecificationDigest: base.assignment.Topology.Digest(), JobControlRevision: base.assignment.JobControlRevision + 1, SchedulingState: model.Closed, CoordinatorEpoch: base.epoch}
	if !fixture.owner.historicalResultsAuthorizeAssignment(fixture.repository.work, next, install.SpecificationDigest) {
		t.Fatalf("valid retained result not recognized: record=%+v provenance=%+v local=%d", record, provenance, base.localNode)
	}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, install)); err != nil {
		t.Fatalf("closed historical-holder assignment rejected: %v", err)
	}
	if fixture.repository.installCalls != 1 || !reflect.DeepEqual(fixture.repository.work.Assignments[0].Assignment, next) {
		t.Fatalf("historical assignment not durably installed: calls=%d assignment=%+v", fixture.repository.installCalls, fixture.repository.work.Assignments)
	}
	if _, err := fixture.gate.Enter(); !errors.Is(err, admission.ErrClosed) {
		t.Fatalf("historical audit assignment opened gate: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*controlFixture, *protocol.AssignmentSetInstall)
	}{
		{name: "random holder", mutate: func(f *controlFixture, _ *protocol.AssignmentSetInstall) { f.repository.work.Results = nil }},
		{name: "wrong provenance node", mutate: func(f *controlFixture, _ *protocol.AssignmentSetInstall) {
			if f.repository.work.Results[0].Provenance.DestinationRole == model.PrimaryReplica {
				f.repository.work.Results[0].Provenance.ReplicaSet.PrimaryNodeID = 99
			} else {
				f.repository.work.Results[0].Provenance.ReplicaSet.SecondaryNodeID = 99
			}
		}},
		{name: "running non-target", mutate: func(_ *controlFixture, candidate *protocol.AssignmentSetInstall) {
			candidate.SchedulingState = model.Running
		}},
		{name: "revision gap", mutate: func(_ *controlFixture, candidate *protocol.AssignmentSetInstall) {
			candidate.Assignment = nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+2)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidateFixture := newControlFixture(t)
			candidateFixture.assignment = base.assignment
			candidateFixture.epoch = base.epoch
			candidateFixture.repository.work.Fence = base.epoch
			candidateFixture.repository.work.Assignments = []store.InstalledAssignment{base.assignment}
			candidateFixture.repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
			candidate := install
			test.mutate(candidateFixture, &candidate)
			candidateSession := candidateFixture.session(t, 2)
			candidateFixture.authenticate(t, candidateSession, 2, 1)
			if _, err := candidateSession.Handle(context.Background(), candidateFixture.frame(t, 2, 2, candidate)); !errors.Is(err, ErrControlStaleAssignment) {
				t.Fatalf("rejection error = %v", err)
			}
			if candidateFixture.repository.installCalls != 0 {
				t.Fatalf("rejected candidate mutated Store %d times", candidateFixture.repository.installCalls)
			}
		})
	}
}

func nonTargetAssignmentForControlTest(t *testing.T, fixture workerTestFixture, revision uint64) model.AssignmentSet {
	t.Helper()
	tasks := append([]model.AssignmentToken(nil), fixture.assignment.Assignment.Tasks...)
	for index := range tasks {
		tasks[index].WorkerID = 2 + uint16(index%2)
		tasks[index].WorkerEpoch = model.WorkerEpoch{byte(tasks[index].WorkerID)}
		tasks[index].Attempt++
		tasks[index].AssignmentRevision = revision
	}
	replicas := append([]model.ResultReplicaSet(nil), fixture.assignment.Assignment.ResultReplicas...)
	for index := range replicas {
		replicas[index].PrimaryNodeID, replicas[index].PrimaryEpoch = 2, model.WorkerEpoch{2}
		replicas[index].SecondaryNodeID, replicas[index].SecondaryEpoch = 3, model.WorkerEpoch{3}
	}
	set, err := model.NewAssignmentSet(fixture.assignment.Assignment.JobID, revision, tasks, replicas, fixture.assignment.Topology)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestControlHistoricalSourceObservesInventoryAndInstallsExactRepairGrant(t *testing.T) {
	fixture := newControlFixture(t)
	base := workerFixture(t)
	var sink model.AssignmentToken
	var oldReplica model.ResultReplicaSet
	for _, candidate := range base.assignment.Assignment.ResultReplicas {
		if candidate.PrimaryNodeID != base.localNode && candidate.SecondaryNodeID != base.localNode {
			continue
		}
		oldReplica = candidate
		for _, token := range base.assignment.Assignment.Tasks {
			if token.Task == candidate.SinkTask {
				sink = token
				break
			}
		}
		break
	}
	if sink == (model.AssignmentToken{}) {
		t.Fatal("fixture has no historical result")
	}
	role := model.PrimaryReplica
	if oldReplica.PrimaryNodeID != base.localNode {
		role = model.SecondaryReplica
	}
	record, provenance := base.result(t, sink, oldReplica, 1, role)
	current := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+1)
	fixture.assignment = base.assignment
	fixture.epoch = base.epoch
	fixture.repository.work.Fence = base.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{{Assignment: current, SpecificationBytes: base.assignment.Topology.CanonicalBytes(), Topology: base.assignment.Topology, JobControlRevision: 2, SchedulingState: model.Closed, CoordinatorEpoch: base.epoch}}
	fixture.repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}

	var source model.AssignmentToken
	for _, token := range current.Tasks {
		stage, _ := controlStage(base.assignment.Topology.Spec(), token.Task.StageID)
		if stage.Role == model.StageSource {
			source = token
			break
		}
	}
	if source == (model.AssignmentToken{}) {
		t.Fatal("fixture has no current source")
	}
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: current.JobID, Source: source.Task, Watermark: 1, RaftIndex: 11, Epoch: base.epoch}, JobControlRevision: 2, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, notice)); err != nil {
		t.Fatalf("historical holder checkpoint rejected: %v", err)
	}

	query := protocol.ResultInventoryQuery{JobID: current.JobID, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, Checkpoints: []protocol.SourceCheckpoint{{Source: source.Task, Watermark: 1}}}
	query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Inventory: &query}))
	if err != nil {
		t.Fatalf("historical inventory rejected: %v", err)
	}
	summary := response.(protocol.WorkerStatus).Inventory
	if summary == nil || summary.RecordCount != 1 {
		t.Fatalf("historical inventory = %#v", summary)
	}
	currentReplica, ok := controlReplica(current, record.SinkTask)
	if !ok {
		t.Fatal("missing current destination replica")
	}
	instruction := protocol.RepairResultPartition{CoordinatorEpoch: base.epoch, JobID: current.JobID, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, SourceNodeID: base.localNode, SourceWorkerEpoch: base.localEpoch, DestinationNodeID: currentReplica.PrimaryNodeID, DestinationWorkerEpoch: currentReplica.PrimaryEpoch, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), Checkpoints: append([]protocol.SourceCheckpoint(nil), query.Checkpoints...), CheckpointDigest: query.CheckpointDigest, InventoryQueryDigest: query.QueryDigest, ExpectedRecordCount: summary.RecordCount, ExpectedTotalBytes: summary.TotalBytes, ExpectedContentDigest: summary.ContentDigest}
	instruction.RepairID = protocol.DeriveRepairID(instruction)
	instruction.InstructionDigest = protocol.RepairInstructionDigest(instruction)
	grant := protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairSource}
	response, err = session.Handle(context.Background(), fixture.frame(t, 2, 4, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &grant}))
	if err != nil {
		t.Fatalf("historical source grant rejected: %v", err)
	}
	status := response.(protocol.WorkerStatus).Repair
	if status == nil || status.Role != protocol.RepairSource || fixture.repository.log[len(fixture.repository.log)-1] != "repair" {
		t.Fatalf("historical repair status=%#v log=%v", status, fixture.repository.log)
	}
	for index, test := range []struct {
		name   string
		mutate func(*protocol.RepairGrant)
	}{
		{name: "source epoch", mutate: func(candidate *protocol.RepairGrant) { candidate.Instruction.SourceWorkerEpoch[0]++ }},
		{name: "destination role", mutate: func(candidate *protocol.RepairGrant) { candidate.Role = protocol.RepairDestination }},
		{name: "destination epoch", mutate: func(candidate *protocol.RepairGrant) { candidate.Instruction.DestinationWorkerEpoch[0]++ }},
		{name: "checkpoint vector", mutate: func(candidate *protocol.RepairGrant) { candidate.Instruction.Checkpoints[0].Watermark++ }},
		{name: "content digest", mutate: func(candidate *protocol.RepairGrant) { candidate.Instruction.ExpectedContentDigest[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := grant
			candidate.Instruction.Checkpoints = append([]protocol.SourceCheckpoint(nil), grant.Instruction.Checkpoints...)
			test.mutate(&candidate)
			candidate.Instruction.CheckpointDigest = protocol.CheckpointVectorDigest(candidate.Instruction.Checkpoints)
			candidate.Instruction.InventoryQueryDigest = protocol.InventoryQueryDigest(protocol.ResultInventoryQuery{JobID: candidate.Instruction.JobID, SinkTask: candidate.Instruction.SinkTask, SpecificationHash: candidate.Instruction.SpecificationHash, AssignmentRevision: candidate.Instruction.AssignmentRevision, AssignmentDigest: candidate.Instruction.AssignmentDigest, Checkpoints: candidate.Instruction.Checkpoints, CheckpointDigest: candidate.Instruction.CheckpointDigest})
			candidate.Instruction.RepairID = protocol.DeriveRepairID(candidate.Instruction)
			candidate.Instruction.InstructionDigest = protocol.RepairInstructionDigest(candidate.Instruction)
			if _, err := session.Handle(context.Background(), fixture.frame(t, 2, byte(index+5), protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &candidate})); err == nil {
				t.Fatal("invalid historical grant accepted")
			}
			if countControlLog(fixture.repository.log, "repair") != 1 {
				t.Fatalf("invalid grant mutated repair state: %v", fixture.repository.log)
			}
		})
	}
}

func countControlLog(log []string, target string) int {
	count := 0
	for _, entry := range log {
		if entry == target {
			count++
		}
	}
	return count
}

func TestControlHistoricalRepairRealStoresGrantOrderRestartAndTransferProgress(t *testing.T) {
	seed := newControlFixture(t)
	base := workerFixture(t)
	var sink model.AssignmentToken
	var oldReplica model.ResultReplicaSet
	for _, candidate := range base.assignment.Assignment.ResultReplicas {
		if candidate.PrimaryNodeID != base.localNode && candidate.SecondaryNodeID != base.localNode {
			continue
		}
		oldReplica = candidate
		for _, token := range base.assignment.Assignment.Tasks {
			if token.Task == candidate.SinkTask {
				sink = token
			}
		}
		break
	}
	if sink == (model.AssignmentToken{}) {
		t.Fatal("fixture has no historical result")
	}
	oldRole := model.PrimaryReplica
	if oldReplica.PrimaryNodeID != base.localNode {
		oldRole = model.SecondaryReplica
	}
	record, provenance := base.result(t, sink, oldReplica, 1, oldRole)
	current := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+1)
	currentInstall := protocol.AssignmentSetInstall{Assignment: current, Specification: base.assignment.Topology.Spec(), SpecificationDigest: base.assignment.Topology.Digest(), JobControlRevision: 2, SchedulingState: model.Closed, CoordinatorEpoch: base.epoch}
	currentReplica, ok := controlReplica(current, record.SinkTask)
	if !ok {
		t.Fatal("missing current result replica")
	}
	destinationNode, destinationEpoch := currentReplica.SecondaryNodeID, currentReplica.SecondaryEpoch
	if destinationNode == base.epoch.Coordinator {
		destinationNode, destinationEpoch = currentReplica.PrimaryNodeID, currentReplica.PrimaryEpoch
	}

	sourcePath, destinationPath := filepath.Join(t.TempDir(), "source"), filepath.Join(t.TempDir(), "destination")
	sourceIdentity := store.Identity{ClusterID: seed.cluster, NodeID: base.localNode}
	sourceOptions := store.Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return base.localEpoch, nil }}
	sourceStore, err := store.Open(sourcePath, sourceIdentity, sourceOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.Fence(base.epoch); err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.InstallAssignment(base.assignment.Assignment, base.assignment.Topology.Spec(), base.assignment.JobControlRevision, model.Running, base.epoch); err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	sourceRepository := &serviceRepository{Store: sourceStore, node: base.localNode, fatal: make(chan error, 1)}
	sourceOwner, err := NewControlOwner(ControlOptions{Config: seed.configuration, ClusterID: seed.cluster, Repository: sourceRepository, Engine: &controlNoopEngine{}, Transfer: seed.transfer, Gate: admission.NewGate(), Membership: seed.members, Clock: clock.NewManual(time.Unix(100, 0))})
	if err != nil {
		t.Fatal(err)
	}
	seed.owner = sourceOwner
	sourceSession := seed.session(t, base.epoch.Coordinator)
	seed.authenticate(t, sourceSession, base.epoch.Coordinator, 1)
	if _, err := sourceSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 2, currentInstall)); err != nil {
		t.Fatalf("install source audit assignment: %v", err)
	}

	var sourceTask model.AssignmentToken
	for _, token := range current.Tasks {
		stage, _ := controlStage(base.assignment.Topology.Spec(), token.Task.StageID)
		if stage.Role == model.StageSource {
			sourceTask = token
			break
		}
	}
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: current.JobID, Source: sourceTask.Task, Watermark: 1, RaftIndex: 17, Epoch: base.epoch}, JobControlRevision: 2, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest}
	if _, err := sourceSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 3, notice)); err != nil {
		t.Fatalf("source checkpoint observation: %v", err)
	}
	query := protocol.ResultInventoryQuery{JobID: current.JobID, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, Checkpoints: []protocol.SourceCheckpoint{{Source: sourceTask.Task, Watermark: 1}}}
	query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	sourceResponse, err := sourceSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 4, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Inventory: &query}))
	if err != nil {
		t.Fatal(err)
	}
	summary := sourceResponse.(protocol.WorkerStatus).Inventory
	if summary == nil || summary.RecordCount != 1 {
		t.Fatalf("source inventory=%#v", summary)
	}

	destinationIdentity := store.Identity{ClusterID: seed.cluster, NodeID: destinationNode}
	destinationOptions := store.Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return destinationEpoch, nil }}
	destinationStore, err := store.Open(destinationPath, destinationIdentity, destinationOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := destinationStore.Fence(base.epoch); err != nil {
		t.Fatal(err)
	}
	if err := destinationStore.InstallAssignment(current, base.assignment.Topology.Spec(), 2, model.Closed, base.epoch); err != nil {
		t.Fatal(err)
	}
	if err := destinationStore.ObserveCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	destinationRepository := &serviceRepository{Store: destinationStore, node: destinationNode, fatal: make(chan error, 1)}
	destinationConfig := seed.configuration
	destinationConfig.NodeID = destinationNode
	destinationOwner, err := NewControlOwner(ControlOptions{Config: destinationConfig, ClusterID: seed.cluster, Repository: destinationRepository, Engine: &controlNoopEngine{}, Transfer: seed.transfer, Gate: admission.NewGate(), Membership: seed.members, Clock: clock.NewManual(time.Unix(100, 0))})
	if err != nil {
		t.Fatal(err)
	}
	destinationSession, err := destinationOwner.NewSession(&net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destinationSession.Close() })
	if _, err := destinationSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 5, seed.handshake(base.epoch.Coordinator))); err != nil {
		t.Fatal(err)
	}
	instruction := protocol.RepairResultPartition{CoordinatorEpoch: base.epoch, JobID: current.JobID, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, SourceNodeID: base.localNode, SourceWorkerEpoch: base.localEpoch, DestinationNodeID: destinationNode, DestinationWorkerEpoch: destinationEpoch, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), Checkpoints: append([]protocol.SourceCheckpoint(nil), query.Checkpoints...), CheckpointDigest: query.CheckpointDigest, InventoryQueryDigest: query.QueryDigest, ExpectedRecordCount: summary.RecordCount, ExpectedTotalBytes: summary.TotalBytes, ExpectedContentDigest: summary.ContentDigest}
	instruction.RepairID = protocol.DeriveRepairID(instruction)
	instruction.InstructionDigest = protocol.RepairInstructionDigest(instruction)
	destinationGrant := protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairDestination}
	destinationResponse, err := destinationSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 6, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &destinationGrant}))
	if err != nil {
		t.Fatalf("destination-first grant: %v", err)
	}
	destinationStatus := destinationResponse.(protocol.WorkerStatus).Repair
	sourceGrant := destinationGrant
	sourceGrant.Role = protocol.RepairSource
	if _, err := sourceSession.Handle(context.Background(), seed.frame(t, base.epoch.Coordinator, 7, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &sourceGrant})); err != nil {
		t.Fatalf("source-second grant: %v", err)
	}
	if err := sourceSession.Close(); err != nil {
		t.Fatal(err)
	}
	if err := destinationSession.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.Close(); err != nil {
		t.Fatal(err)
	}
	if err := destinationStore.Close(); err != nil {
		t.Fatal(err)
	}
	sourceStore, err = store.Open(sourcePath, sourceIdentity, sourceOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceStore.Close()
	destinationStore, err = store.Open(destinationPath, destinationIdentity, destinationOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer destinationStore.Close()
	sourceRepository = &serviceRepository{Store: sourceStore, node: base.localNode, fatal: make(chan error, 1)}
	destinationRepository = &serviceRepository{Store: destinationStore, node: destinationNode, fatal: make(chan error, 1)}
	sourceTransfer, err := NewTransferOwner(TransferOptions{Repository: sourceRepository})
	if err != nil {
		t.Fatal(err)
	}
	destinationTransfer, err := NewTransferOwner(TransferOptions{Repository: destinationRepository})
	if err != nil {
		t.Fatal(err)
	}
	chunk, complete, err := sourceTransfer.NextRepairRecord(context.Background(), TransferPeer{NodeID: destinationNode, WorkerEpoch: destinationEpoch, Role: TransferHistoricalRepair}, instruction.RepairID, *destinationStatus)
	if err != nil || complete || chunk.Record.TupleID != record.TupleID {
		t.Fatalf("restarted source 212: record=%+v complete=%t err=%v", chunk.Record.TupleID, complete, err)
	}
	ack, err := destinationTransfer.ReceiveResultRecord(context.Background(), TransferPeer{NodeID: base.localNode, WorkerEpoch: base.localEpoch, Role: TransferHistoricalRepair}, chunk)
	if err != nil {
		t.Fatalf("destination 213: %v", err)
	}
	if err := sourceTransfer.AcknowledgeRepairRecord(context.Background(), TransferPeer{NodeID: destinationNode, WorkerEpoch: destinationEpoch, Role: TransferHistoricalRepair}, chunk, ack); err != nil {
		t.Fatalf("source 213 progression: %v", err)
	}
	sourceWork, _ := sourceStore.RecoverWork()
	destinationWork, _ := destinationStore.RecoverWork()
	if len(sourceWork.Repairs) != 1 || sourceWork.Repairs[0].State != store.RepairComplete || len(destinationWork.Results) != 1 {
		t.Fatalf("repair progression source=%+v destination results=%d", sourceWork.Repairs, len(destinationWork.Results))
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

// TestControlResultRecordDoesNotRequireGatePermit pins the Task 24 defect #9
// ruling (defect #6 applied to normal replication): a replicated result
// record is authorised by the transfer's own validation, never by the
// process admission gate, so it is received and acknowledged while the gate
// is closed exactly as while it is open.
func TestControlResultRecordDoesNotRequireGatePermit(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	chunk := validControlResultChunk(t, fixture)
	fixture.transfer.ack = protocol.ResultRecordAck{TransferID: chunk.Transfer.TransferID, NodeID: chunk.DestinationNodeID, WorkerEpoch: chunk.DestinationWorkerEpoch, NextOffset: chunk.Transfer.TotalLength, TotalLength: chunk.Transfer.TotalLength, Checksum: chunk.Transfer.Checksum, Complete: true, CoordinatorEpoch: chunk.Provenance.CoordinatorEpoch}
	if response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, chunk)); err != nil {
		t.Fatalf("closed-gate 212: %v", err)
	} else if _, ok := response.(protocol.ResultRecordAck); !ok {
		t.Fatalf("closed-gate 212 response = %#v", response)
	}
	if fixture.transfer.calls != 1 {
		t.Fatalf("closed-gate 212 reached transfer owner %d times, want 1", fixture.transfer.calls)
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

func TestControlResultRecordRejectsAuthenticatedPayloadWorkerEpochMismatchBeforeStore(t *testing.T) {
	fixture := newControlFixture(t)
	chunk := validControlResultChunk(t, fixture)
	durable, err := store.Open(filepath.Join(t.TempDir(), "worker"), store.Identity{ClusterID: fixture.cluster, NodeID: fixture.repository.localNode}, store.Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return fixture.repository.localEpoch, nil }})
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
	repository := &serviceRepository{Store: durable, node: fixture.repository.localNode, fatal: make(chan error, 1)}
	transfer, err := NewTransferOwner(TransferOptions{Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewControlOwner(ControlOptions{Config: fixture.configuration, ClusterID: fixture.cluster, Repository: repository, Engine: &controlNoopEngine{}, Transfer: transfer, Gate: fixture.gate, Membership: fixture.members, Clock: clock.NewManual(time.Unix(100, 0))})
	if err != nil {
		t.Fatal(err)
	}
	fixture.owner = owner
	session := fixture.session(t, 2)
	wrongEpoch := fixture.handshake(2)
	wrongEpoch.WorkerEpoch[0]++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 1, wrongEpoch)); err != nil {
		t.Fatalf("nonzero session epoch handshake: %v", err)
	}
	if err := fixture.gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, chunk)); !errors.Is(err, ErrTransferUnauthorized) {
		t.Fatalf("payload/session worker epoch mismatch error=%v", err)
	}
	work, err := durable.RecoverWork()
	if err != nil || len(work.Results) != 0 {
		t.Fatalf("epoch mismatch mutated result Store: results=%d err=%v", len(work.Results), err)
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
func (repository *controlTestRepository) RecoverWorkBounded() (store.RecoveredWork, error) {
	return repository.RecoverWork()
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
	more := false
	for _, event := range repository.work.PendingEvents {
		if event.TransactionID > last {
			more = true
			break
		}
	}
	return result, last, more, nil
}
func (repository *controlTestRepository) UpsertRepair(repair store.ResultRepairRecord) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	replaced := false
	for index := range repository.work.Repairs {
		if repository.work.Repairs[index].Instruction.RepairID == repair.Instruction.RepairID {
			repository.work.Repairs[index] = repair
			replaced = true
		}
	}
	if !replaced {
		repository.work.Repairs = append(repository.work.Repairs, repair)
	}
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
	pullCalls     int
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
func (transfer *controlTestTransfer) ServeRepairPull(context.Context, TransferPeer, protocol.ResultRepairStatus) (protocol.ResultRecordChunk, bool, error) {
	transfer.pullCalls++
	return protocol.ResultRecordChunk{}, false, ErrTransferUnauthorized
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

func TestControlCheckpointZeroWatermarkConfirmsWithoutEngineMutation(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	var source model.TaskID
	for _, token := range fixture.assignment.Assignment.Tasks {
		stage, ok := controlStage(fixture.assignment.Topology.Spec(), token.Task.StageID)
		if ok && stage.Role == model.StageSource && token.WorkerID == fixture.repository.localNode {
			source = token.Task
			break
		}
	}
	if source == (model.TaskID{}) {
		t.Fatal("fixture requires a local source token")
	}
	// The zero watermark is trivially committed: an untouched source has no
	// durable checkpoint to correlate, so the confirmation must acknowledge
	// without any engine or store mutation.
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: source, Watermark: 0, RaftIndex: 7, Epoch: fixture.epoch}, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, notice))
	if err != nil {
		t.Fatalf("zero-watermark notice: %v", err)
	}
	ack, ok := response.(protocol.CheckpointAck)
	if !ok || ack.Watermark != 0 || ack.Source != source {
		t.Fatalf("zero-watermark response = %#v", response)
	}
	if len(fixture.repository.log) != 0 || fixture.repository.observeCalls != 0 {
		t.Fatalf("zero-watermark confirmation mutated durable state: %v", fixture.repository.log)
	}
}

func TestControlCheckpointHigherIndexDuplicateConfirmsWithoutRemutation(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	var sourceToken model.AssignmentToken
	for _, token := range fixture.assignment.Assignment.Tasks {
		stage, ok := controlStage(fixture.assignment.Topology.Spec(), token.Task.StageID)
		if ok && stage.Role == model.StageSource && token.WorkerID == fixture.repository.localNode {
			sourceToken = token
			break
		}
	}
	if sourceToken == (model.AssignmentToken{}) {
		t.Fatal("fixture requires a local source token")
	}
	cursor := fixture.sourceCursor(t, protocol.SourceCheckpoint{Source: sourceToken.Task, Watermark: 1}, 9)
	cursor.CheckpointRevision = 1
	fixture.repository.work.Sources = []store.SourceCursor{cursor}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)

	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: fixture.assignment.Assignment.JobID, Source: sourceToken.Task, Watermark: 1, RaftIndex: 12, Epoch: fixture.epoch}, JobControlRevision: fixture.assignment.JobControlRevision, AssignmentRevision: fixture.assignment.Assignment.Revision, AssignmentDigest: fixture.assignment.Assignment.Digest}
	// A committed notice redelivered by a later leadership pass carries a
	// higher Raft index: the worker confirms the already-durable checkpoint
	// without re-mutating engine or store state.
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, notice))
	if err != nil {
		t.Fatalf("higher-index duplicate: %v", err)
	}
	if _, ok := response.(protocol.CheckpointAck); !ok || len(fixture.repository.log) != 0 {
		t.Fatalf("higher-index confirmation response/log = %#v/%v", response, fixture.repository.log)
	}
	// The byte-exact duplicate still flows through the serialized engine so
	// the durable store answers it.
	exact := notice
	exact.Notice.RaftIndex = 9
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, 3, exact)); err != nil {
		t.Fatalf("byte-exact duplicate: %v", err)
	}
	if !reflect.DeepEqual(fixture.repository.log, []string{"checkpoint"}) {
		t.Fatalf("byte-exact duplicate bypassed the engine: %v", fixture.repository.log)
	}
}

// TestControlInventoryAcceptsHistoricalCheckpointProof pins the defect #2
// ruling seal-path half: a durable retained checkpoint proof is valid
// committed-watermark evidence even when its coordinator epoch,
// JobControlRevision, and assignment binding are all historical — the
// watermark was committed, an immutable historical fact — so the inventory
// answer must not demand current-authority equality from the proof.
func TestControlInventoryAcceptsHistoricalCheckpointProof(t *testing.T) {
	fixture := newControlFixture(t)
	fixture.repository.work.Fence = fixture.epoch
	fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
	var sourceToken model.AssignmentToken
	for _, token := range fixture.assignment.Assignment.Tasks {
		stage, ok := controlStage(fixture.assignment.Topology.Spec(), token.Task.StageID)
		if ok && stage.Role == model.StageSource && token.WorkerID == fixture.repository.localNode && token.WorkerEpoch == fixture.repository.localEpoch {
			sourceToken = token
			break
		}
	}
	if sourceToken == (model.AssignmentToken{}) {
		t.Fatal("fixture requires a local source token")
	}
	olderEpoch := fixture.epoch
	olderEpoch.Term--
	olderEpoch.BeginIndex--
	olderEpoch.Nonce[0]++
	historical := store.CheckpointAuthority{JobControlRevision: fixture.assignment.JobControlRevision - 1, AssignmentRevision: fixture.assignment.Assignment.Revision - 1, AssignmentDigest: [32]byte{9}, SourceToken: sourceToken, CoordinatorEpoch: olderEpoch}
	cursor := fixture.sourceCursor(t, protocol.SourceCheckpoint{Source: sourceToken.Task, Watermark: 1}, 9)
	cursor.CheckpointRevision = 1
	cursor.CheckpointAuthority = historical
	fixture.repository.work.Sources = []store.SourceCursor{cursor}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	query := fixture.inventoryQuery(t)
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query}))
	if err != nil {
		t.Fatalf("inventory with a historical durable proof: %v", err)
	}
	status := response.(protocol.WorkerStatus)
	if status.Inventory == nil || status.Inventory.QueryDigest != query.QueryDigest {
		t.Fatalf("historical-proof inventory summary = %#v", status.Inventory)
	}
}

func TestControlInventoryTreatsZeroCheckpointVectorAsTriviallyCommitted(t *testing.T) {
	t.Run("local source without cursor", func(t *testing.T) {
		fixture := newControlFixture(t)
		fixture.repository.work.Fence = fixture.epoch
		fixture.repository.work.Assignments = []store.InstalledAssignment{fixture.assignment}
		session := fixture.session(t, 2)
		fixture.authenticate(t, session, 2, 1)
		query := fixture.inventoryQuery(t)
		query.Checkpoints = zeroSourceVector(fixture.assignment)
		query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
		query.QueryDigest = protocol.InventoryQueryDigest(query)
		response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query}))
		if err != nil {
			t.Fatalf("zero-vector inventory: %v", err)
		}
		status := response.(protocol.WorkerStatus)
		if status.Inventory == nil || status.Inventory.RecordCount != 0 || status.Inventory.QueryDigest != query.QueryDigest {
			t.Fatalf("zero-vector summary = %#v", status.Inventory)
		}
	})
	t.Run("replica-only source without observation", func(t *testing.T) {
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
		if source == (model.TaskID{}) {
			t.Fatal("fixture requires a source token")
		}
		query := fixture.inventoryQuery(t)
		query.AssignmentDigest = set.Digest
		query.Checkpoints = zeroSourceVector(fixture.assignment)
		query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
		query.QueryDigest = protocol.InventoryQueryDigest(query)
		response, err := session.Handle(context.Background(), fixture.frame(t, 2, 2, protocol.WorkerStatusRequest{CoordinatorEpoch: fixture.epoch, MaxEvents: 1, Inventory: &query}))
		if err != nil {
			t.Fatalf("replica zero-vector inventory: %v", err)
		}
		status := response.(protocol.WorkerStatus)
		if status.Inventory == nil || status.Inventory.RecordCount != 0 {
			t.Fatalf("replica zero-vector summary = %#v", status.Inventory)
		}
	})
}

// zeroSourceVector lists every source task of one installed assignment at the
// trivially committed zero watermark in canonical order.
func zeroSourceVector(assignment store.InstalledAssignment) []protocol.SourceCheckpoint {
	vector := make([]protocol.SourceCheckpoint, 0)
	for _, token := range assignment.Assignment.Tasks {
		stage, ok := controlStage(assignment.Topology.Spec(), token.Task.StageID)
		if ok && stage.Role == model.StageSource {
			vector = append(vector, protocol.SourceCheckpoint{Source: token.Task})
		}
	}
	sort.Slice(vector, func(i, j int) bool { return controlTaskLess(vector[i].Source, vector[j].Source) })
	return vector
}

// historicalSourceGrantAt installs current on the fixture repository, observes
// a committed checkpoint and the inventory under it, and returns the
// coordinator's deterministic source grant for that revision.
func historicalSourceGrantAt(t *testing.T, fixture *controlFixture, base workerTestFixture, session *ControlSession, current model.AssignmentSet, jobControlRevision uint64, record model.ResultRecord, request *byte) protocol.RepairGrant {
	t.Helper()
	fixture.repository.mu.Lock()
	fixture.repository.work.Assignments = []store.InstalledAssignment{{Assignment: current, SpecificationBytes: base.assignment.Topology.CanonicalBytes(), Topology: base.assignment.Topology, JobControlRevision: jobControlRevision, SchedulingState: model.Closed, CoordinatorEpoch: base.epoch}}
	fixture.repository.mu.Unlock()
	var source model.AssignmentToken
	for _, token := range current.Tasks {
		stage, _ := controlStage(base.assignment.Topology.Spec(), token.Task.StageID)
		if stage.Role == model.StageSource {
			source = token
			break
		}
	}
	if source == (model.AssignmentToken{}) {
		t.Fatal("fixture has no current source")
	}
	notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: current.JobID, Source: source.Task, Watermark: 1, RaftIndex: 11, Epoch: base.epoch}, JobControlRevision: jobControlRevision, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest}
	*request++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, *request, notice)); err != nil {
		t.Fatalf("checkpoint observation at revision %d rejected: %v", current.Revision, err)
	}
	query := protocol.ResultInventoryQuery{JobID: current.JobID, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, Checkpoints: []protocol.SourceCheckpoint{{Source: source.Task, Watermark: 1}}}
	query.CheckpointDigest = protocol.CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	*request++
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, *request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Inventory: &query}))
	if err != nil {
		t.Fatalf("inventory at revision %d rejected: %v", current.Revision, err)
	}
	summary := response.(protocol.WorkerStatus).Inventory
	if summary == nil || summary.RecordCount != 1 {
		t.Fatalf("inventory at revision %d = %#v", current.Revision, summary)
	}
	currentReplica, ok := controlReplica(current, record.SinkTask)
	if !ok {
		t.Fatal("missing current destination replica")
	}
	instruction := protocol.RepairResultPartition{CoordinatorEpoch: base.epoch, JobID: current.JobID, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest, SourceNodeID: base.localNode, SourceWorkerEpoch: base.localEpoch, DestinationNodeID: currentReplica.PrimaryNodeID, DestinationWorkerEpoch: currentReplica.PrimaryEpoch, SinkTask: record.SinkTask, SpecificationHash: base.assignment.Topology.Digest(), Checkpoints: append([]protocol.SourceCheckpoint(nil), query.Checkpoints...), CheckpointDigest: query.CheckpointDigest, InventoryQueryDigest: query.QueryDigest, ExpectedRecordCount: summary.RecordCount, ExpectedTotalBytes: summary.TotalBytes, ExpectedContentDigest: summary.ContentDigest}
	instruction.RepairID = protocol.DeriveRepairID(instruction)
	instruction.InstructionDigest = protocol.RepairInstructionDigest(instruction)
	return protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairSource}
}

func controlRepairByID(t *testing.T, repository *controlTestRepository, id [16]byte) store.ResultRepairRecord {
	t.Helper()
	repository.mu.Lock()
	defer repository.mu.Unlock()
	for _, repair := range repository.work.Repairs {
		if repair.Instruction.RepairID == id {
			return repair
		}
	}
	t.Fatalf("repair %x missing from %+v", id[:4], repository.work.Repairs)
	return store.ResultRepairRecord{}
}

// historicalHolderRecord returns the fixture's retained sink record held by
// the local node under the base assignment, together with its provenance.
func historicalHolderRecord(t *testing.T, base workerTestFixture) (model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	var sink model.AssignmentToken
	var oldReplica model.ResultReplicaSet
	for _, candidate := range base.assignment.Assignment.ResultReplicas {
		if candidate.PrimaryNodeID != base.localNode && candidate.SecondaryNodeID != base.localNode {
			continue
		}
		oldReplica = candidate
		for _, token := range base.assignment.Assignment.Tasks {
			if token.Task == candidate.SinkTask {
				sink = token
				break
			}
		}
		break
	}
	if sink == (model.AssignmentToken{}) {
		t.Fatal("fixture has no historical result")
	}
	role := model.PrimaryReplica
	if oldReplica.PrimaryNodeID != base.localNode {
		role = model.SecondaryReplica
	}
	return base.result(t, sink, oldReplica, 1, role)
}

func newHistoricalHolderControlFixture(t *testing.T) (*controlFixture, workerTestFixture, model.ResultRecord, *ControlSession) {
	t.Helper()
	fixture := newControlFixture(t)
	base := workerFixture(t)
	record, provenance := historicalHolderRecord(t, base)
	fixture.assignment = base.assignment
	fixture.epoch = base.epoch
	fixture.repository.work.Fence = base.epoch
	fixture.repository.work.Results = []store.StoredResult{{Record: record, Provenance: provenance}}
	session := fixture.session(t, 2)
	fixture.authenticate(t, session, 2, 1)
	return fixture, base, record, session
}

// TestControlRepairGrantRefusesDifferentIdentityWhileLivePriorIsCurrent pins
// the unchanged half of the installRepair identity rule: while a prior grant
// with the same (epoch, job, sink, role) identity is live at the installed
// assignment revision, a different RepairID is refused as identity reuse
// without touching durable state.
func TestControlRepairGrantRefusesDifferentIdentityWhileLivePriorIsCurrent(t *testing.T) {
	fixture, base, record, session := newHistoricalHolderControlFixture(t)
	request := byte(1)
	first := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+1)
	live := historicalSourceGrantAt(t, fixture, base, session, first, 2, record, &request)
	request++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &live})); err != nil {
		t.Fatalf("first grant rejected: %v", err)
	}
	other := live
	other.Instruction.Checkpoints = append([]protocol.SourceCheckpoint(nil), live.Instruction.Checkpoints...)
	other.Instruction.ExpectedRecordCount++
	other.Instruction.RepairID = protocol.DeriveRepairID(other.Instruction)
	other.Instruction.InstructionDigest = protocol.RepairInstructionDigest(other.Instruction)
	request++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &other})); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("second grant against a live prior at the current revision err=%v want %v", err, model.ErrIdentityReuse)
	}
	if got := controlRepairByID(t, fixture.repository, live.Instruction.RepairID); got.State != store.RepairPending || countControlLog(fixture.repository.log, "repair") != 1 {
		t.Fatalf("refused grant mutated repair state: %+v log=%v", got, fixture.repository.log)
	}
}

// TestControlRepairGrantReplacesSupersededPrior pins the superseded-grant
// ruling's installRepair rule: once the installed assignment revision has
// advanced past every prior grant with the same (epoch, job, sink, role)
// identity, the coordinator's replacement grant (a different RepairID under
// the current revision) is admitted and the superseded priors are durably
// marked RepairFailed, so the next coordinator pass can re-repair the sink
// after any reassignment instead of wedging on ErrIdentityReuse until a
// leadership change; re-delivery of the dead grant reports its terminal
// state without reviving it.
func TestControlRepairGrantReplacesSupersededPrior(t *testing.T) {
	fixture, base, record, session := newHistoricalHolderControlFixture(t)
	request := byte(1)
	first := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+1)
	live := historicalSourceGrantAt(t, fixture, base, session, first, 2, record, &request)
	request++
	if _, err := session.Handle(context.Background(), fixture.frame(t, 2, request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &live})); err != nil {
		t.Fatalf("first grant rejected: %v", err)
	}
	second := nonTargetAssignmentForControlTest(t, base, base.assignment.Assignment.Revision+2)
	replacement := historicalSourceGrantAt(t, fixture, base, session, second, 3, record, &request)
	request++
	response, err := session.Handle(context.Background(), fixture.frame(t, 2, request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &replacement}))
	if err != nil {
		t.Fatalf("replacement grant after reassignment rejected: %v", err)
	}
	status := response.(protocol.WorkerStatus).Repair
	if status == nil || status.RepairID != replacement.Instruction.RepairID || status.State != protocol.RepairPending {
		t.Fatalf("replacement status=%#v", status)
	}
	superseded := controlRepairByID(t, fixture.repository, live.Instruction.RepairID)
	if superseded.State != store.RepairFailed || superseded.ErrorCode != protocol.WorkerErrorStaleAssignment {
		t.Fatalf("superseded prior not durably failed: %+v", superseded)
	}
	if got := controlRepairByID(t, fixture.repository, replacement.Instruction.RepairID); got.State != store.RepairPending || got.Role != store.RepairSource {
		t.Fatalf("replacement not installed: %+v", got)
	}
	if want := []string{"repair", "repair", "repair"}; countControlLog(fixture.repository.log, "repair") != len(want) {
		t.Fatalf("expected first install, failed-prior marking, replacement install: %v", fixture.repository.log)
	}
	request++
	response, err = session.Handle(context.Background(), fixture.frame(t, 2, request, protocol.WorkerStatusRequest{CoordinatorEpoch: base.epoch, MaxEvents: 1, Repair: &live}))
	if err != nil {
		t.Fatalf("dead grant re-delivery: %v", err)
	}
	if status := response.(protocol.WorkerStatus).Repair; status == nil || status.State != protocol.RepairFailed || status.ErrorCode != protocol.WorkerErrorStaleAssignment {
		t.Fatalf("dead grant status=%#v", status)
	}
	if countControlLog(fixture.repository.log, "repair") != 3 {
		t.Fatalf("dead grant re-delivery mutated repair state: %v", fixture.repository.log)
	}
}
