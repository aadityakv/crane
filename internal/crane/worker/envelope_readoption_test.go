package worker

import (
	"context"
	"testing"

	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
)

// movedTaskAssignment derives revision+1 of the fixture's installed assignment
// in which task moves to node 9 at a fresh epoch with a bumped attempt; every
// other token keeps its incarnation under the new revision.
func movedTaskAssignment(t *testing.T, fixture workerTestFixture, task model.TaskID) store.InstalledAssignment {
	t.Helper()
	prior := fixture.assignment.Assignment
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	for index := range tasks {
		if tasks[index].Task == task {
			tasks[index].WorkerID, tasks[index].WorkerEpoch, tasks[index].Attempt = 9, model.WorkerEpoch{9}, tasks[index].Attempt+1
		}
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	set, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, prior.ResultReplicas, fixture.topology)
	if err != nil {
		t.Fatal(err)
	}
	installed := fixture.assignment
	installed.Assignment = set
	installed.JobControlRevision++
	return installed
}

// TestRetainedOutboxReDerivesEmissionUnderSupersededRevision pins the Task 24
// defect #4 ruling's producer half: an outbox retained under a superseded
// assignment revision whose producing task incarnation is unchanged re-derives
// its emission under the current envelope — the current destination token of
// a moved consumer, the current revision/digest and fence — and the ACK that
// envelope earns binds the durable outbox. A producer whose own task was
// replaced keeps sending its retained envelope, which the receiver refuses.
func TestRetainedOutboxReDerivesEmissionUnderSupersededRevision(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	tuple, exists, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil || !exists {
		t.Fatalf("SourceTuple = %t,%v", exists, err)
	}
	retained, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil || len(retained) == 0 {
		t.Fatalf("retained outboxes=%d err=%v", len(retained), err)
	}
	current := movedTaskAssignment(t, fixture, fixture.transform.Task)
	installAssignment(repository, current)
	repository.mu.Lock()
	repository.work.Outboxes = retained
	for _, outbox := range retained {
		repository.outboxes[outbox.ID] = outbox
	}
	repository.mu.Unlock()

	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	defer func() { cancel(); <-done }()
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() >= 1 })
	sender.mu.Lock()
	messages := append([]protocol.TupleDelivery(nil), sender.deliveries...)
	sender.mu.Unlock()
	movedTransform, _ := findAssignmentToken(current.Assignment, fixture.transform.Task)
	currentSource, _ := findAssignmentToken(current.Assignment, fixture.source.Task)
	identity := protocol.AssignmentSetIdentity{JobID: current.Assignment.JobID, Revision: current.Assignment.Revision, Digest: current.Assignment.Digest}
	for _, message := range messages {
		if message.Assignment != identity || message.Coordinator != fixture.epoch || message.Producer != currentSource || message.Destination != movedTransform {
			t.Fatalf("re-sent emission not under the current envelope: %#v", message)
		}
	}
	ack := protocol.TupleACK{DeliveryID: retained[0].ID, Destination: movedTransform, Assignment: identity, Coordinator: fixture.epoch, Status: protocol.TupleAccepted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatalf("current-envelope ACK refused: %v", err)
	}
	repository.mu.Lock()
	accepted := repository.outboxes[retained[0].ID].Accepted
	repository.mu.Unlock()
	if !accepted {
		t.Fatal("current-envelope ACK did not bind the durable outbox")
	}
	foreign := ack
	foreign.Assignment.Digest[0] ^= 0xFF
	if err := engine.HandleACK(ctx, foreign); err == nil {
		t.Fatal("foreign-envelope ACK accepted")
	}
}

// TestReplacedProducerNeverReDerivesRetainedOutbox pins the fail-closed side:
// when the producing task itself was replaced (moved to another worker), its
// retained outboxes are not re-enveloped by the superseded incarnation.
func TestReplacedProducerNeverReDerivesRetainedOutbox(t *testing.T) {
	fixture := workerFixture(t)
	repository := newFakeRepository(fixture)
	tuple, _, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil {
		t.Fatal(err)
	}
	current := movedTaskAssignment(t, fixture, fixture.source.Task)
	installAssignment(repository, current)
	repository.mu.Lock()
	repository.work.Outboxes = retained
	for _, outbox := range retained {
		repository.outboxes[outbox.ID] = outbox
	}
	repository.mu.Unlock()
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(testEngineOptions(repository, gate, &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	defer func() { cancel(); <-done }()
	<-engine.Ready()
	message, want := engine.emissionForOutbox(retained[0]), deliveryMessageForOutbox(retained[0])
	if message.Assignment != want.Assignment || message.Coordinator != want.Coordinator || message.Producer != want.Producer || message.Destination != want.Destination {
		t.Fatalf("replaced producer re-enveloped its retained outbox: %#v", message)
	}
}

// TestProcessedSinkParentUnderSupersededRevisionCompletesAfterReplication
// pins the sink half end to end: a Processed sink delivery and result retained
// under the superseded replica pair replicate to the replacement partner under
// the current envelope, the local copy re-binds, and the retained parent —
// whose sink incarnation is unchanged — completes.
func TestProcessedSinkParentUnderSupersededRevisionCompletesAfterReplication(t *testing.T) {
	fixture, sink, replica := workerFixtureWithLocalPrimarySink(t)
	repository := newFakeRepository(fixture)
	delivery := fixture.sinkDelivery(t, sink, 1)
	delivery.State = store.Processed
	delivery.Outputs = []model.Tuple{cloneTuple(delivery.Tuple)}
	record, retained := fixture.result(t, sink, replica, 1, model.PrimaryReplica)
	repository.work.Deliveries = []store.DeliveryRecord{delivery}
	repository.deliveries[delivery.ID] = delivery
	repository.results = []store.StoredResult{{Record: record, Provenance: retained}}
	repository.work.Results = append([]store.StoredResult(nil), repository.results...)
	current := partnerChangedAssignment(t, fixture, sink.Task, model.Running)
	installAssignment(repository, current)
	replicator := &fakeResultReplicator{calls: make(chan resultReplicationCall, 1)}
	_, cancel, done := startResultEngine(t, repository, fixture.epoch, replicator)
	defer func() { cancel(); <-done }()
	call := <-replicator.calls
	if want := expectedEnvelope(current, sink.Task, model.SecondaryReplica); call.provenance != want {
		t.Fatalf("replication provenance=%+v want=%+v", call.provenance, want)
	}
	replicator.ack(call)
	local := expectedEnvelope(current, sink.Task, model.PrimaryReplica)
	waitFor(t, func() bool {
		repository.mu.Lock()
		defer repository.mu.Unlock()
		return repository.deliveries[delivery.ID].State == store.Completed && len(repository.results) == 1 && repository.results[0].Provenance == local
	})
}

// TestAcceptedOutboxUnderReplicaOnlyRevisionCompletesOnCurrentEnvelopeACK
// pins the surviving-source shape of leader_loss_after_progress_before_
// checkpoint: an Accepted outbox retained under a revision that changed only
// the sink's replica pair (every token keeps its incarnation) re-sends under
// the current envelope and completes on the sink's current-envelope
// Completed ACK.
func TestAcceptedOutboxUnderReplicaOnlyRevisionCompletesOnCurrentEnvelopeACK(t *testing.T) {
	fixture, sink, _ := workerFixtureWithLocalPrimarySink(t)
	fixture.localNode, fixture.localEpoch = fixture.source.WorkerID, fixture.source.WorkerEpoch
	repository := newFakeRepository(fixture)
	tuple, _, err := model.SourceTuple(fixture.topology, fixture.source.Task, 1)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := deriveSourceOutboxes(fixture.assignment, fixture.source, 1, tuple)
	if err != nil || len(retained) == 0 {
		t.Fatalf("retained outboxes=%d err=%v", len(retained), err)
	}
	for index := range retained {
		retained[index].Accepted = true
		retained[index].RetryDeadlineUnixNano = 1
	}
	current := partnerChangedAssignment(t, fixture, sink.Task, model.Running)
	installAssignment(repository, current)
	repository.mu.Lock()
	repository.work.Outboxes = retained
	for _, outbox := range retained {
		repository.outboxes[outbox.ID] = outbox
	}
	repository.mu.Unlock()
	gate := admission.NewGate()
	if err := gate.Open(fixture.epoch); err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	engine, err := NewEngine(testEngineOptions(repository, gate, sender))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runEngine(t, ctx, engine)
	defer func() { cancel(); <-done }()
	<-engine.Ready()
	waitFor(t, func() bool { return sender.count() >= 1 })
	sender.mu.Lock()
	message := sender.deliveries[0]
	sender.mu.Unlock()
	identity := protocol.AssignmentSetIdentity{JobID: current.Assignment.JobID, Revision: current.Assignment.Revision, Digest: current.Assignment.Digest}
	if message.Assignment != identity || message.Coordinator != fixture.epoch {
		t.Fatalf("re-sent emission not under the current envelope: %#v", message)
	}
	ack := protocol.TupleACK{DeliveryID: message.DeliveryID, Destination: message.Destination, Assignment: message.Assignment, Coordinator: message.Coordinator, Status: protocol.TupleCompleted}
	if err := engine.HandleACK(ctx, ack); err != nil {
		t.Fatalf("current-envelope Completed ACK refused: %v", err)
	}
	repository.mu.Lock()
	completed := repository.outboxes[message.DeliveryID].Completed
	repository.mu.Unlock()
	if !completed {
		t.Fatal("current-envelope Completed ACK did not complete the durable outbox")
	}
}
