package store

import (
	"errors"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// movedSourceSet derives revision+1 of prior in which the source task moves
// to node 3 at a fresh epoch with a bumped attempt while every other token
// (including the local transform that retains custody) keeps its incarnation.
func movedSourceSet(t *testing.T, topology model.ValidatedTopology, prior model.AssignmentSet, bumpDestinationAttempt bool) model.AssignmentSet {
	t.Helper()
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	for index := range tasks {
		switch {
		case tasks[index].Task.StageID == 1:
			tasks[index].WorkerID, tasks[index].WorkerEpoch, tasks[index].Attempt = 3, model.WorkerEpoch{9}, tasks[index].Attempt+1
		case bumpDestinationAttempt && tasks[index].Task.StageID == 2 && tasks[index].WorkerID == 1:
			tasks[index].Attempt++
		}
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	current, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, prior.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	return current
}

// currentEnvelopeOf rebrands a retained delivery under the current set: the
// current producer and destination tokens, revision, digest and fence.
func currentEnvelopeOf(t *testing.T, delivery DeliveryRecord, current model.AssignmentSet, fence model.CoordinatorEpoch) DeliveryRecord {
	t.Helper()
	producer, ok := findToken(current, delivery.Producer.Task)
	if !ok {
		t.Fatal("current set lacks the producer task")
	}
	destination, ok := findToken(current, delivery.Destination.Task)
	if !ok {
		t.Fatal("current set lacks the destination task")
	}
	rebrand := delivery.Clone()
	rebrand.Producer, rebrand.Destination = producer, destination
	rebrand.AssignmentRevision, rebrand.AssignmentDigest, rebrand.CoordinatorEpoch = current.Revision, current.Digest, fence
	return rebrand
}

// TestProbeDeliveryAnswersCurrentAssignmentRebrandOfRetainedCustody pins the
// Task 24 defect #4 ruling's receiver oracle: custody retained under a
// superseded assignment revision answers the current assignment's exact
// derivation of the same logical delivery re-sent by the replaced producer,
// while a changed payload, a replaced destination incarnation and a foreign
// envelope stay identity reuse.
func TestProbeDeliveryAnswersCurrentAssignmentRebrandOfRetainedCustody(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, prior, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(prior, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDeliverySequence(t, topology, prior, epoch, 1)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	current := movedSourceSet(t, topology, prior, false)
	if err := store.InstallAssignment(current, topology.Spec(), 2, model.Running, epoch); err != nil {
		t.Fatalf("install moved-source revision: %v", err)
	}
	rebrand := currentEnvelopeOf(t, delivery, current, epoch)
	state, found, err := store.ProbeDelivery(rebrand)
	if err != nil || !found || state != Received {
		t.Fatalf("current-assignment rebrand probe = %v,%t,%v", state, found, err)
	}
	if state, err := store.Receive(rebrand); err != nil || state != Received {
		t.Fatalf("current-assignment re-delivery = %v,%v", state, err)
	}
	if work := mustRecoverWork(t, store); len(work.Deliveries) != 1 || work.Deliveries[0].AssignmentRevision != prior.Revision {
		t.Fatalf("re-delivery mutated retained custody: %+v", work.Deliveries)
	}

	changed := rebrand
	changed.Tuple = cloneTuple(rebrand.Tuple)
	changed.Tuple.Fields[0].Value.Int64++
	if _, _, err := store.ProbeDelivery(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed payload probe = %v", err)
	}
	foreign := rebrand
	foreign.AssignmentDigest[0] ^= 0xFF
	if _, _, err := store.ProbeDelivery(foreign); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("foreign envelope probe = %v", err)
	}
	// The exact retained-brand replay keeps answering without the rebind.
	state, found, err = store.ProbeDelivery(delivery)
	if err != nil || !found || state != Received {
		t.Fatalf("retained-brand probe = %v,%t,%v", state, found, err)
	}

	// A replaced destination incarnation never re-adopts.
	replaced := movedSourceSet(t, topology, current, true)
	if err := store.InstallAssignment(replaced, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatalf("install replaced-destination revision: %v", err)
	}
	if _, _, err := store.ProbeDelivery(currentEnvelopeOf(t, delivery, replaced, epoch)); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("replaced destination probe = %v", err)
	}
}

// TestMarkProcessedReadoptsRetainedDeliveryUnderSupersededRevision pins the
// execution half: retained Received custody processes under the current
// assignment, deriving its outboxes under the current envelope (current
// producer incarnation token, revision, digest, fence), idempotently and
// durably across reopen; a replaced destination incarnation never processes.
func TestMarkProcessedReadoptsRetainedDeliveryUnderSupersededRevision(t *testing.T) {
	store, path, identity, options := rebindStoreForTest(t)
	topology, prior, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(prior, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDeliverySequence(t, topology, prior, epoch, 1)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	current := movedSourceSet(t, topology, prior, false)
	if err := store.InstallAssignment(current, topology.Spec(), 2, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, topology, current, currentEnvelopeOf(t, delivery, current, epoch))
	if len(outboxes) == 0 {
		t.Fatal("fixture derives no outboxes")
	}
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatalf("MarkProcessed of retained custody under the current assignment: %v", err)
	}
	if err := store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	work := mustRecoverWork(t, store)
	if len(work.Deliveries) != 1 || work.Deliveries[0].State != Processed || len(work.Deliveries[0].OutboxIDs) != len(outboxes) || len(work.Outboxes) != len(outboxes) {
		t.Fatalf("replayed deliveries=%+v outboxes=%d", work.Deliveries, len(work.Outboxes))
	}
	currentProducer, _ := findToken(current, delivery.Destination.Task)
	for _, outbox := range work.Outboxes {
		if outbox.AssignmentRevision != current.Revision || outbox.AssignmentDigest != current.Digest || outbox.CoordinatorEpoch != epoch || outbox.Producer != currentProducer {
			t.Fatalf("durable outbox not under the current envelope: %#v", outbox)
		}
	}

	// A retained Received record whose destination incarnation was replaced
	// never processes under the replacement.
	stray := domainDeliverySequence(t, topology, prior, epoch, 2)
	stray.AssignmentRevision, stray.AssignmentDigest = current.Revision, current.Digest
	stray = currentEnvelopeOf(t, stray, current, epoch)
	if _, err := store.Receive(stray); err != nil {
		t.Fatalf("seed second delivery: %v", err)
	}
	replaced := movedSourceSet(t, topology, current, true)
	if err := store.InstallAssignment(replaced, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	strayOutputs, strayOutboxes := exactProcessedRecords(t, topology, replaced, currentEnvelopeOf(t, stray, replaced, epoch))
	if err := store.MarkProcessed(stray.ID, strayOutputs, strayOutboxes); err == nil {
		t.Fatal("replaced destination incarnation processed retained custody")
	}
}

// TestProbeDeliveryAnswersCompletedSinkCustodyAfterReplicaOnlyRevision pins
// the surviving-source shape end to end against the real store: a direct
// source→sink delivery Completed under revision 1, a revision 2 that changes
// only the sink's replica partner (every token keeps its incarnation), and
// the source's current-envelope re-delivery must be answered Completed.
func TestProbeDeliveryAnswersCompletedSinkCustodyAfterReplicaOnlyRevision(t *testing.T) {
	path := t.TempDir() + "/sink"
	identity := Identity{ClusterID: [16]byte{77}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	spec := model.TopologySpec{SchemaVersion: 1, Name: "direct", RegistryFingerprint: model.RegistryFingerprint(), Stages: []model.StageSpec{
		{StageID: 1, Name: "source", Role: model.StageSource, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "0"}}}},
		{StageID: 2, Name: "sink", Role: model.StageSink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.RoutingShuffle}}}
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	workers := []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: model.WorkerEpoch{7}, SlotCapacity: 4}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{8}, SlotCapacity: 4}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{9}, SlotCapacity: 4}}
	var prior model.AssignmentSet
	found := false
	for candidate := byte(1); candidate != 0 && !found; candidate++ {
		prior, err = model.BuildAssignmentSet(model.JobID{candidate}, topology.Digest(), 1, topology, workers)
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range prior.Tasks {
			if token.Task.StageID == 2 && token.WorkerID == 1 && prior.ResultReplicas[0].PrimaryNodeID == 1 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no placement puts the sink primary on node 1")
	}
	epoch := model.CoordinatorEpoch{Term: 2, BeginIndex: 4, Coordinator: 2, Nonce: [16]byte{5}}
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(prior, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	source, _ := findToken(prior, model.TaskID{JobID: prior.JobID, StageID: 1})
	sink, _ := findToken(prior, model.TaskID{JobID: prior.JobID, StageID: 2})
	tuple, _, err := model.SourceTuple(topology, source.Task, 1)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := topology.WorstCaseCustodyBytes(sink.Task)
	if err != nil {
		t.Fatal(err)
	}
	delivery := DeliveryRecord{ID: model.DeliveryID{Tuple: model.DeriveSourceTupleID(prior.JobID, source.Task, 1), EdgeID: 1, DestinationTask: sink.Task}, Tuple: tuple, Producer: source, Destination: sink, AssignmentRevision: prior.Revision, AssignmentDigest: prior.Digest, CoordinatorEpoch: epoch, State: Received, Reservation: reservation}
	if _, err := workerStore.Receive(delivery); err != nil {
		t.Fatalf("receive: %v", err)
	}
	outputs, err := model.ExecuteOperator(model.OperatorSpec{Name: "collect", Version: 1}, tuple)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerStore.MarkProcessed(delivery.ID, outputs, nil); err != nil {
		t.Fatalf("process: %v", err)
	}
	if err := workerStore.MarkCompleted(delivery.ID); err != nil {
		t.Fatalf("complete: %v", err)
	}
	replicas := append([]model.ResultReplicaSet(nil), prior.ResultReplicas...)
	replicas[0].SecondaryNodeID, replicas[0].SecondaryEpoch = 3, model.WorkerEpoch{9}
	if replicas[0].SecondaryNodeID == prior.ResultReplicas[0].SecondaryNodeID {
		replicas[0].SecondaryNodeID, replicas[0].SecondaryEpoch = 2, model.WorkerEpoch{8}
	}
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	for index := range tasks {
		tasks[index].AssignmentRevision = 2
	}
	current, err := model.NewAssignmentSet(prior.JobID, 2, tasks, replicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	newer := epoch
	newer.Term++
	newer.BeginIndex += 5
	if err := workerStore.Fence(newer); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(current, topology.Spec(), 2, model.Running, newer); err != nil {
		t.Fatalf("install replica-only revision: %v", err)
	}
	rebrand := currentEnvelopeOf(t, delivery, current, newer)
	state, found, err := workerStore.ProbeDelivery(rebrand)
	if err != nil || !found || state != Completed {
		t.Fatalf("current-envelope re-delivery probe = %v,%t,%v", state, found, err)
	}
}
