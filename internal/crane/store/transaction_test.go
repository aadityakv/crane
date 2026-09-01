package store

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestTransactionFenceAssignmentDeliveryAndRecovery(t *testing.T) {
	store, identity, options := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(epoch); err != nil {
		t.Fatalf("exact fence retry: %v", err)
	}
	older := epoch
	older.Term--
	if err := store.Fence(older); err == nil {
		t.Fatal("stale fence accepted")
	}
	collision := epoch
	collision.Nonce[0]++
	if err := store.Fence(collision); err == nil {
		t.Fatal("same ordered epoch with changed identity accepted")
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 7, model.Running, epoch); err != nil {
		t.Fatal(err)
	}

	delivery := domainDelivery(t, topology, assignment, epoch)
	state, err := store.Receive(delivery)
	if err != nil || state != Received {
		t.Fatalf("Receive = %v,%v", state, err)
	}
	if retry, err := store.Receive(delivery); err != nil || retry != Received {
		t.Fatalf("retry = %v,%v", retry, err)
	}
	changed := delivery.Clone()
	changed.Tuple.Fields[0].Value.Int64++
	if _, err := store.Receive(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed duplicate = %v", err)
	}

	out := model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 6}}}}
	outbox := domainOutbox(t, delivery, out, assignment, topology)
	if err := store.MarkProcessed(delivery.ID, []model.Tuple{out}, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProcessed(delivery.ID, []model.Tuple{out}, []OutboxRecord{outbox}); err != nil {
		t.Fatalf("exact processed retry: %v", err)
	}
	changedOutput := out
	changedOutput.Fields[0].Value.Int64++
	if err := store.MarkProcessed(delivery.ID, []model.Tuple{changedOutput}, []OutboxRecord{outbox}); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed processed retry: %v", err)
	}
	if err := store.MarkCompleted(delivery.ID); err == nil {
		t.Fatal("completion before downstream outbox completion accepted")
	}
	if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}

	work := mustRecoverWork(t, store)
	if work.Fence != epoch || len(work.Assignments) != 1 || len(work.Deliveries) != 1 || work.Deliveries[0].State != Completed || len(work.Outboxes) != 1 {
		t.Fatalf("unexpected live work: %+v", work)
	}
	if !bytes.Equal(work.Assignments[0].SpecificationBytes, topology.CanonicalBytes()) || work.Assignments[0].Topology.Digest() != topology.Digest() {
		t.Fatal("assignment topology was not canonically recovered")
	}
	work.Assignments[0].SpecificationBytes[0] ^= 1
	work.Deliveries[0].Tuple.Fields[0].Value.Int64++
	if next := mustRecoverWork(t, store); bytes.Equal(next.Assignments[0].SpecificationBytes, work.Assignments[0].SpecificationBytes) || next.Deliveries[0].Tuple.Fields[0].Value.Int64 == work.Deliveries[0].Tuple.Fields[0].Value.Int64 {
		t.Fatal("RecoverWork aliases store state")
	}

	_ = options
}

func TestTransactionCapacityRejectsBeforeMutationAndReservationIsReleasedOnlyByCheckpoint(t *testing.T) {
	store, identity, _ := openDomainStore(t, 1<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	payload, err := encodeDeliveryRecord(delivery, nil)
	if err != nil {
		t.Fatal(err)
	}
	transactionBytes, err := transactionEncodedSize(Transaction{Records: []Record{{Type: recordDelivery, Payload: payload}}})
	if err != nil {
		t.Fatal(err)
	}
	store.options.MaxBytes = store.state.WALBytes + transactionBytes + delivery.Reservation - 1
	before := store.Recovered()
	if _, err := store.Receive(delivery); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Receive = %v", err)
	}
	if after := store.Recovered(); after != before || len(mustRecoverWork(t, store).Deliveries) != 0 {
		t.Fatal("capacity rejection mutated state")
	}
}

func TestTransactionAssignmentValidationAndAtomicReplacement(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	badSpec := topology.Spec()
	badSpec.RegistryFingerprint[0] ^= 1
	if err := store.InstallAssignment(assignment, badSpec, 1, model.Running, epoch); err == nil {
		t.Fatal("registry mismatch accepted")
	}
	bad := assignment
	bad.Digest[0] ^= 1
	if err := store.InstallAssignment(bad, topology.Spec(), 1, model.Running, epoch); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	if len(mustRecoverWork(t, store).Assignments) != 0 {
		t.Fatal("rejected assignment published")
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	stale := assignment
	stale.Revision = 2
	for i := range stale.Tasks {
		stale.Tasks[i].AssignmentRevision = 2
	}
	if err := store.InstallAssignment(stale, topology.Spec(), 2, model.Running, epoch); err == nil {
		t.Fatal("invalid replacement digest accepted")
	}
	if got := mustRecoverWork(t, store).Assignments[0].Assignment.Digest; got != assignment.Digest {
		t.Fatal("failed replacement mutated assignment")
	}
}

func TestTransactionResultsAreBucketedBySinkAndProvenanceIsSeparate(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	for partition := uint16(0); partition < 2; partition++ {
		record, provenance := domainResult(t, topology, assignment, epoch, partition)
		checksum := record.Checksum
		if err := store.UpsertResult(record, provenance); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertResult(record, provenance); err != nil {
			t.Fatalf("exact retry: %v", err)
		}
		changed := provenance
		changed.AssignmentRevision++
		if err := store.UpsertResult(record, changed); err == nil {
			t.Fatal("conflicting provenance accepted")
		}
		changedValue, _ := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 99}}}})
		changedRecord, err := model.NewResultRecord(record.TupleID, record.SinkTask, record.SpecificationHash, changedValue)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertResult(changedRecord, provenance); !errors.Is(err, model.ErrIdentityReuse) {
			t.Fatalf("changed logical result = %v", err)
		}
		if record.Checksum != checksum {
			t.Fatal("copy provenance changed logical checksum")
		}
	}
	work := mustRecoverWork(t, store)
	if len(work.Results) != 2 || work.Results[0].Record.SinkTask == work.Results[1].Record.SinkTask {
		t.Fatalf("ambiguous buckets: %+v", work.Results)
	}
	for _, result := range work.Results {
		if result.Provenance.ReplicaSet.SinkTask != result.Record.SinkTask {
			t.Fatal("result/provenance sink mismatch")
		}
	}
}

func TestTransactionEventsPaginationAcknowledgmentCheckpointAndRetention(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProcessed(delivery.ID, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCompleted(delivery.ID); err != nil {
		t.Fatal(err)
	}
	record, provenance := domainResult(t, topology, assignment, epoch, 0)
	if err := store.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}

	for id := uint64(1); id <= 3; id++ {
		event := domainFailureEvent(store, assignment, epoch, id)
		if err := store.PersistEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	page, last, more, err := store.PendingEvents(0, 2)
	if err != nil || len(page) != 2 || last != 2 || !more {
		t.Fatalf("page = %d,%d,%v,%v", len(page), last, more, err)
	}
	if err := store.AcknowledgeEvents(2); err != nil {
		t.Fatal(err)
	}
	if got := mustRecoverWork(t, store); len(got.PendingEvents) != 1 || got.PendingEvents[0].TransactionID != 3 || got.NextTransactionID != 4 {
		t.Fatalf("events = %+v next=%d", got.PendingEvents, got.NextTransactionID)
	}

	notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: delivery.ID.Tuple.SourceSequence, RaftIndex: 9, Epoch: epoch}
	if err := store.ApplyCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	work := mustRecoverWork(t, store)
	if len(work.Deliveries) != 1 || work.Deliveries[0].State != Compacted || work.Deliveries[0].Reservation != 0 || len(work.Results) != 1 {
		t.Fatalf("compaction/retention = %+v", work)
	}
	if state, err := store.Receive(delivery); err != nil || state != Compacted {
		t.Fatalf("tombstone retry = %v,%v", state, err)
	}
	older := notice
	older.Watermark--
	if err := store.ApplyCheckpoint(older); err == nil {
		t.Fatal("checkpoint decrease accepted")
	}
}

func TestTransactionRepairRetryProgressAndEpochFence(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	repair := domainRepair(t, topology, assignment, epoch, identity.NodeID, store.WorkerEpoch())
	if err := store.UpsertRepair(repair); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertRepair(repair); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	progress := repair
	progress.State = RepairStreaming
	progress.NextRecord = 3
	progress.NextOffset = 17
	if err := store.UpsertRepair(progress); err != nil {
		t.Fatal(err)
	}
	changed := progress
	changed.Instruction.ExpectedRecordCount++
	if err := store.UpsertRepair(changed); err == nil {
		t.Fatalf("changed instruction = %v", err)
	}
	work := mustRecoverWork(t, store)
	if len(work.Repairs) != 1 || work.Repairs[0].NextRecord != 3 || work.Repairs[0].NextOffset != 17 {
		t.Fatalf("repair progress lost: %+v", work.Repairs)
	}
	newEpoch := epoch
	newEpoch.Term++
	newEpoch.BeginIndex++
	newEpoch.Nonce[0]++
	if err := store.Fence(newEpoch); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertRepair(progress); err == nil {
		t.Fatal("old-fence repair resumed")
	}
}

func TestTransactionSourceOutboxCompletionAndBothEventKinds(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	outbox := domainOutbox(t, delivery, delivery.Tuple, assignment, topology)
	cursor := SourceCursor{Source: delivery.ID.Tuple.SourceTask, NextSequence: 2, EOF: 3}
	if err := store.AdvanceSource(cursor, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
		t.Fatal(err)
	}
	work := mustRecoverWork(t, store)
	if len(work.Sources) != 1 || work.Sources[0] != cursor || len(work.Outboxes) != 1 || !work.Outboxes[0].Completed {
		t.Fatalf("source/outbox = %+v", work)
	}

	var local model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID {
			local = token
			break
		}
	}
	report := model.CompletionReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Source: local.Task, Token: local, Epoch: epoch, Prior: 0, New: 1, EOF: 3, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	event := model.WorkerEvent{WorkerID: identity.NodeID, WorkerEpoch: store.WorkerEpoch(), TransactionID: 1, Kind: model.WorkerEventCompletion, Completion: &report}
	if err := store.PersistEvent(event); err != nil {
		t.Fatal(err)
	}
	if got := mustRecoverWork(t, store).PendingEvents; len(got) != 1 || got[0].Completion == nil {
		t.Fatalf("completion event = %+v", got)
	}
}

func TestTransactionRecoveryPublishesOnlyCompleteValidOwnedWork(t *testing.T) {
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	repair := domainRepair(t, topology, assignment, epoch, identity.NodeID, store.WorkerEpoch())
	repair.State, repair.NextRecord, repair.NextOffset = RepairStreaming, 2, 10
	if err := store.UpsertRepair(repair); err != nil {
		t.Fatal(err)
	}
	before := mustRecoverWork(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after := mustRecoverWork(t, reopened)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("recovered work changed\nbefore=%+v\nafter=%+v", before, after)
	}
	after.Deliveries[0].Tuple.Fields[0].Value.Int64++
	if reflect.DeepEqual(after, mustRecoverWork(t, reopened)) {
		t.Fatal("recovered work aliases live state")
	}
}

func TestTransactionRecoveryRejectsUnknownAndCrossReferenceRecords(t *testing.T) {
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	for _, test := range []struct {
		name        string
		transaction Transaction
	}{
		{name: "unknown schema", transaction: Transaction{Records: []Record{{Type: 100, Payload: []byte("opaque")}}}},
		{name: "missing delivery", transaction: func() Transaction {
			payload, err := encodeDeliveryIDPayload(model.DeliveryID{Tuple: model.TupleID{JobID: model.JobID{1}, SourceTask: model.TaskID{JobID: model.JobID{1}, StageID: 1}, SourceSequence: 1, PathDigest: [32]byte{1}}, EdgeID: 1, DestinationTask: model.TaskID{JobID: model.JobID{1}, StageID: 2}})
			if err != nil {
				t.Fatal(err)
			}
			return Transaction{Records: []Record{{Type: recordDeliveryCompleted, Payload: payload}}}
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/worker"
			store := mustOpen(t, path, identity, 1<<20, model.WorkerEpoch{7})
			if err := commitRawForTest(store, test.transaction); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path, identity, Options{MaxBytes: 1 << 20}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open = %v", err)
			}
		})
	}
	store := mustOpen(t, t.TempDir()+"/worker", identity, 1<<20, model.WorkerEpoch{7})
	defer store.Close()
	if err := store.Commit(Transaction{Records: []Record{{Type: 100, Payload: []byte("opaque")}}}); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("public Commit = %v", err)
	}
}

func TestTransactionConcurrentRecoveryAndLifecycle(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 20; j++ {
				_, _ = store.RecoverWork()
				_, err := store.Receive(delivery)
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("Receive: %v", err)
				}
			}
		}()
	}
	wait.Wait()
	if len(mustRecoverWork(t, store).Deliveries) != 1 {
		t.Fatal("concurrent exact retries duplicated custody")
	}
}

func openDomainStore(t *testing.T, max uint64) (*Store, Identity, Options) {
	t.Helper()
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: max, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(t.TempDir()+"/worker", identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, identity, options
}

func domainAssignment(t *testing.T, local model.WorkerEpoch, node uint16) (model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch) {
	t.Helper()
	spec := model.TopologySpec{SchemaVersion: 1, Name: "job", RegistryFingerprint: model.RegistryFingerprint(), Stages: []model.StageSpec{
		{StageID: 1, Name: "source", Role: model.StageSource, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "3"}, {Key: "start", Value: "0"}}}},
		{StageID: 2, Name: "times", Role: model.StageTransform, Parallelism: 1, Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: "2"}}}},
		{StageID: 3, Name: "sink", Role: model.StageSink, Parallelism: 2, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.RoutingShuffle}, {EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: model.RoutingFieldHash, Field: "value"}}}
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	workers := []model.WorkerPlacement{{NodeID: node, WorkerEpoch: local, SlotCapacity: 4}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{8}, SlotCapacity: 4}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{9}, SlotCapacity: 4}}
	var assignment model.AssignmentSet
	for candidate := byte(1); candidate != 0; candidate++ {
		job := model.JobID{candidate}
		assignment, err = model.BuildAssignmentSet(job, topology.Digest(), 1, topology, workers)
		if err != nil {
			t.Fatal(err)
		}
		localSink, localSource := false, false
		for _, token := range assignment.Tasks {
			if token.WorkerID == node && token.Task.StageID == 2 {
				localSink = true
			}
			if token.WorkerID == node && token.Task.StageID == 1 {
				localSource = true
			}
		}
		allReplicaDuties := true
		for _, replica := range assignment.ResultReplicas {
			if replica.PrimaryNodeID != node && replica.SecondaryNodeID != node {
				allReplicaDuties = false
			}
		}
		if localSink && localSource && allReplicaDuties {
			return topology, assignment, model.CoordinatorEpoch{Term: 2, BeginIndex: 4, Coordinator: 2, Nonce: [16]byte{5}}
		}
	}
	t.Fatal("could not construct a local sink assignment")
	return model.ValidatedTopology{}, model.AssignmentSet{}, model.CoordinatorEpoch{}
}

func domainDelivery(t *testing.T, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch) DeliveryRecord {
	t.Helper()
	var destination model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == 1 && token.Task.StageID == 2 {
			destination = token
			break
		}
	}
	if destination == (model.AssignmentToken{}) {
		t.Skip("rendezvous did not place sink locally")
	}
	producer := assignment.Tasks[0]
	tuple := model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 3}}}}
	tupleID := model.TupleID{JobID: assignment.JobID, SourceTask: producer.Task, SourceSequence: 1, PathDigest: [32]byte{4}}
	id := model.DeliveryID{Tuple: tupleID, EdgeID: 1, DestinationTask: destination.Task}
	reservation, err := topology.WorstCaseCustodyBytes(destination.Task)
	if err != nil {
		t.Fatal(err)
	}
	return DeliveryRecord{ID: id, Tuple: tuple, Producer: producer, Destination: destination, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, CoordinatorEpoch: epoch, State: Received, Reservation: reservation}
}

func domainOutbox(t *testing.T, parent DeliveryRecord, tuple model.Tuple, assignment model.AssignmentSet, topology model.ValidatedTopology) OutboxRecord {
	t.Helper()
	child := parent.ID
	child.EdgeID = 2
	routes, err := model.Route(topology, topology.Spec().Edges[1], child.Tuple, tuple)
	if err != nil {
		t.Fatal(err)
	}
	child.DestinationTask = model.TaskID{JobID: assignment.JobID, StageID: 3, Partition: routes[0]}
	var destination model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task == child.DestinationTask {
			destination = token
			break
		}
	}
	return OutboxRecord{ID: child, Tuple: tuple, Producer: parent.Destination, Destination: destination, AssignmentRevision: parent.AssignmentRevision, AssignmentDigest: parent.AssignmentDigest, CoordinatorEpoch: parent.CoordinatorEpoch}
}

func domainResult(t *testing.T, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, partition uint16) (model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	tuple, _ := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: int64(partition)}}}})
	id := model.TupleID{JobID: assignment.JobID, SourceTask: assignment.Tasks[0].Task, SourceSequence: uint64(partition) + 1, PathDigest: [32]byte{byte(partition) + 1}}
	sink := model.TaskID{JobID: assignment.JobID, StageID: 3, Partition: partition}
	record, err := model.NewResultRecord(id, sink, topology.Digest(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	replica := assignment.ResultReplicas[partition]
	role := model.PrimaryReplica
	if replica.SecondaryNodeID == 1 {
		role = model.SecondaryReplica
	}
	return record, model.ResultCopyProvenance{AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, ReplicaSet: replica, DestinationRole: role, CoordinatorEpoch: epoch}
}

func domainFailureEvent(store *Store, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, id uint64) model.WorkerEvent {
	var token model.AssignmentToken
	for _, candidate := range assignment.Tasks {
		if candidate.WorkerID == store.state.Identity.NodeID && candidate.WorkerEpoch == store.WorkerEpoch() {
			token = candidate
			break
		}
	}
	report := &model.JobFailureReport{JobID: assignment.JobID, JobControlRevision: 1, AssignmentRevision: assignment.Revision, Task: token, Epoch: epoch, TransactionID: id, Code: model.FailureOperator, DetailDigest: [32]byte{1}}
	return model.WorkerEvent{WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch, TransactionID: id, Kind: model.WorkerEventFailure, Failure: report}
}

func domainRepair(t *testing.T, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, node uint16, worker model.WorkerEpoch) ResultRepairRecord {
	t.Helper()
	def := model.RepairResultPartitionDefinition{CoordinatorEpoch: epoch, JobID: assignment.JobID, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceNodeID: node, SourceWorkerEpoch: worker, DestinationNodeID: 2, DestinationWorkerEpoch: model.WorkerEpoch{8}, SinkTask: assignment.ResultReplicas[0].SinkTask, SpecificationHash: topology.Digest(), ExpectedRecordCount: 4, ExpectedTotalBytes: 100, ExpectedContentDigest: [32]byte{1}}
	def.CheckpointDigest = model.CheckpointVectorDigest(nil)
	def.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: def.JobID, SinkTask: def.SinkTask, SpecificationHash: def.SpecificationHash, AssignmentRevision: def.AssignmentRevision, AssignmentDigest: def.AssignmentDigest, Checkpoints: def.Checkpoints, CheckpointDigest: def.CheckpointDigest})
	def.RepairID = model.DeriveRepairID(def)
	return ResultRepairRecord{Instruction: def, InstructionDigest: model.RepairInstructionDigest(def), Role: RepairSource, State: RepairPending}
}

func commitRawForTest(store *Store, transaction Transaction) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.commitLocked(transaction)
}

func mustRecoverWork(t *testing.T, store *Store) RecoveredWork {
	t.Helper()
	work, err := store.RecoverWork()
	if err != nil {
		t.Fatal(err)
	}
	return work
}
