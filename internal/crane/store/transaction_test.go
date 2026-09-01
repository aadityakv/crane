package store

import (
	"bytes"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
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
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && token.WorkerID == identity.NodeID {
			source = token
			break
		}
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
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

func TestTransactionReviewCompactedDefinitionAndFinalEOFRetirement(t *testing.T) {
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
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: 1, RaftIndex: 10, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}
	if state, err := store.Receive(delivery); err != nil || state != Compacted {
		t.Fatalf("exact compact retry = %v,%v", state, err)
	}
	changed := delivery.Clone()
	changed.Tuple.Fields[0].Value.Int64++
	if _, err := store.Receive(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed compact retry = %v", err)
	}
	if got := mustRecoverWork(t, store); len(got.Deliveries) != 1 || got.Deliveries[0].State != Compacted {
		t.Fatalf("pre-EOF tombstones = %+v", got.Deliveries)
	}
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: 3, RaftIndex: 11, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}
	if got := mustRecoverWork(t, store); len(got.Deliveries) != 0 {
		t.Fatalf("final-EOF tombstones retained = %d", len(got.Deliveries))
	}
	if state, err := store.Receive(delivery); err != nil || state != Compacted {
		t.Fatalf("final checkpoint late retry = %v,%v", state, err)
	}
	if _, err := store.Receive(changed); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed late retry after tombstone retirement = %v", err)
	}
}

func TestTransactionReviewProcessedRequiresExactDerivedOutboxes(t *testing.T) {
	newStore := func(t *testing.T) (*Store, Identity, model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch, DeliveryRecord) {
		t.Helper()
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
		return store, identity, topology, assignment, epoch, delivery
	}
	t.Run("missing", func(t *testing.T) {
		store, _, _, _, _, delivery := newStore(t)
		outputs := []model.Tuple{{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 6}}}}}
		if err := store.MarkProcessed(delivery.ID, outputs, nil); err == nil {
			t.Fatal("missing derived outbox accepted")
		}
		if mustRecoverWork(t, store).Deliveries[0].State != Received {
			t.Fatal("missing outbox mutated delivery")
		}
	})
	t.Run("completed initial", func(t *testing.T) {
		store, _, topology, assignment, _, delivery := newStore(t)
		output := model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 6}}}}
		outbox := domainOutbox(t, delivery, output, assignment, topology)
		outbox.Completed = true
		if err := store.MarkProcessed(delivery.ID, []model.Tuple{output}, []OutboxRecord{outbox}); err == nil {
			t.Fatal("pre-completed outbox accepted")
		}
	})
	t.Run("extra and duplicate", func(t *testing.T) {
		store, _, topology, assignment, _, delivery := newStore(t)
		output := model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 6}}}}
		outbox := domainOutbox(t, delivery, output, assignment, topology)
		extra := outbox.Clone()
		extra.ID.Tuple = model.DeriveChildTupleID(delivery.ID.Tuple, delivery.Destination.Task, extra.ID.EdgeID, 1)
		if err := store.MarkProcessed(delivery.ID, []model.Tuple{output}, []OutboxRecord{outbox, extra}); err == nil {
			t.Fatal("extra topology outbox accepted")
		}
		if err := store.MarkProcessed(delivery.ID, []model.Tuple{output}, []OutboxRecord{outbox, outbox}); err == nil {
			t.Fatal("duplicate topology outbox accepted")
		}
	})
	t.Run("raw registered", func(t *testing.T) {
		store, _, _, _, _, delivery := newStore(t)
		delivery.State = Processed
		delivery.Outputs = []model.Tuple{{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 6}}}}}
		payload, err := encodeDeliveryRecord(delivery, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Commit(Transaction{Records: []Record{{Type: recordDeliveryProcessed, Payload: payload}}}); !errors.Is(err, ErrInvalidTransaction) {
			t.Fatalf("raw incomplete processed record = %v", err)
		}
	})
}

func TestTransactionReviewSourceRetryAndCrossReferences(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && token.WorkerID == identity.NodeID {
			source = token
			break
		}
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	cursor := SourceCursor{Source: source.Task, NextSequence: 2, EOF: 3}
	if err := store.AdvanceSource(cursor, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceSource(cursor, []OutboxRecord{outbox}); err != nil {
		t.Fatalf("exact source retry = %v", err)
	}
	changed := outbox.Clone()
	changed.Tuple.Fields[0].Value.Int64++
	if err := store.AdvanceSource(cursor, []OutboxRecord{changed}); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed source retry = %v", err)
	}
	regressed := cursor
	regressed.NextSequence = 1
	if err := store.AdvanceSource(regressed, nil); err == nil {
		t.Fatal("source sequence regression accepted")
	}
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3}, []OutboxRecord{outbox, outbox}); err == nil {
		t.Fatal("within-transaction duplicate outboxes accepted")
	}
	precompleted := domainSourceOutbox(t, topology, assignment, epoch, source, 2)
	precompleted.Completed = true
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3}, []OutboxRecord{precompleted}); err == nil {
		t.Fatal("pre-completed source outbox accepted")
	}
	wrong := domainSourceOutbox(t, topology, assignment, epoch, source, 2)
	wrong.Producer = assignment.Tasks[1]
	payload, err := encodeSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3}, []OutboxRecord{wrong})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(Transaction{Records: []Record{{Type: recordSource, Payload: payload}}}); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("raw wrong-source record = %v", err)
	}
	if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 1, RaftIndex: 9, Epoch: epoch}); err != nil {
		t.Fatal(err)
	}
	second := domainSourceOutbox(t, topology, assignment, epoch, source, 2)
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3, Watermark: 0, RaftIndex: 9}, []OutboxRecord{second}); err == nil {
		t.Fatal("source watermark regression accepted")
	}
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3, Watermark: 1, RaftIndex: 8}, []OutboxRecord{second}); err == nil {
		t.Fatal("source raft-index regression accepted")
	}
	if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: 3, Watermark: 1, RaftIndex: 9}, []OutboxRecord{second}); err != nil {
		t.Fatalf("monotonic source advance = %v", err)
	}
}

func TestTransactionReviewCapacityUsesProspectiveReservations(t *testing.T) {
	setup := func(t *testing.T) (*Store, model.AssignmentSet, model.CoordinatorEpoch, DeliveryRecord, Transaction) {
		t.Helper()
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
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: delivery.ID.Tuple.SourceTask, Watermark: 1, RaftIndex: 8, Epoch: epoch}
		payload, err := encodeCheckpoint(notice)
		if err != nil {
			t.Fatal(err)
		}
		return store, assignment, epoch, delivery, Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}}
	}
	t.Run("release admits", func(t *testing.T) {
		store, assignment, epoch, _, tx := setup(t)
		size, err := transactionEncodedSize(tx)
		if err != nil {
			t.Fatal(err)
		}
		store.options.MaxBytes = store.state.WALBytes + size
		if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: assignment.Tasks[0].Task, Watermark: 1, RaftIndex: 8, Epoch: epoch}); err != nil {
			t.Fatalf("prospective release checkpoint = %v", err)
		}
	})
	t.Run("true rejection", func(t *testing.T) {
		store, assignment, epoch, _, tx := setup(t)
		size, err := transactionEncodedSize(tx)
		if err != nil {
			t.Fatal(err)
		}
		fault := &testFault{point: FaultBeforeAppend, err: errors.New("must not run")}
		store.options.Faults = fault
		store.options.MaxBytes = store.state.WALBytes + size - 1
		before := mustRecoverWork(t, store)
		if err := store.ApplyCheckpoint(model.CheckpointNotice{JobID: assignment.JobID, Source: assignment.Tasks[0].Task, Watermark: 1, RaftIndex: 8, Epoch: epoch}); !errors.Is(err, ErrCapacity) {
			t.Fatalf("capacity = %v", err)
		}
		if fault.calls != 0 || !reflect.DeepEqual(before, mustRecoverWork(t, store)) {
			t.Fatal("true rejection injected fault or mutated")
		}
	})
}

func TestTransactionReviewResultKeyIncludesSinkTask(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	first, p0 := domainResult(t, topology, assignment, epoch, 0)
	_, p1 := domainResult(t, topology, assignment, epoch, 1)
	value, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 7}}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := model.NewResultRecord(first.TupleID, assignment.ResultReplicas[1].SinkTask, topology.Digest(), value)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResult(first, p0); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResult(second, p1); err != nil {
		t.Fatalf("same tuple in distinct sink key = %v", err)
	}
	changedValue, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: 8}}}})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := model.NewResultRecord(first.TupleID, second.SinkTask, topology.Digest(), changedValue)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertResult(changed, p1); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("same complete key changed bytes = %v", err)
	}
	results := mustRecoverWork(t, store).Results
	if len(results) != 2 || !taskLess(results[0].Record.SinkTask, results[1].Record.SinkTask) {
		t.Fatalf("result buckets = %+v", results)
	}
}

func TestTransactionReviewRepairRequiresCurrentReplicaAuthorizationAndBounds(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	valid := domainRepair(t, topology, assignment, epoch, identity.NodeID, store.WorkerEpoch())
	if err := store.UpsertRepair(valid); err != nil {
		t.Fatalf("valid repair = %v", err)
	}
	cases := map[string]func(*ResultRepairRecord){
		"assignment digest": func(r *ResultRepairRecord) { r.Instruction.AssignmentDigest[0] ^= 1 },
		"specification":     func(r *ResultRepairRecord) { r.Instruction.SpecificationHash[0] ^= 1 },
		"sink":              func(r *ResultRepairRecord) { r.Instruction.SinkTask.StageID = 2 },
		"stale fence":       func(r *ResultRepairRecord) { r.Instruction.CoordinatorEpoch.Term-- },
		"stale worker epoch": func(r *ResultRepairRecord) {
			r.Instruction.SourceWorkerEpoch[0] ^= 1
		},
		"wrong local role": func(r *ResultRepairRecord) {
			if r.Role == RepairSource {
				r.Role = RepairDestination
			} else {
				r.Role = RepairSource
			}
		},
		"remote endpoint": func(r *ResultRepairRecord) {
			if r.Instruction.DestinationNodeID == 2 {
				r.Instruction.DestinationNodeID = 3
			} else {
				r.Instruction.DestinationNodeID = 2
			}
			r.Instruction.DestinationWorkerEpoch = model.WorkerEpoch{99}
		},
		"noncanonical checkpoints": func(r *ResultRepairRecord) {
			source := assignment.Tasks[0].Task
			r.Instruction.Checkpoints = []model.SourceCheckpoint{{Source: source, Watermark: 1}, {Source: source, Watermark: 1}}
		},
		"zero nonempty digest": func(r *ResultRepairRecord) { r.Instruction.ExpectedContentDigest = [32]byte{} },
		"oversized total": func(r *ResultRepairRecord) {
			r.Instruction.ExpectedRecordCount = 1
			r.Instruction.ExpectedTotalBytes = (64 << 20) + 1
			r.Instruction.ExpectedContentDigest = [32]byte{2}
		},
		"impossible aggregate": func(r *ResultRepairRecord) {
			r.Instruction.ExpectedRecordCount = 1
			r.Instruction.ExpectedTotalBytes = 1
			r.Instruction.ExpectedContentDigest = [32]byte{2}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := cloneRepair(valid)
			mutate(&candidate)
			rebindRepair(&candidate)
			if err := store.UpsertRepair(candidate); err == nil {
				t.Fatal("unauthorized/impossible repair accepted")
			}
		})
	}
}

func TestTransactionReviewDecodersBoundDeclaredCollectionsBeforeAllocation(t *testing.T) {
	store, identity, _ := openDomainStore(t, 8<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	delivery.State = Processed
	delivery.Outputs = make([]model.Tuple, model.LimitsV1().MaxOperatorOutputs+1)
	if _, err := encodeDeliveryRecord(delivery, nil); err == nil {
		t.Fatal("external excess outputs reached encoding")
	}
	message := protocol.TupleDelivery{DeliveryID: delivery.ID, Tuple: delivery.Tuple, Producer: delivery.Producer, Destination: delivery.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: delivery.ID.Tuple.JobID, Revision: delivery.AssignmentRevision, Digest: delivery.AssignmentDigest}, Coordinator: delivery.CoordinatorEpoch}
	encodedDelivery, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	deliveryWriter := newRecordWriter()
	deliveryWriter.u16(domainRecordSchema)
	deliveryWriter.u8(uint8(Processed))
	deliveryWriter.u64(delivery.Reservation)
	deliveryWriter.blob(encodedDelivery)
	deliveryWriter.u16(uint16(model.LimitsV1().MaxOperatorOutputs + 1))
	malformedDelivery := deliveryWriter.bytes()
	deliveryAllocated := allocatedBytesForTest(func() { _, _, _ = decodeDeliveryRecord(malformedDelivery) }, 8)
	if deliveryAllocated > 2<<20 {
		t.Fatalf("declared output count allocated %d bytes before validation", deliveryAllocated)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 && token.WorkerID == identity.NodeID {
			source = token
			break
		}
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.task(source.Task)
	w.u64(2)
	w.u64(3)
	w.u64(0)
	w.u64(0)
	w.u16(uint16(model.LimitsV1().MaxDerivedDeliveries))
	malformed := w.bytes()
	allocated := allocatedBytesForTest(func() { _, _, _ = decodeSource(malformed) }, 8)
	if allocated > 2<<20 {
		t.Fatalf("declared outbox count allocated %d bytes before minimum-byte validation", allocated)
	}
}

func TestTransactionReviewProspectiveStateCopiesOnlyAffectedCollections(t *testing.T) {
	store, identity, _ := openDomainStore(t, 32<<20)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 128; sequence++ {
		record, provenance := domainResultSequence(t, topology, assignment, epoch, 0, sequence)
		if err := store.UpsertResult(record, provenance); err != nil {
			t.Fatal(err)
		}
	}
	beforeRoot := store.work.indexes.results
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if store.work.indexes.results != beforeRoot {
		t.Fatal("unaffected result index was copied by a fence transaction")
	}
	record, provenance := domainResultSequence(t, topology, assignment, epoch, 0, 129)
	if err := store.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	if shared := sharedResultNodeCount(beforeRoot, store.work.indexes.results); shared < 100 {
		t.Fatalf("result insertion rebuilt unrelated state: shared nodes = %d", shared)
	}
	large := store.work
	small := RecoveredWork{Fence: epoch, NextTransactionID: 1}
	measure := func(work RecoveredWork) float64 {
		return testing.AllocsPerRun(20, func() {
			reducer := &workReducer{current: work}
			if err := reducer.BeginTransaction(1); err != nil {
				panic(err)
			}
			payload, _ := encodeFence(epoch)
			if err := reducer.ConsumeRecord(Record{Type: recordFence, Payload: payload}); err != nil {
				panic(err)
			}
			if err := reducer.CommitTransaction(); err != nil {
				panic(err)
			}
		})
	}
	smallAllocs, largeAllocs := measure(small), measure(large)
	if largeAllocs > smallAllocs+5 {
		t.Fatalf("fence allocations scale with unrelated results: small=%f large=%f", smallAllocs, largeAllocs)
	}
}

func sharedResultNodeCount(left, right *resultNode) int {
	seen := make(map[*resultNode]struct{})
	var collect func(*resultNode)
	collect = func(node *resultNode) {
		if node == nil {
			return
		}
		seen[node] = struct{}{}
		collect(node.left)
		collect(node.right)
	}
	collect(left)
	count := 0
	var visit func(*resultNode)
	visit = func(node *resultNode) {
		if node == nil {
			return
		}
		if _, ok := seen[node]; ok {
			count++
		}
		visit(node.left)
		visit(node.right)
	}
	visit(right)
	return count
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
	tuple, exists, err := model.SourceTuple(topology, producer.Task, 1)
	if err != nil || !exists {
		t.Fatalf("SourceTuple = %+v,%v,%v", tuple, exists, err)
	}
	tupleID := model.DeriveSourceTupleID(assignment.JobID, producer.Task, 1)
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
	child.Tuple = model.DeriveChildTupleID(parent.ID.Tuple, parent.Destination.Task, child.EdgeID, 0)
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

func domainResultSequence(t *testing.T, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, partition uint16, sequence uint64) (model.ResultRecord, model.ResultCopyProvenance) {
	t.Helper()
	record, provenance := domainResult(t, topology, assignment, epoch, partition)
	record.TupleID.SourceSequence = sequence
	record.TupleID.PathDigest[0] = byte(sequence)
	value, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: int64(sequence)}}}})
	if err != nil {
		t.Fatal(err)
	}
	record, err = model.NewResultRecord(record.TupleID, record.SinkTask, record.SpecificationHash, value)
	if err != nil {
		t.Fatal(err)
	}
	return record, provenance
}

func domainSourceOutbox(t *testing.T, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, source model.AssignmentToken, sequence uint64) OutboxRecord {
	t.Helper()
	tuple, ok, err := model.SourceTuple(topology, source.Task, sequence)
	if err != nil || !ok {
		t.Fatalf("SourceTuple = %+v,%v,%v", tuple, ok, err)
	}
	tupleID := model.DeriveSourceTupleID(assignment.JobID, source.Task, sequence)
	edge := topology.Spec().Edges[0]
	partitions, err := model.Route(topology, edge, tupleID, tuple)
	if err != nil {
		t.Fatal(err)
	}
	destinationTask := model.TaskID{JobID: assignment.JobID, StageID: edge.DestinationStageID, Partition: partitions[0]}
	var destination model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task == destinationTask {
			destination = token
			break
		}
	}
	return OutboxRecord{ID: model.DeliveryID{Tuple: tupleID, EdgeID: edge.EdgeID, DestinationTask: destinationTask}, Tuple: tuple, Producer: source, Destination: destination, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, CoordinatorEpoch: epoch}
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
	replica := assignment.ResultReplicas[0]
	sourceNode, sourceEpoch, destinationNode, destinationEpoch := replica.PrimaryNodeID, replica.PrimaryEpoch, replica.SecondaryNodeID, replica.SecondaryEpoch
	role := RepairSource
	if sourceNode != node || sourceEpoch != worker {
		sourceNode, sourceEpoch, destinationNode, destinationEpoch = replica.SecondaryNodeID, replica.SecondaryEpoch, replica.PrimaryNodeID, replica.PrimaryEpoch
	}
	def := model.RepairResultPartitionDefinition{CoordinatorEpoch: epoch, JobID: assignment.JobID, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceNodeID: sourceNode, SourceWorkerEpoch: sourceEpoch, DestinationNodeID: destinationNode, DestinationWorkerEpoch: destinationEpoch, SinkTask: replica.SinkTask, SpecificationHash: topology.Digest(), ExpectedRecordCount: 4, ExpectedTotalBytes: 1000, ExpectedContentDigest: [32]byte{1}}
	def.CheckpointDigest = model.CheckpointVectorDigest(nil)
	def.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: def.JobID, SinkTask: def.SinkTask, SpecificationHash: def.SpecificationHash, AssignmentRevision: def.AssignmentRevision, AssignmentDigest: def.AssignmentDigest, Checkpoints: def.Checkpoints, CheckpointDigest: def.CheckpointDigest})
	def.RepairID = model.DeriveRepairID(def)
	return ResultRepairRecord{Instruction: def, InstructionDigest: model.RepairInstructionDigest(def), Role: role, State: RepairPending}
}

func rebindRepair(repair *ResultRepairRecord) {
	repair.Instruction.CheckpointDigest = model.CheckpointVectorDigest(repair.Instruction.Checkpoints)
	repair.Instruction.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: repair.Instruction.JobID, SinkTask: repair.Instruction.SinkTask, SpecificationHash: repair.Instruction.SpecificationHash, AssignmentRevision: repair.Instruction.AssignmentRevision, AssignmentDigest: repair.Instruction.AssignmentDigest, Checkpoints: repair.Instruction.Checkpoints, CheckpointDigest: repair.Instruction.CheckpointDigest})
	repair.Instruction.RepairID = model.DeriveRepairID(repair.Instruction)
	repair.InstructionDigest = model.RepairInstructionDigest(repair.Instruction)
}

func allocatedBytesForTest(operation func(), repetitions int) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < repetitions; i++ {
		operation()
	}
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
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
