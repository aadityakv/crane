package store

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// outboxProofCounter instruments the expensive once-per-record outbox proof
// (TupleDelivery construction + MarshalTupleDelivery + assignment containment
// + deterministic route validation) for the tests in this file.
type outboxProofCounter struct {
	counts map[model.DeliveryID]int
	total  int
}

func observeOutboxProofs(t *testing.T) *outboxProofCounter {
	t.Helper()
	counter := &outboxProofCounter{counts: make(map[model.DeliveryID]int)}
	prior := outboxProofObserver
	outboxProofObserver = func(record OutboxRecord) {
		counter.counts[record.ID]++
		counter.total++
	}
	t.Cleanup(func() { outboxProofObserver = prior })
	return counter
}

// proofFixture installs one fenced running assignment whose source stage is
// local, so source advances create real topology-derived outboxes.
type proofFixture struct {
	store      *Store
	identity   Identity
	options    Options
	topology   model.ValidatedTopology
	assignment model.AssignmentSet
	epoch      model.CoordinatorEpoch
	source     model.AssignmentToken
	eof        uint64
}

func newProofFixture(t *testing.T, max uint64) *proofFixture {
	t.Helper()
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: max, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &proofFixture{store: store, identity: identity, options: options}
	t.Cleanup(func() { _ = store.Close() })
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 7, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID && token.WorkerEpoch == store.WorkerEpoch() && token.Task.StageID == 1 {
			fixture.source = token
		}
	}
	eof, err := model.SourceEOF(topology, fixture.source.Task)
	if err != nil {
		t.Fatal(err)
	}
	fixture.topology, fixture.assignment, fixture.epoch, fixture.eof = topology, assignment, epoch, eof
	return fixture
}

// advanceProofSource commits one source cursor advance that creates the
// topology-derived outbox for sequence.
func (fixture *proofFixture) advanceProofSource(t *testing.T, sequence uint64) model.DeliveryID {
	t.Helper()
	outbox := domainSourceOutbox(t, fixture.topology, fixture.assignment, fixture.epoch, fixture.source, sequence)
	if err := fixture.store.AdvanceSource(SourceCursor{Source: fixture.source.Task, NextSequence: sequence + 1, EOF: fixture.eof}, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	return outbox.ID
}

// TestOutboxProofRunsExactlyOncePerRecord pins the once-per-record outbox
// proof: a newly created outbox is proven exactly once at its creating commit,
// and later commits that touch other durable records never re-prove it.
func TestOutboxProofRunsExactlyOncePerRecord(t *testing.T) {
	fixture := newProofFixture(t, 8<<20)
	proofs := observeOutboxProofs(t)

	first := fixture.advanceProofSource(t, 1)
	if proofs.counts[first] != 1 {
		t.Fatalf("creating commit proved outbox %d times, want 1", proofs.counts[first])
	}

	second := fixture.advanceProofSource(t, 2)
	if proofs.counts[second] != 1 {
		t.Fatalf("second creating commit proved new outbox %d times, want 1", proofs.counts[second])
	}
	if proofs.counts[first] != 1 {
		t.Fatalf("commit touching other records re-proved unchanged outbox: %d proofs, want 1", proofs.counts[first])
	}

	record, provenance := domainResult(t, fixture.topology, fixture.assignment, fixture.epoch, 0)
	if err := fixture.store.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[first] != 1 || proofs.counts[second] != 1 {
		t.Fatalf("result upsert re-proved outboxes: first=%d second=%d, want 1 and 1", proofs.counts[first], proofs.counts[second])
	}
}

// TestProcessedOutboxProofRunsExactlyOnce pins the same once-per-record proof
// for outboxes created through the processed-delivery path.
func TestProcessedOutboxProofRunsExactlyOnce(t *testing.T) {
	fixture := newProofFixture(t, 8<<20)
	proofs := observeOutboxProofs(t)

	delivery := domainDelivery(t, fixture.topology, fixture.assignment, fixture.epoch)
	if _, err := fixture.store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	outputs, outboxes := exactProcessedRecords(t, fixture.topology, fixture.assignment, delivery)
	if err := fixture.store.MarkProcessed(delivery.ID, outputs, outboxes); err != nil {
		t.Fatal(err)
	}
	if len(outboxes) == 0 {
		t.Fatal("fixture derived no outboxes")
	}
	for _, outbox := range outboxes {
		if proofs.counts[outbox.ID] != 1 {
			t.Fatalf("processed creating commit proved outbox %d times, want 1", proofs.counts[outbox.ID])
		}
	}
	if err := fixture.store.MarkOutboxDispatched(outboxes[0].ID, 42); err != nil {
		t.Fatal(err)
	}
	for _, outbox := range outboxes {
		if proofs.counts[outbox.ID] != 1 {
			t.Fatalf("dispatch commit re-proved outbox: %d proofs, want 1", proofs.counts[outbox.ID])
		}
	}
}

// TestOutboxStateTransitionsDoNotReprove pins the mutation-path audit: the
// only mutations of an existing outbox are the retry/completion state fields
// (Accepted, RetryDeadlineUnixNano, Completed), none of which is an input of
// the proof, so dispatch/accept/complete commits never invalidate it.
func TestOutboxStateTransitionsDoNotReprove(t *testing.T) {
	fixture := newProofFixture(t, 8<<20)
	proofs := observeOutboxProofs(t)

	id := fixture.advanceProofSource(t, 1)
	if proofs.counts[id] != 1 {
		t.Fatalf("creating commit proved outbox %d times, want 1", proofs.counts[id])
	}
	if err := fixture.store.MarkOutboxDispatched(id, 1_000); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.MarkOutboxAccepted(id, 2_000); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.MarkOutboxCompleted(id); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[id] != 1 {
		t.Fatalf("state transitions re-proved outbox: %d proofs, want 1", proofs.counts[id])
	}
}

// TestCheckpointCompactionPrunesProvenOutboxes pins the proof-set pruning
// contract: checkpoint compaction drops covered outboxes from the durable set
// and deletes their proofs from the in-memory set, while surviving outboxes
// stay proven and no proof re-runs during or after compaction.
func TestCheckpointCompactionPrunesProvenOutboxes(t *testing.T) {
	fixture := newProofFixture(t, 16<<20)
	proofs := observeOutboxProofs(t)

	first := fixture.advanceProofSource(t, 1)
	second := fixture.advanceProofSource(t, 2)
	notice := model.CheckpointNotice{JobID: fixture.assignment.JobID, Source: fixture.source.Task, Watermark: 1, RaftIndex: 9, Epoch: fixture.epoch}
	if err := fixture.store.ApplyCheckpoint(notice); err != nil {
		t.Fatal(err)
	}
	work := mustRecoverWork(t, fixture.store)
	if len(work.Outboxes) != 1 || work.Outboxes[0].ID != second {
		t.Fatalf("compaction retained outboxes %+v, want only %v", work.Outboxes, second)
	}
	fixture.store.mu.Lock()
	proven, firstCached := len(fixture.store.validatedOutboxes), false
	if _, ok := fixture.store.validatedOutboxes[first]; ok {
		firstCached = true
	}
	_, secondCached := fixture.store.validatedOutboxes[second]
	fixture.store.mu.Unlock()
	if proven != 1 || firstCached || !secondCached {
		t.Fatalf("proof set after compaction: len=%d compacted=%t survivor=%t, want len=1 compacted=false survivor=true", proven, firstCached, secondCached)
	}
	if proofs.counts[first] != 1 || proofs.counts[second] != 1 {
		t.Fatalf("compaction re-proved outboxes: first=%d second=%d, want 1 and 1", proofs.counts[first], proofs.counts[second])
	}
	if err := fixture.store.MarkOutboxDispatched(second, 11_000); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[first] != 1 || proofs.counts[second] != 1 {
		t.Fatalf("post-compaction commit re-proved outboxes: first=%d second=%d, want 1 and 1", proofs.counts[first], proofs.counts[second])
	}
}

// TestSupersededAssignmentOutboxNotReprovenPerCommit pins the historical
// outbox branch: after the assignment revision advances past an outbox, later
// commits validate it through the cheap authority/token checks and the cached
// proof, never re-marshalling its TupleDelivery.
func TestSupersededAssignmentOutboxNotReprovenPerCommit(t *testing.T) {
	fixture := newProofFixture(t, 8<<20)
	proofs := observeOutboxProofs(t)

	id := fixture.advanceProofSource(t, 1)
	if proofs.counts[id] != 1 {
		t.Fatalf("creating commit proved outbox %d times, want 1", proofs.counts[id])
	}

	superseding, err := model.BuildAssignmentSet(fixture.assignment.JobID, fixture.topology.Digest(), fixture.assignment.Revision+1, fixture.topology, []model.WorkerPlacement{
		{NodeID: fixture.identity.NodeID, WorkerEpoch: fixture.store.WorkerEpoch(), SlotCapacity: 4},
		{NodeID: 2, WorkerEpoch: model.WorkerEpoch{8}, SlotCapacity: 4},
		{NodeID: 3, WorkerEpoch: model.WorkerEpoch{9}, SlotCapacity: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.InstallAssignment(superseding, fixture.topology.Spec(), 8, model.Running, fixture.epoch); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.MarkOutboxDispatched(id, 3_000); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[id] != 1 {
		t.Fatalf("commits under a superseded assignment re-proved the historical outbox: %d proofs, want 1", proofs.counts[id])
	}
}

// TestRecoveryReprovesAndRepopulatesOutboxProofs pins the recovery contract:
// reopening a store clears the in-memory proof set, recovery full-validates
// every recovered outbox again, and the WAL replay repopulates the set so the
// first post-recovery commit does not re-prove replayed outboxes.
func TestRecoveryReprovesAndRepopulatesOutboxProofs(t *testing.T) {
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	first, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignment(t, first.WorkerEpoch(), identity.NodeID)
	if err := first.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := first.InstallAssignment(assignment, topology.Spec(), 7, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID && token.WorkerEpoch == first.WorkerEpoch() && token.Task.StageID == 1 {
			source = token
		}
	}
	eof, err := model.SourceEOF(topology, source.Task)
	if err != nil {
		t.Fatal(err)
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	proofs := observeOutboxProofs(t)
	if err := first.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 2, EOF: eof}, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[outbox.ID] != 1 {
		t.Fatalf("creating commit proved outbox %d times, want 1", proofs.counts[outbox.ID])
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	recovered := proofs.counts[outbox.ID]
	if recovered == 1 {
		t.Fatal("recovery did not re-prove the recovered outbox")
	}
	next := domainSourceOutbox(t, topology, assignment, epoch, source, 2)
	if err := second.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 3, EOF: eof}, []OutboxRecord{next}); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[outbox.ID] != recovered {
		t.Fatalf("first post-recovery commit re-proved a replayed outbox: %d proofs after recovery, %d after commit, want equal", recovered, proofs.counts[outbox.ID])
	}
	if proofs.counts[next.ID] != 1 {
		t.Fatalf("first post-recovery commit proved new outbox %d times, want 1", proofs.counts[next.ID])
	}
	if err := second.MarkOutboxDispatched(next.ID, 7_000); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[next.ID] != 1 || proofs.counts[outbox.ID] != recovered {
		t.Fatalf("later commits re-proved outboxes: first=%d (want %d) second=%d (want 1)", proofs.counts[outbox.ID], recovered, proofs.counts[next.ID])
	}
}

// TestSnapshotRecoveryReprovesOutboxProofs pins the snapshot-generation
// recovery flavor: recovery re-proves snapshot-file outboxes (they re-enter
// the proof set only through the first commit's validation walk), and every
// later commit performs zero proofs for them.
func TestSnapshotRecoveryReprovesOutboxProofs(t *testing.T) {
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	first, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	topology, assignment, epoch := domainAssignment(t, first.WorkerEpoch(), identity.NodeID)
	if err := first.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := first.InstallAssignment(assignment, topology.Spec(), 7, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	var source model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.WorkerID == identity.NodeID && token.WorkerEpoch == first.WorkerEpoch() && token.Task.StageID == 1 {
			source = token
		}
	}
	eof, err := model.SourceEOF(topology, source.Task)
	if err != nil {
		t.Fatal(err)
	}
	outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
	proofs := observeOutboxProofs(t)
	if err := first.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 2, EOF: eof}, []OutboxRecord{outbox}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	recovered := proofs.counts[outbox.ID]
	if recovered == 1 {
		t.Fatal("snapshot recovery did not re-prove the recovered outbox")
	}

	// The snapshot-file outbox re-enters the proof set through exactly one
	// commit-time proof; every later commit is proof-free for it.
	if err := second.MarkOutboxDispatched(outbox.ID, 8_000); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[outbox.ID] != recovered+1 {
		t.Fatalf("first post-snapshot commit proved outbox %d additional times, want 1", proofs.counts[outbox.ID]-recovered)
	}
	if err := second.MarkOutboxAccepted(outbox.ID, 9_000); err != nil {
		t.Fatal(err)
	}
	if proofs.counts[outbox.ID] != recovered+1 {
		t.Fatalf("later commits re-proved the snapshot-recovered outbox: %d proofs, want %d", proofs.counts[outbox.ID], recovered+1)
	}
}
