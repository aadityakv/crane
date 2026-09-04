package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// TestCheckpointAdoptsCommittedWatermarkWithoutPendingReport pins the Task 24
// defect #2 ruling at the durable-store boundary: a checkpoint notice from the
// current fence whose watermark strictly exceeds the durable cursor (or that
// arrives with no cursor at all) is the coordinator's authoritative statement
// of the replicated committed watermark, so the store adopts it under the
// current authority proof without requiring any local CompletionReport and
// then applies the existing compaction rules.
func TestCheckpointAdoptsCommittedWatermarkWithoutPendingReport(t *testing.T) {
	setup := func(t *testing.T) (*Store, model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch, model.AssignmentToken) {
		t.Helper()
		store, identity, _ := openDomainStore(t, 16<<20)
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
		if source == (model.AssignmentToken{}) {
			t.Fatal("fixture requires a local source token")
		}
		return store, topology, assignment, epoch, source
	}
	localSource := func(t *testing.T, assignment model.AssignmentSet, node uint16) model.AssignmentToken {
		t.Helper()
		for _, token := range assignment.Tasks {
			if token.Task.StageID == 1 && token.WorkerID == node {
				return token
			}
		}
		t.Fatal("missing local source token")
		return model.AssignmentToken{}
	}

	t.Run("adopt above durable cursor without completion event", func(t *testing.T) {
		store, topology, assignment, epoch, source := setup(t)
		outbox := domainSourceOutbox(t, topology, assignment, epoch, source, 1)
		if err := store.AdvanceSource(SourceCursor{Source: source.Task, NextSequence: 2, EOF: 3}, []OutboxRecord{outbox}); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkOutboxCompleted(outbox.ID); err != nil {
			t.Fatal(err)
		}
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 2, RaftIndex: 9, Epoch: epoch}
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatalf("committed-watermark adoption above the cursor: %v", err)
		}
		work := mustRecoverWork(t, store)
		if len(work.Sources) != 1 {
			t.Fatalf("sources = %+v", work.Sources)
		}
		cursor := work.Sources[0]
		wantAuthority := CheckpointAuthority{JobControlRevision: 1, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceToken: source, CoordinatorEpoch: epoch}
		if cursor.Watermark != 2 || cursor.RaftIndex != 9 || cursor.CheckpointRevision != 1 || cursor.CheckpointAuthority != wantAuthority {
			t.Fatalf("adopted cursor = %+v want authority %+v", cursor, wantAuthority)
		}
		if cursor.NextSequence != 3 || cursor.EOF != 3 {
			t.Fatalf("adopted cursor must resume above the committed watermark: %+v", cursor)
		}
		if len(work.Outboxes) != 0 {
			t.Fatalf("adoption must compact covered outboxes: %+v", work.Outboxes)
		}
	})

	t.Run("adopt with absent cursor for a reassigned owner", func(t *testing.T) {
		store, _, assignment, epoch, source := setup(t)
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 2, RaftIndex: 12, Epoch: epoch}
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatalf("absent-cursor adoption: %v", err)
		}
		work := mustRecoverWork(t, store)
		if len(work.Sources) != 1 || work.Sources[0].Watermark != 2 || work.Sources[0].RaftIndex != 12 || work.Sources[0].CheckpointRevision != 1 || work.Sources[0].NextSequence != 3 || work.Sources[0].EOF != 3 {
			t.Fatalf("adopted absent cursor = %+v", work.Sources)
		}
		if work.Sources[0].CheckpointAuthority != (CheckpointAuthority{JobControlRevision: 1, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceToken: source, CoordinatorEpoch: epoch}) {
			t.Fatalf("adopted authority = %+v", work.Sources[0].CheckpointAuthority)
		}
	})

	t.Run("adoption survives close and reopen", func(t *testing.T) {
		directory := t.TempDir() + "/worker"
		identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
		options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
		store, err := Open(directory, identity, options)
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
		source := localSource(t, assignment, identity.NodeID)
		notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 2, RaftIndex: 9, Epoch: epoch}
		if err := store.ApplyCheckpoint(notice); err != nil {
			t.Fatalf("adoption before reopen: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(directory, identity, options)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		work := mustRecoverWork(t, reopened)
		if len(work.Sources) != 1 || work.Sources[0].Watermark != 2 || work.Sources[0].CheckpointRevision != 1 || work.Sources[0].CheckpointAuthority.SourceToken != source {
			t.Fatalf("reopened adopted cursor = %+v", work.Sources)
		}
	})

	t.Run("adoption never weakens existing validation", func(t *testing.T) {
		t.Run("watermark above the installed EOF", func(t *testing.T) {
			store, _, assignment, epoch, source := setup(t)
			before := mustRecoverWork(t, store)
			notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 4, RaftIndex: 9, Epoch: epoch}
			if err := store.ApplyCheckpoint(notice); err == nil {
				t.Fatal("watermark beyond the topology EOF adopted")
			}
			if after := mustRecoverWork(t, store); !equalAdoptableWork(after, before) {
				t.Fatal("rejected out-of-bounds adoption mutated durable work")
			}
		})
		t.Run("stale coordinator fence", func(t *testing.T) {
			store, _, assignment, epoch, source := setup(t)
			before := mustRecoverWork(t, store)
			stale := epoch
			stale.Term--
			notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 2, RaftIndex: 9, Epoch: stale}
			if err := store.ApplyCheckpoint(notice); err == nil {
				t.Fatal("notice from a stale fence adopted")
			}
			if after := mustRecoverWork(t, store); !equalAdoptableWork(after, before) {
				t.Fatal("rejected stale-fence adoption mutated durable work")
			}
		})
		t.Run("raft index regression against the durable cursor", func(t *testing.T) {
			store, _, assignment, epoch, source := setup(t)
			first := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 2, RaftIndex: 9, Epoch: epoch}
			if err := store.ApplyCheckpoint(first); err != nil {
				t.Fatal(err)
			}
			before := mustRecoverWork(t, store)
			regressed := first
			regressed.Watermark = 3
			regressed.RaftIndex = 8
			if err := store.ApplyCheckpoint(regressed); err == nil {
				t.Fatal("raft-index regression adopted")
			}
			if after := mustRecoverWork(t, store); !equalAdoptableWork(after, before) {
				t.Fatal("rejected regression mutated durable work")
			}
		})
		t.Run("zero watermark advance", func(t *testing.T) {
			store, _, assignment, epoch, source := setup(t)
			before := mustRecoverWork(t, store)
			notice := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 0, RaftIndex: 9, Epoch: epoch}
			if err := store.ApplyCheckpoint(notice); err == nil {
				t.Fatal("zero-watermark advance adopted")
			}
			if after := mustRecoverWork(t, store); !equalAdoptableWork(after, before) {
				t.Fatal("rejected zero-watermark advance mutated durable work")
			}
		})
		t.Run("same raft index with a higher watermark", func(t *testing.T) {
			store, _, assignment, epoch, source := setup(t)
			first := model.CheckpointNotice{JobID: assignment.JobID, Source: source.Task, Watermark: 1, RaftIndex: 9, Epoch: epoch}
			if err := store.ApplyCheckpoint(first); err != nil {
				t.Fatal(err)
			}
			before := mustRecoverWork(t, store)
			changed := first
			changed.Watermark = 2
			if err := store.ApplyCheckpoint(changed); !errors.Is(err, model.ErrIdentityReuse) {
				t.Fatalf("same-index higher-watermark adoption = %v", err)
			}
			if after := mustRecoverWork(t, store); !equalAdoptableWork(after, before) {
				t.Fatal("rejected identity reuse mutated durable work")
			}
		})
	})
}

// equalAdoptableWork reports full recovered-work equality: adoption
// rejections must never mutate any durable structure.
func equalAdoptableWork(left, right RecoveredWork) bool {
	return reflect.DeepEqual(left, right)
}
