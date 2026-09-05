package store

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/crane/model"
)

// TestRecoverWorkViewWithin pins the borrowed control read: the callback
// observes live state under the same bounded-lock discipline as
// RecoverWorkWithin, without paying the full-store Clone.
func TestRecoverWorkViewWithin(t *testing.T) {
	t.Run("borrow observes live state without cloning", func(t *testing.T) {
		s, _, identity, _ := rebindStoreForTest(t)
		_, assignment, _, newer, delivery := seedRebindWorkForTest(t, s, identity, 1)
		clones := 0
		prior := recoveredWorkCloneObserver
		recoveredWorkCloneObserver = func() { clones++ }
		t.Cleanup(func() { recoveredWorkCloneObserver = prior })
		var observedFence model.CoordinatorEpoch
		var observedAssignments, observedDeliveries int
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error {
			observedFence = work.Fence
			observedAssignments = len(work.Assignments)
			observedDeliveries = len(work.Deliveries)
			if work.Fence != newer {
				t.Fatalf("borrowed fence = %+v, want %+v", work.Fence, newer)
			}
			if len(work.Assignments) != 1 || work.Assignments[0].Assignment.JobID != assignment.JobID {
				t.Fatalf("borrowed assignments = %+v", work.Assignments)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if observedFence != newer || observedAssignments != 1 || observedDeliveries != 1 || delivery.ID == (model.DeliveryID{}) {
			t.Fatalf("borrowed fence=%+v assignments=%d deliveries=%d", observedFence, observedAssignments, observedDeliveries)
		}
		if clones != 0 {
			t.Fatalf("borrowed read cloned recovered work %d times, want 0", clones)
		}
		if _, err := s.RecoverWorkWithin(30 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if clones != 1 {
			t.Fatalf("clone observer counted %d after one bounded clone, want 1", clones)
		}
	})

	t.Run("held store answers ErrBusy within the wait", func(t *testing.T) {
		s, _, _, _ := rebindStoreForTest(t)
		s.mu.Lock()
		started := time.Now()
		err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error { return nil })
		elapsed := time.Since(started)
		s.mu.Unlock()
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("held store borrowed read err = %v, want ErrBusy", err)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("borrowed read blocked %s past its 30ms wait", elapsed)
		}
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error { return nil }); err != nil {
			t.Fatalf("released store borrowed read err = %v", err)
		}
		if err := s.RecoverWorkViewWithin(0, func(work *RecoveredWork) error { return nil }); err != nil {
			t.Fatalf("zero wait must behave like the unbounded read: %v", err)
		}
	})

	t.Run("nil callback is rejected", func(t *testing.T) {
		s, _, _, _ := rebindStoreForTest(t)
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, nil); err == nil {
			t.Fatal("nil borrow callback accepted")
		}
	})

	t.Run("callback error and failure surfaces", func(t *testing.T) {
		s, _, _, _ := rebindStoreForTest(t)
		refused := errors.New("refused")
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error { return refused }); !errors.Is(err, refused) {
			t.Fatalf("callback error = %v, want %v", err, refused)
		}
		s.mu.Lock()
		s.failed = true
		s.mu.Unlock()
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error { return nil }); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("failed store borrowed read err = %v, want ErrUnavailable", err)
		}
	})

	t.Run("retained copies are immune to later commits", func(t *testing.T) {
		s, _, identity, _ := rebindStoreForTest(t)
		_, _, _, newer, _ := seedRebindWorkForTest(t, s, identity, 1)
		var borrowedFence model.CoordinatorEpoch
		if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error {
			borrowedFence = work.Fence
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		superseded := newer
		superseded.Term++
		superseded.BeginIndex++
		superseded.Nonce[0]++
		if err := s.Fence(superseded); err != nil {
			t.Fatal(err)
		}
		if borrowedFence != newer || borrowedFence == superseded {
			t.Fatalf("copied-out fence changed after later commit: %+v", borrowedFence)
		}
	})
}

// TestRecoverWorkViewWithinMirrorsCloneExceptResults pins the borrowed-view
// contract: every recovered-work field the control sessions read mirrors the
// cloned view exactly, EXCEPT Results — the live internal state keeps logical
// records solely in the search indexes (Clone materializes them, the borrowed
// view does not), so borrowed readers must not touch Results.
func TestRecoverWorkViewWithinMirrorsCloneExceptResults(t *testing.T) {
	s, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, _, newer, _ := seedRebindWorkForTest(t, s, identity, 1)
	record, provenance := domainResult(t, topology, assignment, newer, 0)
	if err := s.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}
	clone, err := s.RecoverWorkWithin(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(clone.Results) != 1 {
		t.Fatalf("seeded clone results = %d, want 1", len(clone.Results))
	}
	var mismatch string
	if err := s.RecoverWorkViewWithin(30*time.Millisecond, func(work *RecoveredWork) error {
		// mirrorEqual treats a nil slice and its empty clone the same: Clone
		// materializes empty non-nil slices where the live view keeps nil.
		switch {
		case work.Fence != clone.Fence:
			mismatch = "Fence"
		case !mirrorEqual(work.Assignments, clone.Assignments):
			mismatch = "Assignments"
		case !mirrorEqual(work.Sources, clone.Sources):
			mismatch = "Sources"
		case !mirrorEqual(work.Checkpoints, clone.Checkpoints):
			mismatch = "Checkpoints"
		case !mirrorEqual(work.Deliveries, clone.Deliveries):
			mismatch = "Deliveries"
		case !mirrorEqual(work.Outboxes, clone.Outboxes):
			mismatch = "Outboxes"
		case !mirrorEqual(work.Repairs, clone.Repairs):
			mismatch = "Repairs"
		case !mirrorEqual(work.PendingEvents, clone.PendingEvents):
			mismatch = "PendingEvents"
		case work.NextTransactionID != clone.NextTransactionID:
			mismatch = "NextTransactionID"
		case len(work.Results) != 0:
			// The live internal state keeps logical records solely in the
			// search indexes: Clone materializes Results, the borrowed view
			// must not (that is the one field borrowed readers may not read).
			mismatch = "Results"
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if mismatch != "" {
		t.Fatalf("borrowed view diverged from clone at %s", mismatch)
	}
}

func mirrorEqual[T any](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !reflect.DeepEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

// TestDurableSequenceWithin pins the cheap durable-sequence read: it reports
// the last durable WAL record sequence without cloning recovered work, under
// the same bounded-lock discipline.
func TestDurableSequenceWithin(t *testing.T) {
	s, _, identity, _ := rebindStoreForTest(t)
	seedRebindWorkForTest(t, s, identity, 1)
	clones := 0
	prior := recoveredWorkCloneObserver
	recoveredWorkCloneObserver = func() { clones++ }
	t.Cleanup(func() { recoveredWorkCloneObserver = prior })
	sequence, err := s.DurableSequenceWithin(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != s.Recovered().LastSequence || sequence == 0 {
		t.Fatalf("durable sequence = %d, want %d", sequence, s.Recovered().LastSequence)
	}
	if clones != 0 {
		t.Fatalf("durable sequence read cloned recovered work %d times, want 0", clones)
	}
	s.mu.Lock()
	_, err = s.DurableSequenceWithin(30 * time.Millisecond)
	s.mu.Unlock()
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("held store durable sequence err = %v, want ErrBusy", err)
	}
	if sequence, err = s.DurableSequenceWithin(0); err != nil || sequence == 0 {
		t.Fatalf("zero wait durable sequence = %d, err = %v", sequence, err)
	}
}
