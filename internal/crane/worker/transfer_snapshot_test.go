package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/store"
)

// resetTransferCounters zeroes the repository's observation counters after
// owner construction so a test measures only the traffic it drives itself.
func resetTransferCounters(repository *transferRepository) {
	repository.mu.Lock()
	repository.recoverCalls = 0
	repository.viewCalls = 0
	repository.mu.Unlock()
}

// TestReceiveResultRecordValidatesFromInstalledViewWithoutRecovery pins the
// transfer path's read contract: validating one current-replication result
// chunk reads the repository's immutable installed view exactly once and
// performs zero full RecoverWork deep clones.
func TestReceiveResultRecordValidatesFromInstalledViewWithoutRecovery(t *testing.T) {
	fixture := newTransferFixture(t)
	owner := mustTransferOwner(t, fixture.destination)
	resetTransferCounters(fixture.destination)
	chunk := fixture.normalChunk(t)

	if _, err := owner.ReceiveResultRecord(context.Background(), fixture.sourcePeer(), chunk); err != nil {
		t.Fatal(err)
	}
	if calls := func() int {
		fixture.destination.mu.Lock()
		defer fixture.destination.mu.Unlock()
		return fixture.destination.recoverCalls
	}(); calls != 0 {
		t.Fatalf("replication validation performed %d RecoverWork calls, want 0", calls)
	}
	if calls := func() int {
		fixture.destination.mu.Lock()
		defer fixture.destination.mu.Unlock()
		return fixture.destination.viewCalls
	}(); calls != 1 {
		t.Fatalf("replication validation read the installed view %d times, want 1", calls)
	}
	if !equalStrings(func() []string {
		fixture.destination.mu.Lock()
		defer fixture.destination.mu.Unlock()
		return fixture.destination.log
	}(), []string{"result"}) {
		t.Fatal("replication chunk was not durably persisted")
	}
}

// TestReceiveResultRecordFailsClosedWithoutPublishedView pins the nil-snapshot
// contract: when no installed view has been published, current replication
// fails closed exactly like a repository read failure, persisting nothing.
func TestReceiveResultRecordFailsClosedWithoutPublishedView(t *testing.T) {
	fixture := newTransferFixture(t)
	owner := mustTransferOwner(t, fixture.destination)
	fixture.destination.mu.Lock()
	fixture.destination.viewOverride = func() (map[model.JobID]store.InstalledAssignment, model.CoordinatorEpoch) {
		return nil, model.CoordinatorEpoch{}
	}
	fixture.destination.mu.Unlock()
	resetTransferCounters(fixture.destination)
	chunk := fixture.normalChunk(t)

	if _, err := owner.ReceiveResultRecord(context.Background(), fixture.sourcePeer(), chunk); !errors.Is(err, ErrTransferStaleAuthority) {
		t.Fatalf("unpublished view err = %v, want ErrTransferStaleAuthority", err)
	}
	if log := func() []string {
		fixture.destination.mu.Lock()
		defer fixture.destination.mu.Unlock()
		return fixture.destination.log
	}(); len(log) != 0 {
		t.Fatalf("fail-closed replication persisted %v", log)
	}
}
