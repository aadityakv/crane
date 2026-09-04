package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// seedAdmissionWorkForTest fences one epoch and installs the complete
// assignment set in the requested worker-local scheduling state at the given
// JobControlRevision, exactly the coordinator's +3 install protocol shape.
func seedAdmissionWorkForTest(t *testing.T, store *Store, identity Identity, scheduling model.SchedulingState, jobControlRevision uint64) (model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch) {
	t.Helper()
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), jobControlRevision, scheduling, epoch); err != nil {
		t.Fatal(err)
	}
	return topology, assignment, epoch
}

// TestAssignmentEqualFenceClosedToRunningActivationIsAccepted pins the
// coordinator activation sequence: one AssignmentSet installed Closed and
// then Running at the identical revision, JobControlRevision, and
// CoordinatorEpoch (checkpoint-notice resends and inventory verification in
// between change no replicated revision) must record the Running admission
// state while leaving attempts and custody untouched.
func TestAssignmentEqualFenceClosedToRunningActivationIsAccepted(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Closed, 3)
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatalf("equal-fence Closed→Running activation rejected: %v", err)
	}
	after := mustRecoverWork(t, store)
	expected := before
	expected.Assignments = append([]InstalledAssignment(nil), before.Assignments...)
	expected.Assignments[0].SchedulingState = model.Running
	if !reflect.DeepEqual(after, expected) {
		t.Fatal("activation mutated durable work beyond replacing the scheduling state")
	}
	if after.Assignments[0].Assignment.Digest != assignment.Digest || !equalTokens(after.Assignments[0].Assignment.Tasks, assignment.Tasks) {
		t.Fatal("activation changed assignment attempts or custody")
	}
}

// TestAssignmentEqualFenceRunningToClosedRefenceIsAccepted pins the reverse
// admission progression: the current coordinator may re-fence a Running
// installation back to Closed (before re-verification) at the same fence and
// revision without touching anything else.
func TestAssignmentEqualFenceRunningToClosedRefenceIsAccepted(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Running, 3)
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Closed, epoch); err != nil {
		t.Fatalf("equal-fence Running→Closed re-fence rejected: %v", err)
	}
	after := mustRecoverWork(t, store)
	expected := before
	expected.Assignments = append([]InstalledAssignment(nil), before.Assignments...)
	expected.Assignments[0].SchedulingState = model.Closed
	if !reflect.DeepEqual(after, expected) {
		t.Fatal("re-fence mutated durable work beyond replacing the scheduling state")
	}
}

// TestAssignmentClosedToRunningProgressionSurvivesWALRecovery proves the
// shared reducer path replays the admission progression identically after a
// close/reopen cycle.
func TestAssignmentClosedToRunningProgressionSurvivesWALRecovery(t *testing.T) {
	store, path, identity, options := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Closed, 3)
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatalf("equal-fence Closed→Running activation rejected: %v", err)
	}
	activated := mustRecoverWork(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatalf("activation WAL did not reopen: %v", err)
	}
	defer reopened.Close()
	work := mustRecoverWork(t, reopened)
	if work.Fence != epoch || len(work.Assignments) != 1 || work.Assignments[0].SchedulingState != model.Running {
		t.Fatalf("recovered fence=%+v assignments=%+v", work.Fence, work.Assignments)
	}
	if !reflect.DeepEqual(work, activated) {
		t.Fatal("recovered activation state diverges from the live commit")
	}
}

// TestAssignmentNewerEpochRebindRecordsIncomingSchedulingState extends the
// 5d4bd92 leadership rebind: strictly newer committed authority rebinds
// worker-local admission state regardless of the prior scheduling state.
func TestAssignmentNewerEpochRebindRecordsIncomingSchedulingState(t *testing.T) {
	for _, scheduling := range []model.SchedulingState{model.Closed, model.Running} {
		store, _, identity, _ := rebindStoreForTest(t)
		topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, scheduling, 3)
		newer := epoch
		newer.Term++
		newer.BeginIndex++
		newer.Nonce[0]++
		if err := store.Fence(newer); err != nil {
			t.Fatal(err)
		}
		incoming := model.Running
		if scheduling == model.Running {
			incoming = model.Closed
		}
		if err := store.InstallAssignment(assignment, topology.Spec(), 3, incoming, newer); err != nil {
			t.Fatalf("newer-epoch rebind with differing scheduling state (%d→%d) rejected: %v", scheduling, incoming, err)
		}
		after := mustRecoverWork(t, store)
		if len(after.Assignments) != 1 || after.Assignments[0].CoordinatorEpoch != newer || after.Assignments[0].SchedulingState != incoming {
			t.Fatalf("rebound assignment=%+v", after.Assignments[0])
		}
	}
}

// TestAssignmentEqualFenceStillRejectsNonAdmissionSchedulingChanges keeps
// every other same-revision scheduling combination rejected as identity
// reuse: Draining transitions at equal JobControlRevision and same-state
// installs with changed content.
func TestAssignmentEqualFenceStillRejectsNonAdmissionSchedulingChanges(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Running, 3)
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Draining, epoch); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("equal-fence Running→Draining = %v, want model.ErrIdentityReuse", err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Running, epoch); err != nil {
		t.Fatalf("identical Running replay must stay a no-op: %v", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected scheduling change mutated durable work")
	}

	store2, _, identity2, _ := rebindStoreForTest(t)
	_, assignment2, epoch2 := seedAdmissionWorkForTest(t, store2, identity2, model.Draining, 3)
	before2 := mustRecoverWork(t, store2)
	for _, scheduling := range []model.SchedulingState{model.Closed, model.Running} {
		if err := store2.InstallAssignment(assignment2, topology.Spec(), 3, scheduling, epoch2); !errors.Is(err, model.ErrIdentityReuse) {
			t.Fatalf("equal-fence Draining→%d = %v, want model.ErrIdentityReuse", scheduling, err)
		}
	}
	if after := mustRecoverWork(t, store2); !reflect.DeepEqual(after, before2) {
		t.Fatal("rejected Draining progression mutated durable work")
	}
}

// TestAssignmentEqualFenceProgressionStillRejectsChangedContent keeps the
// admission progressions fenced to identical content: a Closed→Running
// activation whose tokens or JobControlRevision differ stays identity reuse.
func TestAssignmentEqualFenceProgressionStillRejectsChangedContent(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Closed, 3)
	tokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range tokens {
		if tokens[index].WorkerID != 2 {
			tokens[index].WorkerID = 2
			tokens[index].WorkerEpoch = model.WorkerEpoch{8}
			break
		}
	}
	changed, err := model.NewAssignmentSet(assignment.JobID, assignment.Revision, tokens, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Digest == assignment.Digest {
		t.Fatal("fixture did not change assignment content")
	}
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(changed, topology.Spec(), 3, model.Running, epoch); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("Closed→Running with changed tokens = %v, want model.ErrIdentityReuse", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected progression mutated durable work")
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 4, model.Running, epoch); err != nil {
		t.Fatalf("Closed→Running on the lifecycle path (JobControlRevision strictly greater) = %v", err)
	}
	if after := mustRecoverWork(t, store); len(after.Assignments) != 1 || after.Assignments[0].JobControlRevision != 4 || after.Assignments[0].SchedulingState != model.Running {
		t.Fatal("lifecycle-path install was not recorded")
	}
}

// TestAssignmentEqualEpochClosedToClosedIdenticalStaysNoOp pins that the
// exact-duplicate path (everything equal, including scheduling state) never
// writes anything.
func TestAssignmentEqualEpochClosedToClosedIdenticalStaysNoOp(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := seedAdmissionWorkForTest(t, store, identity, model.Closed, 3)
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 3, model.Closed, epoch); err != nil {
		t.Fatalf("identical Closed replay: %v", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("identical Closed replay mutated durable work")
	}
}
