package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"crane/internal/crane/model"
)

func rebindStoreForTest(t *testing.T) (*Store, string, Identity, Options) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	store, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path, identity, options
}

func seedRebindWorkForTest(t *testing.T, store *Store, identity Identity, jobControlRevision uint64) (model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch, model.CoordinatorEpoch, DeliveryRecord) {
	t.Helper()
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), jobControlRevision, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	delivery := domainDelivery(t, topology, assignment, epoch)
	if _, err := store.Receive(delivery); err != nil {
		t.Fatal(err)
	}
	newer := epoch
	newer.Term++
	newer.BeginIndex++
	newer.Nonce[0]++
	if err := store.Fence(newer); err != nil {
		t.Fatal(err)
	}
	return topology, assignment, epoch, newer, delivery
}

func TestAssignmentRebindsIdenticalContentUnderNewerCommittedEpoch(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, _, newer, _ := seedRebindWorkForTest(t, store, identity, 1)
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, newer); err != nil {
		t.Fatalf("identical rebind under newer committed epoch rejected: %v", err)
	}
	after := mustRecoverWork(t, store)
	if len(after.Assignments) != 1 || after.Assignments[0].CoordinatorEpoch != newer {
		t.Fatalf("rebound assignments=%+v", after.Assignments)
	}
	expected := before
	expected.Assignments = append([]InstalledAssignment(nil), before.Assignments...)
	expected.Assignments[0].CoordinatorEpoch = newer
	if !reflect.DeepEqual(after, expected) {
		t.Fatal("rebind mutated durable work beyond the authority rebind")
	}
}

func TestAssignmentRebindSurvivesWALRecovery(t *testing.T) {
	store, path, identity, options := rebindStoreForTest(t)
	topology, assignment, _, newer, delivery := seedRebindWorkForTest(t, store, identity, 1)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, newer); err != nil {
		t.Fatalf("identical rebind under newer committed epoch rejected: %v", err)
	}
	rebound := mustRecoverWork(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, identity, Options{MaxBytes: options.MaxBytes})
	if err != nil {
		t.Fatalf("rebind WAL did not reopen: %v", err)
	}
	defer reopened.Close()
	work := mustRecoverWork(t, reopened)
	if work.Fence != newer || len(work.Assignments) != 1 || work.Assignments[0].CoordinatorEpoch != newer {
		t.Fatalf("recovered fence=%+v assignments=%+v", work.Fence, work.Assignments)
	}
	if len(work.Deliveries) != 1 || work.Deliveries[0].State != Received || work.Deliveries[0].ID != delivery.ID {
		t.Fatalf("recovered deliveries=%+v", work.Deliveries)
	}
	if !reflect.DeepEqual(work, rebound) {
		t.Fatal("recovered rebind state diverges from live commit")
	}
}

func TestAssignmentRebindStillRejectsChangedContentUnderNewerCommittedEpoch(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, _, newer, _ := seedRebindWorkForTest(t, store, identity, 3)
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
	if err := store.InstallAssignment(changed, topology.Spec(), 3, model.Running, newer); !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("changed same-revision content under newer epoch = %v", err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 2, model.Running, newer); err == nil || !errors.Is(err, model.ErrIdentityReuse) {
		t.Fatalf("identical content with regressed job control revision = %v", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected rebinds mutated durable work")
	}
}

func TestAssignmentRebindStillRejectsIdenticalContentUnderOlderEpoch(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch, _, _ := seedRebindWorkForTest(t, store, identity, 1)
	older := epoch
	older.Term--
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, older); err == nil {
		t.Fatal("older-epoch identical content accepted")
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("rejected older-epoch rebind mutated durable work")
	}
}

func TestAssignmentEqualEpochIdenticalReplayStaysNoOp(t *testing.T) {
	store, _, identity, _ := rebindStoreForTest(t)
	topology, assignment, epoch := domainAssignment(t, store.WorkerEpoch(), identity.NodeID)
	if err := store.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	before := mustRecoverWork(t, store)
	if err := store.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatalf("equal-epoch identical replay: %v", err)
	}
	if after := mustRecoverWork(t, store); !reflect.DeepEqual(after, before) {
		t.Fatal("equal-epoch identical replay mutated durable work")
	}
}
