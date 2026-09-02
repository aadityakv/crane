package store

import (
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
)

// supersedingSet derives revision+1 of prior with an unchanged placement: the
// reassignment that supersedes a repair grant need not touch the repaired sink.
func supersedingSet(t *testing.T, topology model.ValidatedTopology, prior model.AssignmentSet) model.AssignmentSet {
	t.Helper()
	tasks := append([]model.AssignmentToken(nil), prior.Tasks...)
	for index := range tasks {
		tasks[index].AssignmentRevision = prior.Revision + 1
	}
	set, err := model.NewAssignmentSet(prior.JobID, prior.Revision+1, tasks, append([]model.ResultReplicaSet(nil), prior.ResultReplicas...), topology)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// TestUpsertRepairAdmitsOnlyFailureForSupersededGrant pins the store half of
// the superseded-repair-grant ruling: once the installed assignment's revision
// advances past a retained grant's, the grant can neither progress nor be
// re-issued, but the worker may durably mark it RepairFailed (the marking
// installRepair performs when it admits the replacement grant), and that
// terminal marking survives WAL replay while a grant at the current revision
// is admitted alongside it.
func TestUpsertRepairAdmitsOnlyFailureForSupersededGrant(t *testing.T) {
	path := t.TempDir() + "/worker"
	identity := Identity{ClusterID: [16]byte{1}, NodeID: 1}
	options := Options{MaxBytes: 8 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = workerStore.Close() }()
	topology, assignment, epoch := domainAssignment(t, workerStore.WorkerEpoch(), identity.NodeID)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	repair := domainRepair(t, topology, assignment, epoch, identity.NodeID, workerStore.WorkerEpoch())
	if err := workerStore.UpsertRepair(repair); err != nil {
		t.Fatal(err)
	}
	progress := repair
	progress.State = RepairStreaming
	progress.NextRecord, progress.NextOffset = 1, 10
	if err := workerStore.UpsertRepair(progress); err != nil {
		t.Fatal(err)
	}

	next := supersedingSet(t, topology, assignment)
	if err := workerStore.InstallAssignment(next, topology.Spec(), 2, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	more := progress
	more.NextRecord, more.NextOffset = 2, 20
	if err := workerStore.UpsertRepair(more); err == nil {
		t.Fatal("superseded grant progressed under the new assignment revision")
	}
	fresh := repair
	fresh.Instruction.ExpectedRecordCount++
	rebindRepair(&fresh)
	if err := workerStore.UpsertRepair(fresh); err == nil {
		t.Fatal("new grant at the superseded revision admitted")
	}
	failed := progress
	failed.State = RepairFailed
	failed.ErrorCode = protocol.WorkerErrorStaleAssignment
	if err := workerStore.UpsertRepair(failed); err != nil {
		t.Fatalf("superseded grant could not be marked failed: %v", err)
	}
	work := mustRecoverWork(t, workerStore)
	if len(work.Repairs) != 1 || work.Repairs[0].State != RepairFailed || work.Repairs[0].ErrorCode != protocol.WorkerErrorStaleAssignment || work.Repairs[0].NextRecord != 1 {
		t.Fatalf("superseded grant not durably failed: %+v", work.Repairs)
	}
	revived := failed
	revived.State = RepairStreaming
	revived.ErrorCode = 0
	if err := workerStore.UpsertRepair(revived); err == nil {
		t.Fatal("failed superseded grant revived")
	}

	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	workerStore, err = Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	work = mustRecoverWork(t, workerStore)
	if len(work.Repairs) != 1 || work.Repairs[0].State != RepairFailed || work.Repairs[0].ErrorCode != protocol.WorkerErrorStaleAssignment {
		t.Fatalf("superseded failure lost across replay: %+v", work.Repairs)
	}
	replacement := domainRepair(t, topology, next, epoch, identity.NodeID, workerStore.WorkerEpoch())
	if err := workerStore.UpsertRepair(replacement); err != nil {
		t.Fatalf("replacement grant at the current revision refused: %v", err)
	}
	work = mustRecoverWork(t, workerStore)
	if len(work.Repairs) != 2 {
		t.Fatalf("repairs=%+v want failed prior plus replacement", work.Repairs)
	}
}
