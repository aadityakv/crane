package sim

import (
	"path/filepath"
	"testing"

	"crane/internal/crane/model"
	"crane/internal/crane/store"
)

// TestActivationRunningInstallAcceptedAtEqualJobControlRevision is the
// end-to-end acceptance pin for the adjudicated activation defect the Task
// 24 harness exposed (originally committed as a failing reproduction while
// the package was BLOCKED): the coordinator's activateJob sequence
// (internal/crane/coordinator/reconcile.go) installs one complete
// AssignmentSet as Closed and then as Running under the SAME committed
// AssignmentSet revision, JobControlRevision, and CoordinatorEpoch —
// checkpoint-notice resends, inventory verification, and repair between the
// two installs never change a replicated revision by design. The durable
// store must accept the Running admission progression and record it.
//
// Chain, all production objects:
//
//	coordinator.activateJob        internal/crane/coordinator/reconcile.go
//	  installAssignment(Closed)    -> store.InstallAssignment ... accepted
//	  resendCheckpointNotices     -> +5 210 notices ... accepted
//	  repairResults               -> +5 208 inventories ... accepted
//	  installAssignment(Running)  -> store.applyAssignment records.go
//	                                 admission progression ... accepted
func TestActivationRunningInstallAcceptedAtEqualJobControlRevision(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "crane-worker")
	clusterID, err := decodeSimClusterID(simClusterIDText)
	if err != nil {
		t.Fatalf("decode cluster id: %v", err)
	}
	identity := store.Identity{ClusterID: clusterID, NodeID: 4}
	opened, err := store.Open(directory, identity, store.Options{MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer opened.Close()

	job := model.JobID{0x5D, 0xEF, 0xEC, 0x70}
	spec := newSimTopology("activation-pin", 1, 2, simStageSpec{})
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	epoch := model.CoordinatorEpoch{Term: 1, BeginIndex: 5, Coordinator: 2, Nonce: [16]byte{9}}
	token := model.AssignmentToken{
		Task: model.TaskID{JobID: job, StageID: 1, Partition: 0}, WorkerID: 4,
		WorkerEpoch: opened.WorkerEpoch(), Attempt: 1, SpecificationHash: topology.Digest(), AssignmentRevision: 1,
	}
	sinkToken := model.AssignmentToken{
		Task: model.TaskID{JobID: job, StageID: 2, Partition: 0}, WorkerID: 4,
		WorkerEpoch: opened.WorkerEpoch(), Attempt: 1, SpecificationHash: topology.Digest(), AssignmentRevision: 1,
	}
	set, err := model.NewAssignmentSet(job, 1, []model.AssignmentToken{token, sinkToken}, []model.ResultReplicaSet{{
		SinkTask:      model.TaskID{JobID: job, StageID: 2, Partition: 0},
		PrimaryNodeID: 4, PrimaryEpoch: opened.WorkerEpoch(),
		SecondaryNodeID: 3, SecondaryEpoch: model.WorkerEpoch{7},
	}}, topology)
	if err != nil {
		t.Fatalf("build assignment set: %v", err)
	}
	const controlRevision = uint64(3)
	if err := opened.Fence(epoch); err != nil {
		t.Fatalf("install fence: %v", err)
	}
	if err := opened.InstallAssignment(set, spec, controlRevision, model.Closed, epoch); err != nil {
		t.Fatalf("Closed install: %v", err)
	}
	if err := opened.InstallAssignment(set, spec, controlRevision, model.Running, epoch); err != nil {
		t.Fatalf("Running install at one JobControlRevision rejected: %v", err)
	}
	work, err := opened.RecoverWork()
	if err != nil {
		t.Fatalf("recover work: %v", err)
	}
	if len(work.Assignments) != 1 || work.Assignments[0].SchedulingState != model.Running {
		t.Fatalf("durable scheduling state = %+v, want one Running installation", work.Assignments)
	}
	if work.Assignments[0].Assignment.Digest != set.Digest || work.Assignments[0].JobControlRevision != controlRevision {
		t.Fatalf("activation changed assignment identity: %+v", work.Assignments[0])
	}
}
