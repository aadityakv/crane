package store

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func TestStoreHistoricalResultHolderPersistsCurrentCheckpointVectorAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/historical-holder"
	identity := Identity{ClusterID: [16]byte{43}, NodeID: 1}
	options := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return model.WorkerEpoch{7}, nil }}
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	topology, prior, epoch := domainAssignment(t, workerStore.WorkerEpoch(), identity.NodeID)
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(prior, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	record, provenance := domainResult(t, topology, prior, epoch, 0)
	if err := workerStore.UpsertResult(record, provenance); err != nil {
		t.Fatal(err)
	}

	current, err := model.BuildAssignmentSet(prior.JobID, topology.Digest(), prior.Revision+1, topology, []model.WorkerPlacement{{NodeID: 2, WorkerEpoch: model.WorkerEpoch{8}, SlotCapacity: 8}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{9}, SlotCapacity: 8}})
	if err != nil {
		t.Fatal(err)
	}
	currentTasks := append([]model.AssignmentToken(nil), current.Tasks...)
	for index := range currentTasks {
		currentTasks[index].Attempt = 2
	}
	current, err = model.NewAssignmentSet(current.JobID, current.Revision, currentTasks, current.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	if assignmentTargetsWorker(current, identity.NodeID, workerStore.WorkerEpoch()) {
		t.Fatal("current fixture still targets historical holder")
	}
	if err := workerStore.InstallAssignment(current, topology.Spec(), 2, model.Closed, epoch); err != nil {
		t.Fatalf("install current audit assignment: %v", err)
	}
	var notices []protocol.CheckpointNotice
	for _, token := range current.Tasks {
		stage, ok := findStage(topology, token.Task.StageID)
		if !ok || stage.Role != model.StageSource {
			continue
		}
		notice := protocol.CheckpointNotice{Notice: model.CheckpointNotice{JobID: current.JobID, Source: token.Task, Watermark: 1, RaftIndex: uint64(len(notices) + 11), Epoch: epoch}, JobControlRevision: 2, AssignmentRevision: current.Revision, AssignmentDigest: current.Digest}
		if err := workerStore.ObserveCheckpoint(notice); err != nil {
			t.Fatalf("historical holder checkpoint %v: %v", token.Task, err)
		}
		notices = append(notices, notice)
	}
	if len(notices) == 0 {
		t.Fatal("fixture has no source checkpoints")
	}
	if _, err := workerStore.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	workerStore, err = Open(path, identity, options)
	if err != nil {
		t.Fatalf("reopen historical holder: %v", err)
	}
	work, err := workerStore.RecoverWork()
	if err != nil || len(work.Results) != 1 || len(work.Checkpoints) != len(notices) || work.Assignments[0].Assignment.Digest != current.Digest {
		t.Fatalf("recovered historical holder: results=%d checkpoints=%d assignment=%+v err=%v", len(work.Results), len(work.Checkpoints), work.Assignments, err)
	}
}

func TestRepairAuthorityPersistsHistoricalSourceToCurrentDestinationAcrossSnapshotReopen(t *testing.T) {
	topology, assignment, epoch, replica := historicalRepairAssignment(t)
	cluster := [16]byte{41}
	historicalNode, historicalEpoch := uint16(30), model.WorkerEpoch{30}
	definition := historicalRepairDefinition(topology, assignment, epoch, replica, historicalNode, historicalEpoch)

	sourcePath := t.TempDir() + "/source"
	sourceOptions := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return historicalEpoch, nil }}
	source := openRepairAuthorityStore(t, sourcePath, Identity{ClusterID: cluster, NodeID: historicalNode}, sourceOptions, topology, assignment, epoch)
	sourceGrant := ResultRepairRecord{Instruction: definition, InstructionDigest: model.RepairInstructionDigest(definition), Role: RepairSource, State: RepairPending}
	sourceGrant.ContentDigest = model.EmptyResultInventoryDigest(sourceGrant.InstructionDigest)
	if err := source.UpsertRepair(sourceGrant); err != nil {
		t.Fatalf("historical source grant: %v", err)
	}
	if _, err := source.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := Open(sourcePath, Identity{ClusterID: cluster, NodeID: historicalNode}, sourceOptions)
	if err != nil {
		t.Fatalf("reopen historical source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if work, err := source.RecoverWork(); err != nil || len(work.Repairs) != 1 || work.Repairs[0].Role != RepairSource {
		t.Fatalf("recovered source repair=%+v err=%v", work.Repairs, err)
	}

	destinationNode, destinationEpoch := definition.DestinationNodeID, definition.DestinationWorkerEpoch
	destinationPath := t.TempDir() + "/destination"
	destinationOptions := Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return destinationEpoch, nil }}
	destination := openRepairAuthorityStore(t, destinationPath, Identity{ClusterID: cluster, NodeID: destinationNode}, destinationOptions, topology, assignment, epoch)
	destinationGrant := sourceGrant
	destinationGrant.Role = RepairDestination
	if err := destination.UpsertRepair(destinationGrant); err != nil {
		t.Fatalf("current destination grant: %v", err)
	}
	if _, err := destination.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	destination, err = Open(destinationPath, Identity{ClusterID: cluster, NodeID: destinationNode}, destinationOptions)
	if err != nil {
		t.Fatalf("reopen current destination: %v", err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	if work, err := destination.RecoverWork(); err != nil || len(work.Repairs) != 1 || work.Repairs[0].Role != RepairDestination {
		t.Fatalf("recovered destination repair=%+v err=%v", work.Repairs, err)
	}
}

func TestRepairAuthorityRejectsNoncurrentDestinationAndWrongLocalRole(t *testing.T) {
	topology, assignment, epoch, replica := historicalRepairAssignment(t)
	historicalNode, historicalEpoch := uint16(30), model.WorkerEpoch{30}
	definition := historicalRepairDefinition(topology, assignment, epoch, replica, historicalNode, historicalEpoch)
	cluster := [16]byte{42}

	openSource := func(t *testing.T) *Store {
		return openRepairAuthorityStore(t, t.TempDir()+"/source", Identity{ClusterID: cluster, NodeID: historicalNode}, Options{MaxBytes: 16 << 20, NewWorkerEpoch: func() (model.WorkerEpoch, error) { return historicalEpoch, nil }}, topology, assignment, epoch)
	}
	base := ResultRepairRecord{Instruction: definition, InstructionDigest: model.RepairInstructionDigest(definition), Role: RepairSource, State: RepairPending}
	base.ContentDigest = model.EmptyResultInventoryDigest(base.InstructionDigest)

	t.Run("destination epoch not current", func(t *testing.T) {
		workerStore := openSource(t)
		bad := base
		bad.Instruction.DestinationWorkerEpoch[0]++
		rebindStoreRepair(&bad)
		if err := workerStore.UpsertRepair(bad); err == nil {
			t.Fatal("noncurrent destination accepted")
		}
	})
	t.Run("source role does not target local worker", func(t *testing.T) {
		workerStore := openSource(t)
		bad := base
		bad.Instruction.SourceNodeID++
		bad.Instruction.SourceWorkerEpoch[0]++
		rebindStoreRepair(&bad)
		if err := workerStore.UpsertRepair(bad); err == nil {
			t.Fatal("wrong local source role accepted")
		}
	})
	t.Run("changed bytes retain identity", func(t *testing.T) {
		workerStore := openSource(t)
		if err := workerStore.UpsertRepair(base); err != nil {
			t.Fatal(err)
		}
		changed := base
		changed.Instruction.ExpectedTotalBytes++
		changed.InstructionDigest = model.RepairInstructionDigest(changed.Instruction)
		if err := workerStore.UpsertRepair(changed); err == nil {
			t.Fatal("changed grant retained RepairID")
		}
	})
}

func historicalRepairAssignment(t *testing.T) (model.ValidatedTopology, model.AssignmentSet, model.CoordinatorEpoch, model.ResultReplicaSet) {
	t.Helper()
	topology, err := model.ValidateTopology(domainTopologySpec(4))
	if err != nil {
		t.Fatal(err)
	}
	workers := []model.WorkerPlacement{{NodeID: 10, WorkerEpoch: model.WorkerEpoch{10}, SlotCapacity: 8}, {NodeID: 20, WorkerEpoch: model.WorkerEpoch{20}, SlotCapacity: 8}}
	assignment, err := model.BuildAssignmentSet(model.JobID{77}, topology.Digest(), 1, topology, workers)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignment.ResultReplicas) == 0 {
		t.Fatal("missing result replica")
	}
	return topology, assignment, model.CoordinatorEpoch{Term: 8, BeginIndex: 12, Coordinator: 10, Nonce: [16]byte{8}}, assignment.ResultReplicas[0]
}

func historicalRepairDefinition(topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch, replica model.ResultReplicaSet, sourceNode uint16, sourceEpoch model.WorkerEpoch) model.RepairResultPartitionDefinition {
	definition := model.RepairResultPartitionDefinition{CoordinatorEpoch: epoch, JobID: assignment.JobID, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceNodeID: sourceNode, SourceWorkerEpoch: sourceEpoch, DestinationNodeID: replica.SecondaryNodeID, DestinationWorkerEpoch: replica.SecondaryEpoch, SinkTask: replica.SinkTask, SpecificationHash: topology.Digest(), CheckpointDigest: model.CheckpointVectorDigest(nil), ExpectedRecordCount: 1, ExpectedTotalBytes: model.ResultArtifactMinRecordBytesV1, ExpectedContentDigest: [32]byte{1}}
	definition.InventoryQueryDigest = model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{JobID: definition.JobID, SinkTask: definition.SinkTask, SpecificationHash: definition.SpecificationHash, AssignmentRevision: definition.AssignmentRevision, AssignmentDigest: definition.AssignmentDigest, CheckpointDigest: definition.CheckpointDigest})
	definition.RepairID = model.DeriveRepairID(definition)
	return definition
}

func openRepairAuthorityStore(t *testing.T, path string, identity Identity, options Options, topology model.ValidatedTopology, assignment model.AssignmentSet, epoch model.CoordinatorEpoch) *Store {
	t.Helper()
	workerStore, err := Open(path, identity, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerStore.Close() })
	if err := workerStore.Fence(epoch); err != nil {
		t.Fatal(err)
	}
	if err := workerStore.InstallAssignment(assignment, topology.Spec(), 1, model.Running, epoch); err != nil {
		t.Fatal(err)
	}
	return workerStore
}

func rebindStoreRepair(repair *ResultRepairRecord) {
	repair.Instruction.RepairID = model.DeriveRepairID(repair.Instruction)
	repair.InstructionDigest = model.RepairInstructionDigest(repair.Instruction)
	repair.ContentDigest = model.EmptyResultInventoryDigest(repair.InstructionDigest)
}
