package store

import (
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

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
