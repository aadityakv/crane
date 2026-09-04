package protocol_test

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func TestModelAndProtocolWorkerControlContractsCannotDrift(t *testing.T) {
	contract := model.WorkerControlContractV1()
	if protocol.WorkerControlSchemaVersion != contract.SchemaVersion ||
		protocol.MaxWorkerStatusEvents != int(contract.MaxStatusEvents) ||
		protocol.MaxInventoryCheckpoints != int(contract.MaxCheckpointVectorEntries) ||
		protocol.MaxTransferChunkBytes != int(contract.MaxTransferChunkBytes) ||
		protocol.MaxTransferTotalBytes != int(contract.MaxTransferTotalBytes) ||
		protocol.MaxWorkerErrorDetailBytes != int(contract.MaxErrorDetailBytes) {
		t.Fatalf("protocol constants drifted from dependency-leaf contract: %#v", contract)
	}
	job := model.JobID{1}
	checkpoints := []model.SourceCheckpoint{{Source: model.TaskID{JobID: job, StageID: 1}, Watermark: 0}}
	checkpointDigest := model.CheckpointVectorDigest(checkpoints)
	definition := model.ResultInventoryQueryDefinition{JobID: job, SinkTask: model.TaskID{JobID: job, StageID: 2}, SpecificationHash: [32]byte{3}, AssignmentRevision: 4, AssignmentDigest: [32]byte{5}, Checkpoints: checkpoints, CheckpointDigest: checkpointDigest}
	query := protocol.ResultInventoryQuery{JobID: definition.JobID, SinkTask: definition.SinkTask, SpecificationHash: definition.SpecificationHash, AssignmentRevision: definition.AssignmentRevision, AssignmentDigest: definition.AssignmentDigest, Checkpoints: checkpoints, CheckpointDigest: checkpointDigest}
	if protocol.CheckpointVectorDigest(checkpoints) != checkpointDigest || protocol.InventoryQueryDigest(query) != model.ResultInventoryQueryDigest(definition) {
		t.Fatal("protocol identity helpers drifted from model contract")
	}
}
