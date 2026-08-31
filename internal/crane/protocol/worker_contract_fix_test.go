package protocol

import (
	"bytes"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestResultReplicaSetContractSchemaMatchesCanonicalEncoderOrder(t *testing.T) {
	var schema string
	for _, candidate := range model.WorkerControlContractV1().NestedSchemas {
		if strings.HasPrefix(candidate, "ResultReplicaSet=") {
			schema = strings.TrimPrefix(candidate, "ResultReplicaSet=")
			break
		}
	}
	if schema == "" {
		t.Fatal("worker-control contract omits ResultReplicaSet schema")
	}
	replica := model.ResultReplicaSet{SinkTask: model.TaskID{JobID: model.JobID{1}, StageID: 2, Partition: 3}, PrimaryNodeID: 0x0405, SecondaryNodeID: 0x0607, PrimaryEpoch: model.WorkerEpoch{8}, SecondaryEpoch: model.WorkerEpoch{9}}
	fieldBytes := map[string][]byte{
		"SinkTask:TaskID":            append(append(append([]byte(nil), replica.SinkTask.JobID[:]...), byte(replica.SinkTask.StageID>>8), byte(replica.SinkTask.StageID)), byte(replica.SinkTask.Partition>>8), byte(replica.SinkTask.Partition)),
		"PrimaryNodeID:u16":          {byte(replica.PrimaryNodeID >> 8), byte(replica.PrimaryNodeID)},
		"SecondaryNodeID:u16":        {byte(replica.SecondaryNodeID >> 8), byte(replica.SecondaryNodeID)},
		"PrimaryEpoch:WorkerEpoch":   replica.PrimaryEpoch[:],
		"SecondaryEpoch:WorkerEpoch": replica.SecondaryEpoch[:],
	}
	var want []byte
	for _, field := range strings.Split(schema, ",") {
		encoded, ok := fieldBytes[field]
		if !ok {
			t.Fatalf("unrecognized ResultReplicaSet contract field %q", field)
		}
		want = append(want, encoded...)
	}
	encoder := workerEncoder{}
	encoder.replica(replica)
	if encoder.err != nil {
		t.Fatal(encoder.err)
	}
	if !bytes.Equal(encoder.output, want) {
		t.Fatalf("contract schema order encodes %x, canonical encoder emits %x", want, encoder.output)
	}
}

func TestProtocolUsesAuthoritativeWorkerControlContract(t *testing.T) {
	contract := model.WorkerControlContractV1()
	if WorkerControlSchemaVersion != contract.SchemaVersion ||
		MaxWorkerStatusEvents != int(contract.MaxStatusEvents) ||
		MaxInventoryCheckpoints != int(contract.MaxCheckpointVectorEntries) ||
		MaxTransferChunkBytes != int(contract.MaxTransferChunkBytes) ||
		MaxTransferTotalBytes != int(contract.MaxTransferTotalBytes) ||
		MaxWorkerErrorDetailBytes != int(contract.MaxErrorDetailBytes) ||
		MaxWorkerControlPayloadBytes+model.LimitsV1().AuthenticatedFrameBytes != contract.MaxControlFrameBytes {
		t.Fatalf("protocol bounds drifted from model contract: %#v", contract)
	}
	fixture := workerFixture(t)
	want := model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{
		JobID: fixture.statusRequest.Inventory.JobID, SinkTask: fixture.statusRequest.Inventory.SinkTask,
		SpecificationHash:  fixture.statusRequest.Inventory.SpecificationHash,
		AssignmentRevision: fixture.statusRequest.Inventory.AssignmentRevision,
		AssignmentDigest:   fixture.statusRequest.Inventory.AssignmentDigest,
		Checkpoints:        fixture.statusRequest.Inventory.Checkpoints,
		CheckpointDigest:   fixture.statusRequest.Inventory.CheckpointDigest,
	})
	if got := InventoryQueryDigest(*fixture.statusRequest.Inventory); got != want {
		t.Fatalf("protocol/model query digests drifted: %x != %x", got, want)
	}
}

func TestRepairGrantRequiresOuterCurrentEpoch(t *testing.T) {
	fixture := workerFixture(t)
	request := WorkerStatusRequest{CoordinatorEpoch: fixture.grant.Instruction.CoordinatorEpoch, MaxEvents: 1, Repair: &fixture.grant}
	request.CoordinatorEpoch.Term++
	if _, err := MarshalWorkerMessage(request); err == nil {
		t.Fatal("accepted repair grant from a different coordinator epoch")
	}
}

func TestZeroCheckpointWatermarkRoundTripsAsDurableEmptyPartition(t *testing.T) {
	fixture := workerFixture(t)
	notice := fixture.checkpoint
	notice.Notice.Watermark = 0
	ack := fixture.checkpointAck
	ack.Watermark = 0
	for _, message := range []WorkerMessage{notice, ack} {
		encoded, err := MarshalWorkerMessage(message)
		if err != nil {
			t.Fatalf("marshal zero checkpoint %d: %v", message.MessageType(), err)
		}
		if _, err := UnmarshalWorkerMessage(message.MessageType(), encoded); err != nil {
			t.Fatalf("decode zero checkpoint %d: %v", message.MessageType(), err)
		}
	}
}

func TestInventoryAndRepairAggregateContracts(t *testing.T) {
	fixture := workerFixture(t)
	status := fixture.status
	queryDigest := status.Inventory.QueryDigest
	status.Inventory.RecordCount = 0
	status.Inventory.TotalBytes = 0
	status.Inventory.ContentDigest = model.EmptyResultInventoryDigest(queryDigest)
	if _, err := MarshalWorkerMessage(status); err != nil {
		t.Fatalf("canonical empty inventory: %v", err)
	}
	for name, mutate := range map[string]func(*ResultInventorySummary){
		"count_only":   func(v *ResultInventorySummary) { v.RecordCount = 1 },
		"bytes_only":   func(v *ResultInventorySummary) { v.TotalBytes = 1 },
		"zero_digest":  func(v *ResultInventorySummary) { v.ContentDigest = [32]byte{} },
		"wrong_digest": func(v *ResultInventorySummary) { v.ContentDigest = sha256.Sum256([]byte("wrong")) },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := status
			summary := *status.Inventory
			mutate(&summary)
			invalid.Inventory = &summary
			if _, err := MarshalWorkerMessage(invalid); err == nil {
				t.Fatal("accepted invalid aggregate")
			}
		})
	}
	completed := fixture.status
	completed.Inventory = nil
	completed.Repair = &ResultRepairStatus{Instruction: fixture.grant.Instruction, RepairID: fixture.grant.Instruction.RepairID, InstructionDigest: fixture.grant.Instruction.InstructionDigest, Role: RepairDestination, State: RepairComplete, RecordCount: fixture.grant.Instruction.ExpectedRecordCount, TotalBytes: fixture.grant.Instruction.ExpectedTotalBytes, ContentDigest: fixture.grant.Instruction.ExpectedContentDigest}
	if _, err := MarshalWorkerMessage(completed); err != nil {
		t.Fatalf("completed exact summary: %v", err)
	}
	for _, mutate := range []func(*ResultRepairStatus){func(v *ResultRepairStatus) { v.RecordCount++ }, func(v *ResultRepairStatus) { v.TotalBytes++ }, func(v *ResultRepairStatus) { v.ContentDigest[0] ^= 1 }} {
		invalid := completed
		value := *completed.Repair
		mutate(&value)
		invalid.Repair = &value
		if _, err := MarshalWorkerMessage(invalid); err == nil {
			t.Fatal("accepted completed repair that disagrees with instruction")
		}
	}
}

func TestResultRecordChunksAreExactSlicesOfCanonicalLogicalStream(t *testing.T) {
	fixture := workerFixture(t)
	stream, err := model.MarshalResultRecord(fixture.recordChunk.Record)
	if err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256(stream)
	base := fixture.recordChunk
	base.Transfer.TotalLength = uint64(len(stream))
	base.Transfer.Checksum = checksum
	base.Transfer.Offset = 1
	base.Transfer.Data = append([]byte(nil), stream[1:len(stream)-1]...)
	base.Transfer.Final = false
	if _, err := MarshalWorkerMessage(base); err != nil {
		t.Fatalf("valid resumed slice: %v", err)
	}
	for name, mutate := range map[string]func(*ResultRecordChunk){
		"total":    func(v *ResultRecordChunk) { v.Transfer.TotalLength++ },
		"checksum": func(v *ResultRecordChunk) { v.Transfer.Checksum[0] ^= 1 },
		"offset":   func(v *ResultRecordChunk) { v.Transfer.Offset++ },
		"data":     func(v *ResultRecordChunk) { v.Transfer.Data[0] ^= 1 },
		"final":    func(v *ResultRecordChunk) { v.Transfer.Final = true },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := base
			invalid.Transfer.Data = append([]byte(nil), base.Transfer.Data...)
			mutate(&invalid)
			if _, err := MarshalWorkerMessage(invalid); err == nil {
				t.Fatal("accepted stream mismatch")
			}
		})
	}
	encoded, err := MarshalWorkerMessage(base)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalWorkerMessage(base.MessageType(), encoded)
	if err != nil || !reflect.DeepEqual(decoded, base) {
		t.Fatalf("retry/resume round trip: %v", err)
	}
	if !bytes.Equal(base.Transfer.Data, stream[base.Transfer.Offset:base.Transfer.Offset+uint64(len(base.Transfer.Data))]) {
		t.Fatal("fixture does not contain exact stream slice")
	}
	ack := fixture.recordAck
	if err := ValidateResultRecordAckCorrelation(fixture.recordChunk, ack); err != nil {
		t.Fatalf("exact ACK correlation: %v", err)
	}
	for name, mutate := range map[string]func(*ResultRecordAck){"total": func(v *ResultRecordAck) { v.TotalLength++ }, "checksum": func(v *ResultRecordAck) { v.Checksum[0] ^= 1 }, "transfer": func(v *ResultRecordAck) { v.TransferID[0] ^= 1 }} {
		t.Run("ack_"+name, func(t *testing.T) {
			invalid := ack
			mutate(&invalid)
			if err := ValidateResultRecordAckCorrelation(fixture.recordChunk, invalid); err == nil {
				t.Fatal("accepted ACK correlation mismatch")
			}
		})
	}
	partial := fixture.recordChunk
	cut := len(stream) / 2
	partial.Transfer.Data = append([]byte(nil), stream[:cut]...)
	partial.Transfer.Final = false
	partialACK := fixture.recordAck
	partialACK.NextOffset = uint64(cut)
	partialACK.Complete = false
	if err := ValidateResultRecordAckCorrelation(partial, partialACK); err != nil {
		t.Fatalf("valid partial ACK correlation: %v", err)
	}
	for name, mutate := range map[string]func(*ResultRecordAck){
		"partial incorrect offset":   func(v *ResultRecordAck) { v.NextOffset++ },
		"partial premature complete": func(v *ResultRecordAck) { v.NextOffset = v.TotalLength; v.Complete = true },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := partialACK
			mutate(&invalid)
			if err := ValidateResultRecordAckCorrelation(partial, invalid); err == nil {
				t.Fatal("accepted ACK that does not advance the supplied partial chunk")
			}
		})
	}
	if err := ValidateResultRecordAckCorrelation(fixture.recordChunk, fixture.recordAck); err != nil {
		t.Fatalf("valid final ACK correlation: %v", err)
	}
	finalIncomplete := fixture.recordAck
	finalIncomplete.NextOffset--
	finalIncomplete.Complete = false
	if err := ValidateResultRecordAckCorrelation(fixture.recordChunk, finalIncomplete); err == nil {
		t.Fatal("accepted incorrect offset for final chunk")
	}
	finalWrongComplete := fixture.recordAck
	finalWrongComplete.Complete = false
	if err := ValidateResultRecordAckCorrelation(fixture.recordChunk, finalWrongComplete); err == nil {
		t.Fatal("accepted incomplete ACK for final chunk")
	}
}
