package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
)

func TestWorkerControlContractV1IsOwnedAndPinsEveryConsensusRule(t *testing.T) {
	wantMessages := make([]WorkerControlMessageDescriptor, 19)
	names := []string{"WorkerHandshake", "WorkerHandshakeAck", "FenceRequest", "FenceResponse", "WorkerRegisterRequest", "WorkerRegisterResponse", "AssignmentSetInstall", "AssignmentSetInstallAck", "WorkerStatusRequest", "WorkerStatus", "CheckpointNotice", "CheckpointAck", "ResultRecordChunk", "ResultRecordAck", "ResultArtifactChunk", "ResultArtifactAck", "ResultFetchRequest", "ResultFetchChunk", "WorkerError"}
	schemas := []string{
		"NodeID:u16,WorkerEpoch:WorkerEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256", "NodeID:u16,WorkerEpoch:WorkerEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256", "CoordinatorEpoch:CoordinatorEpoch", "NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch",
		"NodeID:u16,WorkerEpoch:WorkerEpoch,SlotCapacity:u16,CoordinatorEpoch:CoordinatorEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256", "NodeID:u16,WorkerEpoch:WorkerEpoch,WorkerRevision:u64,CoordinatorEpoch:CoordinatorEpoch,Accepted:bool", "Assignment:AssignmentSet,Specification:TopologySpec,SpecificationDigest:sha256,JobControlRevision:u64,SchedulingState:u8,CoordinatorEpoch:CoordinatorEpoch", "NodeID:u16,WorkerEpoch:WorkerEpoch,JobID:JobID,AssignmentRevision:u64,AssignmentDigest:sha256,JobControlRevision:u64,SchedulingState:u8,CoordinatorEpoch:CoordinatorEpoch",
		"CoordinatorEpoch:CoordinatorEpoch,AfterTransactionID:u64,MaxEvents:u16,Body:oneof(none,ResultInventoryQuery,RepairGrant)", "NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch,StoreTransactionID:u64,AfterTransactionID:u64,Assignments:list(InstalledAssignmentStatus),Events:list(WorkerEvent),LastTransactionID:u64,HasMore:bool,Body:oneof(none,ResultInventorySummary,ResultRepairStatus)", "Notice:CheckpointNotice,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256", "NodeID:u16,WorkerEpoch:WorkerEpoch,JobID:JobID,Source:TaskID,Watermark:u64,RaftIndex:u64,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256,CoordinatorEpoch:CoordinatorEpoch",
		"Transfer:TransferChunk,Record:ResultRecord,Provenance:ResultCopyProvenance,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,RepairID:optional(16),RepairInstructionDigest:optional(sha256)", "TransferID:16,NodeID:u16,WorkerEpoch:WorkerEpoch,RepairID:optional(16),RepairInstructionDigest:optional(sha256),NextOffset:u64,TotalLength:u64,Checksum:sha256,Complete:bool,CoordinatorEpoch:CoordinatorEpoch", "Transfer:TransferChunk,Artifact:ResultArtifact,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch", "TransferID:16,NodeID:u16,WorkerEpoch:WorkerEpoch,Artifact:ResultArtifact,NextOffset:u64,Complete:bool,CoordinatorEpoch:CoordinatorEpoch", "Artifact:ResultArtifact,ReplicaNodeID:u16,ReplicaWorkerEpoch:WorkerEpoch,Offset:u64,CoordinatorEpoch:CoordinatorEpoch", "Transfer:TransferChunk,Artifact:ResultArtifact,SourceNodeID:u16,SourceWorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch", "NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch,RelatedMessage:u16,Code:u16,Retryable:bool,Detail:bytes16",
	}
	for index := range wantMessages {
		wantMessages[index] = WorkerControlMessageDescriptor{Name: names[index], MessageType: uint16(200 + index), SchemaVersion: 1, Schema: schemas[index]}
	}
	want := WorkerControlContract{
		SchemaVersion: 1, MessageTypeMin: 200, MessageTypeMax: 218, ReservedMessageType: 219,
		Messages:        wantMessages,
		NestedSchemas:   []string{"WorkerEpoch=bytes16(nonzero)", "CoordinatorEpoch=Term:u64,BeginIndex:u64,Coordinator:u16,Nonce:bytes16", "JobID=bytes16(nonzero)", "TaskID=JobID:JobID,StageID:u16,Partition:u16", "TupleID=JobID:JobID,SourceTask:TaskID,SourceSequence:u64,PathDigest:sha256", "AssignmentToken=Task:TaskID,WorkerID:u16,WorkerEpoch:WorkerEpoch,Attempt:u64,SpecificationHash:sha256,AssignmentRevision:u64", "ResultReplicaSet=SinkTask:TaskID,PrimaryNodeID:u16,SecondaryNodeID:u16,PrimaryEpoch:WorkerEpoch,SecondaryEpoch:WorkerEpoch", "AssignmentSet=JobID:JobID,Revision:u64,Digest:sha256,Tasks:list(AssignmentToken),ResultReplicas:list(ResultReplicaSet)", "TopologySpec=canonical-topology-v1:bytes", "CheckpointNotice=JobID:JobID,Source:TaskID,Watermark:u64,RaftIndex:u64,Epoch:CoordinatorEpoch", "WorkerEvent=WorkerID:u16,WorkerEpoch:WorkerEpoch,TransactionID:u64,Kind:u8,Body:oneof(CompletionReport,JobFailureReport)", "CompletionReport=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,Source:TaskID,Token:AssignmentToken,Epoch:CoordinatorEpoch,ExpectedCheckpointRevision:u64,Prior:u64,New:u64,EOF:u64,WorkerTransactionID:u64,Digest:sha256", "JobFailureReport=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,Task:AssignmentToken,Epoch:CoordinatorEpoch,TransactionID:u64,Code:u16,DetailDigest:sha256", "SourceCheckpoint=Source:TaskID,Watermark:u64", "ResultInventoryQuery=JobID:JobID,SinkTask:TaskID,SpecificationHash:sha256,AssignmentRevision:u64,AssignmentDigest:sha256,Checkpoints:list(SourceCheckpoint),CheckpointDigest:sha256,QueryDigest:sha256", "ResultInventorySummary=QueryDigest:sha256,RecordCount:u64,TotalBytes:u64,ContentDigest:sha256", "RepairResultPartition=RepairID:bytes16,CoordinatorEpoch:CoordinatorEpoch,JobID:JobID,AssignmentRevision:u64,AssignmentDigest:sha256,SourceNodeID:u16,SourceWorkerEpoch:WorkerEpoch,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,SinkTask:TaskID,SpecificationHash:sha256,Checkpoints:list(SourceCheckpoint),CheckpointDigest:sha256,InventoryQueryDigest:sha256,ExpectedRecordCount:u64,ExpectedTotalBytes:u64,ExpectedContentDigest:sha256,InstructionDigest:sha256", "RepairGrant=Instruction:RepairResultPartition,Role:u8", "ResultRepairStatus=Instruction:RepairResultPartition,RepairID:bytes16,InstructionDigest:sha256,Role:u8,State:u8,RecordCount:u64,TotalBytes:u64,ContentDigest:sha256,ErrorCode:u16", "InstalledAssignmentStatus=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256,SpecificationDigest:sha256,SchedulingState:u8", "TransferChunk=TransferID:bytes16,JobID:JobID,TotalLength:u64,Checksum:sha256,Offset:u64,Data:bytes32,Final:bool", "ResultRecord=TupleID:TupleID,SinkTask:TaskID,SpecificationHash:sha256,Value:bytes16,Checksum:sha256", "ResultRecordStream=SchemaVersion:u16,TupleID:TupleID,SinkTask:TaskID,SpecificationHash:sha256,Value:bytes16,LogicalChecksum:sha256", "ResultCopyProvenance=AssignmentRevision:u64,AssignmentDigest:sha256,ReplicaSet:ResultReplicaSet,DestinationRole:u8,CoordinatorEpoch:CoordinatorEpoch", "ResultArtifact=JobID:JobID,SinkTask:TaskID,SpecificationHash:sha256,RecordCount:u64,TotalLength:u64,Checksum:sha256"},
		MaxStatusEvents: 256, MaxCheckpointVectorEntries: 256,
		MaxTransferChunkBytes: 256 << 10, MaxTransferTotalBytes: 64 << 20,
		MaxErrorDetailBytes: 256, MaxControlFrameBytes: 1 << 20,
		AllowZeroCheckpointWatermark: true, AllowEmptyResultArtifact: true,
		ResultRecordStreamSchemaVersion: 1,
		IdentityDomains: []string{
			"cs425/crane/checkpoint-vector/v1", "cs425/crane/result-inventory-query/v1",
			"cs425/crane/empty-result-inventory/v1", "cs425/crane/repair-id/v1",
			"cs425/crane/repair-instruction/v1", "cs425/crane/result-record-stream/v1",
		},
		Rules: []string{"status-events-strictly-increasing-after-cursor", "status-page-last-transaction-and-has-more-consistent", "checkpoint-vectors-sorted-unique", "aggregate-count-zero-iff-bytes-zero", "empty-aggregate-digest-binds-query-or-instruction", "repair-grant-epoch-equals-current-epoch", "completed-repair-equals-instruction-summary", "result-record-chunks-are-exact-canonical-stream-slices", "result-record-ack-binds-nonzero-stream-total-and-checksum", "transfer-offset-final-and-checksum-correlated"},
	}
	got := WorkerControlContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WorkerControlContractV1() = %#v, want %#v", got, want)
	}
	got.Messages[0].MessageType = 999
	got.NestedSchemas[0] = "mutated"
	got.IdentityDomains[0] = "mutated"
	got.Rules[0] = "mutated"
	if again := WorkerControlContractV1(); !reflect.DeepEqual(again, want) {
		t.Fatalf("contract shares mutable storage: %#v", again)
	}
}

func TestWorkerControlCanonicalHelpersHaveIndependentGoldens(t *testing.T) {
	job := JobID{1}
	checkpoints := []SourceCheckpoint{{Source: TaskID{JobID: job, StageID: 1}, Watermark: 0}, {Source: TaskID{JobID: job, StageID: 1, Partition: 1}, Watermark: 7}}
	checkpointDigest := CheckpointVectorDigest(checkpoints)
	query := ResultInventoryQueryDefinition{JobID: job, SinkTask: TaskID{JobID: job, StageID: 3}, SpecificationHash: [32]byte{2}, AssignmentRevision: 4, AssignmentDigest: [32]byte{5}, Checkpoints: checkpoints, CheckpointDigest: checkpointDigest}
	queryDigest := ResultInventoryQueryDigest(query)
	emptyDigest := EmptyResultInventoryDigest(queryDigest)
	for name, value := range map[string][32]byte{"checkpoint": checkpointDigest, "query": queryDigest, "empty": emptyDigest} {
		if got := hex.EncodeToString(value[:]); got == "" || got == hex.EncodeToString(make([]byte, 32)) {
			t.Fatalf("%s digest is empty: %s", name, got)
		}
	}
	// Literal is updated only when the independently reviewed contract intentionally changes.
	if got := hex.EncodeToString(emptyDigest[:]); got != "c9758f02facf9ebde95a534e04a65e4e63f8ed749f29cc59a9ceef405ba45d2c" {
		t.Fatalf("empty inventory digest = %s", got)
	}
}

func TestCanonicalResultRecordStreamRoundTripGoldenAndNonzeroEmptyTupleValue(t *testing.T) {
	job := JobID{1}
	value, err := MarshalTuple(Tuple{})
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewResultRecord(DeriveSourceTupleID(job, TaskID{JobID: job, StageID: 1}, 1), TaskID{JobID: job, StageID: 3}, [32]byte{7}, value)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalResultRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || ResultRecordStreamChecksum(record) != sha256.Sum256(encoded) {
		t.Fatal("canonical result stream/checksum mismatch")
	}
	decoded, err := UnmarshalResultRecord(encoded)
	if err != nil || !reflect.DeepEqual(decoded, record) {
		t.Fatalf("round trip = %#v, %v", decoded, err)
	}
	streamDigest := sha256.Sum256(encoded)
	if got := hex.EncodeToString(streamDigest[:]); got != "559e89f0a89601d811048defb7d0e4c581eddfbacad95c1a4583d08690a7580b" {
		t.Fatalf("result stream golden = %s", got)
	}
	for cut := range encoded {
		if _, err := UnmarshalResultRecord(encoded[:cut]); err == nil {
			t.Fatalf("accepted stream truncation at %d", cut)
		}
	}
	if _, err := UnmarshalResultRecord(append(append([]byte(nil), encoded...), 0)); err == nil {
		t.Fatal("accepted trailing result stream byte")
	}
	ownedValue := append([]byte(nil), decoded.Value...)
	encoded[132] ^= 1
	if !bytes.Equal(decoded.Value, ownedValue) {
		t.Fatal("decoded result aliases encoded bytes")
	}
}

func FuzzUnmarshalResultRecord(f *testing.F) {
	job := JobID{1}
	value, _ := MarshalTuple(Tuple{})
	record, _ := NewResultRecord(DeriveSourceTupleID(job, TaskID{JobID: job, StageID: 1}, 1), TaskID{JobID: job, StageID: 2}, [32]byte{3}, value)
	encoded, _ := MarshalResultRecord(record)
	f.Add(encoded)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		maximum := 2 + 76 + 20 + 32 + 2 + int(LimitsV1().MaxTuplePayloadBytes) + 32
		if len(input) > maximum+1 {
			input = input[:maximum+1]
		}
		_, _ = UnmarshalResultRecord(input)
	})
}
