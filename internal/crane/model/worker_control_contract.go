package model

import "crypto/sha256"

const workerControlContractFingerprintDomain = "cs425/crane/worker-control-contract/v1\x00"

const (
	WorkerControlSchemaVersionV1    uint16 = 1
	WorkerControlMaxStatusEventsV1         = 256
	WorkerControlMaxCheckpointsV1          = 256
	WorkerControlMaxTransferChunkV1        = 256 << 10
	WorkerControlMaxTransferTotalV1        = 64 << 20
	WorkerControlMaxErrorDetailV1          = 256
	WorkerControlMaxFrameBytesV1           = 1 << 20
)

// WorkerControlMessageDescriptor identifies one owned +5 message schema.
type WorkerControlMessageDescriptor struct {
	Name          string
	MessageType   uint16
	SchemaVersion uint16
	Schema        string
}

// WorkerControlContract contains every compatibility-sensitive +5 rule.
// Callers receive owned slices from WorkerControlContractV1.
type WorkerControlContract struct {
	SchemaVersion                   uint16
	MessageTypeMin                  uint16
	MessageTypeMax                  uint16
	ReservedMessageType             uint16
	Messages                        []WorkerControlMessageDescriptor
	NestedSchemas                   []string
	MaxStatusEvents                 uint16
	MaxCheckpointVectorEntries      uint16
	MaxTransferChunkBytes           uint64
	MaxTransferTotalBytes           uint64
	MaxErrorDetailBytes             uint64
	MaxControlFrameBytes            uint64
	AllowZeroCheckpointWatermark    bool
	AllowEmptyResultArtifact        bool
	ResultRecordStreamSchemaVersion uint16
	IdentityDomains                 []string
	Rules                           []string
}

var workerControlIdentityDomainsV1 = []string{
	"cs425/crane/checkpoint-vector/v1",
	"cs425/crane/result-inventory-query/v1",
	"cs425/crane/empty-result-inventory/v1",
	"cs425/crane/repair-id/v1",
	"cs425/crane/repair-instruction/v1",
	"cs425/crane/result-record-stream/v1",
}

var workerControlMessageNamesV1 = []string{
	"WorkerHandshake", "WorkerHandshakeAck", "FenceRequest", "FenceResponse",
	"WorkerRegisterRequest", "WorkerRegisterResponse", "AssignmentSetInstall",
	"AssignmentSetInstallAck", "WorkerStatusRequest", "WorkerStatus",
	"CheckpointNotice", "CheckpointAck", "ResultRecordChunk", "ResultRecordAck",
	"ResultArtifactChunk", "ResultArtifactAck", "ResultFetchRequest",
	"ResultFetchChunk", "WorkerError",
}

var workerControlMessageSchemasV1 = []string{
	"NodeID:u16,WorkerEpoch:WorkerEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256",
	"CoordinatorEpoch:CoordinatorEpoch",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,SlotCapacity:u16,CoordinatorEpoch:CoordinatorEpoch,ConsensusFingerprint:sha256,RegistryFingerprint:sha256",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,WorkerRevision:u64,CoordinatorEpoch:CoordinatorEpoch,Accepted:bool",
	"Assignment:AssignmentSet,Specification:TopologySpec,SpecificationDigest:sha256,JobControlRevision:u64,SchedulingState:u8,CoordinatorEpoch:CoordinatorEpoch",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,JobID:JobID,AssignmentRevision:u64,AssignmentDigest:sha256,JobControlRevision:u64,SchedulingState:u8,CoordinatorEpoch:CoordinatorEpoch",
	"CoordinatorEpoch:CoordinatorEpoch,AfterTransactionID:u64,MaxEvents:u16,Body:oneof(none,ResultInventoryQuery,RepairGrant)",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch,StoreTransactionID:u64,AfterTransactionID:u64,Assignments:list(InstalledAssignmentStatus),Events:list(WorkerEvent),LastTransactionID:u64,HasMore:bool,Body:oneof(none,ResultInventorySummary,ResultRepairStatus)",
	"Notice:CheckpointNotice,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,JobID:JobID,Source:TaskID,Watermark:u64,RaftIndex:u64,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256,CoordinatorEpoch:CoordinatorEpoch",
	"Transfer:TransferChunk,Record:ResultRecord,Provenance:ResultCopyProvenance,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,RepairID:optional(16),RepairInstructionDigest:optional(sha256)",
	"TransferID:16,NodeID:u16,WorkerEpoch:WorkerEpoch,RepairID:optional(16),RepairInstructionDigest:optional(sha256),NextOffset:u64,TotalLength:u64,Checksum:sha256,Complete:bool,CoordinatorEpoch:CoordinatorEpoch",
	"Transfer:TransferChunk,Artifact:ResultArtifact,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch",
	"TransferID:16,NodeID:u16,WorkerEpoch:WorkerEpoch,Artifact:ResultArtifact,NextOffset:u64,Complete:bool,CoordinatorEpoch:CoordinatorEpoch",
	"Artifact:ResultArtifact,ReplicaNodeID:u16,ReplicaWorkerEpoch:WorkerEpoch,Offset:u64,CoordinatorEpoch:CoordinatorEpoch",
	"Transfer:TransferChunk,Artifact:ResultArtifact,SourceNodeID:u16,SourceWorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch",
	"NodeID:u16,WorkerEpoch:WorkerEpoch,CoordinatorEpoch:CoordinatorEpoch,RelatedMessage:u16,Code:u16,Retryable:bool,Detail:bytes16",
}

var workerControlRulesV1 = []string{
	"status-events-strictly-increasing-after-cursor",
	"status-page-last-transaction-and-has-more-consistent",
	"checkpoint-vectors-sorted-unique",
	"aggregate-count-zero-iff-bytes-zero",
	"empty-aggregate-digest-binds-query-or-instruction",
	"repair-grant-epoch-equals-current-epoch",
	"completed-repair-equals-instruction-summary",
	"result-record-chunks-are-exact-canonical-stream-slices",
	"result-record-ack-binds-nonzero-stream-total-and-checksum",
	"transfer-offset-final-and-checksum-correlated",
}

var workerControlNestedSchemasV1 = []string{
	"WorkerEpoch=bytes16(nonzero)",
	"CoordinatorEpoch=Term:u64,BeginIndex:u64,Coordinator:u16,Nonce:bytes16",
	"JobID=bytes16(nonzero)",
	"TaskID=JobID:JobID,StageID:u16,Partition:u16",
	"TupleID=JobID:JobID,SourceTask:TaskID,SourceSequence:u64,PathDigest:sha256",
	"AssignmentToken=Task:TaskID,WorkerID:u16,WorkerEpoch:WorkerEpoch,Attempt:u64,SpecificationHash:sha256,AssignmentRevision:u64",
	"ResultReplicaSet=SinkTask:TaskID,PrimaryNodeID:u16,SecondaryNodeID:u16,PrimaryEpoch:WorkerEpoch,SecondaryEpoch:WorkerEpoch",
	"AssignmentSet=JobID:JobID,Revision:u64,Digest:sha256,Tasks:list(AssignmentToken),ResultReplicas:list(ResultReplicaSet)",
	"TopologySpec=canonical-topology-v1:bytes",
	"CheckpointNotice=JobID:JobID,Source:TaskID,Watermark:u64,RaftIndex:u64,Epoch:CoordinatorEpoch",
	"WorkerEvent=WorkerID:u16,WorkerEpoch:WorkerEpoch,TransactionID:u64,Kind:u8,Body:oneof(CompletionReport,JobFailureReport)",
	"CompletionReport=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,Source:TaskID,Token:AssignmentToken,Epoch:CoordinatorEpoch,ExpectedCheckpointRevision:u64,Prior:u64,New:u64,EOF:u64,WorkerTransactionID:u64,Digest:sha256",
	"JobFailureReport=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,Task:AssignmentToken,Epoch:CoordinatorEpoch,TransactionID:u64,Code:u16,DetailDigest:sha256",
	"SourceCheckpoint=Source:TaskID,Watermark:u64",
	"ResultInventoryQuery=JobID:JobID,SinkTask:TaskID,SpecificationHash:sha256,AssignmentRevision:u64,AssignmentDigest:sha256,Checkpoints:list(SourceCheckpoint),CheckpointDigest:sha256,QueryDigest:sha256",
	"ResultInventorySummary=QueryDigest:sha256,RecordCount:u64,TotalBytes:u64,ContentDigest:sha256",
	"RepairResultPartition=RepairID:bytes16,CoordinatorEpoch:CoordinatorEpoch,JobID:JobID,AssignmentRevision:u64,AssignmentDigest:sha256,SourceNodeID:u16,SourceWorkerEpoch:WorkerEpoch,DestinationNodeID:u16,DestinationWorkerEpoch:WorkerEpoch,SinkTask:TaskID,SpecificationHash:sha256,Checkpoints:list(SourceCheckpoint),CheckpointDigest:sha256,InventoryQueryDigest:sha256,ExpectedRecordCount:u64,ExpectedTotalBytes:u64,ExpectedContentDigest:sha256,InstructionDigest:sha256",
	"RepairGrant=Instruction:RepairResultPartition,Role:u8",
	"ResultRepairStatus=Instruction:RepairResultPartition,RepairID:bytes16,InstructionDigest:sha256,Role:u8,State:u8,RecordCount:u64,TotalBytes:u64,ContentDigest:sha256,ErrorCode:u16",
	"InstalledAssignmentStatus=JobID:JobID,JobControlRevision:u64,AssignmentRevision:u64,AssignmentDigest:sha256,SpecificationDigest:sha256,SchedulingState:u8",
	"TransferChunk=TransferID:bytes16,JobID:JobID,TotalLength:u64,Checksum:sha256,Offset:u64,Data:bytes32,Final:bool",
	"ResultRecord=TupleID:TupleID,SinkTask:TaskID,SpecificationHash:sha256,Value:bytes16,Checksum:sha256",
	"ResultRecordStream=SchemaVersion:u16,TupleID:TupleID,SinkTask:TaskID,SpecificationHash:sha256,Value:bytes16,LogicalChecksum:sha256",
	"ResultCopyProvenance=AssignmentRevision:u64,AssignmentDigest:sha256,ReplicaSet:ResultReplicaSet,DestinationRole:u8,CoordinatorEpoch:CoordinatorEpoch",
	"ResultArtifact=JobID:JobID,SinkTask:TaskID,SpecificationHash:sha256,RecordCount:u64,TotalLength:u64,Checksum:sha256",
}

// WorkerControlContractV1 returns the immutable v1 +5 worker-control contract.
func WorkerControlContractV1() WorkerControlContract {
	messages := make([]WorkerControlMessageDescriptor, 19)
	for index := range messages {
		messages[index] = WorkerControlMessageDescriptor{Name: workerControlMessageNamesV1[index], MessageType: uint16(200 + index), SchemaVersion: 1, Schema: workerControlMessageSchemasV1[index]}
	}
	return WorkerControlContract{
		SchemaVersion:                   WorkerControlSchemaVersionV1,
		MessageTypeMin:                  200,
		MessageTypeMax:                  218,
		ReservedMessageType:             219,
		Messages:                        messages,
		NestedSchemas:                   append([]string(nil), workerControlNestedSchemasV1...),
		MaxStatusEvents:                 WorkerControlMaxStatusEventsV1,
		MaxCheckpointVectorEntries:      WorkerControlMaxCheckpointsV1,
		MaxTransferChunkBytes:           WorkerControlMaxTransferChunkV1,
		MaxTransferTotalBytes:           WorkerControlMaxTransferTotalV1,
		MaxErrorDetailBytes:             WorkerControlMaxErrorDetailV1,
		MaxControlFrameBytes:            WorkerControlMaxFrameBytesV1,
		AllowZeroCheckpointWatermark:    true,
		AllowEmptyResultArtifact:        true,
		ResultRecordStreamSchemaVersion: 1,
		IdentityDomains:                 append([]string(nil), workerControlIdentityDomainsV1...),
		Rules:                           append([]string(nil), workerControlRulesV1...),
	}
}

func canonicalWorkerControlContractBytes(contract WorkerControlContract) []byte {
	encoded := appendString([]byte(workerControlContractFingerprintDomain), "crane-worker-control")
	encoded = appendUint16(encoded, contract.SchemaVersion)
	encoded = appendUint16(encoded, contract.MessageTypeMin)
	encoded = appendUint16(encoded, contract.MessageTypeMax)
	encoded = appendUint16(encoded, contract.ReservedMessageType)
	encoded = appendUint16(encoded, uint16(len(contract.Messages)))
	for _, message := range contract.Messages {
		encoded = appendString(encoded, message.Name)
		encoded = appendUint16(encoded, message.MessageType)
		encoded = appendUint16(encoded, message.SchemaVersion)
		encoded = appendString(encoded, message.Schema)
	}
	encoded = appendUint16(encoded, uint16(len(contract.NestedSchemas)))
	for _, schema := range contract.NestedSchemas {
		encoded = appendString(encoded, schema)
	}
	encoded = appendUint16(encoded, contract.MaxStatusEvents)
	encoded = appendUint16(encoded, contract.MaxCheckpointVectorEntries)
	encoded = appendUint64(encoded, contract.MaxTransferChunkBytes)
	encoded = appendUint64(encoded, contract.MaxTransferTotalBytes)
	encoded = appendUint64(encoded, contract.MaxErrorDetailBytes)
	encoded = appendUint64(encoded, contract.MaxControlFrameBytes)
	encoded = appendBool(encoded, contract.AllowZeroCheckpointWatermark)
	encoded = appendBool(encoded, contract.AllowEmptyResultArtifact)
	encoded = appendUint16(encoded, contract.ResultRecordStreamSchemaVersion)
	encoded = appendUint16(encoded, uint16(len(contract.IdentityDomains)))
	for _, domain := range contract.IdentityDomains {
		encoded = appendString(encoded, domain)
	}
	encoded = appendUint16(encoded, uint16(len(contract.Rules)))
	for _, rule := range contract.Rules {
		encoded = appendString(encoded, rule)
	}
	return encoded
}

// SourceCheckpoint is one sorted checkpoint-vector entry.
type SourceCheckpoint struct {
	Source    TaskID
	Watermark uint64
}

// ResultInventoryQueryDefinition is the dependency-leaf inventory identity.
type ResultInventoryQueryDefinition struct {
	JobID              JobID
	SinkTask           TaskID
	SpecificationHash  [32]byte
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	Checkpoints        []SourceCheckpoint
	CheckpointDigest   [32]byte
}

// CheckpointVectorDigest hashes the canonical ordered checkpoint vector.
func CheckpointVectorDigest(checkpoints []SourceCheckpoint) [32]byte {
	encoded := []byte(workerControlIdentityDomainsV1[0] + "\x00")
	encoded = appendUint16(encoded, uint16(len(checkpoints)))
	for _, checkpoint := range checkpoints {
		encoded = appendTaskID(encoded, checkpoint.Source)
		encoded = appendUint64(encoded, checkpoint.Watermark)
	}
	return sha256.Sum256(encoded)
}

// ResultInventoryQueryDigest binds an inventory query and checkpoint vector.
func ResultInventoryQueryDigest(query ResultInventoryQueryDefinition) [32]byte {
	encoded := []byte(workerControlIdentityDomainsV1[1] + "\x00")
	encoded = append(encoded, query.JobID[:]...)
	encoded = appendTaskID(encoded, query.SinkTask)
	encoded = append(encoded, query.SpecificationHash[:]...)
	encoded = appendUint64(encoded, query.AssignmentRevision)
	encoded = append(encoded, query.AssignmentDigest[:]...)
	encoded = appendUint16(encoded, uint16(len(query.Checkpoints)))
	for _, checkpoint := range query.Checkpoints {
		encoded = appendTaskID(encoded, checkpoint.Source)
		encoded = appendUint64(encoded, checkpoint.Watermark)
	}
	encoded = append(encoded, query.CheckpointDigest[:]...)
	return sha256.Sum256(encoded)
}

// EmptyResultInventoryDigest is the sole digest for an empty context-bound
// inventory or repair result.
func EmptyResultInventoryDigest(contextDigest [32]byte) [32]byte {
	return sha256.Sum256(append([]byte(workerControlIdentityDomainsV1[2]+"\x00"), contextDigest[:]...))
}

// RepairResultPartitionDefinition contains every field that identifies one
// bilateral historical repair, independent of either endpoint role.
type RepairResultPartitionDefinition struct {
	RepairID               [16]byte
	CoordinatorEpoch       CoordinatorEpoch
	JobID                  JobID
	AssignmentRevision     uint64
	AssignmentDigest       [32]byte
	SourceNodeID           uint16
	SourceWorkerEpoch      WorkerEpoch
	DestinationNodeID      uint16
	DestinationWorkerEpoch WorkerEpoch
	SinkTask               TaskID
	SpecificationHash      [32]byte
	Checkpoints            []SourceCheckpoint
	CheckpointDigest       [32]byte
	InventoryQueryDigest   [32]byte
	ExpectedRecordCount    uint64
	ExpectedTotalBytes     uint64
	ExpectedContentDigest  [32]byte
}

// DeriveRepairID derives the stable role-independent repair identity.
func DeriveRepairID(definition RepairResultPartitionDefinition) [16]byte {
	sum := sha256.Sum256(appendRepairResultPartitionDefinition([]byte(workerControlIdentityDomainsV1[3]+"\x00"), definition, false))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

// RepairInstructionDigest binds the complete current repair instruction.
func RepairInstructionDigest(definition RepairResultPartitionDefinition) [32]byte {
	return sha256.Sum256(appendRepairResultPartitionDefinition([]byte(workerControlIdentityDomainsV1[4]+"\x00"), definition, true))
}

func appendRepairResultPartitionDefinition(encoded []byte, definition RepairResultPartitionDefinition, includeID bool) []byte {
	if includeID {
		encoded = append(encoded, definition.RepairID[:]...)
	}
	encoded = appendCoordinatorEpoch(encoded, definition.CoordinatorEpoch)
	encoded = append(encoded, definition.JobID[:]...)
	encoded = appendUint64(encoded, definition.AssignmentRevision)
	encoded = append(encoded, definition.AssignmentDigest[:]...)
	encoded = appendUint16(encoded, definition.SourceNodeID)
	encoded = append(encoded, definition.SourceWorkerEpoch[:]...)
	encoded = appendUint16(encoded, definition.DestinationNodeID)
	encoded = append(encoded, definition.DestinationWorkerEpoch[:]...)
	encoded = appendTaskID(encoded, definition.SinkTask)
	encoded = append(encoded, definition.SpecificationHash[:]...)
	encoded = appendUint16(encoded, uint16(len(definition.Checkpoints)))
	for _, checkpoint := range definition.Checkpoints {
		encoded = appendTaskID(encoded, checkpoint.Source)
		encoded = appendUint64(encoded, checkpoint.Watermark)
	}
	encoded = append(encoded, definition.CheckpointDigest[:]...)
	encoded = append(encoded, definition.InventoryQueryDigest[:]...)
	encoded = appendUint64(encoded, definition.ExpectedRecordCount)
	encoded = appendUint64(encoded, definition.ExpectedTotalBytes)
	return append(encoded, definition.ExpectedContentDigest[:]...)
}
