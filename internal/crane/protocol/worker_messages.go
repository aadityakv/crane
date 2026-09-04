package protocol

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/wire"
)

const (
	// WorkerControlSchemaVersion is the schema version stamped on every worker-control payload.
	WorkerControlSchemaVersion uint16 = model.WorkerControlSchemaVersionV1
	// MaxWorkerControlPayloadBytes bounds a worker-control payload so the framed message fits one wire frame.
	MaxWorkerControlPayloadBytes = model.WorkerControlMaxFrameBytesV1 - wire.FixedHeaderSize - wire.MACSize
	// MaxWorkerStatusEvents caps the events carried by one WorkerStatus page.
	MaxWorkerStatusEvents = model.WorkerControlMaxStatusEventsV1
	// MaxInventoryCheckpoints caps the checkpoint vector carried by inventory queries and repair instructions.
	MaxInventoryCheckpoints = model.WorkerControlMaxCheckpointsV1
	// MaxTransferChunkBytes caps the data carried by one TransferChunk.
	MaxTransferChunkBytes = model.WorkerControlMaxTransferChunkV1
	// MaxTransferTotalBytes caps the total length of any single result transfer.
	MaxTransferTotalBytes = model.WorkerControlMaxTransferTotalV1
	// MaxWorkerErrorDetailBytes caps the free-form detail attached to a WorkerError.
	MaxWorkerErrorDetailBytes = model.WorkerControlMaxErrorDetailV1
)

var (
	// ErrMalformedWorkerMessage reports a payload that is not the canonical encoding of any message.
	ErrMalformedWorkerMessage = errors.New("malformed Crane worker-control message")
	// ErrUnsupportedWorkerSchema reports a payload stamped with a schema version this build does not speak.
	ErrUnsupportedWorkerSchema = errors.New("unsupported Crane worker-control schema")
	// ErrUnexpectedWorkerMessage reports a message type outside the worker-control range or different from the one expected.
	ErrUnexpectedWorkerMessage = errors.New("unexpected Crane worker-control message type")
	// ErrInvalidWorkerMessage reports a well-formed message whose fields violate the worker-control contract.
	ErrInvalidWorkerMessage = errors.New("invalid Crane worker-control message")
	// ErrWorkerMessageTooLarge reports a payload larger than MaxWorkerControlPayloadBytes.
	ErrWorkerMessageTooLarge = errors.New("crane worker-control message too large")
)

// WorkerMessage is implemented by every message that travels over a worker-control session.
type WorkerMessage interface{ MessageType() wire.MessageType }

// TransferID identifies one result transfer across the chunks and acknowledgements that make it up.
type TransferID [16]byte

// WorkerHandshake opens a worker-control session by declaring the worker's identity and build fingerprints.
type WorkerHandshake struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

// MessageType reports the wire type that carries a WorkerHandshake.
func (WorkerHandshake) MessageType() wire.MessageType { return wire.MessageCraneWorkerHandshake }

// WorkerHandshakeAck answers a WorkerHandshake with the accepting side's identity, capacity, and fingerprints.
type WorkerHandshakeAck struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	SlotCapacity         uint16
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

// MessageType reports the wire type that carries a WorkerHandshakeAck.
func (WorkerHandshakeAck) MessageType() wire.MessageType { return wire.MessageCraneWorkerHandshakeAck }

// FenceRequest asks a worker to fence itself to the named coordinator epoch and reject older ones.
type FenceRequest struct{ CoordinatorEpoch model.CoordinatorEpoch }

// MessageType reports the wire type that carries a FenceRequest.
func (FenceRequest) MessageType() wire.MessageType { return wire.MessageCraneWorkerFenceRequest }

// FenceResponse confirms the coordinator epoch a worker is now fenced to.
type FenceResponse struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	CoordinatorEpoch model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a FenceResponse.
func (FenceResponse) MessageType() wire.MessageType { return wire.MessageCraneWorkerFenceResponse }

// WorkerRegisterRequest asks the coordinator to register a worker epoch with the given slot capacity.
type WorkerRegisterRequest struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	SlotCapacity         uint16
	CoordinatorEpoch     model.CoordinatorEpoch
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

// MessageType reports the wire type that carries a WorkerRegisterRequest.
func (WorkerRegisterRequest) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerRegisterRequest
}

// WorkerRegisterResponse reports whether a registration was accepted and the worker revision it produced.
type WorkerRegisterResponse struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	WorkerRevision   uint64
	CoordinatorEpoch model.CoordinatorEpoch
	Accepted         bool
}

// MessageType reports the wire type that carries a WorkerRegisterResponse.
func (WorkerRegisterResponse) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerRegisterResponse
}

// AssignmentSetInstall delivers a job's topology and assignment set to a worker together with the scheduling state to run it under.
type AssignmentSetInstall struct {
	Assignment          model.AssignmentSet
	Specification       model.TopologySpec
	SpecificationDigest [32]byte
	JobControlRevision  uint64
	SchedulingState     model.SchedulingState
	CoordinatorEpoch    model.CoordinatorEpoch
}

// MessageType reports the wire type that carries an AssignmentSetInstall.
func (AssignmentSetInstall) MessageType() wire.MessageType {
	return wire.MessageCraneAssignmentSetInstall
}

// AssignmentSetInstallAck confirms which assignment revision and scheduling state a worker has installed for a job.
type AssignmentSetInstallAck struct {
	NodeID             uint16
	WorkerEpoch        model.WorkerEpoch
	JobID              model.JobID
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	JobControlRevision uint64
	SchedulingState    model.SchedulingState
	CoordinatorEpoch   model.CoordinatorEpoch
}

// MessageType reports the wire type that carries an AssignmentSetInstallAck.
func (AssignmentSetInstallAck) MessageType() wire.MessageType {
	return wire.MessageCraneAssignmentSetInstallAck
}

// SourceCheckpoint is the per-source watermark entry carried by inventory queries and repair instructions.
type SourceCheckpoint = model.SourceCheckpoint

// ResultInventoryQuery asks a worker to summarize the result records it holds for a sink task at a checkpoint vector.
type ResultInventoryQuery struct {
	JobID              model.JobID
	SinkTask           model.TaskID
	SpecificationHash  [32]byte
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	Checkpoints        []SourceCheckpoint
	CheckpointDigest   [32]byte
	QueryDigest        [32]byte
}

// ResultInventorySummary answers a ResultInventoryQuery with the count, size, and content digest of the matching records.
type ResultInventorySummary struct {
	QueryDigest   [32]byte
	RecordCount   uint64
	TotalBytes    uint64
	ContentDigest [32]byte
}

// RepairEndpointRole names which end of a result repair a worker plays.
type RepairEndpointRole uint8

const (
	// RepairSource streams the repaired records to the destination.
	RepairSource RepairEndpointRole = iota + 1
	// RepairDestination receives and persists the repaired records.
	RepairDestination
)

// RepairResultPartition instructs one worker to copy a sink task's result partition to another, bound to the inventory that justified it.
type RepairResultPartition struct {
	RepairID               [16]byte
	CoordinatorEpoch       model.CoordinatorEpoch
	JobID                  model.JobID
	AssignmentRevision     uint64
	AssignmentDigest       [32]byte
	SourceNodeID           uint16
	SourceWorkerEpoch      model.WorkerEpoch
	DestinationNodeID      uint16
	DestinationWorkerEpoch model.WorkerEpoch
	SinkTask               model.TaskID
	SpecificationHash      [32]byte
	Checkpoints            []SourceCheckpoint
	CheckpointDigest       [32]byte
	InventoryQueryDigest   [32]byte
	ExpectedRecordCount    uint64
	ExpectedTotalBytes     uint64
	ExpectedContentDigest  [32]byte
	InstructionDigest      [32]byte
}

// RepairGrant hands a worker a repair instruction together with the role it must play in it.
type RepairGrant struct {
	Instruction RepairResultPartition
	Role        RepairEndpointRole
}

// ResultRepairState tracks how far a granted repair has progressed on a worker.
type ResultRepairState uint8

const (
	// RepairPending means the repair was accepted but no records have moved yet.
	RepairPending ResultRepairState = iota + 1
	// RepairStreaming means records are in flight between the endpoints.
	RepairStreaming
	// RepairComplete means the destination holds the full expected partition.
	RepairComplete
	// RepairFailed means the repair stopped with the reported error code.
	RepairFailed
)

// ResultRepairStatus reports a worker's progress on a granted repair together with the aggregate it has seen so far.
type ResultRepairStatus struct {
	Instruction       RepairResultPartition
	RepairID          [16]byte
	InstructionDigest [32]byte
	Role              RepairEndpointRole
	State             ResultRepairState
	RecordCount       uint64
	TotalBytes        uint64
	ContentDigest     [32]byte
	ErrorCode         WorkerErrorCode
}

// WorkerStatusRequest polls a worker for its status page after a transaction cursor, optionally attaching an inventory query or repair grant.
type WorkerStatusRequest struct {
	CoordinatorEpoch   model.CoordinatorEpoch
	AfterTransactionID uint64
	MaxEvents          uint16
	Inventory          *ResultInventoryQuery
	Repair             *RepairGrant
}

// MessageType reports the wire type that carries a WorkerStatusRequest.
func (WorkerStatusRequest) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerStatusRequest
}

// InstalledAssignmentStatus summarizes the revision and scheduling state a worker currently holds for one job.
type InstalledAssignmentStatus struct {
	JobID               model.JobID
	JobControlRevision  uint64
	AssignmentRevision  uint64
	AssignmentDigest    [32]byte
	SpecificationDigest [32]byte
	SchedulingState     model.SchedulingState
}

// WorkerStatus is one page of a worker's installed assignments and ordered events, plus any inventory or repair answer.
type WorkerStatus struct {
	NodeID             uint16
	WorkerEpoch        model.WorkerEpoch
	CoordinatorEpoch   model.CoordinatorEpoch
	StoreTransactionID uint64
	AfterTransactionID uint64
	Assignments        []InstalledAssignmentStatus
	Events             []model.WorkerEvent
	LastTransactionID  uint64
	HasMore            bool
	// AdmissionEpoch is the coordinator epoch the worker's process admission
	// gate is currently open under; the zero epoch reports a closed gate (a
	// fresh or fenced process that has not yet received its Running install).
	AdmissionEpoch model.CoordinatorEpoch
	Inventory      *ResultInventorySummary
	Repair         *ResultRepairStatus
}

// MessageType reports the wire type that carries a WorkerStatus.
func (WorkerStatus) MessageType() wire.MessageType { return wire.MessageCraneWorkerStatusReport }

// CheckpointNotice tells a worker that a source checkpoint was committed under a specific assignment revision.
type CheckpointNotice struct {
	Notice             model.CheckpointNotice
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
}

// MessageType reports the wire type that carries a CheckpointNotice.
func (CheckpointNotice) MessageType() wire.MessageType { return wire.MessageCraneCheckpointNotice }

// CheckpointAck confirms that a worker applied a checkpoint notice for one source task.
type CheckpointAck struct {
	NodeID             uint16
	WorkerEpoch        model.WorkerEpoch
	JobID              model.JobID
	Source             model.TaskID
	Watermark          uint64
	RaftIndex          uint64
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	CoordinatorEpoch   model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a CheckpointAck.
func (CheckpointAck) MessageType() wire.MessageType { return wire.MessageCraneCheckpointAck }

// TransferChunk is one contiguous slice of a checksummed byte stream being transferred between workers.
type TransferChunk struct {
	TransferID  TransferID
	JobID       model.JobID
	TotalLength uint64
	Checksum    [32]byte
	Offset      uint64
	Data        []byte
	Final       bool
}

// ResultRecordChunk carries part of a canonical result record stream to a replica, with the provenance that authorizes the copy.
type ResultRecordChunk struct {
	Transfer                TransferChunk
	Record                  model.ResultRecord
	Provenance              model.ResultCopyProvenance
	DestinationNodeID       uint16
	DestinationWorkerEpoch  model.WorkerEpoch
	RepairID                [16]byte
	RepairInstructionDigest [32]byte
}

// MessageType reports the wire type that carries a ResultRecordChunk.
func (ResultRecordChunk) MessageType() wire.MessageType { return wire.MessageCraneResultRecordChunk }

// ResultRecordAck acknowledges a ResultRecordChunk and reports the next offset the receiver expects.
type ResultRecordAck struct {
	TransferID              TransferID
	NodeID                  uint16
	WorkerEpoch             model.WorkerEpoch
	RepairID                [16]byte
	RepairInstructionDigest [32]byte
	NextOffset              uint64
	TotalLength             uint64
	Checksum                [32]byte
	Complete                bool
	CoordinatorEpoch        model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a ResultRecordAck.
func (ResultRecordAck) MessageType() wire.MessageType { return wire.MessageCraneResultRecordAck }

// ResultArtifact identifies a sink task's sealed result stream by its size and checksum.
type ResultArtifact struct {
	JobID             model.JobID
	SinkTask          model.TaskID
	SpecificationHash [32]byte
	RecordCount       uint64
	TotalLength       uint64
	Checksum          [32]byte
}

// ResultArtifactChunk carries part of a sealed result artifact to a replica.
type ResultArtifactChunk struct {
	Transfer               TransferChunk
	Artifact               ResultArtifact
	DestinationNodeID      uint16
	DestinationWorkerEpoch model.WorkerEpoch
	CoordinatorEpoch       model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a ResultArtifactChunk.
func (ResultArtifactChunk) MessageType() wire.MessageType {
	return wire.MessageCraneResultArtifactChunk
}

// ResultArtifactAck acknowledges a ResultArtifactChunk and reports the next offset the receiver expects.
type ResultArtifactAck struct {
	TransferID       TransferID
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	Artifact         ResultArtifact
	NextOffset       uint64
	Complete         bool
	CoordinatorEpoch model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a ResultArtifactAck.
func (ResultArtifactAck) MessageType() wire.MessageType { return wire.MessageCraneResultArtifactAck }

// ResultFetchRequest asks a replica to stream a sealed result artifact from the given offset.
type ResultFetchRequest struct {
	Artifact           ResultArtifact
	ReplicaNodeID      uint16
	ReplicaWorkerEpoch model.WorkerEpoch
	Offset             uint64
	CoordinatorEpoch   model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a ResultFetchRequest.
func (ResultFetchRequest) MessageType() wire.MessageType { return wire.MessageCraneResultFetchRequest }

// ResultFetchChunk carries part of a sealed result artifact in answer to a ResultFetchRequest.
type ResultFetchChunk struct {
	Transfer          TransferChunk
	Artifact          ResultArtifact
	SourceNodeID      uint16
	SourceWorkerEpoch model.WorkerEpoch
	CoordinatorEpoch  model.CoordinatorEpoch
}

// MessageType reports the wire type that carries a ResultFetchChunk.
func (ResultFetchChunk) MessageType() wire.MessageType { return wire.MessageCraneResultFetchChunk }

// WorkerErrorCode classifies why a worker rejected or failed a worker-control message.
type WorkerErrorCode uint16

const (
	// WorkerErrorMalformed reports a message that could not be decoded or validated.
	WorkerErrorMalformed WorkerErrorCode = iota + 1
	// WorkerErrorUnauthorized reports a peer that is not allowed to send the message.
	WorkerErrorUnauthorized
	// WorkerErrorStaleEpoch reports a message from a coordinator or worker epoch that has been superseded.
	WorkerErrorStaleEpoch
	// WorkerErrorStaleAssignment reports a message bound to an assignment revision the worker no longer holds.
	WorkerErrorStaleAssignment
	// WorkerErrorCapacity reports that the worker has no room to accept the request right now.
	WorkerErrorCapacity
	// WorkerErrorUnavailable reports that the requested data or service is not present on the worker.
	WorkerErrorUnavailable
	// WorkerErrorCorrupt reports that the worker found its local copy of the requested data damaged.
	WorkerErrorCorrupt
)

// WorkerError tells a peer that a related message was rejected, with a code and whether a retry may succeed.
type WorkerError struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	CoordinatorEpoch model.CoordinatorEpoch
	RelatedMessage   wire.MessageType
	Code             WorkerErrorCode
	Retryable        bool
	Detail           []byte
}

// MessageType reports the wire type that carries a WorkerError.
func (WorkerError) MessageType() wire.MessageType { return wire.MessageCraneWorkerError }

// CheckpointVectorDigest computes the digest that binds a checkpoint vector into inventory queries and repairs.
func CheckpointVectorDigest(entries []SourceCheckpoint) [32]byte {
	return model.CheckpointVectorDigest(entries)
}

// InventoryQueryDigest computes the digest that identifies an inventory query independent of who asked it.
func InventoryQueryDigest(query ResultInventoryQuery) [32]byte {
	return model.ResultInventoryQueryDigest(model.ResultInventoryQueryDefinition{
		JobID: query.JobID, SinkTask: query.SinkTask, SpecificationHash: query.SpecificationHash,
		AssignmentRevision: query.AssignmentRevision, AssignmentDigest: query.AssignmentDigest,
		Checkpoints: query.Checkpoints, CheckpointDigest: query.CheckpointDigest,
	})
}

// DeriveRepairID derives the deterministic repair identifier for an instruction from its identity fields.
func DeriveRepairID(instruction RepairResultPartition) [16]byte {
	return model.DeriveRepairID(repairDefinition(instruction))
}

// RepairInstructionDigest computes the digest that a worker must echo to prove it acted on exactly this instruction.
func RepairInstructionDigest(instruction RepairResultPartition) [32]byte {
	return model.RepairInstructionDigest(repairDefinition(instruction))
}

func repairDefinition(r RepairResultPartition) model.RepairResultPartitionDefinition {
	return model.RepairResultPartitionDefinition{
		RepairID: r.RepairID, CoordinatorEpoch: r.CoordinatorEpoch, JobID: r.JobID,
		AssignmentRevision: r.AssignmentRevision, AssignmentDigest: r.AssignmentDigest,
		SourceNodeID: r.SourceNodeID, SourceWorkerEpoch: r.SourceWorkerEpoch,
		DestinationNodeID: r.DestinationNodeID, DestinationWorkerEpoch: r.DestinationWorkerEpoch,
		SinkTask: r.SinkTask, SpecificationHash: r.SpecificationHash, Checkpoints: r.Checkpoints,
		CheckpointDigest: r.CheckpointDigest, InventoryQueryDigest: r.InventoryQueryDigest,
		ExpectedRecordCount: r.ExpectedRecordCount, ExpectedTotalBytes: r.ExpectedTotalBytes,
		ExpectedContentDigest: r.ExpectedContentDigest,
	}
}

func validateHandshake(node uint16, epoch model.WorkerEpoch, consensus, registry [32]byte) error {
	if node == 0 {
		return errors.New("zero node")
	}
	if err := epoch.Validate(); err != nil {
		return err
	}
	if consensus == ([32]byte{}) || consensus != model.ConsensusFingerprint() {
		return errors.New("consensus fingerprint mismatch")
	}
	if registry == ([32]byte{}) || registry != model.RegistryFingerprint() {
		return errors.New("registry fingerprint mismatch")
	}
	return nil
}
func validateScheduling(s model.SchedulingState) error {
	if s < model.Closed || s > model.Draining {
		return errors.New("unknown scheduling state")
	}
	return nil
}
func validateCheckpoints(job model.JobID, entries []SourceCheckpoint, digest [32]byte) error {
	if len(entries) == 0 || len(entries) > MaxInventoryCheckpoints {
		return errors.New("checkpoint vector outside bounds")
	}
	for i, e := range entries {
		if e.Source.Validate() != nil || e.Source.JobID != job {
			return errors.New("invalid checkpoint entry")
		}
		if i > 0 && !taskLess(entries[i-1].Source, e.Source) {
			return errors.New("checkpoint vector not sorted unique")
		}
	}
	if digest == ([32]byte{}) || digest != CheckpointVectorDigest(entries) {
		return errors.New("checkpoint digest mismatch")
	}
	return nil
}
func taskLess(a, b model.TaskID) bool {
	if a.JobID != b.JobID {
		return string(a.JobID[:]) < string(b.JobID[:])
	}
	if a.StageID != b.StageID {
		return a.StageID < b.StageID
	}
	return a.Partition < b.Partition
}
func (q ResultInventoryQuery) validate() error {
	if q.JobID.Validate() != nil || q.SinkTask.Validate() != nil || q.SinkTask.JobID != q.JobID || q.SpecificationHash == ([32]byte{}) || q.AssignmentRevision == 0 || q.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid inventory identity")
	}
	if err := validateCheckpoints(q.JobID, q.Checkpoints, q.CheckpointDigest); err != nil {
		return err
	}
	if q.QueryDigest == ([32]byte{}) || q.QueryDigest != InventoryQueryDigest(q) {
		return errors.New("inventory query digest mismatch")
	}
	return nil
}
func (r RepairResultPartition) validate() error {
	if r.JobID.Validate() != nil || r.SinkTask.Validate() != nil || r.SinkTask.JobID != r.JobID || r.AssignmentRevision == 0 || r.AssignmentDigest == ([32]byte{}) || r.SpecificationHash == ([32]byte{}) || r.SourceNodeID == 0 || r.DestinationNodeID == 0 || r.SourceNodeID == r.DestinationNodeID || r.ExpectedTotalBytes > MaxTransferTotalBytes || r.ExpectedContentDigest == ([32]byte{}) {
		return errors.New("invalid repair identity")
	}
	if r.CoordinatorEpoch.Validate() != nil || r.SourceWorkerEpoch.Validate() != nil || r.DestinationWorkerEpoch.Validate() != nil {
		return errors.New("invalid repair epoch")
	}
	if err := validateCheckpoints(r.JobID, r.Checkpoints, r.CheckpointDigest); err != nil {
		return err
	}
	wantQuery := InventoryQueryDigest(ResultInventoryQuery{JobID: r.JobID, SinkTask: r.SinkTask, SpecificationHash: r.SpecificationHash, AssignmentRevision: r.AssignmentRevision, AssignmentDigest: r.AssignmentDigest, Checkpoints: r.Checkpoints, CheckpointDigest: r.CheckpointDigest})
	if r.InventoryQueryDigest == ([32]byte{}) || r.InventoryQueryDigest != wantQuery {
		return errors.New("repair inventory query digest mismatch")
	}
	if err := validateResultAggregate(r.ExpectedRecordCount, r.ExpectedTotalBytes, r.ExpectedContentDigest, r.InventoryQueryDigest); err != nil {
		return err
	}
	if r.RepairID == ([16]byte{}) || r.RepairID != DeriveRepairID(r) {
		return errors.New("repair ID mismatch")
	}
	if r.InstructionDigest == ([32]byte{}) || r.InstructionDigest != RepairInstructionDigest(r) {
		return errors.New("repair instruction digest mismatch")
	}
	return nil
}

func validateResultAggregate(count, total uint64, digest, context [32]byte) error {
	if total > MaxTransferTotalBytes || (count == 0) != (total == 0) {
		return errors.New("result aggregate count/bytes mismatch")
	}
	if count == 0 {
		if digest != model.EmptyResultInventoryDigest(context) {
			return errors.New("empty result aggregate digest mismatch")
		}
		return nil
	}
	if digest == ([32]byte{}) {
		return errors.New("zero nonempty result aggregate digest")
	}
	return nil
}
func (t TransferChunk) validate() error {
	if t.TransferID == ([16]byte{}) || t.JobID.Validate() != nil || t.TotalLength > MaxTransferTotalBytes || t.Checksum == ([32]byte{}) || len(t.Data) > MaxTransferChunkBytes || t.Offset > t.TotalLength || uint64(len(t.Data)) > t.TotalLength-t.Offset {
		return errors.New("invalid transfer bounds")
	}
	if t.TotalLength == 0 {
		if t.Offset != 0 || len(t.Data) != 0 || !t.Final || t.Checksum != sha256.Sum256(nil) {
			return errors.New("invalid empty transfer")
		}
		return nil
	}
	if len(t.Data) == 0 {
		return errors.New("empty nonterminal transfer chunk")
	}
	end := t.Offset + uint64(len(t.Data))
	if t.Final != (end == t.TotalLength) {
		return errors.New("final flag/offset mismatch")
	}
	return nil
}
func (a ResultArtifact) validate() error {
	if a.JobID.Validate() != nil || a.SinkTask.Validate() != nil || a.SinkTask.JobID != a.JobID || a.SpecificationHash == ([32]byte{}) || a.TotalLength > MaxTransferTotalBytes || a.Checksum == ([32]byte{}) {
		return errors.New("invalid artifact")
	}
	if a.TotalLength == 0 && (a.RecordCount != 0 || a.Checksum != sha256.Sum256(nil)) || a.TotalLength > 0 && a.RecordCount == 0 {
		return errors.New("artifact count/length mismatch")
	}
	return nil
}
func validateReplicaDestination(p model.ResultCopyProvenance, node uint16, epoch model.WorkerEpoch) error {
	switch p.DestinationRole {
	case model.PrimaryReplica:
		if node != p.ReplicaSet.PrimaryNodeID || epoch != p.ReplicaSet.PrimaryEpoch {
			return errors.New("primary destination mismatch")
		}
	case model.SecondaryReplica:
		if node != p.ReplicaSet.SecondaryNodeID || epoch != p.ReplicaSet.SecondaryEpoch {
			return errors.New("secondary destination mismatch")
		}
	default:
		return errors.New("unknown replica role")
	}
	return nil
}

func validateWorkerMessage(message WorkerMessage) error {
	switch m := message.(type) {
	case WorkerHandshake:
		return validateHandshake(m.NodeID, m.WorkerEpoch, m.ConsensusFingerprint, m.RegistryFingerprint)
	case WorkerHandshakeAck:
		if err := validateHandshake(m.NodeID, m.WorkerEpoch, m.ConsensusFingerprint, m.RegistryFingerprint); err != nil {
			return err
		}
		if m.SlotCapacity == 0 || uint64(m.SlotCapacity) > model.LimitsV1().MaxWorkerSlots {
			return errors.New("slot capacity outside bounds")
		}
		return nil
	case FenceRequest:
		return m.CoordinatorEpoch.Validate()
	case FenceResponse:
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil {
			return errors.New("invalid fence worker")
		}
		return m.CoordinatorEpoch.Validate()
	case WorkerRegisterRequest:
		if err := validateHandshake(m.NodeID, m.WorkerEpoch, m.ConsensusFingerprint, m.RegistryFingerprint); err != nil {
			return err
		}
		if m.SlotCapacity == 0 || uint64(m.SlotCapacity) > model.LimitsV1().MaxWorkerSlots {
			return errors.New("slot capacity outside bounds")
		}
		return m.CoordinatorEpoch.Validate()
	case WorkerRegisterResponse:
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.WorkerRevision == 0 {
			return errors.New("invalid registration response")
		}
		return m.CoordinatorEpoch.Validate()
	case AssignmentSetInstall:
		return m.validate()
	case AssignmentSetInstallAck:
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.JobID.Validate() != nil || m.AssignmentRevision == 0 || m.AssignmentDigest == ([32]byte{}) || m.JobControlRevision == 0 {
			return errors.New("invalid assignment ACK")
		}
		if err := validateScheduling(m.SchedulingState); err != nil {
			return err
		}
		return m.CoordinatorEpoch.Validate()
	case WorkerStatusRequest:
		return m.validate()
	case WorkerStatus:
		return m.validate()
	case CheckpointNotice:
		if err := m.Notice.Validate(); err != nil {
			return err
		}
		if m.JobControlRevision == 0 || m.AssignmentRevision == 0 || m.AssignmentDigest == ([32]byte{}) {
			return errors.New("invalid checkpoint assignment fence")
		}
		return nil
	case CheckpointAck:
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.JobID.Validate() != nil || m.Source.Validate() != nil || m.Source.JobID != m.JobID || m.RaftIndex == 0 || m.JobControlRevision == 0 || m.AssignmentRevision == 0 || m.AssignmentDigest == ([32]byte{}) {
			return errors.New("invalid checkpoint ACK")
		}
		return m.CoordinatorEpoch.Validate()
	case ResultRecordChunk:
		return m.validate()
	case ResultRecordAck:
		return m.validate()
	case ResultArtifactChunk:
		return m.validate()
	case ResultArtifactAck:
		return m.validate()
	case ResultFetchRequest:
		return m.validate()
	case ResultFetchChunk:
		return m.validate()
	case WorkerError:
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.CoordinatorEpoch.Validate() != nil || m.RelatedMessage < 200 || m.RelatedMessage > 218 || m.Code < WorkerErrorMalformed || m.Code > WorkerErrorCorrupt || len(m.Detail) > MaxWorkerErrorDetailBytes {
			return errors.New("invalid worker error")
		}
		return nil
	default:
		return ErrUnexpectedWorkerMessage
	}
}

func (m AssignmentSetInstall) validate() error {
	validated, err := model.ValidateTopology(m.Specification)
	if err != nil {
		return err
	}
	if m.SpecificationDigest == ([32]byte{}) || m.SpecificationDigest != validated.Digest() {
		return errors.New("specification digest mismatch")
	}
	if m.Specification.RegistryFingerprint != model.RegistryFingerprint() {
		return errors.New("registry mismatch")
	}
	if err := m.Assignment.Validate(validated); err != nil {
		return err
	}
	if m.JobControlRevision == 0 {
		return errors.New("zero job-control revision")
	}
	if err := validateScheduling(m.SchedulingState); err != nil {
		return err
	}
	return m.CoordinatorEpoch.Validate()
}
func (m WorkerStatusRequest) validate() error {
	if m.CoordinatorEpoch.Validate() != nil || m.MaxEvents == 0 || m.MaxEvents > MaxWorkerStatusEvents {
		return errors.New("invalid status request")
	}
	if m.Inventory != nil && m.Repair != nil {
		return errors.New("inventory and repair are exclusive")
	}
	if m.Inventory != nil {
		return m.Inventory.validate()
	}
	if m.Repair != nil {
		if m.Repair.Role < RepairSource || m.Repair.Role > RepairDestination {
			return errors.New("invalid repair role")
		}
		if m.Repair.Instruction.CoordinatorEpoch != m.CoordinatorEpoch {
			return errors.New("repair grant coordinator epoch mismatch")
		}
		return m.Repair.Instruction.validate()
	}
	return nil
}
func (m WorkerStatus) validate() error {
	if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.CoordinatorEpoch.Validate() != nil || m.StoreTransactionID == 0 || len(m.Assignments) > int(model.LimitsV1().MaxRetainedJobs) || len(m.Events) > MaxWorkerStatusEvents || m.Inventory != nil && m.Repair != nil {
		return errors.New("invalid worker status bounds")
	}
	if m.AdmissionEpoch != (model.CoordinatorEpoch{}) && m.AdmissionEpoch.Validate() != nil {
		return errors.New("invalid admission epoch")
	}
	for i, a := range m.Assignments {
		if a.JobID.Validate() != nil || a.JobControlRevision == 0 || a.AssignmentRevision == 0 || a.AssignmentDigest == ([32]byte{}) || a.SpecificationDigest == ([32]byte{}) || validateScheduling(a.SchedulingState) != nil {
			return errors.New("invalid installed assignment")
		}
		if i > 0 && !jobLess(m.Assignments[i-1].JobID, a.JobID) {
			return errors.New("assignments not sorted unique")
		}
	}
	prior := m.AfterTransactionID
	for _, e := range m.Events {
		if err := e.Validate(); err != nil {
			return err
		}
		if e.WorkerID != m.NodeID || e.WorkerEpoch != m.WorkerEpoch || e.TransactionID <= prior {
			return errors.New("events not globally ordered")
		}
		prior = e.TransactionID
	}
	if len(m.Events) > 0 && m.LastTransactionID != prior {
		return errors.New("last transaction mismatch")
	}
	if len(m.Events) == 0 && (m.HasMore || m.LastTransactionID != m.AfterTransactionID) {
		return errors.New("empty page cursor mismatch")
	}
	if m.LastTransactionID < m.AfterTransactionID || m.LastTransactionID > m.StoreTransactionID {
		return errors.New("cursor exceeds store transaction")
	}
	if m.Inventory != nil {
		if m.Inventory.QueryDigest == ([32]byte{}) {
			return errors.New("invalid inventory query digest")
		}
		if err := validateResultAggregate(m.Inventory.RecordCount, m.Inventory.TotalBytes, m.Inventory.ContentDigest, m.Inventory.QueryDigest); err != nil {
			return err
		}
	}
	if m.Repair != nil {
		if err := m.Repair.Instruction.validate(); err != nil {
			return err
		}
		if m.Repair.Instruction.CoordinatorEpoch != m.CoordinatorEpoch {
			return errors.New("repair status coordinator epoch mismatch")
		}
		if m.Repair.RepairID != m.Repair.Instruction.RepairID || m.Repair.InstructionDigest != m.Repair.Instruction.InstructionDigest || m.Repair.Role < RepairSource || m.Repair.Role > RepairDestination || m.Repair.State < RepairPending || m.Repair.State > RepairFailed {
			return errors.New("invalid repair status")
		}
		failed := m.Repair.State == RepairFailed
		validError := m.Repair.ErrorCode >= WorkerErrorMalformed && m.Repair.ErrorCode <= WorkerErrorCorrupt
		if failed != validError {
			return errors.New("repair state/error mismatch")
		}
		context := m.Repair.InstructionDigest
		if m.Repair.State == RepairComplete {
			context = m.Repair.Instruction.InventoryQueryDigest
		}
		if err := validateResultAggregate(m.Repair.RecordCount, m.Repair.TotalBytes, m.Repair.ContentDigest, context); err != nil {
			return err
		}
		if m.Repair.State == RepairComplete && (m.Repair.RecordCount != m.Repair.Instruction.ExpectedRecordCount || m.Repair.TotalBytes != m.Repair.Instruction.ExpectedTotalBytes || m.Repair.ContentDigest != m.Repair.Instruction.ExpectedContentDigest) {
			return errors.New("completed repair summary mismatch")
		}
	}
	return nil
}
func jobLess(a, b model.JobID) bool { return string(a[:]) < string(b[:]) }
func (m ResultRecordChunk) validate() error {
	if err := m.Transfer.validate(); err != nil {
		return err
	}
	if err := m.Record.Validate(); err != nil {
		return err
	}
	if m.Transfer.JobID != m.Record.TupleID.JobID {
		return errors.New("record transfer mismatch")
	}
	stream, err := model.MarshalResultRecord(m.Record)
	if err != nil {
		return err
	}
	wantChecksum := sha256.Sum256(stream)
	end := m.Transfer.Offset + uint64(len(m.Transfer.Data))
	if m.Transfer.TotalLength != uint64(len(stream)) || m.Transfer.Checksum != wantChecksum || end > uint64(len(stream)) || !bytes.Equal(m.Transfer.Data, stream[m.Transfer.Offset:end]) {
		return errors.New("record transfer is not an exact canonical stream slice")
	}
	if err := m.Provenance.Validate(m.Record); err != nil {
		return err
	}
	if err := validateReplicaDestination(m.Provenance, m.DestinationNodeID, m.DestinationWorkerEpoch); err != nil {
		return err
	}
	zeroID := m.RepairID == ([16]byte{})
	zeroDigest := m.RepairInstructionDigest == ([32]byte{})
	if zeroID != zeroDigest {
		return errors.New("partial repair binding")
	}
	return nil
}
func (m ResultRecordAck) validate() error {
	if m.TransferID == ([16]byte{}) || m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.TotalLength == 0 || m.TotalLength > MaxTransferTotalBytes || m.NextOffset > m.TotalLength || m.Complete != (m.NextOffset == m.TotalLength) || m.Checksum == ([32]byte{}) || m.CoordinatorEpoch.Validate() != nil {
		return errors.New("invalid record ACK")
	}
	if (m.RepairID == ([16]byte{})) != (m.RepairInstructionDigest == ([32]byte{})) {
		return errors.New("partial repair ACK binding")
	}
	return nil
}

// ValidateResultRecordAckCorrelation validates an ACK against the exact
// canonical record transfer it advances.
func ValidateResultRecordAckCorrelation(chunk ResultRecordChunk, ack ResultRecordAck) error {
	if err := chunk.validate(); err != nil {
		return err
	}
	if err := ack.validate(); err != nil {
		return err
	}
	chunkLength := uint64(len(chunk.Transfer.Data))
	if chunk.Transfer.Offset > ^uint64(0)-chunkLength {
		return errors.New("record chunk end offset overflow")
	}
	expectedNextOffset := chunk.Transfer.Offset + chunkLength
	if ack.TransferID != chunk.Transfer.TransferID || ack.NodeID != chunk.DestinationNodeID || ack.WorkerEpoch != chunk.DestinationWorkerEpoch || ack.RepairID != chunk.RepairID || ack.RepairInstructionDigest != chunk.RepairInstructionDigest || ack.TotalLength != chunk.Transfer.TotalLength || ack.Checksum != chunk.Transfer.Checksum || ack.CoordinatorEpoch != chunk.Provenance.CoordinatorEpoch {
		return errors.New("record ACK does not bind the canonical transfer")
	}
	if ack.NextOffset != expectedNextOffset || ack.Complete != chunk.Transfer.Final {
		return errors.New("record ACK does not advance the supplied chunk")
	}
	return nil
}
func (m ResultArtifactChunk) validate() error {
	if err := m.Transfer.validate(); err != nil {
		return err
	}
	if err := m.Artifact.validate(); err != nil {
		return err
	}
	if m.Transfer.JobID != m.Artifact.JobID || m.Transfer.TotalLength != m.Artifact.TotalLength || m.Transfer.Checksum != m.Artifact.Checksum || m.DestinationNodeID == 0 || m.DestinationWorkerEpoch.Validate() != nil {
		return errors.New("artifact transfer mismatch")
	}
	return m.CoordinatorEpoch.Validate()
}
func (m ResultArtifactAck) validate() error {
	if m.TransferID == ([16]byte{}) || m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.Artifact.validate() != nil || m.NextOffset > m.Artifact.TotalLength || m.Complete != (m.NextOffset == m.Artifact.TotalLength) {
		return errors.New("invalid artifact ACK")
	}
	return m.CoordinatorEpoch.Validate()
}
func (m ResultFetchRequest) validate() error {
	if m.Artifact.validate() != nil || m.ReplicaNodeID == 0 || m.ReplicaWorkerEpoch.Validate() != nil || m.Offset > m.Artifact.TotalLength || m.Artifact.TotalLength > 0 && m.Offset == m.Artifact.TotalLength {
		return errors.New("invalid fetch request")
	}
	return m.CoordinatorEpoch.Validate()
}
func (m ResultFetchChunk) validate() error {
	if err := m.Transfer.validate(); err != nil {
		return err
	}
	if m.Artifact.validate() != nil || m.Transfer.JobID != m.Artifact.JobID || m.Transfer.TotalLength != m.Artifact.TotalLength || m.Transfer.Checksum != m.Artifact.Checksum || m.SourceNodeID == 0 || m.SourceWorkerEpoch.Validate() != nil {
		return errors.New("invalid fetch chunk")
	}
	return m.CoordinatorEpoch.Validate()
}

func invalidWorker(message WorkerMessage, err error) error {
	return fmt.Errorf("%w: message %d: %v", ErrInvalidWorkerMessage, message.MessageType(), err)
}
