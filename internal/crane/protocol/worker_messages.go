package protocol

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	WorkerControlSchemaVersion   uint16 = 1
	MaxWorkerControlPayloadBytes        = (1 << 20) - wire.FixedHeaderSize - wire.MACSize
	MaxWorkerStatusEvents               = 256
	MaxInventoryCheckpoints             = 256
	MaxTransferChunkBytes               = 256 << 10
	MaxTransferTotalBytes               = 64 << 20
	MaxWorkerErrorDetailBytes           = 256
)

var (
	ErrMalformedWorkerMessage  = errors.New("malformed Crane worker-control message")
	ErrUnsupportedWorkerSchema = errors.New("unsupported Crane worker-control schema")
	ErrUnexpectedWorkerMessage = errors.New("unexpected Crane worker-control message type")
	ErrInvalidWorkerMessage    = errors.New("invalid Crane worker-control message")
	ErrWorkerMessageTooLarge   = errors.New("Crane worker-control message too large")
)

type WorkerMessage interface{ MessageType() wire.MessageType }
type TransferID [16]byte

type WorkerHandshake struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

func (WorkerHandshake) MessageType() wire.MessageType { return wire.MessageCraneWorkerHandshake }

type WorkerHandshakeAck struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

func (WorkerHandshakeAck) MessageType() wire.MessageType { return wire.MessageCraneWorkerHandshakeAck }

type FenceRequest struct{ CoordinatorEpoch model.CoordinatorEpoch }

func (FenceRequest) MessageType() wire.MessageType { return wire.MessageCraneWorkerFenceRequest }

type FenceResponse struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	CoordinatorEpoch model.CoordinatorEpoch
}

func (FenceResponse) MessageType() wire.MessageType { return wire.MessageCraneWorkerFenceResponse }

type WorkerRegisterRequest struct {
	NodeID               uint16
	WorkerEpoch          model.WorkerEpoch
	SlotCapacity         uint16
	CoordinatorEpoch     model.CoordinatorEpoch
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

func (WorkerRegisterRequest) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerRegisterRequest
}

type WorkerRegisterResponse struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	WorkerRevision   uint64
	CoordinatorEpoch model.CoordinatorEpoch
	Accepted         bool
}

func (WorkerRegisterResponse) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerRegisterResponse
}

type AssignmentSetInstall struct {
	Assignment          model.AssignmentSet
	Specification       model.TopologySpec
	SpecificationDigest [32]byte
	JobControlRevision  uint64
	SchedulingState     model.SchedulingState
	CoordinatorEpoch    model.CoordinatorEpoch
}

func (AssignmentSetInstall) MessageType() wire.MessageType {
	return wire.MessageCraneAssignmentSetInstall
}

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

func (AssignmentSetInstallAck) MessageType() wire.MessageType {
	return wire.MessageCraneAssignmentSetInstallAck
}

type SourceCheckpoint struct {
	Source    model.TaskID
	Watermark uint64
}
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
type ResultInventorySummary struct {
	QueryDigest   [32]byte
	RecordCount   uint64
	TotalBytes    uint64
	ContentDigest [32]byte
}
type RepairEndpointRole uint8

const (
	RepairSource RepairEndpointRole = iota + 1
	RepairDestination
)

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
	ExpectedRecordCount    uint64
	ExpectedTotalBytes     uint64
	ExpectedContentDigest  [32]byte
	InstructionDigest      [32]byte
}
type RepairGrant struct {
	Instruction RepairResultPartition
	Role        RepairEndpointRole
}
type ResultRepairState uint8

const (
	RepairPending ResultRepairState = iota + 1
	RepairStreaming
	RepairComplete
	RepairFailed
)

type ResultRepairStatus struct {
	RepairID          [16]byte
	InstructionDigest [32]byte
	Role              RepairEndpointRole
	State             ResultRepairState
	RecordCount       uint64
	TotalBytes        uint64
	ContentDigest     [32]byte
	ErrorCode         WorkerErrorCode
}

type WorkerStatusRequest struct {
	CoordinatorEpoch   model.CoordinatorEpoch
	AfterTransactionID uint64
	MaxEvents          uint16
	Inventory          *ResultInventoryQuery
	Repair             *RepairGrant
}

func (WorkerStatusRequest) MessageType() wire.MessageType {
	return wire.MessageCraneWorkerStatusRequest
}

type InstalledAssignmentStatus struct {
	JobID               model.JobID
	JobControlRevision  uint64
	AssignmentRevision  uint64
	AssignmentDigest    [32]byte
	SpecificationDigest [32]byte
	SchedulingState     model.SchedulingState
}
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
	Inventory          *ResultInventorySummary
	Repair             *ResultRepairStatus
}

func (WorkerStatus) MessageType() wire.MessageType { return wire.MessageCraneWorkerStatusReport }

type CheckpointNotice struct {
	Notice             model.CheckpointNotice
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
}

func (CheckpointNotice) MessageType() wire.MessageType { return wire.MessageCraneCheckpointNotice }

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

func (CheckpointAck) MessageType() wire.MessageType { return wire.MessageCraneCheckpointAck }

type TransferChunk struct {
	TransferID  TransferID
	JobID       model.JobID
	TotalLength uint64
	Checksum    [32]byte
	Offset      uint64
	Data        []byte
	Final       bool
}
type ResultRecordChunk struct {
	Transfer                TransferChunk
	Record                  model.ResultRecord
	Provenance              model.ResultCopyProvenance
	DestinationNodeID       uint16
	DestinationWorkerEpoch  model.WorkerEpoch
	RepairID                [16]byte
	RepairInstructionDigest [32]byte
}

func (ResultRecordChunk) MessageType() wire.MessageType { return wire.MessageCraneResultRecordChunk }

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

func (ResultRecordAck) MessageType() wire.MessageType { return wire.MessageCraneResultRecordAck }

type ResultArtifact struct {
	JobID             model.JobID
	SinkTask          model.TaskID
	SpecificationHash [32]byte
	RecordCount       uint64
	TotalLength       uint64
	Checksum          [32]byte
}
type ResultArtifactChunk struct {
	Transfer               TransferChunk
	Artifact               ResultArtifact
	DestinationNodeID      uint16
	DestinationWorkerEpoch model.WorkerEpoch
	CoordinatorEpoch       model.CoordinatorEpoch
}

func (ResultArtifactChunk) MessageType() wire.MessageType {
	return wire.MessageCraneResultArtifactChunk
}

type ResultArtifactAck struct {
	TransferID       TransferID
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	Artifact         ResultArtifact
	NextOffset       uint64
	Complete         bool
	CoordinatorEpoch model.CoordinatorEpoch
}

func (ResultArtifactAck) MessageType() wire.MessageType { return wire.MessageCraneResultArtifactAck }

type ResultFetchRequest struct {
	Artifact           ResultArtifact
	ReplicaNodeID      uint16
	ReplicaWorkerEpoch model.WorkerEpoch
	Offset             uint64
	CoordinatorEpoch   model.CoordinatorEpoch
}

func (ResultFetchRequest) MessageType() wire.MessageType { return wire.MessageCraneResultFetchRequest }

type ResultFetchChunk struct {
	Transfer          TransferChunk
	Artifact          ResultArtifact
	SourceNodeID      uint16
	SourceWorkerEpoch model.WorkerEpoch
	CoordinatorEpoch  model.CoordinatorEpoch
}

func (ResultFetchChunk) MessageType() wire.MessageType { return wire.MessageCraneResultFetchChunk }

type WorkerErrorCode uint16

const (
	WorkerErrorMalformed WorkerErrorCode = iota + 1
	WorkerErrorUnauthorized
	WorkerErrorStaleEpoch
	WorkerErrorStaleAssignment
	WorkerErrorCapacity
	WorkerErrorUnavailable
	WorkerErrorCorrupt
)

type WorkerError struct {
	NodeID           uint16
	WorkerEpoch      model.WorkerEpoch
	CoordinatorEpoch model.CoordinatorEpoch
	RelatedMessage   wire.MessageType
	Code             WorkerErrorCode
	Retryable        bool
	Detail           []byte
}

func (WorkerError) MessageType() wire.MessageType { return wire.MessageCraneWorkerError }

func CheckpointVectorDigest(entries []SourceCheckpoint) [32]byte {
	return sha256.Sum256(appendCheckpointVector([]byte("cs425/crane/checkpoint-vector/v1\x00"), entries))
}
func InventoryQueryDigest(query ResultInventoryQuery) [32]byte {
	b := append([]byte("cs425/crane/result-inventory-query/v1\x00"), query.JobID[:]...)
	b = appendTaskIDWorker(b, query.SinkTask)
	b = append(b, query.SpecificationHash[:]...)
	b = appendUint64Worker(b, query.AssignmentRevision)
	b = append(b, query.AssignmentDigest[:]...)
	b = appendCheckpointVector(b, query.Checkpoints)
	b = append(b, query.CheckpointDigest[:]...)
	return sha256.Sum256(b)
}
func DeriveRepairID(instruction RepairResultPartition) [16]byte {
	sum := sha256.Sum256(appendRepairDefinition([]byte("cs425/crane/repair-id/v1\x00"), instruction, false))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}
func RepairInstructionDigest(instruction RepairResultPartition) [32]byte {
	return sha256.Sum256(appendRepairDefinition([]byte("cs425/crane/repair-instruction/v1\x00"), instruction, true))
}

func appendCheckpointVector(b []byte, entries []SourceCheckpoint) []byte {
	b = appendUint16Worker(b, uint16(len(entries)))
	for _, entry := range entries {
		b = appendTaskIDWorker(b, entry.Source)
		b = appendUint64Worker(b, entry.Watermark)
	}
	return b
}
func appendRepairDefinition(b []byte, r RepairResultPartition, includeID bool) []byte {
	if includeID {
		b = append(b, r.RepairID[:]...)
	}
	b = appendCoordinatorWorker(b, r.CoordinatorEpoch)
	b = append(b, r.JobID[:]...)
	b = appendUint64Worker(b, r.AssignmentRevision)
	b = append(b, r.AssignmentDigest[:]...)
	b = appendUint16Worker(b, r.SourceNodeID)
	b = append(b, r.SourceWorkerEpoch[:]...)
	b = appendUint16Worker(b, r.DestinationNodeID)
	b = append(b, r.DestinationWorkerEpoch[:]...)
	b = appendTaskIDWorker(b, r.SinkTask)
	b = append(b, r.SpecificationHash[:]...)
	b = appendCheckpointVector(b, r.Checkpoints)
	b = append(b, r.CheckpointDigest[:]...)
	b = appendUint64Worker(b, r.ExpectedRecordCount)
	b = appendUint64Worker(b, r.ExpectedTotalBytes)
	return append(b, r.ExpectedContentDigest[:]...)
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
	if r.RepairID == ([16]byte{}) || r.RepairID != DeriveRepairID(r) {
		return errors.New("repair ID mismatch")
	}
	if r.InstructionDigest == ([32]byte{}) || r.InstructionDigest != RepairInstructionDigest(r) {
		return errors.New("repair instruction digest mismatch")
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
		return validateHandshake(m.NodeID, m.WorkerEpoch, m.ConsensusFingerprint, m.RegistryFingerprint)
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
		if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.JobID.Validate() != nil || m.Source.Validate() != nil || m.Source.JobID != m.JobID || m.Watermark == 0 || m.RaftIndex == 0 || m.JobControlRevision == 0 || m.AssignmentRevision == 0 || m.AssignmentDigest == ([32]byte{}) {
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
		return m.Repair.Instruction.validate()
	}
	return nil
}
func (m WorkerStatus) validate() error {
	if m.NodeID == 0 || m.WorkerEpoch.Validate() != nil || m.CoordinatorEpoch.Validate() != nil || m.StoreTransactionID == 0 || len(m.Assignments) > int(model.LimitsV1().MaxRetainedJobs) || len(m.Events) > MaxWorkerStatusEvents || m.Inventory != nil && m.Repair != nil {
		return errors.New("invalid worker status bounds")
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
		if m.Inventory.QueryDigest == ([32]byte{}) || m.Inventory.ContentDigest == ([32]byte{}) || m.Inventory.TotalBytes > MaxTransferTotalBytes {
			return errors.New("invalid inventory summary")
		}
	}
	if m.Repair != nil {
		if m.Repair.RepairID == ([16]byte{}) || m.Repair.InstructionDigest == ([32]byte{}) || m.Repair.Role < RepairSource || m.Repair.Role > RepairDestination || m.Repair.State < RepairPending || m.Repair.State > RepairFailed || m.Repair.TotalBytes > MaxTransferTotalBytes {
			return errors.New("invalid repair status")
		}
		failed := m.Repair.State == RepairFailed
		validError := m.Repair.ErrorCode >= WorkerErrorMalformed && m.Repair.ErrorCode <= WorkerErrorCorrupt
		if failed != validError || m.Repair.State == RepairComplete && m.Repair.ContentDigest == ([32]byte{}) || m.Repair.State != RepairFailed && (m.Repair.RecordCount != 0 || m.Repair.TotalBytes != 0) && m.Repair.ContentDigest == ([32]byte{}) {
			return errors.New("repair state/error mismatch")
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

func appendUint16Worker(b []byte, v uint16) []byte { return append(b, byte(v>>8), byte(v)) }
func appendUint64Worker(b []byte, v uint64) []byte {
	for shift := 56; shift >= 0; shift -= 8 {
		b = append(b, byte(v>>uint(shift)))
	}
	return b
}
func appendTaskIDWorker(b []byte, t model.TaskID) []byte {
	b = append(b, t.JobID[:]...)
	b = appendUint16Worker(b, t.StageID)
	return appendUint16Worker(b, t.Partition)
}
func appendCoordinatorWorker(b []byte, e model.CoordinatorEpoch) []byte {
	b = appendUint64Worker(b, e.Term)
	b = appendUint64Worker(b, e.BeginIndex)
	b = appendUint16Worker(b, e.Coordinator)
	return append(b, e.Nonce[:]...)
}

func invalidWorker(message WorkerMessage, err error) error {
	return fmt.Errorf("%w: message %d: %v", ErrInvalidWorkerMessage, message.MessageType(), err)
}
