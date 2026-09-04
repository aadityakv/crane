package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/wire"
)

// MarshalWorkerMessage validates and emits the sole canonical payload for IDs 200-218.
func MarshalWorkerMessage(message WorkerMessage) ([]byte, error) {
	if message == nil {
		return nil, ErrUnexpectedWorkerMessage
	}
	if message.MessageType() < wire.MessageCraneWorkerHandshake || message.MessageType() > wire.MessageCraneWorkerError {
		return nil, ErrUnexpectedWorkerMessage
	}
	if err := validateWorkerMessage(message); err != nil {
		return nil, invalidWorker(message, err)
	}
	e := workerEncoder{}
	e.u16(WorkerControlSchemaVersion)
	e.u16(uint16(message.MessageType()))
	if err := e.message(message); err != nil {
		return nil, err
	}
	return e.owned(), nil
}

// UnmarshalWorkerMessage decodes one complete canonical payload for IDs 200-218.
func UnmarshalWorkerMessage(messageType wire.MessageType, encoded []byte) (WorkerMessage, error) {
	if messageType < wire.MessageCraneWorkerHandshake || messageType > wire.MessageCraneWorkerError {
		return nil, ErrUnexpectedWorkerMessage
	}
	if len(encoded) > MaxWorkerControlPayloadBytes {
		return nil, fmt.Errorf("%w: %d", ErrWorkerMessageTooLarge, len(encoded))
	}
	d := workerDecoder{input: encoded}
	version, err := d.u16()
	if err != nil {
		return nil, err
	}
	if version != WorkerControlSchemaVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedWorkerSchema, version)
	}
	got, err := d.u16()
	if err != nil {
		return nil, err
	}
	if wire.MessageType(got) != messageType {
		return nil, fmt.Errorf("%w: got %d want %d", ErrUnexpectedWorkerMessage, got, messageType)
	}
	message, err := d.message(messageType)
	if err != nil {
		return nil, err
	}
	if err := d.finish(); err != nil {
		return nil, err
	}
	if err := validateWorkerMessage(message); err != nil {
		return nil, invalidWorker(message, err)
	}
	canonical, err := MarshalWorkerMessage(message)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("%w: non-canonical payload", ErrMalformedWorkerMessage)
	}
	return message, nil
}

func MarshalWorkerHandshake(v WorkerHandshake) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalWorkerHandshake(b []byte) (WorkerHandshake, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerHandshake, b)
	if e != nil {
		return WorkerHandshake{}, e
	}
	return v.(WorkerHandshake), nil
}
func MarshalWorkerHandshakeAck(v WorkerHandshakeAck) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalWorkerHandshakeAck(b []byte) (WorkerHandshakeAck, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerHandshakeAck, b)
	if e != nil {
		return WorkerHandshakeAck{}, e
	}
	return v.(WorkerHandshakeAck), nil
}
func MarshalFenceRequest(v FenceRequest) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalFenceRequest(b []byte) (FenceRequest, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerFenceRequest, b)
	if e != nil {
		return FenceRequest{}, e
	}
	return v.(FenceRequest), nil
}
func MarshalFenceResponse(v FenceResponse) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalFenceResponse(b []byte) (FenceResponse, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerFenceResponse, b)
	if e != nil {
		return FenceResponse{}, e
	}
	return v.(FenceResponse), nil
}
func MarshalWorkerRegisterRequest(v WorkerRegisterRequest) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalWorkerRegisterRequest(b []byte) (WorkerRegisterRequest, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerRegisterRequest, b)
	if e != nil {
		return WorkerRegisterRequest{}, e
	}
	return v.(WorkerRegisterRequest), nil
}
func MarshalWorkerRegisterResponse(v WorkerRegisterResponse) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalWorkerRegisterResponse(b []byte) (WorkerRegisterResponse, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerRegisterResponse, b)
	if e != nil {
		return WorkerRegisterResponse{}, e
	}
	return v.(WorkerRegisterResponse), nil
}
func MarshalAssignmentSetInstall(v AssignmentSetInstall) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalAssignmentSetInstall(b []byte) (AssignmentSetInstall, error) {
	return unmarshalAssignmentSetInstallWith(b, model.DecodeTopology)
}
func MarshalAssignmentSetInstallAck(v AssignmentSetInstallAck) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalAssignmentSetInstallAck(b []byte) (AssignmentSetInstallAck, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneAssignmentSetInstallAck, b)
	if e != nil {
		return AssignmentSetInstallAck{}, e
	}
	return v.(AssignmentSetInstallAck), nil
}
func MarshalWorkerStatusRequest(v WorkerStatusRequest) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalWorkerStatusRequest(b []byte) (WorkerStatusRequest, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerStatusRequest, b)
	if e != nil {
		return WorkerStatusRequest{}, e
	}
	return v.(WorkerStatusRequest), nil
}
func MarshalWorkerStatus(v WorkerStatus) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalWorkerStatus(b []byte) (WorkerStatus, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerStatusReport, b)
	if e != nil {
		return WorkerStatus{}, e
	}
	return v.(WorkerStatus), nil
}
func MarshalCheckpointNotice(v CheckpointNotice) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalCheckpointNotice(b []byte) (CheckpointNotice, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneCheckpointNotice, b)
	if e != nil {
		return CheckpointNotice{}, e
	}
	return v.(CheckpointNotice), nil
}
func MarshalCheckpointAck(v CheckpointAck) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalCheckpointAck(b []byte) (CheckpointAck, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneCheckpointAck, b)
	if e != nil {
		return CheckpointAck{}, e
	}
	return v.(CheckpointAck), nil
}
func MarshalResultRecordChunk(v ResultRecordChunk) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalResultRecordChunk(b []byte) (ResultRecordChunk, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultRecordChunk, b)
	if e != nil {
		return ResultRecordChunk{}, e
	}
	return v.(ResultRecordChunk), nil
}
func MarshalResultRecordAck(v ResultRecordAck) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalResultRecordAck(b []byte) (ResultRecordAck, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultRecordAck, b)
	if e != nil {
		return ResultRecordAck{}, e
	}
	return v.(ResultRecordAck), nil
}
func MarshalResultArtifactChunk(v ResultArtifactChunk) ([]byte, error) {
	return MarshalWorkerMessage(v)
}
func UnmarshalResultArtifactChunk(b []byte) (ResultArtifactChunk, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultArtifactChunk, b)
	if e != nil {
		return ResultArtifactChunk{}, e
	}
	return v.(ResultArtifactChunk), nil
}
func MarshalResultArtifactAck(v ResultArtifactAck) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalResultArtifactAck(b []byte) (ResultArtifactAck, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultArtifactAck, b)
	if e != nil {
		return ResultArtifactAck{}, e
	}
	return v.(ResultArtifactAck), nil
}
func MarshalResultFetchRequest(v ResultFetchRequest) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalResultFetchRequest(b []byte) (ResultFetchRequest, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultFetchRequest, b)
	if e != nil {
		return ResultFetchRequest{}, e
	}
	return v.(ResultFetchRequest), nil
}
func MarshalResultFetchChunk(v ResultFetchChunk) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalResultFetchChunk(b []byte) (ResultFetchChunk, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneResultFetchChunk, b)
	if e != nil {
		return ResultFetchChunk{}, e
	}
	return v.(ResultFetchChunk), nil
}
func MarshalWorkerError(v WorkerError) ([]byte, error) { return MarshalWorkerMessage(v) }
func UnmarshalWorkerError(b []byte) (WorkerError, error) {
	v, e := UnmarshalWorkerMessage(wire.MessageCraneWorkerError, b)
	if e != nil {
		return WorkerError{}, e
	}
	return v.(WorkerError), nil
}

type topologyDecodeFunc func([]byte) (model.ValidatedTopology, error)

func unmarshalAssignmentSetInstallWith(encoded []byte, decode topologyDecodeFunc) (AssignmentSetInstall, error) {
	if len(encoded) > MaxWorkerControlPayloadBytes {
		return AssignmentSetInstall{}, ErrWorkerMessageTooLarge
	}
	if len(encoded) < 12 {
		return AssignmentSetInstall{}, ErrMalformedWorkerMessage
	}
	if binary.BigEndian.Uint16(encoded[:2]) != WorkerControlSchemaVersion || wire.MessageType(binary.BigEndian.Uint16(encoded[2:4])) != wire.MessageCraneAssignmentSetInstall {
		return AssignmentSetInstall{}, ErrUnexpectedWorkerMessage
	}
	declared := binary.BigEndian.Uint64(encoded[4:12])
	if declared > model.LimitsV1().MaxTopologyBytes-8 {
		return AssignmentSetInstall{}, fmt.Errorf("%w: declared topology %d", ErrWorkerMessageTooLarge, declared)
	}
	if declared > uint64(len(encoded)-12) {
		return AssignmentSetInstall{}, ErrMalformedWorkerMessage
	}
	topoBytes := encoded[4 : 12+declared]
	validated, err := decode(topoBytes)
	if err != nil {
		return AssignmentSetInstall{}, fmt.Errorf("%w: topology: %v", ErrInvalidWorkerMessage, err)
	}
	v, err := UnmarshalWorkerMessage(wire.MessageCraneAssignmentSetInstall, encoded)
	if err != nil {
		return AssignmentSetInstall{}, err
	}
	install := v.(AssignmentSetInstall)
	if install.SpecificationDigest != validated.Digest() {
		return AssignmentSetInstall{}, ErrInvalidWorkerMessage
	}
	return install, nil
}

type workerEncoder struct {
	output []byte
	err    error
}

func (e *workerEncoder) owned() []byte { return append([]byte(nil), e.output...) }
func (e *workerEncoder) add(v []byte) {
	if e.err != nil {
		return
	}
	if len(v) > MaxWorkerControlPayloadBytes-len(e.output) {
		e.err = ErrWorkerMessageTooLarge
		return
	}
	e.output = append(e.output, v...)
}
func (e *workerEncoder) u8(v byte) { e.add([]byte{v}) }
func (e *workerEncoder) bool(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}
func (e *workerEncoder) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	e.add(b[:])
}
func (e *workerEncoder) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	e.add(b[:])
}
func (e *workerEncoder) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	e.add(b[:])
}
func (e *workerEncoder) bytes16(v []byte) {
	if len(v) > math.MaxUint16 {
		e.err = ErrWorkerMessageTooLarge
		return
	}
	e.u16(uint16(len(v)))
	e.add(v)
}
func (e *workerEncoder) bytes32(v []byte) {
	if uint64(len(v)) > math.MaxUint32 {
		e.err = ErrWorkerMessageTooLarge
		return
	}
	e.u32(uint32(len(v)))
	e.add(v)
}
func (e *workerEncoder) epoch(v model.CoordinatorEpoch) {
	e.u64(v.Term)
	e.u64(v.BeginIndex)
	e.u16(v.Coordinator)
	e.add(v.Nonce[:])
}
func (e *workerEncoder) task(v model.TaskID) { e.add(v.JobID[:]); e.u16(v.StageID); e.u16(v.Partition) }
func (e *workerEncoder) tupleID(v model.TupleID) {
	e.add(v.JobID[:])
	e.task(v.SourceTask)
	e.u64(v.SourceSequence)
	e.add(v.PathDigest[:])
}
func (e *workerEncoder) worker(node uint16, epoch model.WorkerEpoch) { e.u16(node); e.add(epoch[:]) }
func (e *workerEncoder) token(v model.AssignmentToken) {
	e.task(v.Task)
	e.worker(v.WorkerID, v.WorkerEpoch)
	e.u64(v.Attempt)
	e.add(v.SpecificationHash[:])
	e.u64(v.AssignmentRevision)
}
func (e *workerEncoder) replica(v model.ResultReplicaSet) {
	e.task(v.SinkTask)
	e.u16(v.PrimaryNodeID)
	e.u16(v.SecondaryNodeID)
	e.add(v.PrimaryEpoch[:])
	e.add(v.SecondaryEpoch[:])
}
func (e *workerEncoder) assignment(v model.AssignmentSet) {
	e.add(v.JobID[:])
	e.u64(v.Revision)
	e.add(v.Digest[:])
	e.u16(uint16(len(v.Tasks)))
	for _, t := range v.Tasks {
		e.token(t)
	}
	e.u16(uint16(len(v.ResultReplicas)))
	for _, r := range v.ResultReplicas {
		e.replica(r)
	}
}
func (e *workerEncoder) checkpoints(v []SourceCheckpoint) {
	e.u16(uint16(len(v)))
	for _, c := range v {
		e.task(c.Source)
		e.u64(c.Watermark)
	}
}
func (e *workerEncoder) inventory(v ResultInventoryQuery) {
	e.add(v.JobID[:])
	e.task(v.SinkTask)
	e.add(v.SpecificationHash[:])
	e.u64(v.AssignmentRevision)
	e.add(v.AssignmentDigest[:])
	e.checkpoints(v.Checkpoints)
	e.add(v.CheckpointDigest[:])
	e.add(v.QueryDigest[:])
}
func (e *workerEncoder) repair(v RepairResultPartition) {
	e.add(v.RepairID[:])
	e.epoch(v.CoordinatorEpoch)
	e.add(v.JobID[:])
	e.u64(v.AssignmentRevision)
	e.add(v.AssignmentDigest[:])
	e.worker(v.SourceNodeID, v.SourceWorkerEpoch)
	e.worker(v.DestinationNodeID, v.DestinationWorkerEpoch)
	e.task(v.SinkTask)
	e.add(v.SpecificationHash[:])
	e.checkpoints(v.Checkpoints)
	e.add(v.CheckpointDigest[:])
	e.add(v.InventoryQueryDigest[:])
	e.u64(v.ExpectedRecordCount)
	e.u64(v.ExpectedTotalBytes)
	e.add(v.ExpectedContentDigest[:])
	e.add(v.InstructionDigest[:])
}
func (e *workerEncoder) transfer(v TransferChunk) {
	e.add(v.TransferID[:])
	e.add(v.JobID[:])
	e.u64(v.TotalLength)
	e.add(v.Checksum[:])
	e.u64(v.Offset)
	e.bytes32(v.Data)
	e.bool(v.Final)
}
func (e *workerEncoder) artifact(v ResultArtifact) {
	e.add(v.JobID[:])
	e.task(v.SinkTask)
	e.add(v.SpecificationHash[:])
	e.u64(v.RecordCount)
	e.u64(v.TotalLength)
	e.add(v.Checksum[:])
}
func (e *workerEncoder) record(v model.ResultRecord) {
	e.tupleID(v.TupleID)
	e.task(v.SinkTask)
	e.add(v.SpecificationHash[:])
	e.bytes16(v.Value)
	e.add(v.Checksum[:])
}
func (e *workerEncoder) provenance(v model.ResultCopyProvenance) {
	e.u64(v.AssignmentRevision)
	e.add(v.AssignmentDigest[:])
	e.replica(v.ReplicaSet)
	e.u8(byte(v.DestinationRole))
	e.epoch(v.CoordinatorEpoch)
}
func (e *workerEncoder) event(v model.WorkerEvent) {
	e.worker(v.WorkerID, v.WorkerEpoch)
	e.u64(v.TransactionID)
	e.u8(byte(v.Kind))
	if v.Completion != nil {
		r := v.Completion
		e.add(r.JobID[:])
		e.u64(r.JobControlRevision)
		e.u64(r.AssignmentRevision)
		e.task(r.Source)
		e.token(r.Token)
		e.epoch(r.Epoch)
		e.u64(r.ExpectedCheckpointRevision)
		e.u64(r.Prior)
		e.u64(r.New)
		e.u64(r.EOF)
		e.u64(r.WorkerTransactionID)
		e.add(r.Digest[:])
	} else {
		r := v.Failure
		e.add(r.JobID[:])
		e.u64(r.JobControlRevision)
		e.u64(r.AssignmentRevision)
		e.token(r.Task)
		e.epoch(r.Epoch)
		e.u64(r.TransactionID)
		e.u16(uint16(r.Code))
		e.add(r.DetailDigest[:])
	}
}

func (e *workerEncoder) message(message WorkerMessage) error {
	switch m := message.(type) {
	case WorkerHandshake:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.add(m.ConsensusFingerprint[:])
		e.add(m.RegistryFingerprint[:])
	case WorkerHandshakeAck:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.u16(m.SlotCapacity)
		e.add(m.ConsensusFingerprint[:])
		e.add(m.RegistryFingerprint[:])
	case FenceRequest:
		e.epoch(m.CoordinatorEpoch)
	case FenceResponse:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.epoch(m.CoordinatorEpoch)
	case WorkerRegisterRequest:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.u16(m.SlotCapacity)
		e.epoch(m.CoordinatorEpoch)
		e.add(m.ConsensusFingerprint[:])
		e.add(m.RegistryFingerprint[:])
	case WorkerRegisterResponse:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.u64(m.WorkerRevision)
		e.epoch(m.CoordinatorEpoch)
		e.bool(m.Accepted)
	case AssignmentSetInstall:
		validated, _ := model.ValidateTopology(m.Specification)
		e.add(validated.CanonicalBytes())
		e.assignment(m.Assignment)
		e.add(m.SpecificationDigest[:])
		e.u64(m.JobControlRevision)
		e.u8(byte(m.SchedulingState))
		e.epoch(m.CoordinatorEpoch)
	case AssignmentSetInstallAck:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.add(m.JobID[:])
		e.u64(m.AssignmentRevision)
		e.add(m.AssignmentDigest[:])
		e.u64(m.JobControlRevision)
		e.u8(byte(m.SchedulingState))
		e.epoch(m.CoordinatorEpoch)
	case WorkerStatusRequest:
		e.epoch(m.CoordinatorEpoch)
		e.u64(m.AfterTransactionID)
		e.u16(m.MaxEvents)
		if m.Inventory != nil {
			e.u8(1)
			e.inventory(*m.Inventory)
		} else if m.Repair != nil {
			e.u8(2)
			e.repair(m.Repair.Instruction)
			e.u8(byte(m.Repair.Role))
		} else {
			e.u8(0)
		}
	case WorkerStatus:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.epoch(m.CoordinatorEpoch)
		e.u64(m.StoreTransactionID)
		e.u64(m.AfterTransactionID)
		e.u16(uint16(len(m.Assignments)))
		for _, a := range m.Assignments {
			e.add(a.JobID[:])
			e.u64(a.JobControlRevision)
			e.u64(a.AssignmentRevision)
			e.add(a.AssignmentDigest[:])
			e.add(a.SpecificationDigest[:])
			e.u8(byte(a.SchedulingState))
		}
		e.u16(uint16(len(m.Events)))
		for _, v := range m.Events {
			e.event(v)
		}
		e.u64(m.LastTransactionID)
		e.bool(m.HasMore)
		e.epoch(m.AdmissionEpoch)
		if m.Inventory != nil {
			e.u8(1)
			e.add(m.Inventory.QueryDigest[:])
			e.u64(m.Inventory.RecordCount)
			e.u64(m.Inventory.TotalBytes)
			e.add(m.Inventory.ContentDigest[:])
		} else if m.Repair != nil {
			e.u8(2)
			e.repair(m.Repair.Instruction)
			e.add(m.Repair.RepairID[:])
			e.add(m.Repair.InstructionDigest[:])
			e.u8(byte(m.Repair.Role))
			e.u8(byte(m.Repair.State))
			e.u64(m.Repair.RecordCount)
			e.u64(m.Repair.TotalBytes)
			e.add(m.Repair.ContentDigest[:])
			e.u16(uint16(m.Repair.ErrorCode))
		} else {
			e.u8(0)
		}
	case CheckpointNotice:
		e.add(m.Notice.JobID[:])
		e.task(m.Notice.Source)
		e.u64(m.Notice.Watermark)
		e.u64(m.Notice.RaftIndex)
		e.epoch(m.Notice.Epoch)
		e.u64(m.JobControlRevision)
		e.u64(m.AssignmentRevision)
		e.add(m.AssignmentDigest[:])
	case CheckpointAck:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.add(m.JobID[:])
		e.task(m.Source)
		e.u64(m.Watermark)
		e.u64(m.RaftIndex)
		e.u64(m.JobControlRevision)
		e.u64(m.AssignmentRevision)
		e.add(m.AssignmentDigest[:])
		e.epoch(m.CoordinatorEpoch)
	case ResultRecordChunk:
		e.transfer(m.Transfer)
		e.record(m.Record)
		e.provenance(m.Provenance)
		e.worker(m.DestinationNodeID, m.DestinationWorkerEpoch)
		e.add(m.RepairID[:])
		e.add(m.RepairInstructionDigest[:])
	case ResultRecordAck:
		e.add(m.TransferID[:])
		e.worker(m.NodeID, m.WorkerEpoch)
		e.add(m.RepairID[:])
		e.add(m.RepairInstructionDigest[:])
		e.u64(m.NextOffset)
		e.u64(m.TotalLength)
		e.add(m.Checksum[:])
		e.bool(m.Complete)
		e.epoch(m.CoordinatorEpoch)
	case ResultArtifactChunk:
		e.transfer(m.Transfer)
		e.artifact(m.Artifact)
		e.worker(m.DestinationNodeID, m.DestinationWorkerEpoch)
		e.epoch(m.CoordinatorEpoch)
	case ResultArtifactAck:
		e.add(m.TransferID[:])
		e.worker(m.NodeID, m.WorkerEpoch)
		e.artifact(m.Artifact)
		e.u64(m.NextOffset)
		e.bool(m.Complete)
		e.epoch(m.CoordinatorEpoch)
	case ResultFetchRequest:
		e.artifact(m.Artifact)
		e.worker(m.ReplicaNodeID, m.ReplicaWorkerEpoch)
		e.u64(m.Offset)
		e.epoch(m.CoordinatorEpoch)
	case ResultFetchChunk:
		e.transfer(m.Transfer)
		e.artifact(m.Artifact)
		e.worker(m.SourceNodeID, m.SourceWorkerEpoch)
		e.epoch(m.CoordinatorEpoch)
	case WorkerError:
		e.worker(m.NodeID, m.WorkerEpoch)
		e.epoch(m.CoordinatorEpoch)
		e.u16(uint16(m.RelatedMessage))
		e.u16(uint16(m.Code))
		e.bool(m.Retryable)
		e.bytes16(m.Detail)
	default:
		return ErrUnexpectedWorkerMessage
	}
	return e.err
}

type workerDecoder struct {
	input  []byte
	offset int
}

func (d *workerDecoder) remaining() int { return len(d.input) - d.offset }
func (d *workerDecoder) take(n int) ([]byte, error) {
	if n < 0 || n > d.remaining() {
		return nil, fmt.Errorf("%w: truncated field", ErrMalformedWorkerMessage)
	}
	v := d.input[d.offset : d.offset+n]
	d.offset += n
	return v, nil
}
func (d *workerDecoder) finish() error {
	if d.remaining() != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedWorkerMessage, d.remaining())
	}
	return nil
}
func (d *workerDecoder) u8() (byte, error) {
	v, e := d.take(1)
	if e != nil {
		return 0, e
	}
	return v[0], nil
}
func (d *workerDecoder) bool() (bool, error) {
	v, e := d.u8()
	if e != nil {
		return false, e
	}
	if v > 1 {
		return false, ErrMalformedWorkerMessage
	}
	return v == 1, nil
}
func (d *workerDecoder) u16() (uint16, error) {
	v, e := d.take(2)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint16(v), nil
}
func (d *workerDecoder) u32() (uint32, error) {
	v, e := d.take(4)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint32(v), nil
}
func (d *workerDecoder) u64() (uint64, error) {
	v, e := d.take(8)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint64(v), nil
}
func (d *workerDecoder) fixed16() (v [16]byte, err error) {
	b, err := d.take(16)
	if err == nil {
		copy(v[:], b)
	}
	return
}
func (d *workerDecoder) fixed32() (v [32]byte, err error) {
	b, err := d.take(32)
	if err == nil {
		copy(v[:], b)
	}
	return
}
func (d *workerDecoder) bytes16(limit uint64) ([]byte, error) {
	n, e := d.u16()
	if e != nil {
		return nil, e
	}
	if uint64(n) > limit {
		return nil, ErrWorkerMessageTooLarge
	}
	v, e := d.take(int(n))
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), v...), nil
}
func (d *workerDecoder) bytes32(limit uint64) ([]byte, error) {
	n, e := d.u32()
	if e != nil {
		return nil, e
	}
	if uint64(n) > limit {
		return nil, ErrWorkerMessageTooLarge
	}
	if uint64(n) > uint64(d.remaining()) {
		return nil, ErrMalformedWorkerMessage
	}
	v, e := d.take(int(n))
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), v...), nil
}
func (d *workerDecoder) topology(limit uint64) ([]byte, error) {
	start := d.offset
	n, err := d.u64()
	if err != nil {
		return nil, err
	}
	if limit < 8 || n > limit-8 {
		return nil, ErrWorkerMessageTooLarge
	}
	if n > uint64(d.remaining()) || n > uint64(math.MaxInt) {
		return nil, ErrMalformedWorkerMessage
	}
	if _, err := d.take(int(n)); err != nil {
		return nil, err
	}
	return append([]byte(nil), d.input[start:d.offset]...), nil
}
func (d *workerDecoder) epoch() (v model.CoordinatorEpoch, err error) {
	v.Term, err = d.u64()
	if err != nil {
		return
	}
	v.BeginIndex, err = d.u64()
	if err != nil {
		return
	}
	v.Coordinator, err = d.u16()
	if err != nil {
		return
	}
	v.Nonce, err = d.fixed16()
	return
}
func (d *workerDecoder) task() (v model.TaskID, err error) {
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.StageID, err = d.u16()
	if err != nil {
		return
	}
	v.Partition, err = d.u16()
	return
}
func (d *workerDecoder) tupleID() (v model.TupleID, err error) {
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.SourceTask, err = d.task()
	if err != nil {
		return
	}
	v.SourceSequence, err = d.u64()
	if err != nil {
		return
	}
	v.PathDigest, err = d.fixed32()
	return
}
func (d *workerDecoder) worker() (node uint16, epoch model.WorkerEpoch, err error) {
	node, err = d.u16()
	if err != nil {
		return
	}
	raw, e := d.fixed16()
	err = e
	epoch = model.WorkerEpoch(raw)
	return
}
func (d *workerDecoder) token() (v model.AssignmentToken, err error) {
	v.Task, err = d.task()
	if err != nil {
		return
	}
	v.WorkerID, v.WorkerEpoch, err = d.worker()
	if err != nil {
		return
	}
	v.Attempt, err = d.u64()
	if err != nil {
		return
	}
	v.SpecificationHash, err = d.fixed32()
	if err != nil {
		return
	}
	v.AssignmentRevision, err = d.u64()
	return
}
func (d *workerDecoder) replica() (v model.ResultReplicaSet, err error) {
	v.SinkTask, err = d.task()
	if err != nil {
		return
	}
	v.PrimaryNodeID, err = d.u16()
	if err != nil {
		return
	}
	v.SecondaryNodeID, err = d.u16()
	if err != nil {
		return
	}
	raw, er := d.fixed16()
	if er != nil {
		return v, er
	}
	v.PrimaryEpoch = model.WorkerEpoch(raw)
	raw, err = d.fixed16()
	v.SecondaryEpoch = model.WorkerEpoch(raw)
	return
}
func (d *workerDecoder) assignment() (v model.AssignmentSet, err error) {
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.Revision, err = d.u64()
	if err != nil {
		return
	}
	v.Digest, err = d.fixed32()
	if err != nil {
		return
	}
	count, err := d.u16()
	if err != nil {
		return v, err
	}
	if uint64(count) > model.LimitsV1().MaxTasksPerJob || int(count) > d.remaining()/86 {
		return v, ErrMalformedWorkerMessage
	}
	v.Tasks = make([]model.AssignmentToken, int(count))
	for i := range v.Tasks {
		v.Tasks[i], err = d.token()
		if err != nil {
			return v, err
		}
	}
	rc, err := d.u16()
	if err != nil {
		return v, err
	}
	if uint64(rc) > model.LimitsV1().MaxTasksPerStage || int(rc) > d.remaining()/56 {
		return v, ErrMalformedWorkerMessage
	}
	v.ResultReplicas = make([]model.ResultReplicaSet, int(rc))
	for i := range v.ResultReplicas {
		v.ResultReplicas[i], err = d.replica()
		if err != nil {
			return v, err
		}
	}
	return
}
func (d *workerDecoder) checkpoints(job model.JobID) (v []SourceCheckpoint, err error) {
	count, err := d.u16()
	if err != nil {
		return nil, err
	}
	if count == 0 || count > MaxInventoryCheckpoints || int(count) > d.remaining()/28 {
		return nil, ErrMalformedWorkerMessage
	}
	v = make([]SourceCheckpoint, int(count))
	for i := range v {
		v[i].Source, err = d.task()
		if err != nil {
			return nil, err
		}
		v[i].Watermark, err = d.u64()
		if err != nil {
			return nil, err
		}
		if v[i].Source.JobID != job {
			return nil, ErrInvalidWorkerMessage
		}
	}
	return
}
func (d *workerDecoder) inventory() (v ResultInventoryQuery, err error) {
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.SinkTask, err = d.task()
	if err != nil {
		return
	}
	v.SpecificationHash, err = d.fixed32()
	if err != nil {
		return
	}
	v.AssignmentRevision, err = d.u64()
	if err != nil {
		return
	}
	v.AssignmentDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.Checkpoints, err = d.checkpoints(v.JobID)
	if err != nil {
		return
	}
	v.CheckpointDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.QueryDigest, err = d.fixed32()
	return
}
func (d *workerDecoder) repair() (v RepairResultPartition, err error) {
	v.RepairID, err = d.fixed16()
	if err != nil {
		return
	}
	v.CoordinatorEpoch, err = d.epoch()
	if err != nil {
		return
	}
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.AssignmentRevision, err = d.u64()
	if err != nil {
		return
	}
	v.AssignmentDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.SourceNodeID, v.SourceWorkerEpoch, err = d.worker()
	if err != nil {
		return
	}
	v.DestinationNodeID, v.DestinationWorkerEpoch, err = d.worker()
	if err != nil {
		return
	}
	v.SinkTask, err = d.task()
	if err != nil {
		return
	}
	v.SpecificationHash, err = d.fixed32()
	if err != nil {
		return
	}
	v.Checkpoints, err = d.checkpoints(v.JobID)
	if err != nil {
		return
	}
	v.CheckpointDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.InventoryQueryDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.ExpectedRecordCount, err = d.u64()
	if err != nil {
		return
	}
	v.ExpectedTotalBytes, err = d.u64()
	if err != nil {
		return
	}
	v.ExpectedContentDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.InstructionDigest, err = d.fixed32()
	return
}
func (d *workerDecoder) transfer() (v TransferChunk, err error) {
	raw, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.TransferID = TransferID(raw)
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.TotalLength, err = d.u64()
	if err != nil {
		return
	}
	if v.TotalLength > MaxTransferTotalBytes {
		return v, ErrWorkerMessageTooLarge
	}
	v.Checksum, err = d.fixed32()
	if err != nil {
		return
	}
	v.Offset, err = d.u64()
	if err != nil {
		return
	}
	v.Data, err = d.bytes32(MaxTransferChunkBytes)
	if err != nil {
		return
	}
	v.Final, err = d.bool()
	return
}
func (d *workerDecoder) artifact() (v ResultArtifact, err error) {
	job, err := d.fixed16()
	if err != nil {
		return v, err
	}
	v.JobID = model.JobID(job)
	v.SinkTask, err = d.task()
	if err != nil {
		return
	}
	v.SpecificationHash, err = d.fixed32()
	if err != nil {
		return
	}
	v.RecordCount, err = d.u64()
	if err != nil {
		return
	}
	v.TotalLength, err = d.u64()
	if err != nil {
		return
	}
	v.Checksum, err = d.fixed32()
	return
}
func (d *workerDecoder) record() (v model.ResultRecord, err error) {
	v.TupleID, err = d.tupleID()
	if err != nil {
		return
	}
	v.SinkTask, err = d.task()
	if err != nil {
		return
	}
	v.SpecificationHash, err = d.fixed32()
	if err != nil {
		return
	}
	v.Value, err = d.bytes16(model.LimitsV1().MaxTuplePayloadBytes)
	if err != nil {
		return
	}
	v.Checksum, err = d.fixed32()
	return
}
func (d *workerDecoder) provenance() (v model.ResultCopyProvenance, err error) {
	v.AssignmentRevision, err = d.u64()
	if err != nil {
		return
	}
	v.AssignmentDigest, err = d.fixed32()
	if err != nil {
		return
	}
	v.ReplicaSet, err = d.replica()
	if err != nil {
		return
	}
	role, err := d.u8()
	if err != nil {
		return v, err
	}
	v.DestinationRole = model.ResultReplicaRole(role)
	v.CoordinatorEpoch, err = d.epoch()
	return
}
func (d *workerDecoder) event() (v model.WorkerEvent, err error) {
	v.WorkerID, v.WorkerEpoch, err = d.worker()
	if err != nil {
		return
	}
	v.TransactionID, err = d.u64()
	if err != nil {
		return
	}
	kind, err := d.u8()
	if err != nil {
		return v, err
	}
	v.Kind = model.WorkerEventKind(kind)
	switch v.Kind {
	case model.WorkerEventCompletion:
		r := &model.CompletionReport{}
		job, e := d.fixed16()
		if e != nil {
			return v, e
		}
		r.JobID = model.JobID(job)
		r.JobControlRevision, err = d.u64()
		if err != nil {
			return
		}
		r.AssignmentRevision, err = d.u64()
		if err != nil {
			return
		}
		r.Source, err = d.task()
		if err != nil {
			return
		}
		r.Token, err = d.token()
		if err != nil {
			return
		}
		r.Epoch, err = d.epoch()
		if err != nil {
			return
		}
		r.ExpectedCheckpointRevision, err = d.u64()
		if err != nil {
			return
		}
		r.Prior, err = d.u64()
		if err != nil {
			return
		}
		r.New, err = d.u64()
		if err != nil {
			return
		}
		r.EOF, err = d.u64()
		if err != nil {
			return
		}
		r.WorkerTransactionID, err = d.u64()
		if err != nil {
			return
		}
		r.Digest, err = d.fixed32()
		v.Completion = r
	case model.WorkerEventFailure:
		r := &model.JobFailureReport{}
		job, e := d.fixed16()
		if e != nil {
			return v, e
		}
		r.JobID = model.JobID(job)
		r.JobControlRevision, err = d.u64()
		if err != nil {
			return
		}
		r.AssignmentRevision, err = d.u64()
		if err != nil {
			return
		}
		r.Task, err = d.token()
		if err != nil {
			return
		}
		r.Epoch, err = d.epoch()
		if err != nil {
			return
		}
		r.TransactionID, err = d.u64()
		if err != nil {
			return
		}
		code, e := d.u16()
		if e != nil {
			return v, e
		}
		r.Code = model.FailureCode(code)
		r.DetailDigest, err = d.fixed32()
		v.Failure = r
	default:
		return v, ErrInvalidWorkerMessage
	}
	return
}

func (d *workerDecoder) message(mt wire.MessageType) (WorkerMessage, error) {
	switch mt {
	case wire.MessageCraneWorkerHandshake:
		n, e1, e := d.worker()
		if e != nil {
			return nil, e
		}
		c, e := d.fixed32()
		if e != nil {
			return nil, e
		}
		r, e := d.fixed32()
		return WorkerHandshake{n, e1, c, r}, e
	case wire.MessageCraneWorkerHandshakeAck:
		n, e1, e := d.worker()
		if e != nil {
			return nil, e
		}
		slots, e := d.u16()
		if e != nil {
			return nil, e
		}
		c, e := d.fixed32()
		if e != nil {
			return nil, e
		}
		r, e := d.fixed32()
		return WorkerHandshakeAck{n, e1, slots, c, r}, e
	case wire.MessageCraneWorkerFenceRequest:
		e, err := d.epoch()
		return FenceRequest{e}, err
	case wire.MessageCraneWorkerFenceResponse:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		e, err := d.epoch()
		return FenceResponse{n, we, e}, err
	case wire.MessageCraneWorkerRegisterRequest:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		slots, err := d.u16()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		if err != nil {
			return nil, err
		}
		c, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		r, err := d.fixed32()
		return WorkerRegisterRequest{n, we, slots, ep, c, r}, err
	case wire.MessageCraneWorkerRegisterResponse:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		rev, err := d.u64()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		if err != nil {
			return nil, err
		}
		ok, err := d.bool()
		return WorkerRegisterResponse{n, we, rev, ep, ok}, err
	case wire.MessageCraneAssignmentSetInstall:
		topo, err := d.topology(model.LimitsV1().MaxTopologyBytes)
		if err != nil {
			return nil, err
		}
		validated, err := model.DecodeTopology(topo)
		if err != nil {
			return nil, err
		}
		set, err := d.assignment()
		if err != nil {
			return nil, err
		}
		digest, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		jobRev, err := d.u64()
		if err != nil {
			return nil, err
		}
		state, err := d.u8()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return AssignmentSetInstall{set, validated.Spec(), digest, jobRev, model.SchedulingState(state), ep}, err
	case wire.MessageCraneAssignmentSetInstallAck:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		job, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		ar, err := d.u64()
		if err != nil {
			return nil, err
		}
		ad, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		jr, err := d.u64()
		if err != nil {
			return nil, err
		}
		s, err := d.u8()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return AssignmentSetInstallAck{n, we, model.JobID(job), ar, ad, jr, model.SchedulingState(s), ep}, err
	case wire.MessageCraneWorkerStatusRequest:
		ep, err := d.epoch()
		if err != nil {
			return nil, err
		}
		after, err := d.u64()
		if err != nil {
			return nil, err
		}
		max, err := d.u16()
		if err != nil {
			return nil, err
		}
		tag, err := d.u8()
		if err != nil {
			return nil, err
		}
		v := WorkerStatusRequest{CoordinatorEpoch: ep, AfterTransactionID: after, MaxEvents: max}
		switch tag {
		case 0:
		case 1:
			q, e := d.inventory()
			if e != nil {
				return nil, e
			}
			v.Inventory = &q
		case 2:
			r, e := d.repair()
			if e != nil {
				return nil, e
			}
			role, e := d.u8()
			if e != nil {
				return nil, e
			}
			v.Repair = &RepairGrant{r, RepairEndpointRole(role)}
		default:
			return nil, ErrMalformedWorkerMessage
		}
		return v, nil
	case wire.MessageCraneWorkerStatusReport:
		return d.status()
	case wire.MessageCraneCheckpointNotice:
		return d.checkpointNotice()
	case wire.MessageCraneCheckpointAck:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		job, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		source, err := d.task()
		if err != nil {
			return nil, err
		}
		wm, err := d.u64()
		if err != nil {
			return nil, err
		}
		ri, err := d.u64()
		if err != nil {
			return nil, err
		}
		jr, err := d.u64()
		if err != nil {
			return nil, err
		}
		ar, err := d.u64()
		if err != nil {
			return nil, err
		}
		ad, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return CheckpointAck{NodeID: n, WorkerEpoch: we, JobID: model.JobID(job), Source: source, Watermark: wm, RaftIndex: ri, JobControlRevision: jr, AssignmentRevision: ar, AssignmentDigest: ad, CoordinatorEpoch: ep}, err
	case wire.MessageCraneResultRecordChunk:
		tr, err := d.transfer()
		if err != nil {
			return nil, err
		}
		rec, err := d.record()
		if err != nil {
			return nil, err
		}
		p, err := d.provenance()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		rid, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		dig, err := d.fixed32()
		return ResultRecordChunk{tr, rec, p, n, we, rid, dig}, err
	case wire.MessageCraneResultRecordAck:
		id, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		rid, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		dig, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		next, err := d.u64()
		if err != nil {
			return nil, err
		}
		total, err := d.u64()
		if err != nil {
			return nil, err
		}
		sum, err := d.fixed32()
		if err != nil {
			return nil, err
		}
		complete, err := d.bool()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return ResultRecordAck{TransferID(id), n, we, rid, dig, next, total, sum, complete, ep}, err
	case wire.MessageCraneResultArtifactChunk:
		tr, err := d.transfer()
		if err != nil {
			return nil, err
		}
		a, err := d.artifact()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return ResultArtifactChunk{tr, a, n, we, ep}, err
	case wire.MessageCraneResultArtifactAck:
		id, err := d.fixed16()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		a, err := d.artifact()
		if err != nil {
			return nil, err
		}
		next, err := d.u64()
		if err != nil {
			return nil, err
		}
		complete, err := d.bool()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return ResultArtifactAck{TransferID(id), n, we, a, next, complete, ep}, err
	case wire.MessageCraneResultFetchRequest:
		a, err := d.artifact()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		off, err := d.u64()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return ResultFetchRequest{a, n, we, off, ep}, err
	case wire.MessageCraneResultFetchChunk:
		tr, err := d.transfer()
		if err != nil {
			return nil, err
		}
		a, err := d.artifact()
		if err != nil {
			return nil, err
		}
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		return ResultFetchChunk{tr, a, n, we, ep}, err
	case wire.MessageCraneWorkerError:
		n, we, err := d.worker()
		if err != nil {
			return nil, err
		}
		ep, err := d.epoch()
		if err != nil {
			return nil, err
		}
		related, err := d.u16()
		if err != nil {
			return nil, err
		}
		code, err := d.u16()
		if err != nil {
			return nil, err
		}
		retry, err := d.bool()
		if err != nil {
			return nil, err
		}
		detail, err := d.bytes16(MaxWorkerErrorDetailBytes)
		return WorkerError{n, we, ep, wire.MessageType(related), WorkerErrorCode(code), retry, detail}, err
	default:
		return nil, ErrUnexpectedWorkerMessage
	}
}

func (d *workerDecoder) status() (WorkerMessage, error) {
	n, we, err := d.worker()
	if err != nil {
		return nil, err
	}
	ep, err := d.epoch()
	if err != nil {
		return nil, err
	}
	store, err := d.u64()
	if err != nil {
		return nil, err
	}
	after, err := d.u64()
	if err != nil {
		return nil, err
	}
	count, err := d.u16()
	if err != nil {
		return nil, err
	}
	if uint64(count) > model.LimitsV1().MaxRetainedJobs || int(count) > d.remaining()/97 {
		return nil, ErrMalformedWorkerMessage
	}
	assignments := make([]InstalledAssignmentStatus, int(count))
	for i := range assignments {
		job, e := d.fixed16()
		if e != nil {
			return nil, e
		}
		assignments[i].JobID = model.JobID(job)
		assignments[i].JobControlRevision, e = d.u64()
		if e != nil {
			return nil, e
		}
		assignments[i].AssignmentRevision, e = d.u64()
		if e != nil {
			return nil, e
		}
		assignments[i].AssignmentDigest, e = d.fixed32()
		if e != nil {
			return nil, e
		}
		assignments[i].SpecificationDigest, e = d.fixed32()
		if e != nil {
			return nil, e
		}
		state, e := d.u8()
		if e != nil {
			return nil, e
		}
		assignments[i].SchedulingState = model.SchedulingState(state)
	}
	ec, err := d.u16()
	if err != nil {
		return nil, err
	}
	if ec > MaxWorkerStatusEvents {
		return nil, ErrMalformedWorkerMessage
	}
	events := make([]model.WorkerEvent, int(ec))
	for i := range events {
		events[i], err = d.event()
		if err != nil {
			return nil, err
		}
	}
	last, err := d.u64()
	if err != nil {
		return nil, err
	}
	more, err := d.bool()
	if err != nil {
		return nil, err
	}
	admission, err := d.epoch()
	if err != nil {
		return nil, err
	}
	tag, err := d.u8()
	if err != nil {
		return nil, err
	}
	v := WorkerStatus{NodeID: n, WorkerEpoch: we, CoordinatorEpoch: ep, StoreTransactionID: store, AfterTransactionID: after, Assignments: assignments, Events: events, LastTransactionID: last, HasMore: more, AdmissionEpoch: admission}
	switch tag {
	case 0:
	case 1:
		q, er := d.fixed32()
		if er != nil {
			return nil, er
		}
		rc, er := d.u64()
		if er != nil {
			return nil, er
		}
		total, er := d.u64()
		if er != nil {
			return nil, er
		}
		content, er := d.fixed32()
		if er != nil {
			return nil, er
		}
		v.Inventory = &ResultInventorySummary{q, rc, total, content}
	case 2:
		instruction, er := d.repair()
		if er != nil {
			return nil, er
		}
		id, er := d.fixed16()
		if er != nil {
			return nil, er
		}
		dig, er := d.fixed32()
		if er != nil {
			return nil, er
		}
		role, er := d.u8()
		if er != nil {
			return nil, er
		}
		state, er := d.u8()
		if er != nil {
			return nil, er
		}
		rc, er := d.u64()
		if er != nil {
			return nil, er
		}
		total, er := d.u64()
		if er != nil {
			return nil, er
		}
		content, er := d.fixed32()
		if er != nil {
			return nil, er
		}
		code, er := d.u16()
		if er != nil {
			return nil, er
		}
		v.Repair = &ResultRepairStatus{Instruction: instruction, RepairID: id, InstructionDigest: dig, Role: RepairEndpointRole(role), State: ResultRepairState(state), RecordCount: rc, TotalBytes: total, ContentDigest: content, ErrorCode: WorkerErrorCode(code)}
	default:
		return nil, ErrMalformedWorkerMessage
	}
	return v, nil
}
func (d *workerDecoder) checkpointNotice() (WorkerMessage, error) {
	job, err := d.fixed16()
	if err != nil {
		return nil, err
	}
	source, err := d.task()
	if err != nil {
		return nil, err
	}
	wm, err := d.u64()
	if err != nil {
		return nil, err
	}
	ri, err := d.u64()
	if err != nil {
		return nil, err
	}
	ep, err := d.epoch()
	if err != nil {
		return nil, err
	}
	jr, err := d.u64()
	if err != nil {
		return nil, err
	}
	ar, err := d.u64()
	if err != nil {
		return nil, err
	}
	ad, err := d.fixed32()
	return CheckpointNotice{model.CheckpointNotice{JobID: model.JobID(job), Source: source, Watermark: wm, RaftIndex: ri, Epoch: ep}, jr, ar, ad}, err
}

const maxTupleMessagePayloadBytes = wire.MaxCraneDatagramBytesV1 - wire.FixedHeaderSize - wire.MACSize

const (
	tupleMessagePrefixBytes      = 2 + 2
	tupleDeliveryIDBytes         = 16 + (16 + 2 + 2) + 8 + 32 + 2 + (16 + 2 + 2)
	tupleAssignmentTokenBytes    = (16 + 2 + 2) + 2 + 16 + 8 + 32 + 8
	tupleAssignmentIdentityBytes = 16 + 8 + 32
	tupleCoordinatorEpochBytes   = 8 + 8 + 2 + 16
	tupleLengthBytes             = 2
	minimumCanonicalTupleBytes   = 2

	// TupleDeliveryFixedPayloadBytes is schema/type + DeliveryID + tuple length
	// + producer token + destination token + complete set identity + epoch.
	TupleDeliveryFixedPayloadBytes = tupleMessagePrefixBytes + tupleDeliveryIDBytes + tupleLengthBytes +
		2*tupleAssignmentTokenBytes + tupleAssignmentIdentityBytes + tupleCoordinatorEpochBytes
	// TupleDeliveryMinPayloadBytes includes the canonical empty-tuple count.
	TupleDeliveryMinPayloadBytes = TupleDeliveryFixedPayloadBytes + minimumCanonicalTupleBytes
	// TupleACKPayloadBytes is the one exact v1 ACK payload size.
	TupleACKPayloadBytes = tupleMessagePrefixBytes + tupleDeliveryIDBytes + tupleAssignmentTokenBytes +
		tupleAssignmentIdentityBytes + tupleCoordinatorEpochBytes + 1
	// TupleNACKPayloadBytes is the one exact v1 NACK payload size.
	TupleNACKPayloadBytes = tupleMessagePrefixBytes + tupleDeliveryIDBytes + tupleAssignmentTokenBytes +
		tupleAssignmentIdentityBytes + tupleCoordinatorEpochBytes + 2
)

// TupleDeliveryMaxPayloadBytes derives the exact v1 delivery ceiling from the
// sole canonical tuple bound in model; it introduces no second tuple limit.
func TupleDeliveryMaxPayloadBytes() int {
	return TupleDeliveryFixedPayloadBytes + int(model.LimitsV1().MaxTuplePayloadBytes)
}

// MarshalTupleDelivery validates and encodes one canonical v1 tuple delivery payload.
func MarshalTupleDelivery(message TupleDelivery) ([]byte, error) {
	if err := message.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	tuple, err := model.MarshalTuple(message.Tuple)
	if err != nil {
		return nil, fmt.Errorf("%w: tuple: %v", ErrInvalidTupleMessage, err)
	}
	encoder := tupleEncoder{}
	if err := encoder.prefix(TupleDeliverySchemaVersion, message.MessageType()); err != nil {
		return nil, err
	}
	if err := encoder.deliveryID(message.DeliveryID); err != nil {
		return nil, err
	}
	if err := encoder.bytes16(tuple); err != nil {
		return nil, err
	}
	if err := encoder.assignmentToken(message.Producer); err != nil {
		return nil, err
	}
	if err := encoder.assignmentToken(message.Destination); err != nil {
		return nil, err
	}
	if err := encoder.assignmentIdentity(message.Assignment); err != nil {
		return nil, err
	}
	if err := encoder.coordinatorEpoch(message.Coordinator); err != nil {
		return nil, err
	}
	return encoder.ownedBytes(), nil
}

// UnmarshalTupleDelivery decodes one complete canonical v1 tuple delivery payload.
func UnmarshalTupleDelivery(encoded []byte) (TupleDelivery, error) {
	return unmarshalTupleDeliveryWith(encoded, model.UnmarshalTuple)
}

type tupleDecodeFunc func([]byte) (model.Tuple, error)

func unmarshalTupleDeliveryWith(encoded []byte, decodeTuple tupleDecodeFunc) (TupleDelivery, error) {
	decoder, err := newTupleDecoder(encoded, TupleDeliverySchemaVersion, wire.MessageCraneTupleDelivery)
	if err != nil {
		return TupleDelivery{}, err
	}
	if err := preflightTupleDelivery(encoded); err != nil {
		return TupleDelivery{}, err
	}
	deliveryID, err := decoder.deliveryID()
	if err != nil {
		return TupleDelivery{}, err
	}
	tupleBytes, err := decoder.boundedBytes16(model.LimitsV1().MaxTuplePayloadBytes)
	if err != nil {
		return TupleDelivery{}, err
	}
	tuple, err := decodeTuple(tupleBytes)
	if err != nil {
		return TupleDelivery{}, fmt.Errorf("%w: tuple: %v", ErrInvalidTupleMessage, err)
	}
	canonical, err := model.MarshalTuple(tuple)
	if err != nil || !bytes.Equal(canonical, tupleBytes) {
		return TupleDelivery{}, fmt.Errorf("%w: tuple is not canonical", ErrInvalidTupleMessage)
	}
	producer, err := decoder.assignmentToken()
	if err != nil {
		return TupleDelivery{}, err
	}
	destination, err := decoder.assignmentToken()
	if err != nil {
		return TupleDelivery{}, err
	}
	assignment, err := decoder.assignmentIdentity()
	if err != nil {
		return TupleDelivery{}, err
	}
	coordinator, err := decoder.coordinatorEpoch()
	if err != nil {
		return TupleDelivery{}, err
	}
	if err := decoder.finish(); err != nil {
		return TupleDelivery{}, err
	}
	message := TupleDelivery{DeliveryID: deliveryID, Tuple: tuple, Producer: producer, Destination: destination, Assignment: assignment, Coordinator: coordinator}
	if err := message.validate(); err != nil {
		return TupleDelivery{}, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	return message, nil
}

// MarshalTupleACK validates and encodes one canonical v1 tuple acknowledgement.
func MarshalTupleACK(message TupleACK) ([]byte, error) {
	if err := message.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	encoder := tupleEncoder{}
	if err := encoder.prefix(TupleACKSchemaVersion, message.MessageType()); err != nil {
		return nil, err
	}
	if err := encoder.deliveryID(message.DeliveryID); err != nil {
		return nil, err
	}
	if err := encoder.assignmentToken(message.Destination); err != nil {
		return nil, err
	}
	if err := encoder.assignmentIdentity(message.Assignment); err != nil {
		return nil, err
	}
	if err := encoder.coordinatorEpoch(message.Coordinator); err != nil {
		return nil, err
	}
	if err := encoder.byte(byte(message.Status)); err != nil {
		return nil, err
	}
	return encoder.ownedBytes(), nil
}

// UnmarshalTupleACK decodes one complete canonical v1 tuple acknowledgement.
func UnmarshalTupleACK(encoded []byte) (TupleACK, error) {
	decoder, err := newTupleDecoder(encoded, TupleACKSchemaVersion, wire.MessageCraneTupleDeliveryAck)
	if err != nil {
		return TupleACK{}, err
	}
	if len(encoded) != TupleACKPayloadBytes {
		return TupleACK{}, fmt.Errorf("%w: ACK payload is %d bytes, want %d", ErrMalformedTupleMessage, len(encoded), TupleACKPayloadBytes)
	}
	deliveryID, err := decoder.deliveryID()
	if err != nil {
		return TupleACK{}, err
	}
	destination, err := decoder.assignmentToken()
	if err != nil {
		return TupleACK{}, err
	}
	assignment, err := decoder.assignmentIdentity()
	if err != nil {
		return TupleACK{}, err
	}
	coordinator, err := decoder.coordinatorEpoch()
	if err != nil {
		return TupleACK{}, err
	}
	status, err := decoder.byte()
	if err != nil {
		return TupleACK{}, err
	}
	if err := decoder.finish(); err != nil {
		return TupleACK{}, err
	}
	message := TupleACK{DeliveryID: deliveryID, Destination: destination, Assignment: assignment, Coordinator: coordinator, Status: TupleACKStatus(status)}
	if err := message.validate(); err != nil {
		return TupleACK{}, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	return message, nil
}

// MarshalTupleNACK validates and encodes one canonical v1 typed tuple rejection.
func MarshalTupleNACK(message TupleNACK) ([]byte, error) {
	if err := message.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	encoder := tupleEncoder{}
	if err := encoder.prefix(TupleNACKSchemaVersion, message.MessageType()); err != nil {
		return nil, err
	}
	if err := encoder.deliveryID(message.DeliveryID); err != nil {
		return nil, err
	}
	if err := encoder.assignmentToken(message.Destination); err != nil {
		return nil, err
	}
	if err := encoder.assignmentIdentity(message.Assignment); err != nil {
		return nil, err
	}
	if err := encoder.coordinatorEpoch(message.Coordinator); err != nil {
		return nil, err
	}
	if err := encoder.uint16(uint16(message.Code)); err != nil {
		return nil, err
	}
	return encoder.ownedBytes(), nil
}

// UnmarshalTupleNACK decodes one complete canonical v1 typed tuple rejection.
func UnmarshalTupleNACK(encoded []byte) (TupleNACK, error) {
	decoder, err := newTupleDecoder(encoded, TupleNACKSchemaVersion, wire.MessageCraneTupleDeliveryNack)
	if err != nil {
		return TupleNACK{}, err
	}
	if len(encoded) != TupleNACKPayloadBytes {
		return TupleNACK{}, fmt.Errorf("%w: NACK payload is %d bytes, want %d", ErrMalformedTupleMessage, len(encoded), TupleNACKPayloadBytes)
	}
	deliveryID, err := decoder.deliveryID()
	if err != nil {
		return TupleNACK{}, err
	}
	destination, err := decoder.assignmentToken()
	if err != nil {
		return TupleNACK{}, err
	}
	assignment, err := decoder.assignmentIdentity()
	if err != nil {
		return TupleNACK{}, err
	}
	coordinator, err := decoder.coordinatorEpoch()
	if err != nil {
		return TupleNACK{}, err
	}
	code, err := decoder.uint16()
	if err != nil {
		return TupleNACK{}, err
	}
	if err := decoder.finish(); err != nil {
		return TupleNACK{}, err
	}
	message := TupleNACK{DeliveryID: deliveryID, Destination: destination, Assignment: assignment, Coordinator: coordinator, Code: TupleNACKCode(code)}
	if err := message.validate(); err != nil {
		return TupleNACK{}, fmt.Errorf("%w: %v", ErrInvalidTupleMessage, err)
	}
	return message, nil
}

type tupleEncoder struct {
	buffer [maxTupleMessagePayloadBytes]byte
	offset int
}

func (encoder *tupleEncoder) ownedBytes() []byte {
	return append([]byte(nil), encoder.buffer[:encoder.offset]...)
}

func (encoder *tupleEncoder) add(value []byte) error {
	if len(value) > len(encoder.buffer)-encoder.offset {
		return ErrTupleMessageTooLarge
	}
	copy(encoder.buffer[encoder.offset:], value)
	encoder.offset += len(value)
	return nil
}

func (encoder *tupleEncoder) byte(value byte) error {
	return encoder.add([]byte{value})
}

func (encoder *tupleEncoder) uint16(value uint16) error {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return encoder.add(encoded[:])
}

func (encoder *tupleEncoder) uint64(value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoder.add(encoded[:])
}

func (encoder *tupleEncoder) prefix(schema uint16, message wire.MessageType) error {
	if err := encoder.uint16(schema); err != nil {
		return err
	}
	return encoder.uint16(uint16(message))
}

func (encoder *tupleEncoder) bytes16(value []byte) error {
	if len(value) > mathMaxUint16 {
		return ErrTupleMessageTooLarge
	}
	if err := encoder.uint16(uint16(len(value))); err != nil {
		return err
	}
	return encoder.add(value)
}

func (encoder *tupleEncoder) taskID(value model.TaskID) error {
	if err := encoder.add(value.JobID[:]); err != nil {
		return err
	}
	if err := encoder.uint16(value.StageID); err != nil {
		return err
	}
	return encoder.uint16(value.Partition)
}

func (encoder *tupleEncoder) tupleID(value model.TupleID) error {
	if err := encoder.add(value.JobID[:]); err != nil {
		return err
	}
	if err := encoder.taskID(value.SourceTask); err != nil {
		return err
	}
	if err := encoder.uint64(value.SourceSequence); err != nil {
		return err
	}
	return encoder.add(value.PathDigest[:])
}

func (encoder *tupleEncoder) deliveryID(value model.DeliveryID) error {
	if err := encoder.tupleID(value.Tuple); err != nil {
		return err
	}
	if err := encoder.uint16(value.EdgeID); err != nil {
		return err
	}
	return encoder.taskID(value.DestinationTask)
}

func (encoder *tupleEncoder) assignmentToken(value model.AssignmentToken) error {
	if err := encoder.taskID(value.Task); err != nil {
		return err
	}
	if err := encoder.uint16(value.WorkerID); err != nil {
		return err
	}
	if err := encoder.add(value.WorkerEpoch[:]); err != nil {
		return err
	}
	if err := encoder.uint64(value.Attempt); err != nil {
		return err
	}
	if err := encoder.add(value.SpecificationHash[:]); err != nil {
		return err
	}
	return encoder.uint64(value.AssignmentRevision)
}

func (encoder *tupleEncoder) assignmentIdentity(value AssignmentSetIdentity) error {
	if err := encoder.add(value.JobID[:]); err != nil {
		return err
	}
	if err := encoder.uint64(value.Revision); err != nil {
		return err
	}
	return encoder.add(value.Digest[:])
}

func (encoder *tupleEncoder) coordinatorEpoch(value model.CoordinatorEpoch) error {
	if err := encoder.uint64(value.Term); err != nil {
		return err
	}
	if err := encoder.uint64(value.BeginIndex); err != nil {
		return err
	}
	if err := encoder.uint16(value.Coordinator); err != nil {
		return err
	}
	return encoder.add(value.Nonce[:])
}

const mathMaxUint16 = int(^uint16(0))

type tupleDecoder struct {
	input  []byte
	offset int
}

func newTupleDecoder(input []byte, schema uint16, message wire.MessageType) (tupleDecoder, error) {
	if len(input) > maxTupleMessagePayloadBytes {
		return tupleDecoder{}, fmt.Errorf("%w: payload is %d bytes, maximum is %d", ErrTupleMessageTooLarge, len(input), maxTupleMessagePayloadBytes)
	}
	decoder := tupleDecoder{input: input}
	gotSchema, err := decoder.uint16()
	if err != nil {
		return tupleDecoder{}, err
	}
	if gotSchema != schema {
		return tupleDecoder{}, fmt.Errorf("%w: version %d", ErrUnsupportedTupleSchema, gotSchema)
	}
	gotMessage, err := decoder.uint16()
	if err != nil {
		return tupleDecoder{}, err
	}
	if wire.MessageType(gotMessage) != message {
		return tupleDecoder{}, fmt.Errorf("%w: got %d, want %d", ErrUnexpectedMessage, gotMessage, message)
	}
	return decoder, nil
}

// MarshalControlMessage validates and emits the sole canonical payload for IDs 240-249.
func MarshalControlMessage(message ControlMessage) ([]byte, error) {
	if message == nil || message.MessageType() < wire.MessageCraneSubmitRequest || message.MessageType() > wire.MessageCraneControlError {
		return nil, ErrUnexpectedControlMessage
	}
	if err := validateControlMessage(message); err != nil {
		return nil, invalidControl(message, err)
	}
	encoder := controlEncoder{}
	encoder.u16(ControlSchemaVersion)
	encoder.u16(uint16(message.MessageType()))
	if err := encoder.message(message); err != nil {
		return nil, err
	}
	return encoder.owned(), nil
}

// UnmarshalControlMessage decodes one complete canonical payload for IDs 240-249.
func UnmarshalControlMessage(messageType wire.MessageType, encoded []byte) (ControlMessage, error) {
	return unmarshalControlMessageWith(messageType, encoded, model.DecodeTopology)
}

// ControlEncodingLayout returns the top-level order exercised by the actual encoder.
func ControlEncodingLayout(message ControlMessage) ([]model.PublicControlFieldDescriptor, error) {
	if message == nil || message.MessageType() < wire.MessageCraneSubmitRequest || message.MessageType() > wire.MessageCraneControlError {
		return nil, ErrUnexpectedControlMessage
	}
	if err := validateControlMessage(message); err != nil {
		return nil, invalidControl(message, err)
	}
	encoder := controlEncoder{trace: true}
	if err := encoder.message(message); err != nil {
		return nil, err
	}
	return append([]model.PublicControlFieldDescriptor(nil), encoder.layout...), nil
}

// ControlNestedEncodingLayouts returns nested layouts traced through the same
// primitive appenders used by the public-control encoder.
func ControlNestedEncodingLayouts() []model.PublicControlNestedDescriptor {
	encoder := controlEncoder{traceNested: true, nestedSeen: make(map[string]bool)}
	encoder.request(model.ClientRequestID{})
	encoder.job(model.JobID{})
	encoder.task(model.TaskID{})
	encoder.tuple(model.TupleID{})
	encoder.topology(model.TopologySpec{})
	encoder.resultEntry(model.ResultRecord{})
	encoder.pageBinding(ResultPageRequest{})
	result := append([]model.PublicControlNestedDescriptor(nil), encoder.nestedLayouts...)
	for index := range result {
		result[index].Fields = append([]model.PublicControlFieldDescriptor(nil), result[index].Fields...)
	}
	return result
}

// ControlEnumDomains returns names paired with the actual accepted enum constants.
func ControlEnumDomains() []model.PublicControlEnumDescriptor {
	return []model.PublicControlEnumDescriptor{
		{Name: "JobState", Values: []string{fmt.Sprintf("Pending=%d", JobPending), fmt.Sprintf("Deploying=%d", JobDeploying), fmt.Sprintf("Running=%d", JobRunning), fmt.Sprintf("Draining=%d", JobDraining), fmt.Sprintf("Succeeded=%d", JobSucceeded), fmt.Sprintf("Failed=%d", JobFailed), fmt.Sprintf("Canceled=%d", JobCanceled)}},
		{Name: "ControlErrorCode", Values: []string{fmt.Sprintf("Malformed=%d", ControlErrorMalformed), fmt.Sprintf("UnsupportedSchema=%d", ControlErrorUnsupportedSchema), fmt.Sprintf("InvalidRequest=%d", ControlErrorInvalidRequest), fmt.Sprintf("Starting=%d", ControlErrorStarting), fmt.Sprintf("NotLeader=%d", ControlErrorNotLeader), fmt.Sprintf("StaleRequest=%d", ControlErrorStaleRequest), fmt.Sprintf("SkippedRequest=%d", ControlErrorSkippedRequest), fmt.Sprintf("IdentityReuse=%d", ControlErrorIdentityReuse), fmt.Sprintf("NotFound=%d", ControlErrorNotFound), fmt.Sprintf("RevisionMismatch=%d", ControlErrorRevisionMismatch), fmt.Sprintf("CapacityExhausted=%d", ControlErrorCapacityExhausted), fmt.Sprintf("PageLimitTooSmall=%d", ControlErrorPageLimitTooSmall), fmt.Sprintf("ResultUnavailable=%d", ControlErrorResultUnavailable), fmt.Sprintf("CorruptResult=%d", ControlErrorCorruptResult), fmt.Sprintf("ResultTooLarge=%d", ControlErrorResultTooLarge)}},
		{Name: "FailureCode", Values: []string{fmt.Sprintf("Operator=%d", model.FailureOperator), fmt.Sprintf("TupleInvalid=%d", model.FailureTupleInvalid), fmt.Sprintf("Storage=%d", model.FailureStorage)}},
	}
}

// ControlErrorCodeMatrix derives the exact binding/code matrix from the real validator.
func ControlErrorCodeMatrix() []string {
	rows := []struct {
		name    string
		message wire.MessageType
	}{
		{name: "Unbound"},
		{name: "SubmitRequest", message: wire.MessageCraneSubmitRequest},
		{name: "CancelRequest", message: wire.MessageCraneCancelRequest},
		{name: "StatusRequest", message: wire.MessageCraneStatusRequest},
		{name: "ResultPageRequest", message: wire.MessageCraneResultPageRequest},
	}
	result := make([]string, len(rows))
	for index, row := range rows {
		values := make([]string, 0, int(ControlErrorResultTooLarge))
		for code := ControlErrorMalformed; code <= ControlErrorResultTooLarge; code++ {
			allowed := predecodeControlError(code)
			if row.message != 0 {
				allowed = controlErrorCodeCompatible(row.message, code)
			}
			if allowed {
				values = append(values, controlErrorCodeName(code))
			}
		}
		result[index] = row.name + "=" + strings.Join(values, ",")
	}
	return result
}

func controlErrorCodeName(code ControlErrorCode) string {
	switch code {
	case ControlErrorMalformed:
		return "Malformed"
	case ControlErrorUnsupportedSchema:
		return "UnsupportedSchema"
	case ControlErrorInvalidRequest:
		return "InvalidRequest"
	case ControlErrorStarting:
		return "Starting"
	case ControlErrorNotLeader:
		return "NotLeader"
	case ControlErrorStaleRequest:
		return "StaleRequest"
	case ControlErrorSkippedRequest:
		return "SkippedRequest"
	case ControlErrorIdentityReuse:
		return "IdentityReuse"
	case ControlErrorNotFound:
		return "NotFound"
	case ControlErrorRevisionMismatch:
		return "RevisionMismatch"
	case ControlErrorCapacityExhausted:
		return "CapacityExhausted"
	case ControlErrorPageLimitTooSmall:
		return "PageLimitTooSmall"
	case ControlErrorResultUnavailable:
		return "ResultUnavailable"
	case ControlErrorCorruptResult:
		return "CorruptResult"
	case ControlErrorResultTooLarge:
		return "ResultTooLarge"
	default:
		return ""
	}
}

// MarshalSubmitRequest emits a canonical submit request.
func MarshalSubmitRequest(value SubmitRequest) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalSubmitRequest decodes a canonical submit request.
func UnmarshalSubmitRequest(encoded []byte) (SubmitRequest, error) {
	return unmarshalSubmitRequestWith(encoded, model.DecodeTopology)
}

// MarshalSubmitResponse emits a canonical submit response.
func MarshalSubmitResponse(value SubmitResponse) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalSubmitResponse decodes a canonical submit response.
func UnmarshalSubmitResponse(encoded []byte) (SubmitResponse, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneSubmitResponse, encoded)
	if err != nil {
		return SubmitResponse{}, err
	}
	return value.(SubmitResponse), nil
}

// MarshalCancelRequest emits a canonical cancel request.
func MarshalCancelRequest(value CancelRequest) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalCancelRequest decodes a canonical cancel request.
func UnmarshalCancelRequest(encoded []byte) (CancelRequest, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneCancelRequest, encoded)
	if err != nil {
		return CancelRequest{}, err
	}
	return value.(CancelRequest), nil
}

// MarshalCancelResponse emits a canonical cancel response.
func MarshalCancelResponse(value CancelResponse) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalCancelResponse decodes a canonical cancel response.
func UnmarshalCancelResponse(encoded []byte) (CancelResponse, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneCancelResponse, encoded)
	if err != nil {
		return CancelResponse{}, err
	}
	return value.(CancelResponse), nil
}

// MarshalStatusRequest emits a canonical status request.
func MarshalStatusRequest(value StatusRequest) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalStatusRequest decodes a canonical status request.
func UnmarshalStatusRequest(encoded []byte) (StatusRequest, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneStatusRequest, encoded)
	if err != nil {
		return StatusRequest{}, err
	}
	return value.(StatusRequest), nil
}

// MarshalStatusResponse emits a canonical status response.
func MarshalStatusResponse(value StatusResponse) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalStatusResponse decodes a canonical status response.
func UnmarshalStatusResponse(encoded []byte) (StatusResponse, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneStatusResponse, encoded)
	if err != nil {
		return StatusResponse{}, err
	}
	return value.(StatusResponse), nil
}

// MarshalResultPageRequest emits a canonical result-page request.
func MarshalResultPageRequest(value ResultPageRequest) ([]byte, error) {
	return MarshalControlMessage(value)
}

// UnmarshalResultPageRequest decodes a canonical result-page request.
func UnmarshalResultPageRequest(encoded []byte) (ResultPageRequest, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneResultPageRequest, encoded)
	if err != nil {
		return ResultPageRequest{}, err
	}
	return value.(ResultPageRequest), nil
}

// MarshalResultPageResponse emits a canonical result-page response.
func MarshalResultPageResponse(value ResultPageResponse) ([]byte, error) {
	return MarshalControlMessage(value)
}

// UnmarshalResultPageResponse decodes a canonical result-page response.
func UnmarshalResultPageResponse(encoded []byte) (ResultPageResponse, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneResultPageResponse, encoded)
	if err != nil {
		return ResultPageResponse{}, err
	}
	return value.(ResultPageResponse), nil
}

// MarshalLeaderRedirect emits a canonical leader redirect.
func MarshalLeaderRedirect(value LeaderRedirect) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalLeaderRedirect decodes a canonical leader redirect.
func UnmarshalLeaderRedirect(encoded []byte) (LeaderRedirect, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneLeaderRedirect, encoded)
	if err != nil {
		return LeaderRedirect{}, err
	}
	return value.(LeaderRedirect), nil
}

// MarshalControlError emits a canonical public-control error.
func MarshalControlError(value ControlError) ([]byte, error) { return MarshalControlMessage(value) }

// UnmarshalControlError decodes a canonical public-control error.
func UnmarshalControlError(encoded []byte) (ControlError, error) {
	value, err := UnmarshalControlMessage(wire.MessageCraneControlError, encoded)
	if err != nil {
		return ControlError{}, err
	}
	return value.(ControlError), nil
}

func unmarshalSubmitRequestWith(encoded []byte, decode topologyDecodeFunc) (SubmitRequest, error) {
	value, err := unmarshalControlMessageWith(wire.MessageCraneSubmitRequest, encoded, decode)
	if err != nil {
		return SubmitRequest{}, err
	}
	return value.(SubmitRequest), nil
}

func unmarshalControlMessageWith(messageType wire.MessageType, encoded []byte, decode topologyDecodeFunc) (ControlMessage, error) {
	if messageType < wire.MessageCraneSubmitRequest || messageType > wire.MessageCraneControlError {
		return nil, ErrUnexpectedControlMessage
	}
	if len(encoded) > MaxControlPayloadBytes {
		return nil, fmt.Errorf("%w: %d", ErrControlMessageTooLarge, len(encoded))
	}
	decoder := controlDecoder{input: encoded, decodeTopology: decode}
	version, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	if version != ControlSchemaVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedControlSchema, version)
	}
	gotType, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	if wire.MessageType(gotType) != messageType {
		return nil, fmt.Errorf("%w: got %d want %d", ErrUnexpectedControlMessage, gotType, messageType)
	}
	message, err := decoder.message(messageType)
	if err != nil {
		return nil, err
	}
	if err := decoder.finish(); err != nil {
		return nil, err
	}
	if err := validateControlMessage(message); err != nil {
		return nil, invalidControl(message, err)
	}
	canonical, err := MarshalControlMessage(message)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("%w: non-canonical payload", ErrMalformedControlMessage)
	}
	return message, nil
}

type controlEncoder struct {
	output        []byte
	err           error
	trace         bool
	layout        []model.PublicControlFieldDescriptor
	traceNested   bool
	nestedSeen    map[string]bool
	nestedLayouts []model.PublicControlNestedDescriptor
}

func (encoder *controlEncoder) owned() []byte { return append([]byte(nil), encoder.output...) }

func (encoder *controlEncoder) add(value []byte) {
	if encoder.err != nil {
		return
	}
	if len(value) > MaxControlPayloadBytes-len(encoder.output) {
		encoder.err = ErrControlMessageTooLarge
		return
	}
	encoder.output = append(encoder.output, value...)
}

func (encoder *controlEncoder) field(name, encoding string, appendValue func()) {
	if encoder.trace {
		encoder.layout = append(encoder.layout, model.PublicControlFieldDescriptor{Name: name, Encoding: encoding})
	}
	appendValue()
}

func (encoder *controlEncoder) nested(layout string, appendFields func(func(string, string, func()))) {
	record := encoder.traceNested && !encoder.nestedSeen[layout]
	descriptorIndex := -1
	if record {
		encoder.nestedSeen[layout] = true
		encoder.nestedLayouts = append(encoder.nestedLayouts, model.PublicControlNestedDescriptor{Name: layout})
		descriptorIndex = len(encoder.nestedLayouts) - 1
	}
	field := func(name, encoding string, appendValue func()) {
		if record {
			encoder.nestedLayouts[descriptorIndex].Fields = append(encoder.nestedLayouts[descriptorIndex].Fields, model.PublicControlFieldDescriptor{Name: name, Encoding: encoding})
		}
		appendValue()
	}
	appendFields(field)
}

func (encoder *controlEncoder) u8(value byte) { encoder.add([]byte{value}) }
func (encoder *controlEncoder) bool(value bool) {
	if value {
		encoder.u8(1)
	} else {
		encoder.u8(0)
	}
}
func (encoder *controlEncoder) u16(value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	encoder.add(encoded[:])
}
func (encoder *controlEncoder) u32(value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	encoder.add(encoded[:])
}
func (encoder *controlEncoder) u64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	encoder.add(encoded[:])
}
func (encoder *controlEncoder) bytes16(value []byte) {
	if len(value) > math.MaxUint16 {
		encoder.err = ErrControlMessageTooLarge
		return
	}
	encoder.u16(uint16(len(value)))
	encoder.add(value)
}
func (encoder *controlEncoder) job(value model.JobID) {
	encoder.nested("JobID", func(field func(string, string, func())) {
		field("Value", "bytes16(nonzero)", func() { encoder.add(value[:]) })
	})
}
func (encoder *controlEncoder) request(value model.ClientRequestID) {
	encoder.nested("ClientRequestID", func(field func(string, string, func())) {
		field("ClientID", "bytes16(nonzero)", func() { encoder.add(value.ClientID[:]) })
		field("Sequence", "u64(nonzero)", func() { encoder.u64(value.Sequence) })
	})
}
func (encoder *controlEncoder) task(value model.TaskID) {
	encoder.nested("TaskID", func(field func(string, string, func())) {
		field("JobID", "JobID", func() { encoder.job(value.JobID) })
		field("StageID", "u16(nonzero)", func() { encoder.u16(value.StageID) })
		field("Partition", "u16", func() { encoder.u16(value.Partition) })
	})
}
func (encoder *controlEncoder) tuple(value model.TupleID) {
	encoder.nested("TupleID", func(field func(string, string, func())) {
		field("JobID", "JobID", func() { encoder.job(value.JobID) })
		field("SourceTask", "TaskID", func() { encoder.task(value.SourceTask) })
		field("SourceSequence", "u64(nonzero)", func() { encoder.u64(value.SourceSequence) })
		field("PathDigest", "sha256(nonzero)", func() { encoder.add(value.PathDigest[:]) })
	})
}
func (encoder *controlEncoder) topology(value model.TopologySpec) {
	encoder.nested("TopologySpec", func(field func(string, string, func())) {
		field("CanonicalTopology", "bytes64", func() {
			validated, _ := model.ValidateTopology(value)
			encoder.add(validated.CanonicalBytes())
		})
	})
}
func (encoder *controlEncoder) resultEntry(record model.ResultRecord) {
	encoder.nested("ResultRecordEntry", func(field func(string, string, func())) {
		stream, _ := model.MarshalResultRecord(record)
		field("Length", "u32", func() { encoder.u32(uint32(len(stream))) })
		field("Record", "canonical-result-record-v1", func() { encoder.add(stream) })
	})
}
func (encoder *controlEncoder) pageBinding(value ResultPageRequest) {
	encoder.nested("ResultPageBinding", func(field func(string, string, func())) {
		field("JobID", "JobID", func() { encoder.job(value.JobID) })
		field("ManifestDigest", "sha256", func() { encoder.add(value.ManifestDigest[:]) })
		field("HasLastTuple", "bool", func() { encoder.bool(value.HasLastTuple) })
		field("Last", "TupleID", func() { encoder.tuple(value.Last) })
		field("PageBytes", "u32", func() { encoder.u32(value.PageBytes) })
	})
}

func (encoder *controlEncoder) message(message ControlMessage) error {
	switch value := message.(type) {
	case SubmitRequest:
		encoder.field("Request", "ClientRequestID", func() { encoder.request(value.Request) })
		encoder.field("Topology", "TopologySpec", func() { encoder.topology(value.Topology) })
		encoder.field("Digest", "sha256", func() { encoder.add(value.Digest[:]) })
	case SubmitResponse:
		encoder.field("Request", "ClientRequestID", func() { encoder.request(value.Request) })
		encoder.field("Digest", "sha256", func() { encoder.add(value.Digest[:]) })
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("JobControlRevision", "u64", func() { encoder.u64(value.JobControlRevision) })
		encoder.field("State", "u8", func() { encoder.u8(byte(value.State)) })
	case CancelRequest:
		encoder.field("Request", "ClientRequestID", func() { encoder.request(value.Request) })
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("ExpectedJobControlRevision", "u64", func() { encoder.u64(value.ExpectedJobControlRevision) })
		encoder.field("Digest", "sha256", func() { encoder.add(value.Digest[:]) })
	case CancelResponse:
		encoder.field("Request", "ClientRequestID", func() { encoder.request(value.Request) })
		encoder.field("Digest", "sha256", func() { encoder.add(value.Digest[:]) })
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("JobControlRevision", "u64", func() { encoder.u64(value.JobControlRevision) })
		encoder.field("State", "u8", func() { encoder.u8(byte(value.State)) })
	case StatusRequest:
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
	case StatusResponse:
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("AppliedIndex", "u64", func() { encoder.u64(value.AppliedIndex) })
		encoder.field("TopologyDigest", "sha256", func() { encoder.add(value.TopologyDigest[:]) })
		encoder.field("JobControlRevision", "u64", func() { encoder.u64(value.JobControlRevision) })
		encoder.field("State", "u8", func() { encoder.u8(byte(value.State)) })
		encoder.field("HasAssignment", "bool", func() { encoder.bool(value.HasAssignment) })
		encoder.field("AssignmentRevision", "u64", func() { encoder.u64(value.AssignmentRevision) })
		encoder.field("AssignmentDigest", "sha256", func() { encoder.add(value.AssignmentDigest[:]) })
		encoder.field("SourceTaskCount", "u16", func() { encoder.u16(value.SourceTaskCount) })
		encoder.field("ResultPartitionCount", "u16", func() { encoder.u16(value.ResultPartitionCount) })
		encoder.field("CompletedSourceTasks", "u16", func() { encoder.u16(value.CompletedSourceTasks) })
		encoder.field("ManifestCount", "u16", func() { encoder.u16(value.ManifestCount) })
		encoder.field("HasManifestSet", "bool", func() { encoder.bool(value.HasManifestSet) })
		encoder.field("ManifestSetDigest", "sha256", func() { encoder.add(value.ManifestSetDigest[:]) })
		encoder.field("HasFailure", "bool", func() { encoder.bool(value.HasFailure) })
		encoder.field("FailureCode", "u16", func() { encoder.u16(uint16(value.FailureCode)) })
		encoder.field("FailureDetailDigest", "sha256", func() { encoder.add(value.FailureDetailDigest[:]) })
	case ResultPageRequest:
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("ManifestDigest", "sha256", func() { encoder.add(value.ManifestDigest[:]) })
		encoder.field("HasLastTuple", "bool", func() { encoder.bool(value.HasLastTuple) })
		encoder.field("Last", "TupleID", func() { encoder.tuple(value.Last) })
		encoder.field("PageBytes", "u32", func() { encoder.u32(value.PageBytes) })
	case ResultPageResponse:
		encoder.field("JobID", "JobID", func() { encoder.job(value.JobID) })
		encoder.field("ManifestDigest", "sha256", func() { encoder.add(value.ManifestDigest[:]) })
		encoder.field("RequestHasLastTuple", "bool", func() { encoder.bool(value.RequestHasLastTuple) })
		encoder.field("RequestLast", "TupleID", func() { encoder.tuple(value.RequestLast) })
		encoder.field("PageBytes", "u32", func() { encoder.u32(value.PageBytes) })
		encoder.field("Records", "list(ResultRecordEntry)", func() {
			encoder.u16(uint16(len(value.Records)))
			for _, record := range value.Records {
				encoder.resultEntry(record)
			}
		})
		encoder.field("NextHasLastTuple", "bool", func() { encoder.bool(value.NextHasLastTuple) })
		encoder.field("NextLast", "TupleID", func() { encoder.tuple(value.NextLast) })
		encoder.field("End", "bool", func() { encoder.bool(value.End) })
	case LeaderRedirect:
		encoder.field("Endpoints", "list(string16)", func() {
			encoder.u16(uint16(len(value.Endpoints)))
			for _, endpoint := range value.Endpoints {
				encoder.bytes16([]byte(endpoint))
			}
		})
	case ControlError:
		encoder.field("RelatedMessage", "u16", func() { encoder.u16(uint16(value.RelatedMessage)) })
		encoder.field("Code", "u16", func() { encoder.u16(uint16(value.Code)) })
		encoder.field("Retryable", "bool", func() { encoder.bool(value.Retryable) })
		encoder.field("HasClientRequest", "bool", func() { encoder.bool(value.HasClientRequest) })
		encoder.field("ClientRequest", "ClientRequestID", func() { encoder.request(value.ClientRequest) })
		encoder.field("ClientDigest", "sha256", func() { encoder.add(value.ClientDigest[:]) })
		encoder.field("HasStatusRequest", "bool", func() { encoder.bool(value.HasStatusRequest) })
		encoder.field("StatusJobID", "JobID", func() { encoder.job(value.StatusJobID) })
		encoder.field("HasResultPage", "bool", func() { encoder.bool(value.HasResultPage) })
		encoder.field("ResultPage", "ResultPageBinding", func() { encoder.pageBinding(value.ResultPage) })
		encoder.field("RequiredBytes", "u32", func() { encoder.u32(value.RequiredBytes) })
		encoder.field("Detail", "bytes16", func() { encoder.bytes16(value.Detail) })
	default:
		return ErrUnexpectedControlMessage
	}
	return encoder.err
}

type controlDecoder struct {
	input          []byte
	offset         int
	decodeTopology topologyDecodeFunc
}

func (decoder *controlDecoder) remaining() int { return len(decoder.input) - decoder.offset }
func (decoder *controlDecoder) take(length int) ([]byte, error) {
	if length < 0 || length > decoder.remaining() {
		return nil, fmt.Errorf("%w: truncated field", ErrMalformedControlMessage)
	}
	value := decoder.input[decoder.offset : decoder.offset+length]
	decoder.offset += length
	return value, nil
}
func (decoder *controlDecoder) finish() error {
	if decoder.remaining() != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedControlMessage, decoder.remaining())
	}
	return nil
}
func (decoder *controlDecoder) u8() (byte, error) {
	value, err := decoder.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}
func (decoder *controlDecoder) bool() (bool, error) {
	value, err := decoder.u8()
	if err != nil {
		return false, err
	}
	if value > 1 {
		return false, ErrMalformedControlMessage
	}
	return value == 1, nil
}
func (decoder *controlDecoder) u16() (uint16, error) {
	value, err := decoder.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}
func (decoder *controlDecoder) u32() (uint32, error) {
	value, err := decoder.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}
func (decoder *controlDecoder) u64() (uint64, error) {
	value, err := decoder.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}
func (decoder *controlDecoder) bytes16(limit int) ([]byte, error) {
	length, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	if int(length) > limit || int(length) > decoder.remaining() {
		return nil, ErrMalformedControlMessage
	}
	value, err := decoder.take(int(length))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}
func (decoder *controlDecoder) digest() ([32]byte, error) {
	value, err := decoder.take(32)
	var digest [32]byte
	if err == nil {
		copy(digest[:], value)
	}
	return digest, err
}
func (decoder *controlDecoder) job() (model.JobID, error) {
	value, err := decoder.take(16)
	var job model.JobID
	if err == nil {
		copy(job[:], value)
	}
	return job, err
}
func (decoder *controlDecoder) request() (model.ClientRequestID, error) {
	clientBytes, err := decoder.take(16)
	if err != nil {
		return model.ClientRequestID{}, err
	}
	sequence, err := decoder.u64()
	var client model.ClientID
	copy(client[:], clientBytes)
	return model.ClientRequestID{ClientID: client, Sequence: sequence}, err
}
func (decoder *controlDecoder) task() (model.TaskID, error) {
	job, err := decoder.job()
	if err != nil {
		return model.TaskID{}, err
	}
	stage, err := decoder.u16()
	if err != nil {
		return model.TaskID{}, err
	}
	partition, err := decoder.u16()
	return model.TaskID{JobID: job, StageID: stage, Partition: partition}, err
}
func (decoder *controlDecoder) tuple() (model.TupleID, error) {
	job, err := decoder.job()
	if err != nil {
		return model.TupleID{}, err
	}
	source, err := decoder.task()
	if err != nil {
		return model.TupleID{}, err
	}
	sequence, err := decoder.u64()
	if err != nil {
		return model.TupleID{}, err
	}
	digest, err := decoder.digest()
	return model.TupleID{JobID: job, SourceTask: source, SourceSequence: sequence, PathDigest: digest}, err
}
func (decoder *controlDecoder) pageBinding() (ResultPageRequest, error) {
	job, err := decoder.job()
	if err != nil {
		return ResultPageRequest{}, err
	}
	manifest, err := decoder.digest()
	if err != nil {
		return ResultPageRequest{}, err
	}
	hasLast, err := decoder.bool()
	if err != nil {
		return ResultPageRequest{}, err
	}
	last, err := decoder.tuple()
	if err != nil {
		return ResultPageRequest{}, err
	}
	pageBytes, err := decoder.u32()
	return ResultPageRequest{JobID: job, ManifestDigest: manifest, HasLastTuple: hasLast, Last: last, PageBytes: pageBytes}, err
}
func (decoder *controlDecoder) topology() (model.ValidatedTopology, error) {
	if decoder.remaining() < 8 {
		return model.ValidatedTopology{}, ErrMalformedControlMessage
	}
	declared := binary.BigEndian.Uint64(decoder.input[decoder.offset : decoder.offset+8])
	maximumBody := model.LimitsV1().MaxTopologyBytes - 8
	if declared > maximumBody {
		return model.ValidatedTopology{}, fmt.Errorf("%w: declared topology %d", ErrControlMessageTooLarge, declared)
	}
	total := declared + 8
	if total > uint64(decoder.remaining()) {
		return model.ValidatedTopology{}, ErrMalformedControlMessage
	}
	encoded := decoder.input[decoder.offset : decoder.offset+int(total)]
	validated, err := decoder.decodeTopology(encoded)
	if err != nil {
		return model.ValidatedTopology{}, fmt.Errorf("%w: topology: %v", ErrInvalidControlMessage, err)
	}
	decoder.offset += int(total)
	return validated, nil
}
func (decoder *controlDecoder) resultRecords() ([]model.ResultRecord, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	if int(count) > MaxResultPageRecords || int(count) > decoder.remaining()/int(MinEncodedResultRecordBytes) {
		return nil, ErrMalformedControlMessage
	}
	records := make([]model.ResultRecord, int(count))
	for index := range records {
		length, err := decoder.u32()
		if err != nil {
			return nil, err
		}
		if length+4 > MaxEncodedResultRecordBytes || int(length) > decoder.remaining() {
			return nil, ErrMalformedControlMessage
		}
		encoded, err := decoder.take(int(length))
		if err != nil {
			return nil, err
		}
		records[index], err = model.UnmarshalResultRecord(encoded)
		if err != nil {
			return nil, fmt.Errorf("%w: result record %d: %v", ErrInvalidControlMessage, index, err)
		}
	}
	return records, nil
}

func (decoder *controlDecoder) message(messageType wire.MessageType) (ControlMessage, error) {
	switch messageType {
	case wire.MessageCraneSubmitRequest:
		request, err := decoder.request()
		if err != nil {
			return nil, err
		}
		topology, err := decoder.topology()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.digest()
		return SubmitRequest{Request: request, Topology: topology.Spec(), Digest: digest}, err
	case wire.MessageCraneSubmitResponse:
		request, err := decoder.request()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.digest()
		if err != nil {
			return nil, err
		}
		job, err := decoder.job()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		state, err := decoder.u8()
		return SubmitResponse{Request: request, Digest: digest, JobID: job, JobControlRevision: revision, State: JobState(state)}, err
	case wire.MessageCraneCancelRequest:
		request, err := decoder.request()
		if err != nil {
			return nil, err
		}
		job, err := decoder.job()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.digest()
		return CancelRequest{Request: request, JobID: job, ExpectedJobControlRevision: revision, Digest: digest}, err
	case wire.MessageCraneCancelResponse:
		request, err := decoder.request()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.digest()
		if err != nil {
			return nil, err
		}
		job, err := decoder.job()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		state, err := decoder.u8()
		return CancelResponse{Request: request, Digest: digest, JobID: job, JobControlRevision: revision, State: JobState(state)}, err
	case wire.MessageCraneStatusRequest:
		job, err := decoder.job()
		return StatusRequest{JobID: job}, err
	case wire.MessageCraneStatusResponse:
		return decoder.statusResponse()
	case wire.MessageCraneResultPageRequest:
		return decoder.pageBinding()
	case wire.MessageCraneResultPageResponse:
		return decoder.resultPageResponse()
	case wire.MessageCraneLeaderRedirect:
		return decoder.leaderRedirect()
	case wire.MessageCraneControlError:
		return decoder.controlError()
	default:
		return nil, ErrUnexpectedControlMessage
	}
}

func (decoder *controlDecoder) statusResponse() (ControlMessage, error) {
	job, err := decoder.job()
	if err != nil {
		return nil, err
	}
	applied, err := decoder.u64()
	if err != nil {
		return nil, err
	}
	topology, err := decoder.digest()
	if err != nil {
		return nil, err
	}
	jobRevision, err := decoder.u64()
	if err != nil {
		return nil, err
	}
	state, err := decoder.u8()
	if err != nil {
		return nil, err
	}
	hasAssignment, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	assignmentRevision, err := decoder.u64()
	if err != nil {
		return nil, err
	}
	assignmentDigest, err := decoder.digest()
	if err != nil {
		return nil, err
	}
	sources, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	resultPartitions, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	completed, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	manifests, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	hasManifest, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	manifestDigest, err := decoder.digest()
	if err != nil {
		return nil, err
	}
	hasFailure, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	failureCode, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	failureDigest, err := decoder.digest()
	return StatusResponse{JobID: job, AppliedIndex: applied, TopologyDigest: topology, JobControlRevision: jobRevision, State: JobState(state), HasAssignment: hasAssignment, AssignmentRevision: assignmentRevision, AssignmentDigest: assignmentDigest, SourceTaskCount: sources, ResultPartitionCount: resultPartitions, CompletedSourceTasks: completed, ManifestCount: manifests, HasManifestSet: hasManifest, ManifestSetDigest: manifestDigest, HasFailure: hasFailure, FailureCode: model.FailureCode(failureCode), FailureDetailDigest: failureDigest}, err
}

func (decoder *controlDecoder) resultPageResponse() (ControlMessage, error) {
	job, err := decoder.job()
	if err != nil {
		return nil, err
	}
	manifest, err := decoder.digest()
	if err != nil {
		return nil, err
	}
	hasRequest, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	requestLast, err := decoder.tuple()
	if err != nil {
		return nil, err
	}
	pageBytes, err := decoder.u32()
	if err != nil {
		return nil, err
	}
	records, err := decoder.resultRecords()
	if err != nil {
		return nil, err
	}
	hasNext, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	nextLast, err := decoder.tuple()
	if err != nil {
		return nil, err
	}
	end, err := decoder.bool()
	return ResultPageResponse{JobID: job, ManifestDigest: manifest, RequestHasLastTuple: hasRequest, RequestLast: requestLast, PageBytes: pageBytes, Records: records, NextHasLastTuple: hasNext, NextLast: nextLast, End: end}, err
}

func (decoder *controlDecoder) leaderRedirect() (ControlMessage, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	if int(count) > MaxLeaderRedirectEndpoints || int(count) > decoder.remaining()/2 {
		return nil, ErrMalformedControlMessage
	}
	endpoints := make([]string, int(count))
	for index := range endpoints {
		encoded, err := decoder.bytes16(MaxControlEndpointBytes)
		if err != nil {
			return nil, err
		}
		endpoints[index] = string(encoded)
	}
	return LeaderRedirect{Endpoints: endpoints}, nil
}

func (decoder *controlDecoder) controlError() (ControlMessage, error) {
	related, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	code, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	retryable, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	hasClient, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	client, err := decoder.request()
	if err != nil {
		return nil, err
	}
	clientDigest, err := decoder.digest()
	if err != nil {
		return nil, err
	}
	hasStatus, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	statusJob, err := decoder.job()
	if err != nil {
		return nil, err
	}
	hasPage, err := decoder.bool()
	if err != nil {
		return nil, err
	}
	page, err := decoder.pageBinding()
	if err != nil {
		return nil, err
	}
	required, err := decoder.u32()
	if err != nil {
		return nil, err
	}
	detail, err := decoder.bytes16(MaxControlErrorDetailBytes)
	return ControlError{RelatedMessage: wire.MessageType(related), Code: ControlErrorCode(code), Retryable: retryable, HasClientRequest: hasClient, ClientRequest: client, ClientDigest: clientDigest, HasStatusRequest: hasStatus, StatusJobID: statusJob, HasResultPage: hasPage, ResultPage: page, RequiredBytes: required, Detail: detail}, err
}

func preflightTupleDelivery(input []byte) error {
	maximum := TupleDeliveryMaxPayloadBytes()
	if len(input) < TupleDeliveryMinPayloadBytes {
		return fmt.Errorf("%w: delivery payload is %d bytes, minimum is %d", ErrMalformedTupleMessage, len(input), TupleDeliveryMinPayloadBytes)
	}
	if len(input) > maximum {
		return fmt.Errorf("%w: delivery payload is %d bytes, maximum is %d", ErrTupleMessageTooLarge, len(input), maximum)
	}
	lengthOffset := tupleMessagePrefixBytes + tupleDeliveryIDBytes
	declared := int(binary.BigEndian.Uint16(input[lengthOffset : lengthOffset+tupleLengthBytes]))
	if uint64(declared) > model.LimitsV1().MaxTuplePayloadBytes {
		return fmt.Errorf("%w: declared tuple length %d exceeds %d", ErrMalformedTupleMessage, declared, model.LimitsV1().MaxTuplePayloadBytes)
	}
	expected := TupleDeliveryFixedPayloadBytes + declared
	if len(input) != expected {
		return fmt.Errorf("%w: delivery payload is %d bytes, declared shape requires %d", ErrMalformedTupleMessage, len(input), expected)
	}
	return nil
}

func (decoder *tupleDecoder) remaining() int { return len(decoder.input) - decoder.offset }

func (decoder *tupleDecoder) finish() error {
	if decoder.remaining() != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformedTupleMessage, decoder.remaining())
	}
	return nil
}

func (decoder *tupleDecoder) take(length int) ([]byte, error) {
	if length < 0 || length > decoder.remaining() {
		return nil, fmt.Errorf("%w: truncated fixed field", ErrMalformedTupleMessage)
	}
	end := decoder.offset + length
	value := decoder.input[decoder.offset:end]
	decoder.offset = end
	return value, nil
}

func (decoder *tupleDecoder) byte() (byte, error) {
	value, err := decoder.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (decoder *tupleDecoder) uint16() (uint16, error) {
	value, err := decoder.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func (decoder *tupleDecoder) uint64() (uint64, error) {
	value, err := decoder.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}

func (decoder *tupleDecoder) boundedBytes16(limit uint64) ([]byte, error) {
	length, err := decoder.uint16()
	if err != nil {
		return nil, err
	}
	if uint64(length) > limit {
		return nil, fmt.Errorf("%w: declared byte length %d exceeds %d", ErrMalformedTupleMessage, length, limit)
	}
	return decoder.take(int(length))
}

func (decoder *tupleDecoder) taskID() (model.TaskID, error) {
	var value model.TaskID
	job, err := decoder.take(len(value.JobID))
	if err != nil {
		return model.TaskID{}, err
	}
	copy(value.JobID[:], job)
	value.StageID, err = decoder.uint16()
	if err != nil {
		return model.TaskID{}, err
	}
	value.Partition, err = decoder.uint16()
	if err != nil {
		return model.TaskID{}, err
	}
	return value, nil
}

func (decoder *tupleDecoder) tupleID() (model.TupleID, error) {
	var value model.TupleID
	job, err := decoder.take(len(value.JobID))
	if err != nil {
		return model.TupleID{}, err
	}
	copy(value.JobID[:], job)
	value.SourceTask, err = decoder.taskID()
	if err != nil {
		return model.TupleID{}, err
	}
	value.SourceSequence, err = decoder.uint64()
	if err != nil {
		return model.TupleID{}, err
	}
	digest, err := decoder.take(len(value.PathDigest))
	if err != nil {
		return model.TupleID{}, err
	}
	copy(value.PathDigest[:], digest)
	return value, nil
}

func (decoder *tupleDecoder) deliveryID() (model.DeliveryID, error) {
	var value model.DeliveryID
	var err error
	value.Tuple, err = decoder.tupleID()
	if err != nil {
		return model.DeliveryID{}, err
	}
	value.EdgeID, err = decoder.uint16()
	if err != nil {
		return model.DeliveryID{}, err
	}
	value.DestinationTask, err = decoder.taskID()
	if err != nil {
		return model.DeliveryID{}, err
	}
	return value, nil
}

func (decoder *tupleDecoder) assignmentToken() (model.AssignmentToken, error) {
	var value model.AssignmentToken
	var err error
	value.Task, err = decoder.taskID()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	value.WorkerID, err = decoder.uint16()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	epoch, err := decoder.take(len(value.WorkerEpoch))
	if err != nil {
		return model.AssignmentToken{}, err
	}
	copy(value.WorkerEpoch[:], epoch)
	value.Attempt, err = decoder.uint64()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	specification, err := decoder.take(len(value.SpecificationHash))
	if err != nil {
		return model.AssignmentToken{}, err
	}
	copy(value.SpecificationHash[:], specification)
	value.AssignmentRevision, err = decoder.uint64()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	return value, nil
}

func (decoder *tupleDecoder) assignmentIdentity() (AssignmentSetIdentity, error) {
	var value AssignmentSetIdentity
	job, err := decoder.take(len(value.JobID))
	if err != nil {
		return AssignmentSetIdentity{}, err
	}
	copy(value.JobID[:], job)
	value.Revision, err = decoder.uint64()
	if err != nil {
		return AssignmentSetIdentity{}, err
	}
	digest, err := decoder.take(len(value.Digest))
	if err != nil {
		return AssignmentSetIdentity{}, err
	}
	copy(value.Digest[:], digest)
	return value, nil
}

func (decoder *tupleDecoder) coordinatorEpoch() (model.CoordinatorEpoch, error) {
	var value model.CoordinatorEpoch
	var err error
	value.Term, err = decoder.uint64()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	value.BeginIndex, err = decoder.uint64()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	value.Coordinator, err = decoder.uint16()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	nonce, err := decoder.take(len(value.Nonce))
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	copy(value.Nonce[:], nonce)
	return value, nil
}
