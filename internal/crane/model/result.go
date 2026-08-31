package model

import (
	"bytes"
	"crypto/sha256"
	"errors"
)

const (
	// ResultArtifactMinRecordBytesV1 is the smallest complete length-prefixed
	// canonical result record stored in a sealed artifact.
	ResultArtifactMinRecordBytesV1 = PublicControlMinEncodedResultRecordBytesV1
	// ResultArtifactMaxRecordBytesV1 is the largest complete length-prefixed
	// canonical result record stored in a sealed artifact.
	ResultArtifactMaxRecordBytesV1 = PublicControlMaxEncodedResultRecordBytesV1
	// ResultArtifactMaxRecordCountV1 is the maximum minimum-size records that
	// fit the v1 64-MiB per-job artifact budget.
	ResultArtifactMaxRecordCountV1 uint64 = (64 << 20) / ResultArtifactMinRecordBytesV1
)

// ResultReplicaRole selects one destination from a committed replica set.
type ResultReplicaRole uint8

const (
	PrimaryReplica ResultReplicaRole = iota + 1
	SecondaryReplica
)

// ResultRecord is immutable logical collect output independent of current copies.
type ResultRecord struct {
	TupleID           TupleID
	SinkTask          TaskID
	SpecificationHash [32]byte
	Value             []byte
	Checksum          [32]byte
}

// ResultCopyProvenance fences one physical copy without changing logical identity.
type ResultCopyProvenance struct {
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
	ReplicaSet         ResultReplicaSet
	DestinationRole    ResultReplicaRole
	CoordinatorEpoch   CoordinatorEpoch
}

// NewResultRecord owns value and calculates its immutable logical checksum.
func NewResultRecord(tuple TupleID, sink TaskID, specificationHash [32]byte, value []byte) (ResultRecord, error) {
	if uint64(len(value)) > LimitsV1().MaxTuplePayloadBytes {
		return ResultRecord{}, errors.New("result value exceeds canonical tuple bound before copy")
	}
	decoded, err := UnmarshalTuple(value)
	if err != nil {
		return ResultRecord{}, errors.New("result value is not a canonical tuple")
	}
	canonical, err := MarshalTuple(decoded)
	if err != nil || !bytes.Equal(canonical, value) {
		return ResultRecord{}, errors.New("result value is not canonical")
	}
	record := ResultRecord{TupleID: tuple, SinkTask: sink, SpecificationHash: specificationHash, Value: append([]byte(nil), value...)}
	record.Checksum = resultChecksum(record)
	if err := record.Validate(); err != nil {
		return ResultRecord{}, err
	}
	return record, nil
}

// Validate checks immutable result identity and checksum.
func (record ResultRecord) Validate() error {
	if err := record.TupleID.Validate(); err != nil {
		return err
	}
	if err := record.SinkTask.Validate(); err != nil || record.SinkTask.JobID != record.TupleID.JobID {
		return errors.New("invalid or foreign result sink task")
	}
	if record.SpecificationHash == ([32]byte{}) {
		return errors.New("zero result specification hash")
	}
	if record.Value == nil {
		return errors.New("nil result value")
	}
	if uint64(len(record.Value)) > LimitsV1().MaxTuplePayloadBytes {
		return errors.New("result value exceeds canonical tuple bound")
	}
	decoded, err := UnmarshalTuple(record.Value)
	if err != nil {
		return errors.New("result value is not a canonical tuple")
	}
	canonical, err := MarshalTuple(decoded)
	if err != nil || !bytes.Equal(canonical, record.Value) {
		return errors.New("result value is not canonical")
	}
	if record.Checksum == ([32]byte{}) || record.Checksum != resultChecksum(record) {
		return errors.New("result checksum mismatch")
	}
	return nil
}

// Validate checks exact current-copy provenance against one logical record.
func (provenance ResultCopyProvenance) Validate(record ResultRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if provenance.AssignmentRevision == 0 || provenance.AssignmentDigest == ([32]byte{}) {
		return errors.New("zero result copy assignment fence")
	}
	if err := provenance.ReplicaSet.Validate(); err != nil {
		return err
	}
	if provenance.ReplicaSet.SinkTask != record.SinkTask {
		return errors.New("result replica set does not match logical sink task")
	}
	switch provenance.DestinationRole {
	case PrimaryReplica, SecondaryReplica:
	default:
		return errors.New("unknown result destination role")
	}
	return provenance.CoordinatorEpoch.Validate()
}

func resultChecksum(record ResultRecord) [32]byte {
	encoded := appendTaskID(nil, record.SinkTask)
	encoded = appendTupleID(encoded, record.TupleID)
	encoded = append(encoded, record.SpecificationHash[:]...)
	encoded = appendUint16(encoded, uint16(len(record.Value)))
	encoded = append(encoded, record.Value...)
	return sha256.Sum256(encoded)
}

// MarshalResultRecord returns the versioned canonical logical result stream.
func MarshalResultRecord(record ResultRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	encoded := appendUint16(nil, WorkerControlContractV1().ResultRecordStreamSchemaVersion)
	encoded = appendTupleID(encoded, record.TupleID)
	encoded = appendTaskID(encoded, record.SinkTask)
	encoded = append(encoded, record.SpecificationHash[:]...)
	encoded = appendUint16(encoded, uint16(len(record.Value)))
	encoded = append(encoded, record.Value...)
	encoded = append(encoded, record.Checksum[:]...)
	return encoded, nil
}

// UnmarshalResultRecord accepts only a complete canonical logical result stream.
func UnmarshalResultRecord(encoded []byte) (ResultRecord, error) {
	// schema + tuple ID + sink task + spec hash + value length + checksum
	const fixedBytes = 2 + 76 + 20 + 32 + 2 + 32
	if len(encoded) < fixedBytes || uint64(len(encoded)) > uint64(fixedBytes)+LimitsV1().MaxTuplePayloadBytes {
		return ResultRecord{}, errors.New("result record stream size outside bounds")
	}
	reader := checkedReader{input: encoded}
	schema, err := reader.uint16()
	if err != nil || schema != WorkerControlContractV1().ResultRecordStreamSchemaVersion {
		return ResultRecord{}, errors.New("unsupported result record stream schema")
	}
	var record ResultRecord
	if record.TupleID, err = readResultTupleID(&reader); err != nil {
		return ResultRecord{}, err
	}
	if record.SinkTask, err = readResultTaskID(&reader); err != nil {
		return ResultRecord{}, err
	}
	if reader.remaining() < 32 {
		return ResultRecord{}, errors.New("truncated result specification hash")
	}
	copy(record.SpecificationHash[:], reader.input[reader.offset:reader.offset+32])
	reader.offset += 32
	if record.Value, err = reader.bytes(LimitsV1().MaxTuplePayloadBytes); err != nil {
		return ResultRecord{}, err
	}
	if reader.remaining() != 32 {
		return ResultRecord{}, errors.New("truncated or trailing result checksum bytes")
	}
	copy(record.Checksum[:], reader.input[reader.offset:])
	reader.offset += 32
	if err := record.Validate(); err != nil {
		return ResultRecord{}, err
	}
	return record, nil
}

// ResultRecordStreamChecksum hashes the full canonical logical stream.
func ResultRecordStreamChecksum(record ResultRecord) [32]byte {
	encoded, err := MarshalResultRecord(record)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(encoded)
}

func readResultTaskID(reader *checkedReader) (TaskID, error) {
	if reader.remaining() < 20 {
		return TaskID{}, errors.New("truncated result task ID")
	}
	var task TaskID
	copy(task.JobID[:], reader.input[reader.offset:reader.offset+16])
	reader.offset += 16
	var err error
	if task.StageID, err = reader.uint16(); err != nil {
		return TaskID{}, err
	}
	if task.Partition, err = reader.uint16(); err != nil {
		return TaskID{}, err
	}
	return task, nil
}

func readResultTupleID(reader *checkedReader) (TupleID, error) {
	if reader.remaining() < 76 {
		return TupleID{}, errors.New("truncated result tuple ID")
	}
	var tuple TupleID
	copy(tuple.JobID[:], reader.input[reader.offset:reader.offset+16])
	reader.offset += 16
	var err error
	if tuple.SourceTask, err = readResultTaskID(reader); err != nil {
		return TupleID{}, err
	}
	if tuple.SourceSequence, err = reader.uint64(); err != nil {
		return TupleID{}, err
	}
	copy(tuple.PathDigest[:], reader.input[reader.offset:reader.offset+32])
	reader.offset += 32
	return tuple, nil
}
