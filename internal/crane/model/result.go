package model

import (
	"bytes"
	"crypto/sha256"
	"errors"
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
