package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	identityClient   byte = 1
	identityInternal byte = 2
)

// MarshalBeginCoordinatorEpoch emits the sole canonical concrete Task 9 command.
func MarshalBeginCoordinatorEpoch(command BeginCoordinatorEpoch) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCommand, err)
	}
	encoded := make([]byte, 0, int(commandFixedEnvelopeBytes+internalEnvelopeBytes+beginTargetBytes))
	encoded = appendU16(encoded, command.Envelope.SchemaVersion)
	encoded = append(encoded, command.Envelope.ConsensusFingerprint[:]...)
	encoded = appendU16(encoded, uint16(command.Envelope.Kind))
	encoded = append(encoded, identityInternal)
	internal := command.Envelope.Internal
	encoded = append(encoded, internal.ID[:]...)
	encoded = append(encoded, internal.Digest[:]...)
	encoded = appendSubject(encoded, internal.Subject)
	encoded = appendU64(encoded, internal.ExpectedRevision)
	encoded = append(encoded, beginTarget(command)...)
	return append([]byte(nil), encoded...), nil
}

// UnmarshalBeginCoordinatorEpoch decodes only complete canonical Begin commands.
func UnmarshalBeginCoordinatorEpoch(encoded []byte) (BeginCoordinatorEpoch, error) {
	if uint64(len(encoded)) != commandFixedEnvelopeBytes+internalEnvelopeBytes+beginTargetBytes {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: command length %d", ErrMalformedCommand, len(encoded))
	}
	decoder := commandDecoder{input: encoded}
	version, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	fingerprint, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	kind, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	selector, err := decoder.byte()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	if selector != identityInternal {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: begin identity selector %d", ErrMalformedCommand, selector)
	}
	idBytes, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	digest, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	subject, err := decoder.subject()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	expectedRevision, err := decoder.u64()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	coordinator, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	nonce, err := decoder.array16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	if !decoder.done() {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: trailing bytes", ErrMalformedCommand)
	}
	command := BeginCoordinatorEpoch{
		Envelope:    Envelope{SchemaVersion: version, ConsensusFingerprint: fingerprint, Kind: CommandKind(kind), Internal: &InternalEnvelope{ID: InternalCommandID(idBytes), Digest: digest, Subject: subject, ExpectedRevision: expectedRevision}},
		Coordinator: coordinator, Nonce: nonce,
	}
	if err := command.Validate(); err != nil {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: %v", ErrInvalidCommand, err)
	}
	canonical, err := MarshalBeginCoordinatorEpoch(command)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: noncanonical command", ErrMalformedCommand)
	}
	return command, nil
}

// MarshalCommandResult emits the fixed, bounded canonical result schema.
func MarshalCommandResult(result CommandResult) ([]byte, error) {
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedCommandResult, err)
	}
	encoded := make([]byte, 0, int(commandResultBytes))
	encoded = appendU16(encoded, CommandSchemaVersion)
	encoded = appendU16(encoded, uint16(result.Code))
	encoded = append(encoded, byte(result.Subject))
	encoded = appendU64(encoded, result.Revision)
	encoded = append(encoded, result.JobID[:]...)
	encoded = appendU16(encoded, result.WorkerID)
	encoded = appendU64(encoded, result.Epoch.Term)
	encoded = appendU64(encoded, result.Epoch.BeginIndex)
	encoded = appendU16(encoded, result.Epoch.Coordinator)
	encoded = append(encoded, result.Epoch.Nonce[:]...)
	if uint64(len(encoded)) != commandResultBytes {
		return nil, errors.New("impossible command result size")
	}
	return append([]byte(nil), encoded...), nil
}

// UnmarshalCommandResult accepts only the complete canonical fixed result.
func UnmarshalCommandResult(encoded []byte) (CommandResult, error) {
	if uint64(len(encoded)) != commandResultBytes {
		return CommandResult{}, fmt.Errorf("%w: result length %d", ErrMalformedCommandResult, len(encoded))
	}
	decoder := commandDecoder{input: encoded}
	version, _ := decoder.u16()
	if version != CommandSchemaVersion {
		return CommandResult{}, fmt.Errorf("%w: schema %d", ErrMalformedCommandResult, version)
	}
	code, _ := decoder.u16()
	subject, _ := decoder.byte()
	revision, _ := decoder.u64()
	jobBytes, _ := decoder.array16()
	worker, _ := decoder.u16()
	term, _ := decoder.u64()
	index, _ := decoder.u64()
	coordinator, _ := decoder.u16()
	nonce, _ := decoder.array16()
	result := CommandResult{Code: ResultCode(code), Subject: SubjectKind(subject), Revision: revision, JobID: model.JobID(jobBytes), WorkerID: worker, Epoch: model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: coordinator, Nonce: nonce}}
	if err := result.Validate(); err != nil {
		return CommandResult{}, fmt.Errorf("%w: %v", ErrMalformedCommandResult, err)
	}
	return result, nil
}

func internalDigest(envelope Envelope, target []byte) [32]byte {
	encoded := make([]byte, 0, len(internalCommandDigestDomain)+int(commandFixedEnvelopeBytes+internalEnvelopeBytes-32))
	encoded = append(encoded, internalCommandDigestDomain...)
	encoded = appendU16(encoded, envelope.SchemaVersion)
	encoded = append(encoded, envelope.ConsensusFingerprint[:]...)
	encoded = appendU16(encoded, uint16(envelope.Kind))
	encoded = append(encoded, identityInternal)
	if envelope.Internal == nil {
		return sha256.Sum256(encoded)
	}
	encoded = append(encoded, envelope.Internal.ID[:]...)
	encoded = appendSubject(encoded, envelope.Internal.Subject)
	encoded = appendU64(encoded, envelope.Internal.ExpectedRevision)
	hash := sha256.New()
	_, _ = hash.Write(encoded)
	_, _ = hash.Write(target)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func beginTarget(command BeginCoordinatorEpoch) []byte {
	target := make([]byte, 0, beginTargetBytes)
	target = appendU16(target, command.Coordinator)
	return append(target, command.Nonce[:]...)
}

func beginAppliedTarget(term uint64, command BeginCoordinatorEpoch) []byte {
	target := make([]byte, 0, 8+beginTargetBytes)
	target = appendU64(target, term)
	return append(target, beginTarget(command)...)
}

func appendSubject(encoded []byte, subject SubjectKey) []byte {
	encoded = append(encoded, byte(subject.Kind))
	encoded = append(encoded, subject.JobID[:]...)
	encoded = append(encoded, subject.TaskID.JobID[:]...)
	encoded = appendU16(encoded, subject.TaskID.StageID)
	encoded = appendU16(encoded, subject.TaskID.Partition)
	return appendU16(encoded, subject.WorkerID)
}

func appendU16(encoded []byte, value uint16) []byte {
	var fixed [2]byte
	binary.BigEndian.PutUint16(fixed[:], value)
	return append(encoded, fixed[:]...)
}

func appendU64(encoded []byte, value uint64) []byte {
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], value)
	return append(encoded, fixed[:]...)
}

type commandDecoder struct {
	input  []byte
	offset int
}

func (decoder *commandDecoder) done() bool { return decoder.offset == len(decoder.input) }

func (decoder *commandDecoder) take(length int) ([]byte, error) {
	if length < 0 || length > len(decoder.input)-decoder.offset {
		return nil, fmt.Errorf("%w: truncated field", ErrMalformedCommand)
	}
	end := decoder.offset + length
	value := decoder.input[decoder.offset:end]
	decoder.offset = end
	return value, nil
}

func (decoder *commandDecoder) byte() (byte, error) {
	value, err := decoder.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (decoder *commandDecoder) u16() (uint16, error) {
	value, err := decoder.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func (decoder *commandDecoder) u64() (uint64, error) {
	value, err := decoder.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}

func (decoder *commandDecoder) array16() ([16]byte, error) {
	value, err := decoder.take(16)
	var result [16]byte
	copy(result[:], value)
	return result, err
}

func (decoder *commandDecoder) array32() ([32]byte, error) {
	value, err := decoder.take(32)
	var result [32]byte
	copy(result[:], value)
	return result, err
}

func (decoder *commandDecoder) subject() (SubjectKey, error) {
	kind, err := decoder.byte()
	if err != nil {
		return SubjectKey{}, err
	}
	job, err := decoder.array16()
	if err != nil {
		return SubjectKey{}, err
	}
	taskJob, err := decoder.array16()
	if err != nil {
		return SubjectKey{}, err
	}
	stage, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	partition, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	worker, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	return SubjectKey{Kind: SubjectKind(kind), JobID: model.JobID(job), TaskID: model.TaskID{JobID: model.JobID(taskJob), StageID: stage, Partition: partition}, WorkerID: worker}, nil
}
