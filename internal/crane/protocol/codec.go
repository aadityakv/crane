package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const maxTupleMessagePayloadBytes = wire.MaxCraneDatagramBytesV1 - wire.FixedHeaderSize - wire.MACSize

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
	decoder, err := newTupleDecoder(encoded, TupleDeliverySchemaVersion, wire.MessageCraneTupleDelivery)
	if err != nil {
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
	tuple, err := model.UnmarshalTuple(tupleBytes)
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
