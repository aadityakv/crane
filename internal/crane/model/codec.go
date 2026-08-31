package model

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var errTuplePayloadTooLarge = errors.New("tuple exceeds complete payload limit")

// MarshalTuple returns the sole canonical binary representation of a tuple.
func MarshalTuple(tuple Tuple) ([]byte, error) {
	if err := tuple.Validate(); err != nil {
		return nil, err
	}
	size, err := canonicalTupleSize(tuple)
	if err != nil {
		return nil, err
	}
	writer := newCheckedWriter(int(size))
	if err := writer.uint16(uint16(len(tuple.Fields))); err != nil {
		return nil, err
	}
	for _, field := range tuple.Fields {
		if err := writer.string(field.Name); err != nil {
			return nil, err
		}
		if err := writer.byte(byte(field.Value.Type)); err != nil {
			return nil, err
		}
		switch field.Value.Type {
		case ValueInt64:
			if err := writer.uint64(uint64(field.Value.Int64)); err != nil {
				return nil, err
			}
		case ValueString:
			if err := writer.string(field.Value.String); err != nil {
				return nil, err
			}
		case ValueBytes:
			if err := writer.bytes(field.Value.Bytes); err != nil {
				return nil, err
			}
		}
	}
	return writer.ownedBytes(), nil
}

// UnmarshalTuple decodes only complete canonical tuple bytes.
func UnmarshalTuple(encoded []byte) (Tuple, error) {
	if uint64(len(encoded)) > LimitsV1().MaxTuplePayloadBytes {
		return Tuple{}, errTuplePayloadTooLarge
	}
	reader := checkedReader{input: encoded}
	count, err := reader.uint16()
	if err != nil {
		return Tuple{}, err
	}
	if uint64(count) > LimitsV1().MaxTupleFields {
		return Tuple{}, errors.New("tuple field count exceeds limit")
	}
	if int(count) > reader.remaining()/3 {
		return Tuple{}, errors.New("tuple field count cannot fit remaining bytes")
	}
	tuple := Tuple{Fields: make([]Field, int(count))}
	for index := range tuple.Fields {
		name, err := reader.string(LimitsV1().MaxIdentifierBytes)
		if err != nil {
			return Tuple{}, fmt.Errorf("tuple field %d name: %w", index, err)
		}
		tag, err := reader.byte()
		if err != nil {
			return Tuple{}, fmt.Errorf("tuple field %d tag: %w", index, err)
		}
		value := Value{Type: ValueType(tag)}
		switch value.Type {
		case ValueInt64:
			encodedInt, err := reader.uint64()
			if err != nil {
				return Tuple{}, fmt.Errorf("tuple field %d int64: %w", index, err)
			}
			value.Int64 = int64(encodedInt)
		case ValueString:
			value.String, err = reader.string(LimitsV1().MaxTuplePayloadBytes)
			if err != nil {
				return Tuple{}, fmt.Errorf("tuple field %d string: %w", index, err)
			}
		case ValueBytes:
			value.Bytes, err = reader.bytes(LimitsV1().MaxTuplePayloadBytes)
			if err != nil {
				return Tuple{}, fmt.Errorf("tuple field %d bytes: %w", index, err)
			}
		default:
			return Tuple{}, errors.New("unknown tuple value tag")
		}
		tuple.Fields[index] = Field{Name: name, Value: value}
	}
	if !reader.done() {
		return Tuple{}, errors.New("trailing tuple bytes")
	}
	if err := tuple.Validate(); err != nil {
		return Tuple{}, err
	}
	return tuple, nil
}

type checkedWriter struct{ output []byte }

func newCheckedWriter(capacity int) checkedWriter {
	if capacity < 0 {
		capacity = 0
	}
	return checkedWriter{output: make([]byte, 0, capacity)}
}

func (writer *checkedWriter) byte(value byte) error {
	writer.output = append(writer.output, value)
	return nil
}

func (writer *checkedWriter) uint16(value uint16) error {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	writer.output = append(writer.output, encoded[:]...)
	return nil
}

func (writer *checkedWriter) uint64(value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	writer.output = append(writer.output, encoded[:]...)
	return nil
}

func (writer *checkedWriter) string(value string) error {
	if len(value) > int(^uint16(0)) {
		return errors.New("length exceeds uint16 codec bound")
	}
	if err := writer.uint16(uint16(len(value))); err != nil {
		return err
	}
	writer.output = append(writer.output, value...)
	return nil
}

func (writer *checkedWriter) bytes(value []byte) error {
	if len(value) > int(^uint16(0)) {
		return errors.New("length exceeds uint16 codec bound")
	}
	if err := writer.uint16(uint16(len(value))); err != nil {
		return err
	}
	writer.output = append(writer.output, value...)
	return nil
}

func (writer *checkedWriter) ownedBytes() []byte { return append([]byte(nil), writer.output...) }

type checkedReader struct {
	input  []byte
	offset int
}

func (reader *checkedReader) remaining() int { return len(reader.input) - reader.offset }

func (reader *checkedReader) done() bool { return reader.offset == len(reader.input) }

func (reader *checkedReader) byte() (byte, error) {
	if reader.remaining() < 1 {
		return 0, errors.New("truncated byte")
	}
	value := reader.input[reader.offset]
	reader.offset++
	return value, nil
}

func (reader *checkedReader) uint16() (uint16, error) {
	if reader.remaining() < 2 {
		return 0, errors.New("truncated uint16")
	}
	value := binary.BigEndian.Uint16(reader.input[reader.offset : reader.offset+2])
	reader.offset += 2
	return value, nil
}

func (reader *checkedReader) uint64() (uint64, error) {
	if reader.remaining() < 8 {
		return 0, errors.New("truncated uint64")
	}
	value := binary.BigEndian.Uint64(reader.input[reader.offset : reader.offset+8])
	reader.offset += 8
	return value, nil
}

func (reader *checkedReader) string(limit uint64) (string, error) {
	value, err := reader.bytes(limit)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *checkedReader) bytes(limit uint64) ([]byte, error) {
	length, err := reader.uint16()
	if err != nil {
		return nil, err
	}
	if uint64(length) > limit {
		return nil, errors.New("declared length exceeds codec limit")
	}
	if int(length) > reader.remaining() {
		return nil, errors.New("truncated length-prefixed bytes")
	}
	end := reader.offset + int(length)
	value := append([]byte(nil), reader.input[reader.offset:end]...)
	reader.offset = end
	return value, nil
}
