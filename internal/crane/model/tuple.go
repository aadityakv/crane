package model

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// ValueType is the stable tag for one tuple scalar value.
type ValueType uint8

const (
	// ValueInt64 tags a signed 64-bit integer value.
	ValueInt64 ValueType = iota + 1
	// ValueString tags a UTF-8 string value.
	ValueString
	// ValueBytes tags an opaque byte-slice value.
	ValueBytes
)

// Value is one tagged tuple scalar. Only the field selected by Type may carry data.
type Value struct {
	Type   ValueType
	Int64  int64
	String string
	Bytes  []byte
}

// Field is one named tuple value.
type Field struct {
	Name  string
	Value Value
}

// Tuple is a canonical, sorted list of uniquely named fields.
type Tuple struct {
	Fields []Field
}

// Validate checks canonical field ordering, bounds, and scalar representation.
func (tuple Tuple) Validate() error {
	maxFields := LimitsV1().MaxTupleFields
	if uint64(len(tuple.Fields)) > maxFields {
		return fmt.Errorf("tuple has %d fields, maximum is %d", len(tuple.Fields), maxFields)
	}
	previous := ""
	for index, field := range tuple.Fields {
		if err := validateFieldName(field.Name); err != nil {
			return fmt.Errorf("tuple field %d: %w", index, err)
		}
		if index > 0 && previous >= field.Name {
			return errors.New("tuple fields are not sorted and unique")
		}
		if err := field.Value.Validate(); err != nil {
			return fmt.Errorf("tuple field %q: %w", field.Name, err)
		}
		previous = field.Name
	}
	_, err := canonicalTupleSize(tuple)
	return err
}

// Validate checks that a value has a known canonical tag and bounded payload.
func (value Value) Validate() error {
	limit := LimitsV1().MaxTuplePayloadBytes
	switch value.Type {
	case ValueInt64:
		if value.String != "" || len(value.Bytes) != 0 {
			return errors.New("int64 value has inactive payload")
		}
	case ValueString:
		if value.Int64 != 0 || len(value.Bytes) != 0 {
			return errors.New("string value has inactive payload")
		}
		if !utf8.ValidString(value.String) {
			return errors.New("string value is not valid UTF-8")
		}
		if uint64(len(value.String)) > limit {
			return errors.New("string value exceeds payload limit")
		}
	case ValueBytes:
		if value.Int64 != 0 || value.String != "" {
			return errors.New("bytes value has inactive payload")
		}
		if uint64(len(value.Bytes)) > limit {
			return errors.New("bytes value exceeds payload limit")
		}
	default:
		return errors.New("unknown tuple value tag")
	}
	return nil
}

func canonicalTupleSize(tuple Tuple) (uint64, error) {
	size := v1Uint16Bytes
	for _, field := range tuple.Fields {
		fieldSize, ok := checkedAddUint64(v1Uint16Bytes, uint64(len(field.Name)))
		if !ok {
			return 0, errors.New("tuple field name size overflow")
		}
		fieldSize, ok = checkedAddUint64(fieldSize, 1)
		if !ok {
			return 0, errors.New("tuple value tag size overflow")
		}
		valueSize := uint64(0)
		switch field.Value.Type {
		case ValueInt64:
			valueSize = v1Uint64Bytes
		case ValueString:
			valueSize, ok = checkedAddUint64(v1Uint16Bytes, uint64(len(field.Value.String)))
			if !ok {
				return 0, errors.New("tuple string value size overflow")
			}
		case ValueBytes:
			valueSize, ok = checkedAddUint64(v1Uint16Bytes, uint64(len(field.Value.Bytes)))
			if !ok {
				return 0, errors.New("tuple bytes value size overflow")
			}
		}
		fieldSize, ok = checkedAddUint64(fieldSize, valueSize)
		if !ok {
			return 0, errors.New("tuple field size overflow")
		}
		size, ok = checkedAddUint64(size, fieldSize)
		if !ok {
			return 0, errors.New("tuple size overflow")
		}
	}
	if size > LimitsV1().MaxTuplePayloadBytes {
		return 0, errors.New("tuple exceeds complete payload limit")
	}
	return size, nil
}

func validateFieldName(name string) error {
	if name == "" {
		return errors.New("empty field name")
	}
	if !utf8.ValidString(name) {
		return errors.New("field name is not valid UTF-8")
	}
	if uint64(len(name)) > LimitsV1().MaxIdentifierBytes {
		return errors.New("field name exceeds identifier limit")
	}
	return nil
}
