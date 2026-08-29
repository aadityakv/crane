package wire

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"reflect"
)

// EncodeGob encodes one concrete value without registering an interface schema.
func EncodeGob(value any) ([]byte, error) {
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(value); err != nil {
		return nil, fmt.Errorf("encode gob value: %w", err)
	}
	return encoded.Bytes(), nil
}

// DecodeGob decodes exactly one gob value into a caller-provided concrete pointer.
func DecodeGob(encoded []byte, destination any) error {
	destinationValue := reflect.ValueOf(destination)
	if !destinationValue.IsValid() || destinationValue.Kind() != reflect.Pointer || destinationValue.IsNil() {
		return errors.New("gob destination must be a non-nil pointer")
	}
	if destinationValue.Elem().Kind() == reflect.Interface {
		return errors.New("gob destination must point to a concrete type")
	}

	decoder := gob.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode gob value: %w", err)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("gob payload contains more than one value")
		}
		return fmt.Errorf("gob payload contains trailing data: %w", err)
	}
	return nil
}
