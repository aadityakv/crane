package wire

import (
	"bytes"
	"testing"
)

type gobTestMessage struct {
	Name  string
	Count int
}

func TestGobRoundTripUsesCallerProvidedConcretePointer(t *testing.T) {
	want := gobTestMessage{Name: "probe", Count: 3}
	encoded, err := EncodeGob(want)
	if err != nil {
		t.Fatal(err)
	}

	var got gobTestMessage
	if err := DecodeGob(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded message = %#v, want %#v", got, want)
	}
}

func TestGobDecodeRejectsNonConcreteDestinations(t *testing.T) {
	encoded, err := EncodeGob(gobTestMessage{Name: "probe"})
	if err != nil {
		t.Fatal(err)
	}

	var nilPointer *gobTestMessage
	var interfaceDestination any
	tests := []struct {
		name        string
		destination any
	}{
		{name: "value", destination: gobTestMessage{}},
		{name: "nil_pointer", destination: nilPointer},
		{name: "interface_pointer", destination: &interfaceDestination},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DecodeGob(encoded, tt.destination); err == nil {
				t.Fatal("DecodeGob accepted a non-concrete destination")
			}
		})
	}
}

func TestGobDecodeRejectsBytesAfterOneValue(t *testing.T) {
	first, err := EncodeGob(gobTestMessage{Name: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeGob(gobTestMessage{Name: "second"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		encoded []byte
	}{
		{name: "second_value", encoded: bytes.Join([][]byte{first, second}, nil)},
		{name: "zero_byte", encoded: append(append([]byte(nil), first...), 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var decoded gobTestMessage
			if err := DecodeGob(tt.encoded, &decoded); err == nil {
				t.Fatal("DecodeGob accepted bytes after the first gob value")
			}
		})
	}
}
