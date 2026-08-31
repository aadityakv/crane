package model

import (
	"bytes"
	"encoding/hex"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestCodecTupleGoldenBigEndianTaggedValues(t *testing.T) {
	tuple := Tuple{Fields: []Field{
		{Name: "age", Value: Value{Type: ValueInt64, Int64: -2}},
		{Name: "msg", Value: Value{Type: ValueString, String: "hi"}},
		{Name: "raw", Value: Value{Type: ValueBytes, Bytes: []byte{0, 0xff}}},
	}}
	got, err := MarshalTuple(tuple)
	if err != nil {
		t.Fatalf("MarshalTuple: %v", err)
	}
	const want = "0003000361676501fffffffffffffffe00036d73670200026869000372617703000200ff"
	if hex.EncodeToString(got) != want {
		t.Fatalf("MarshalTuple() = %x, want %s", got, want)
	}
	decoded, err := UnmarshalTuple(got)
	if err != nil {
		t.Fatalf("UnmarshalTuple: %v", err)
	}
	if !reflect.DeepEqual(decoded, tuple) {
		t.Fatalf("decoded = %#v, want %#v", decoded, tuple)
	}
}

func TestCodecTupleRejectsNonCanonicalAndMalformedInputs(t *testing.T) {
	valid, err := hex.DecodeString("000100016103000100")
	if err != nil {
		t.Fatal(err)
	}
	for name, encoded := range map[string][]byte{
		"trailing bytes":          append(append([]byte(nil), valid...), 0),
		"truncated field name":    {0, 1, 0, 2, 'a'},
		"truncated value length":  {0, 1, 0, 1, 'a', byte(ValueBytes), 0, 1},
		"unknown tag":             {0, 1, 0, 1, 'a', 99},
		"unsorted":                {0, 2, 0, 1, 'b', byte(ValueBytes), 0, 0, 0, 1, 'a', byte(ValueBytes), 0, 0},
		"duplicate":               {0, 2, 0, 1, 'a', byte(ValueBytes), 0, 0, 0, 1, 'a', byte(ValueBytes), 0, 0},
		"count before allocation": {0xff, 0xff},
		"payload over bound":      append([]byte{0, 1, 0, 1, 'a', byte(ValueBytes), 2, 1}, bytes.Repeat([]byte{1}, 513)...),
	} {
		if _, err := UnmarshalTuple(encoded); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestCodecTupleReturnsOwnedDecodedBytes(t *testing.T) {
	encoded, err := hex.DecodeString("000100037261770300020102")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalTuple(encoded)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] = 0xff
	if got := decoded.Fields[0].Value.Bytes; !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("decoded value shares encoded input: %x", got)
	}
	decoded.Fields[0].Value.Bytes[0] = 0xee
	again, err := UnmarshalTuple([]byte{0, 1, 0, 3, 'r', 'a', 'w', byte(ValueBytes), 0, 2, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := again.Fields[0].Value.Bytes; !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("later decode observed prior caller mutation: %x", got)
	}
}

func TestCodecTupleRandomInputsNeverPanic(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	for index := 0; index < 10_000; index++ {
		input := make([]byte, random.IntN(2_048))
		for byteIndex := range input {
			input[byteIndex] = byte(random.Uint64())
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("UnmarshalTuple panicked for input %x: %v", input, recovered)
				}
			}()
			_, _ = UnmarshalTuple(input)
		}()
	}
}

func FuzzUnmarshalTuple(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add([]byte{0xff, 0xff})
	f.Add([]byte{0, 1, 0, 1, 'a', byte(ValueInt64), 0, 0, 0, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = UnmarshalTuple(input)
	})
}
