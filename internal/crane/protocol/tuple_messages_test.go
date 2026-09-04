package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/wire"
)

func TestTupleTrafficGoldenVectorsAndMessageAssociation(t *testing.T) {
	delivery := testTupleDelivery()
	wantDeliveryHex := "00010118" +
		"01010101010101010101010101010101" +
		"0101010101010101010101010101010100010002" +
		"0000000000000003" +
		"0404040404040404040404040404040404040404040404040404040404040404" +
		"0005" +
		"0101010101010101010101010101010100060007" +
		"000c000100037261770300020809" +
		"0101010101010101010101010101010100080009000a" +
		"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b" +
		"000000000000000c" +
		"0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d" +
		"000000000000000e" +
		"0101010101010101010101010101010100060007000f" +
		"10101010101010101010101010101010" +
		"0000000000000011" +
		"0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d" +
		"000000000000000e" +
		"01010101010101010101010101010101" +
		"000000000000000e" +
		"1212121212121212121212121212121212121212121212121212121212121212" +
		"00000000000000130000000000000014001516161616161616161616161616161616"

	// The ACK and NACK fixtures and goldens are spelled out independently of
	// the delivery so a field-order or field-selection drift between the three
	// layouts cannot hide behind shared slices.
	ack, nack := testTupleACK(), testTupleNACK()
	wantACKHex := "0001011901010101010101010101010101010101010101010101010101010101" +
		"0101010100010002000000000000000304040404040404040404040404040404" +
		"0404040404040404040404040404040400050101010101010101010101010101" +
		"0101000600070101010101010101010101010101010100060007000f10101010" +
		"10101010101010101010101000000000000000110d0d0d0d0d0d0d0d0d0d0d0d" +
		"0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d000000000000000e01010101" +
		"010101010101010101010101000000000000000e121212121212121212121212" +
		"1212121212121212121212121212121212121212000000000000001300000000" +
		"0000001400151616161616161616161616161616161601"
	wantNACKHex := "0001011a01010101010101010101010101010101010101010101010101010101" +
		"0101010100010002000000000000000304040404040404040404040404040404" +
		"0404040404040404040404040404040400050101010101010101010101010101" +
		"0101000600070101010101010101010101010101010100060007000f10101010" +
		"10101010101010101010101000000000000000110d0d0d0d0d0d0d0d0d0d0d0d" +
		"0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d000000000000000e01010101" +
		"010101010101010101010101000000000000000e121212121212121212121212" +
		"1212121212121212121212121212121212121212000000000000001300000000" +
		"000000140015161616161616161616161616161616160007"

	tests := []struct {
		name     string
		message  wire.MessageType
		marshal  func() ([]byte, error)
		decode   func([]byte) (any, error)
		want     any
		wantHex  string
		wrongOne func([]byte) error
	}{
		{
			name: "delivery", message: wire.MessageCraneTupleDelivery,
			marshal: func() ([]byte, error) { return MarshalTupleDelivery(delivery) },
			decode:  func(input []byte) (any, error) { return UnmarshalTupleDelivery(input) },
			want:    delivery, wantHex: wantDeliveryHex,
			wrongOne: func(input []byte) error { _, err := UnmarshalTupleACK(input); return err },
		},
		{
			name: "ack", message: wire.MessageCraneTupleDeliveryAck,
			marshal:  func() ([]byte, error) { return MarshalTupleACK(ack) },
			decode:   func(input []byte) (any, error) { return UnmarshalTupleACK(input) },
			want:     ack,
			wantHex:  wantACKHex,
			wrongOne: func(input []byte) error { _, err := UnmarshalTupleNACK(input); return err },
		},
		{
			name: "nack", message: wire.MessageCraneTupleDeliveryNack,
			marshal:  func() ([]byte, error) { return MarshalTupleNACK(nack) },
			decode:   func(input []byte) (any, error) { return UnmarshalTupleNACK(input) },
			want:     nack,
			wantHex:  wantNACKHex,
			wrongOne: func(input []byte) error { _, err := UnmarshalTupleDelivery(input); return err },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.want.(interface{ MessageType() wire.MessageType }).MessageType(); got != test.message {
				t.Fatalf("MessageType() = %d, want %d", got, test.message)
			}
			encoded, err := test.marshal()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			want, err := hex.DecodeString(test.wantHex)
			if err != nil {
				t.Fatalf("invalid hand-derived golden: %v", err)
			}
			if !bytes.Equal(encoded, want) {
				t.Fatalf("encoded = %x\nwant    = %x", encoded, want)
			}
			decoded, err := test.decode(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(decoded, test.want) {
				t.Fatalf("decoded = %#v, want %#v", decoded, test.want)
			}
			if err := test.wrongOne(encoded); !errors.Is(err, ErrUnexpectedMessage) {
				t.Fatalf("wrong message decoder error = %v, want ErrUnexpectedMessage", err)
			}
		})
	}
}

func TestTupleDeliveryExactMaximumFitsCompleteDatagram(t *testing.T) {
	delivery := maximumTupleDelivery()
	encodedTuple, err := model.MarshalTuple(delivery.Tuple)
	if err != nil {
		t.Fatalf("MarshalTuple exact maximum: %v", err)
	}
	if got, want := len(encodedTuple), int(model.LimitsV1().MaxTuplePayloadBytes); got != want {
		t.Fatalf("canonical tuple bytes = %d, want %d", got, want)
	}
	payload, err := MarshalTupleDelivery(delivery)
	if err != nil {
		t.Fatalf("MarshalTupleDelivery maximum: %v", err)
	}
	const wantPayloadBytes = 878
	if len(payload) != wantPayloadBytes {
		t.Fatalf("maximum delivery payload = %d, want %d", len(payload), wantPayloadBytes)
	}
	if got := len(payload) + wire.FixedHeaderSize + wire.MACSize; got != 965 || got > wire.MaxCraneDatagramBytesV1 {
		t.Fatalf("maximum complete delivery frame = %d, want 965 and <= %d", got, wire.MaxCraneDatagramBytesV1)
	}

	delivery.Tuple.Fields[0].Value.Bytes = make([]byte, 505)
	if _, err := MarshalTupleDelivery(delivery); !errors.Is(err, ErrInvalidTupleMessage) {
		t.Fatalf("513-byte canonical tuple error = %v, want ErrInvalidTupleMessage", err)
	}
}

func TestTupleTrafficExactSchemaSizeRules(t *testing.T) {
	if TupleDeliveryFixedPayloadBytes != 4+98+2+86+86+56+34 {
		t.Fatalf("delivery fixed bytes = %d, want schema+type+DeliveryID+tuple-length+producer+destination+set+epoch = 366", TupleDeliveryFixedPayloadBytes)
	}
	if TupleDeliveryMinPayloadBytes != TupleDeliveryFixedPayloadBytes+2 {
		t.Fatalf("delivery minimum bytes = %d, want 368", TupleDeliveryMinPayloadBytes)
	}
	if TupleDeliveryMaxPayloadBytes() != TupleDeliveryFixedPayloadBytes+int(model.LimitsV1().MaxTuplePayloadBytes) {
		t.Fatalf("delivery maximum bytes = %d, want 878", TupleDeliveryMaxPayloadBytes())
	}
	if TupleACKPayloadBytes != 4+98+86+56+34+1 {
		t.Fatalf("ACK bytes = %d, want 279", TupleACKPayloadBytes)
	}
	if TupleNACKPayloadBytes != 4+98+86+56+34+2 {
		t.Fatalf("NACK bytes = %d, want 280", TupleNACKPayloadBytes)
	}

	minimum := testTupleDelivery()
	minimum.Tuple = model.Tuple{}
	minimumBytes := mustMarshalDelivery(t, minimum)
	if len(minimumBytes) != TupleDeliveryMinPayloadBytes {
		t.Fatalf("minimum delivery bytes = %d, want %d", len(minimumBytes), TupleDeliveryMinPayloadBytes)
	}
	maximumBytes := mustMarshalDelivery(t, maximumTupleDelivery())
	if len(maximumBytes) != TupleDeliveryMaxPayloadBytes() {
		t.Fatalf("maximum delivery bytes = %d, want %d", len(maximumBytes), TupleDeliveryMaxPayloadBytes())
	}
	if got := len(mustMarshalACK(t, testTupleACK())); got != TupleACKPayloadBytes {
		t.Fatalf("ACK bytes = %d, want %d", got, TupleACKPayloadBytes)
	}
	if got := len(mustMarshalNACK(t, testTupleNACK())); got != TupleNACKPayloadBytes {
		t.Fatalf("NACK bytes = %d, want %d", got, TupleNACKPayloadBytes)
	}
}

func maximumTupleDelivery() TupleDelivery {
	job := model.JobID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	source := model.TaskID{JobID: job, StageID: math.MaxUint16, Partition: math.MaxUint16}
	destination := model.TaskID{JobID: job, StageID: math.MaxUint16, Partition: math.MaxUint16}
	epoch := model.WorkerEpoch{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	digest := [32]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	token := model.AssignmentToken{Task: destination, WorkerID: math.MaxUint16, WorkerEpoch: epoch, Attempt: math.MaxUint64, SpecificationHash: digest, AssignmentRevision: math.MaxUint64}
	return TupleDelivery{
		DeliveryID: model.DeliveryID{
			Tuple:  model.TupleID{JobID: job, SourceTask: source, SourceSequence: math.MaxUint64, PathDigest: digest},
			EdgeID: math.MaxUint16, DestinationTask: destination,
		},
		Tuple:       model.Tuple{Fields: []model.Field{{Name: "a", Value: model.Value{Type: model.ValueBytes, Bytes: make([]byte, 504)}}}},
		Producer:    token,
		Destination: token,
		Assignment:  AssignmentSetIdentity{JobID: job, Revision: math.MaxUint64, Digest: digest},
		Coordinator: model.CoordinatorEpoch{Term: math.MaxUint64, BeginIndex: math.MaxUint64, Coordinator: math.MaxUint16, Nonce: [16]byte(epoch)},
	}
}

func TestTupleTrafficRejectsEveryTruncationAndTrailingInput(t *testing.T) {
	inputs := []struct {
		name    string
		encoded []byte
		decode  func([]byte) error
	}{
		{name: "delivery", encoded: mustMarshalDelivery(t, testTupleDelivery()), decode: func(input []byte) error { _, err := UnmarshalTupleDelivery(input); return err }},
		{name: "ack", encoded: mustMarshalACK(t, testTupleACK()), decode: func(input []byte) error { _, err := UnmarshalTupleACK(input); return err }},
		{name: "nack", encoded: mustMarshalNACK(t, testTupleNACK()), decode: func(input []byte) error { _, err := UnmarshalTupleNACK(input); return err }},
	}
	for _, input := range inputs {
		t.Run(input.name, func(t *testing.T) {
			for end := 0; end < len(input.encoded); end++ {
				if err := input.decode(input.encoded[:end]); err == nil {
					t.Fatalf("accepted truncation at %d/%d", end, len(input.encoded))
				}
			}
			trailing := append(append([]byte(nil), input.encoded...), 0)
			if err := input.decode(trailing); !errors.Is(err, ErrMalformedTupleMessage) {
				t.Fatalf("trailing input error = %v, want ErrMalformedTupleMessage", err)
			}
		})
	}
}

func TestTupleDeliveryRejectsImpossibleDeclaredTupleLengthBeforeAllocation(t *testing.T) {
	valid := mustMarshalDelivery(t, testTupleDelivery())
	// Prefix (4) + DeliveryID (98) points at the tuple uint16 length.
	for _, declared := range []uint16{513, math.MaxUint16} {
		malformed := append([]byte(nil), valid...)
		malformed[102], malformed[103] = byte(declared>>8), byte(declared)
		if _, err := UnmarshalTupleDelivery(malformed); !errors.Is(err, ErrMalformedTupleMessage) {
			t.Fatalf("declared tuple length %d error = %v, want ErrMalformedTupleMessage", declared, err)
		}
	}
}

func TestTupleDeliveryPreflightRejectsShapeBeforeTupleDecoder(t *testing.T) {
	valid := mustMarshalDelivery(t, testTupleDelivery())
	validCalls := 0
	if _, err := unmarshalTupleDeliveryWith(valid, func(input []byte) (model.Tuple, error) {
		validCalls++
		return model.UnmarshalTuple(input)
	}); err != nil {
		t.Fatalf("valid injected decode: %v", err)
	}
	if validCalls != 1 {
		t.Fatalf("valid tuple decoder calls = %d, want 1", validCalls)
	}
	tests := []struct {
		name  string
		input []byte
	}{
		{name: "fixed suffix missing one byte", input: valid[:len(valid)-1]},
		{name: "valid frame plus one byte", input: append(append([]byte(nil), valid...), 0)},
		{name: "maximum tuple prefix without fixed suffix", input: append(append([]byte(nil), valid[:102]...), 2, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			decode := func([]byte) (model.Tuple, error) {
				calls++
				panic("tuple decoder must not run before exact frame-shape preflight")
			}
			if _, err := unmarshalTupleDeliveryWith(test.input, decode); !errors.Is(err, ErrMalformedTupleMessage) {
				t.Fatalf("preflight error = %v, want ErrMalformedTupleMessage", err)
			}
			if calls != 0 {
				t.Fatalf("tuple decoder calls = %d, want 0", calls)
			}
		})
	}
}

func TestTupleTrafficExactSizesRejectPlusOneAndTruncatedSuffix(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
		decode  func([]byte) error
	}{
		{name: "delivery", encoded: mustMarshalDelivery(t, testTupleDelivery()), decode: decodeDeliveryError},
		{name: "ack", encoded: mustMarshalACK(t, testTupleACK()), decode: decodeACKError},
		{name: "nack", encoded: mustMarshalNACK(t, testTupleNACK()), decode: decodeNACKError},
	}
	for _, test := range tests {
		t.Run(test.name+"/plus-one", func(t *testing.T) {
			input := append(append([]byte(nil), test.encoded...), 0)
			if err := test.decode(input); !errors.Is(err, ErrMalformedTupleMessage) {
				t.Fatalf("plus-one error = %v, want ErrMalformedTupleMessage", err)
			}
		})
		t.Run(test.name+"/truncated-suffix", func(t *testing.T) {
			if err := test.decode(test.encoded[:len(test.encoded)-1]); !errors.Is(err, ErrMalformedTupleMessage) {
				t.Fatalf("truncated suffix error = %v, want ErrMalformedTupleMessage", err)
			}
		})
	}
	maximum := mustMarshalDelivery(t, maximumTupleDelivery())
	if _, err := UnmarshalTupleDelivery(append(maximum, 0)); !errors.Is(err, ErrTupleMessageTooLarge) {
		t.Fatalf("maximum delivery plus one error = %v, want ErrTupleMessageTooLarge", err)
	}
	minimum := mustMarshalDelivery(t, func() TupleDelivery {
		value := testTupleDelivery()
		value.Tuple = model.Tuple{}
		return value
	}())
	if _, err := UnmarshalTupleDelivery(minimum[:len(minimum)-1]); !errors.Is(err, ErrMalformedTupleMessage) {
		t.Fatalf("minimum delivery minus one error = %v, want ErrMalformedTupleMessage", err)
	}
}

func TestTupleMalformedDeclarationAllocatesFixedBytesNotDeclaredBytes(t *testing.T) {
	const maximumAllocatedBytesPerInput = int64(2_048)
	inputs := make([]struct {
		name  string
		input []byte
	}, 0, 24)
	for _, declared := range []uint16{0, 1, 511, 512, 513, math.MaxUint16} {
		inputs = append(inputs,
			struct {
				name  string
				input []byte
			}{name: fmt.Sprintf("declared-%d-suffix-minus-one", declared), input: malformedDeliveryShape(declared, -1, uint64(declared)+1)},
			struct {
				name  string
				input []byte
			}{name: fmt.Sprintf("declared-%d-suffix-plus-one", declared), input: malformedDeliveryShape(declared, 1, uint64(declared)+2)},
		)
	}
	random := rand.New(rand.NewPCG(0x425, 0x65535))
	for index := 0; index < 8; index++ {
		declared := uint16(random.Uint32())
		delta := -1
		if random.Uint32()%2 == 0 {
			delta = 1
		}
		inputs = append(inputs, struct {
			name  string
			input []byte
		}{name: fmt.Sprintf("random-%d-declared-%d-delta-%d", index, declared, delta), input: malformedDeliveryShape(declared, delta, random.Uint64())})
	}

	worstBytes := int64(0)
	worstName := ""
	for _, test := range inputs {
		t.Run(test.name, func(t *testing.T) {
			if test.input[0] != 0 || test.input[1] != byte(TupleDeliverySchemaVersion) || test.input[2] != 1 || test.input[3] != 24 {
				t.Fatalf("input does not preserve valid delivery schema/type prefix: %x", test.input[:4])
			}
			decoderCalls := 0
			if _, err := unmarshalTupleDeliveryWith(test.input, func([]byte) (model.Tuple, error) {
				decoderCalls++
				panic("malformed shape reached tuple decoder")
			}); err == nil {
				t.Fatal("malformed delivery shape was accepted")
			}
			if decoderCalls != 0 {
				t.Fatalf("tuple decoder calls = %d, want 0", decoderCalls)
			}
			allocated := benchmarkTupleDecodeBytes(test.input)
			if allocated > worstBytes {
				worstBytes, worstName = allocated, test.name
			}
			if allocated > maximumAllocatedBytesPerInput {
				t.Fatalf("allocated %d bytes/op for one malformed input, want <= %d", allocated, maximumAllocatedBytesPerInput)
			}
		})
	}
	t.Logf("worst individual malformed input allocation: %d bytes/op (%s)", worstBytes, worstName)
}

func malformedDeliveryShape(declared uint16, suffixDelta int, seed uint64) []byte {
	expected := TupleDeliveryFixedPayloadBytes + int(declared)
	length := expected + suffixDelta
	if length < TupleDeliveryMinPayloadBytes || length > TupleDeliveryMaxPayloadBytes() {
		length = TupleDeliveryMinPayloadBytes
	}
	input := make([]byte, length)
	input[1] = byte(TupleDeliverySchemaVersion)
	input[2], input[3] = 1, 24
	input[102], input[103] = byte(declared>>8), byte(declared)
	random := rand.New(rand.NewPCG(seed, seed^0x280282))
	for offset := 104; offset < len(input); offset++ {
		input[offset] = byte(random.Uint32())
	}
	return input
}

func TestTupleTrafficDecodersRejectPayloadLimitPlusOneBeforeFields(t *testing.T) {
	oversized := make([]byte, maxTupleMessagePayloadBytes+1)
	oversized[1] = 1
	for _, test := range []struct {
		name   string
		decode func([]byte) error
	}{
		{name: "delivery", decode: decodeDeliveryError},
		{name: "ack", decode: decodeACKError},
		{name: "nack", decode: decodeNACKError},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.decode(oversized); !errors.Is(err, ErrTupleMessageTooLarge) {
				t.Fatalf("limit+1 error = %v, want ErrTupleMessageTooLarge", err)
			}
		})
	}
}

func TestTupleDeliveryValidationRejectsCrossReferencesAndZeroValues(t *testing.T) {
	base := testTupleDelivery()
	foreignJob := model.JobID{99}
	tests := []struct {
		name   string
		mutate func(*TupleDelivery)
	}{
		{name: "zero delivery", mutate: func(value *TupleDelivery) { value.DeliveryID = model.DeliveryID{} }},
		{name: "foreign tuple source", mutate: func(value *TupleDelivery) { value.DeliveryID.Tuple.SourceTask.JobID = foreignJob }},
		{name: "zero edge", mutate: func(value *TupleDelivery) { value.DeliveryID.EdgeID = 0 }},
		{name: "foreign delivery destination", mutate: func(value *TupleDelivery) { value.DeliveryID.DestinationTask.JobID = foreignJob }},
		{name: "foreign producer", mutate: func(value *TupleDelivery) { value.Producer.Task.JobID = foreignJob }},
		{name: "wrong destination route", mutate: func(value *TupleDelivery) { value.Destination.Task.Partition++ }},
		{name: "zero producer attempt", mutate: func(value *TupleDelivery) { value.Producer.Attempt = 0 }},
		{name: "zero destination epoch", mutate: func(value *TupleDelivery) { value.Destination.WorkerEpoch = model.WorkerEpoch{} }},
		{name: "producer revision differs from current set", mutate: func(value *TupleDelivery) { value.Producer.AssignmentRevision++ }},
		{name: "destination revision differs from current set", mutate: func(value *TupleDelivery) { value.Destination.AssignmentRevision++ }},
		{name: "token specification mismatch", mutate: func(value *TupleDelivery) { value.Destination.SpecificationHash[0]++ }},
		{name: "foreign assignment set", mutate: func(value *TupleDelivery) { value.Assignment.JobID = foreignJob }},
		{name: "zero assignment revision", mutate: func(value *TupleDelivery) { value.Assignment.Revision = 0 }},
		{name: "zero assignment digest", mutate: func(value *TupleDelivery) { value.Assignment.Digest = [32]byte{} }},
		{name: "zero coordinator epoch", mutate: func(value *TupleDelivery) { value.Coordinator = model.CoordinatorEpoch{} }},
		{name: "noncanonical tuple", mutate: func(value *TupleDelivery) {
			value.Tuple.Fields = []model.Field{{Name: "z", Value: model.Value{Type: model.ValueInt64}}, {Name: "a", Value: model.Value{Type: model.ValueInt64}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			if _, err := MarshalTupleDelivery(value); !errors.Is(err, ErrInvalidTupleMessage) {
				t.Fatalf("MarshalTupleDelivery error = %v, want ErrInvalidTupleMessage", err)
			}
		})
	}
}

func TestTupleACKAndNACKValidateExactDestinationFencesAndEnums(t *testing.T) {
	ack := testTupleACK()
	nack := testTupleNACK()
	ackTests := []struct {
		name   string
		mutate func(*TupleACK)
	}{
		{name: "unknown status", mutate: func(value *TupleACK) { value.Status = TupleACKStatus(0) }},
		{name: "wrong destination", mutate: func(value *TupleACK) { value.Destination.Task.Partition++ }},
		{name: "stale revision", mutate: func(value *TupleACK) { value.Destination.AssignmentRevision-- }},
		{name: "foreign set", mutate: func(value *TupleACK) { value.Assignment.JobID[0]++ }},
		{name: "zero coordinator", mutate: func(value *TupleACK) { value.Coordinator = model.CoordinatorEpoch{} }},
	}
	for _, test := range ackTests {
		t.Run("ack/"+test.name, func(t *testing.T) {
			value := ack
			test.mutate(&value)
			if _, err := MarshalTupleACK(value); !errors.Is(err, ErrInvalidTupleMessage) {
				t.Fatalf("MarshalTupleACK error = %v, want ErrInvalidTupleMessage", err)
			}
		})
	}
	for _, status := range []TupleACKStatus{TupleAccepted, TupleCompleted} {
		value := ack
		value.Status = status
		if _, err := MarshalTupleACK(value); err != nil {
			t.Fatalf("valid ACK status %d: %v", status, err)
		}
	}

	nackTests := []struct {
		name   string
		mutate func(*TupleNACK)
	}{
		{name: "unknown code zero", mutate: func(value *TupleNACK) { value.Code = 0 }},
		{name: "unknown code high", mutate: func(value *TupleNACK) { value.Code = TupleNACKCode(8) }},
		{name: "wrong destination", mutate: func(value *TupleNACK) { value.Destination.Task.StageID++ }},
		{name: "zero worker epoch", mutate: func(value *TupleNACK) { value.Destination.WorkerEpoch = model.WorkerEpoch{} }},
		{name: "stale revision", mutate: func(value *TupleNACK) { value.Assignment.Revision++ }},
		{name: "zero digest", mutate: func(value *TupleNACK) { value.Assignment.Digest = [32]byte{} }},
	}
	for _, test := range nackTests {
		t.Run("nack/"+test.name, func(t *testing.T) {
			value := nack
			test.mutate(&value)
			if _, err := MarshalTupleNACK(value); !errors.Is(err, ErrInvalidTupleMessage) {
				t.Fatalf("MarshalTupleNACK error = %v, want ErrInvalidTupleMessage", err)
			}
		})
	}
	for code := TupleNACKNotReady; code <= TupleNACKOverloaded; code++ {
		value := nack
		value.Code = code
		if _, err := MarshalTupleNACK(value); err != nil {
			t.Fatalf("valid NACK code %d: %v", code, err)
		}
	}
}

func TestTupleTrafficDecodersRejectMutatedCanonicalFields(t *testing.T) {
	delivery := mustMarshalDelivery(t, testTupleDelivery())
	ack := mustMarshalACK(t, testTupleACK())
	nack := mustMarshalNACK(t, testTupleNACK())
	tests := []struct {
		name   string
		input  []byte
		mutate func([]byte)
		decode func([]byte) error
	}{
		{name: "delivery schema", input: delivery, mutate: func(b []byte) { b[1]++ }, decode: decodeDeliveryError},
		{name: "delivery type", input: delivery, mutate: func(b []byte) { b[3]++ }, decode: decodeDeliveryError},
		{name: "delivery destination token route", input: delivery, mutate: func(b []byte) { b[221]++ }, decode: decodeDeliveryError},
		{name: "delivery tuple noncanonical tag", input: delivery, mutate: func(b []byte) { b[111] = 0xff }, decode: decodeDeliveryError},
		{name: "ack schema", input: ack, mutate: func(b []byte) { b[1]++ }, decode: decodeACKError},
		{name: "ack type", input: ack, mutate: func(b []byte) { b[3]++ }, decode: decodeACKError},
		{name: "ack status", input: ack, mutate: func(b []byte) { b[len(b)-1] = 3 }, decode: decodeACKError},
		{name: "nack schema", input: nack, mutate: func(b []byte) { b[1]++ }, decode: decodeNACKError},
		{name: "nack type", input: nack, mutate: func(b []byte) { b[3]-- }, decode: decodeNACKError},
		{name: "nack code", input: nack, mutate: func(b []byte) { b[len(b)-1] = 8 }, decode: decodeNACKError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := append([]byte(nil), test.input...)
			test.mutate(input)
			if err := test.decode(input); err == nil {
				t.Fatal("mutated canonical input was accepted")
			}
		})
	}
}

func TestTupleDeliveryDecodeReturnsOwnedTupleBytes(t *testing.T) {
	encoded := mustMarshalDelivery(t, testTupleDelivery())
	pristine := append([]byte(nil), encoded...)
	decoded, err := UnmarshalTupleDelivery(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleDelivery: %v", err)
	}
	encoded[114] ^= 0xff
	if got := decoded.Tuple.Fields[0].Value.Bytes; !bytes.Equal(got, []byte{8, 9}) {
		t.Fatalf("decoded tuple aliased input: %x", got)
	}
	decoded.Tuple.Fields[0].Value.Bytes[0] ^= 0xff
	again, err := UnmarshalTupleDelivery(pristine)
	if err != nil {
		t.Fatalf("second UnmarshalTupleDelivery: %v", err)
	}
	if got := again.Tuple.Fields[0].Value.Bytes; !bytes.Equal(got, []byte{8, 9}) {
		t.Fatalf("decoded tuple mutation changed source: %x", got)
	}
}

func TestTupleTrafficRandomMalformedInputsDoNotPanic(t *testing.T) {
	random := rand.New(rand.NewPCG(0x425, 0x280282))
	for index := 0; index < 5_000; index++ {
		input := make([]byte, random.IntN(4_096))
		for offset := range input {
			input[offset] = byte(random.Uint32())
		}
		assertNoTupleCodecPanic(t, input)
	}
}

func FuzzUnmarshalTupleDelivery(f *testing.F) {
	valid := mustMarshalDelivery(f, testTupleDelivery())
	// Byte 102 starts the embedded tuple: length, field count, name length,
	// name "raw", value type at byte 112, value length at bytes 113-114.
	for _, seed := range tupleFuzzSeeds(valid, wire.MessageCraneTupleDeliveryAck, map[int]byte{112: 0xff}) {
		f.Add(seed)
	}
	f.Add(mutatedBytes(valid, map[int]byte{102: 0xff, 103: 0xff})) // impossible tuple length
	f.Add(mutatedBytes(valid, map[int]byte{104: 0xff, 105: 0xff})) // impossible field count
	f.Add(mutatedBytes(valid, map[int]byte{113: 0xff, 114: 0xff})) // impossible value length
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = UnmarshalTupleDelivery(input)
	})
}

func FuzzUnmarshalTupleACK(f *testing.F) {
	valid := mustMarshalACK(f, testTupleACK())
	// Byte 278 is the trailing TupleACKStatus enum.
	for _, seed := range tupleFuzzSeeds(valid, wire.MessageCraneTupleDeliveryNack, map[int]byte{278: 0xff}) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = UnmarshalTupleACK(input)
	})
}

func FuzzUnmarshalTupleNACK(f *testing.F) {
	valid := mustMarshalNACK(f, testTupleNACK())
	// Bytes 278-279 are the trailing big-endian TupleNACKCode enum.
	for _, seed := range tupleFuzzSeeds(valid, wire.MessageCraneTupleDelivery, map[int]byte{278: 0xff, 279: 0xff}) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = UnmarshalTupleNACK(input)
	})
}

// tupleFuzzSeeds derives the shared malformed-specimen corpus from one valid
// encoding: empty, valid, one byte short, one trailing byte, an unsupported
// schema version, another message type, and an out-of-domain enum value.
func tupleFuzzSeeds(valid []byte, wrongType wire.MessageType, invalidEnum map[int]byte) [][]byte {
	return [][]byte{
		{},
		valid,
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0x00),
		mutatedBytes(valid, map[int]byte{0: 0x00, 1: 0x02}),
		mutatedBytes(valid, map[int]byte{2: byte(wrongType >> 8), 3: byte(wrongType)}),
		mutatedBytes(valid, invalidEnum),
	}
}

// mutatedBytes copies input and overwrites the listed byte offsets.
func mutatedBytes(input []byte, overrides map[int]byte) []byte {
	result := append([]byte(nil), input...)
	for offset, value := range overrides {
		result[offset] = value
	}
	return result
}

func testTupleDelivery() TupleDelivery {
	job := model.JobID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	source := model.TaskID{JobID: job, StageID: 1, Partition: 2}
	tupleID := model.TupleID{JobID: job, SourceTask: source, SourceSequence: 3, PathDigest: [32]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}}
	destinationTask := model.TaskID{JobID: job, StageID: 6, Partition: 7}
	specification := [32]byte{13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13}
	return TupleDelivery{
		DeliveryID: model.DeliveryID{Tuple: tupleID, EdgeID: 5, DestinationTask: destinationTask},
		Tuple:      model.Tuple{Fields: []model.Field{{Name: "raw", Value: model.Value{Type: model.ValueBytes, Bytes: []byte{8, 9}}}}},
		Producer: model.AssignmentToken{
			Task: model.TaskID{JobID: job, StageID: 8, Partition: 9}, WorkerID: 10,
			WorkerEpoch: model.WorkerEpoch{11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11},
			Attempt:     12, SpecificationHash: specification, AssignmentRevision: 14,
		},
		Destination: model.AssignmentToken{
			Task: destinationTask, WorkerID: 15,
			WorkerEpoch: model.WorkerEpoch{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
			Attempt:     17, SpecificationHash: specification, AssignmentRevision: 14,
		},
		Assignment:  AssignmentSetIdentity{JobID: job, Revision: 14, Digest: [32]byte{18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18}},
		Coordinator: model.CoordinatorEpoch{Term: 19, BeginIndex: 20, Coordinator: 21, Nonce: [16]byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22}},
	}
}

// testTupleACK spells out the ACK fixture independently of testTupleDelivery
// so the ACK golden does not inherit the delivery layout by construction.
func testTupleACK() TupleACK {
	job := model.JobID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	specification := [32]byte{13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13}
	return TupleACK{
		DeliveryID: model.DeliveryID{
			Tuple: model.TupleID{
				JobID: job, SourceTask: model.TaskID{JobID: job, StageID: 1, Partition: 2}, SourceSequence: 3,
				PathDigest: [32]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
			},
			EdgeID: 5, DestinationTask: model.TaskID{JobID: job, StageID: 6, Partition: 7},
		},
		Destination: model.AssignmentToken{
			Task: model.TaskID{JobID: job, StageID: 6, Partition: 7}, WorkerID: 15,
			WorkerEpoch: model.WorkerEpoch{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
			Attempt:     17, SpecificationHash: specification, AssignmentRevision: 14,
		},
		Assignment:  AssignmentSetIdentity{JobID: job, Revision: 14, Digest: [32]byte{18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18}},
		Coordinator: model.CoordinatorEpoch{Term: 19, BeginIndex: 20, Coordinator: 21, Nonce: [16]byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22}},
		Status:      TupleAccepted,
	}
}

// testTupleNACK spells out the NACK fixture independently of testTupleDelivery
// and testTupleACK for the same reason.
func testTupleNACK() TupleNACK {
	job := model.JobID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	specification := [32]byte{13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13}
	return TupleNACK{
		DeliveryID: model.DeliveryID{
			Tuple: model.TupleID{
				JobID: job, SourceTask: model.TaskID{JobID: job, StageID: 1, Partition: 2}, SourceSequence: 3,
				PathDigest: [32]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
			},
			EdgeID: 5, DestinationTask: model.TaskID{JobID: job, StageID: 6, Partition: 7},
		},
		Destination: model.AssignmentToken{
			Task: model.TaskID{JobID: job, StageID: 6, Partition: 7}, WorkerID: 15,
			WorkerEpoch: model.WorkerEpoch{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
			Attempt:     17, SpecificationHash: specification, AssignmentRevision: 14,
		},
		Assignment:  AssignmentSetIdentity{JobID: job, Revision: 14, Digest: [32]byte{18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18}},
		Coordinator: model.CoordinatorEpoch{Term: 19, BeginIndex: 20, Coordinator: 21, Nonce: [16]byte{22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22}},
		Code:        TupleNACKOverloaded,
	}
}

type testingFataler interface {
	Helper()
	Fatalf(string, ...any)
}

func mustMarshalDelivery(t testingFataler, value TupleDelivery) []byte {
	t.Helper()
	encoded, err := MarshalTupleDelivery(value)
	if err != nil {
		t.Fatalf("MarshalTupleDelivery: %v", err)
	}
	return encoded
}

func mustMarshalACK(t testingFataler, value TupleACK) []byte {
	t.Helper()
	encoded, err := MarshalTupleACK(value)
	if err != nil {
		t.Fatalf("MarshalTupleACK: %v", err)
	}
	return encoded
}

func mustMarshalNACK(t testingFataler, value TupleNACK) []byte {
	t.Helper()
	encoded, err := MarshalTupleNACK(value)
	if err != nil {
		t.Fatalf("MarshalTupleNACK: %v", err)
	}
	return encoded
}

func decodeDeliveryError(input []byte) error { _, err := UnmarshalTupleDelivery(input); return err }
func decodeACKError(input []byte) error      { _, err := UnmarshalTupleACK(input); return err }
func decodeNACKError(input []byte) error     { _, err := UnmarshalTupleNACK(input); return err }

func benchmarkTupleDecodeBytes(input []byte) int64 {
	// Amortized bytes/op over a fixed iteration count via the monotonic
	// process TotalAlloc counter; no protocol test runs in parallel, so the
	// counter is not polluted by concurrent allocations. This measures the
	// same per-input allocation bound as testing.Benchmark did without
	// spending a full benchtime second per malformed input.
	const iterations = 4096
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for index := 0; index < iterations; index++ {
		_, _ = UnmarshalTupleDelivery(input)
	}
	runtime.ReadMemStats(&after)
	return int64(after.TotalAlloc-before.TotalAlloc) / iterations
}

func assertNoTupleCodecPanic(t *testing.T, input []byte) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("tuple codec panicked for %d bytes: %v", len(input), recovered)
		}
	}()
	_, _ = UnmarshalTupleDelivery(input)
	_, _ = UnmarshalTupleACK(input)
	_, _ = UnmarshalTupleNACK(input)
}
