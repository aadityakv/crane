package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func testHeader() Header {
	return Header{
		Version:         Version1,
		Message:         MessageSWIMPing,
		ClusterID:       [16]byte{1, 2, 3},
		SenderID:        7,
		RequestID:       RequestID{9, 8, 7},
		TimestampMillis: 123456,
		Codec:           CodecGob,
	}
}

func TestFrameRoundTripAndTamperRejection(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	header := testHeader()
	encoded, err := Encode(header, []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	frame, err := Decode(encoded, auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Header != header || string(frame.Payload) != "payload" {
		t.Fatalf("decoded frame = %#v", frame)
	}

	encoded[len(encoded)-MACSize-1] ^= 0xff
	if _, err := Decode(encoded, auth, DefaultLimits()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("tampered decode error = %v", err)
	}
}

func TestFrameEncodingUsesExactCanonicalBytes(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	encoded, err := Encode(testHeader(), []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	const wantHex = "435334320001000101020300000000000000000000000000000709080700000000000000000000000000000000000001e24001000000077061796c6f61646056bc837993f056b5d1994fd21493c2da899022e0877d9d0ab460f08b6a86b3"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("encoded frame = %s, want %s", got, wantHex)
	}
}

func TestFrameAcceptsAuthenticatedBinaryCodec(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	header := testHeader()
	header.Message = MessageRaftPreVoteRequest
	header.Codec = CodecBinary

	encoded, err := Encode(header, []byte{0, 1, 2, 3}, auth, DefaultLimits())
	if err != nil {
		t.Fatalf("Encode binary frame: %v", err)
	}
	frame, err := Decode(encoded, auth, DefaultLimits())
	if err != nil {
		t.Fatalf("Decode binary frame: %v", err)
	}
	if frame.Header != header || !bytes.Equal(frame.Payload, []byte{0, 1, 2, 3}) {
		t.Fatalf("decoded binary frame = %#v", frame)
	}
}

func TestFrameRejectsMalformedAndUnsupportedHeaders(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	valid, err := Encode(testHeader(), []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{name: "bad_magic", mutate: func(frame []byte) []byte { frame[0] ^= 0xff; return frame }, want: ErrMalformed},
		{name: "unsupported_version", mutate: func(frame []byte) []byte { binary.BigEndian.PutUint16(frame[4:6], 2); return frame }, want: ErrUnsupportedVersion},
		{name: "unknown_codec", mutate: func(frame []byte) []byte { frame[50] = 0xff; return frame }, want: ErrUnsupportedCodec},
		{name: "truncated_header", mutate: func(frame []byte) []byte { return frame[:FixedHeaderSize-1] }, want: ErrMalformed},
		{name: "truncated_mac", mutate: func(frame []byte) []byte { return frame[:len(frame)-1] }, want: ErrMalformed},
		{name: "mismatched_payload_length", mutate: func(frame []byte) []byte { binary.BigEndian.PutUint32(frame[51:55], 8); return frame }, want: ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := append([]byte(nil), valid...)
			input = tt.mutate(input)
			if _, err := Decode(input, auth, DefaultLimits()); !errors.Is(err, tt.want) {
				t.Fatalf("Decode error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestFrameRejectsOversizedBodiesBeforeAuthentication(t *testing.T) {
	limits := DefaultLimits()
	auth := &recordingAuthenticator{}

	tooLarge := make([]byte, limits.MaxFrameSize+1)
	if _, err := Decode(tooLarge, auth, limits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized body error = %v", err)
	}
	if auth.verifyCalls != 0 {
		t.Fatalf("oversized body invoked authentication %d times", auth.verifyCalls)
	}

	header := make([]byte, FixedHeaderSize+MACSize)
	copy(header[:4], "CS42")
	binary.BigEndian.PutUint16(header[4:6], Version1)
	binary.BigEndian.PutUint16(header[6:8], uint16(MessageSWIMPing))
	header[50] = byte(CodecGob)
	binary.BigEndian.PutUint32(header[51:55], ^uint32(0))
	if _, err := Decode(header, auth, limits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("overflowing declared payload error = %v", err)
	}
	if auth.verifyCalls != 0 {
		t.Fatalf("overflowing declared payload invoked authentication %d times", auth.verifyCalls)
	}
}

func TestFrameRejectsLimitOutsideCanonicalLengthDomain(t *testing.T) {
	if uint64(math.MaxInt) <= uint64(math.MaxUint32) {
		t.Skip("int cannot represent a limit outside the uint32 frame domain")
	}
	limits := DefaultLimits()
	limits.MaxFrameSize = math.MaxInt
	if _, err := Encode(testHeader(), nil, NewHMACAuthenticator(testKey), limits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("unrepresentable frame limit error = %v", err)
	}
}

func TestFrameRejectsSWIMDatagramOver1200Bytes(t *testing.T) {
	limits := DefaultLimits()
	overhead := FixedHeaderSize + MACSize
	payload := make([]byte, limits.MaxSWIMDatagramSize-overhead+1)
	if _, err := Encode(testHeader(), payload, NewHMACAuthenticator(testKey), limits); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized SWIM Encode error = %v", err)
	}
}

func TestFrameRejectsEverySWIMDatagramTypeOver1200BytesOnDecode(t *testing.T) {
	auth := NewHMACAuthenticator([]byte("0123456789abcdef0123456789abcdef"))
	limits := DefaultLimits()
	overhead := FixedHeaderSize + MACSize
	payload := make([]byte, limits.MaxSWIMDatagramSize-overhead+1)
	encodeLimits := limits
	encodeLimits.MaxSWIMDatagramSize++

	messageTypes := []MessageType{
		MessageSWIMPing,
		MessageSWIMAck,
		MessageSWIMPingReq,
		MessageSWIMIndirectAck,
		MessageSWIMGossip,
		MessageSWIMDigest,
	}
	for _, messageType := range messageTypes {
		t.Run(fmt.Sprint(messageType), func(t *testing.T) {
			header := Header{Version: Version1, Message: messageType, SenderID: 1, Codec: CodecGob}
			encoded, err := Encode(header, payload, auth, encodeLimits)
			if err != nil {
				t.Fatalf("Encode with enlarged datagram limit: %v", err)
			}
			if _, err := Decode(encoded, auth, limits); !errors.Is(err, ErrTooLarge) {
				t.Fatalf("Decode %d-byte datagram error = %v, want ErrTooLarge", len(encoded), err)
			}
		})
	}
}

func TestFrameDoesNotApplyDatagramLimitToSWIMTCPMessages(t *testing.T) {
	auth := NewHMACAuthenticator([]byte("0123456789abcdef0123456789abcdef"))
	limits := DefaultLimits()
	payload := make([]byte, limits.MaxSWIMDatagramSize)
	header := Header{Version: Version1, Message: MessageSWIMJoinRequest, SenderID: 1, Codec: CodecGob}
	encoded, err := Encode(header, payload, auth, limits)
	if err != nil {
		t.Fatalf("Encode TCP message over datagram limit: %v", err)
	}
	if _, err := Decode(encoded, auth, limits); err != nil {
		t.Fatalf("Decode TCP message over datagram limit: %v", err)
	}
}

func TestFrameRejectsWrongKeyAndExpectedCluster(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	encoded, err := Encode(testHeader(), []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded, NewHMACAuthenticator([]byte("abcdef0123456789abcdef0123456789")), DefaultLimits()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong-key Decode error = %v", err)
	}

	otherCluster := [16]byte{4, 5, 6}
	limits := DefaultLimits()
	limits.ExpectedClusterID = &otherCluster
	if _, err := Decode(encoded, auth, limits); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong-cluster Decode error = %v", err)
	}
}

func TestFramePayloadOwnershipIsDefensive(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	payload := []byte("payload")
	encoded, err := Encode(testHeader(), payload, auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	frame, err := Decode(encoded, auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	encoded[FixedHeaderSize] = 'Y'
	if got := string(frame.Payload); got != "payload" {
		t.Fatalf("decoded payload changed through caller buffer: %q", got)
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	auth := NewHMACAuthenticator(testKey)
	valid, err := Encode(testHeader(), []byte("payload"), auth, DefaultLimits())
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(nil))
	f.Add([]byte("CS42"))
	f.Add(valid)
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = Decode(input, auth, DefaultLimits())
	})
}

type recordingAuthenticator struct {
	verifyCalls int
}

func (*recordingAuthenticator) Sign([]byte) []byte {
	return make([]byte, MACSize)
}

func (a *recordingAuthenticator) Verify([]byte, []byte) bool {
	a.verifyCalls++
	return true
}
