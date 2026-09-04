package raft

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/wire"
)

var task5ApplicationFingerprint = [32]byte{
	0x10, 0xa4, 0x4b, 0x7b, 0xf1, 0x19, 0xae, 0xc8,
	0x50, 0x37, 0xe3, 0x43, 0x68, 0x03, 0x23, 0xc2,
	0x20, 0xee, 0x02, 0xb0, 0x9a, 0x62, 0x72, 0x98,
	0xdc, 0x89, 0x65, 0xfb, 0xa4, 0xae, 0x02, 0x1b,
}

func TestHandshakeApplicationFingerprintCanonicalV2Layouts(t *testing.T) {
	voterFingerprint := VoterFingerprint{1}
	tests := []struct {
		name    string
		rpc     RPC
		message wire.MessageType
		wantHex string
	}{
		{
			name: "request", message: wire.MessageRaftHandshake,
			rpc:     Handshake{SenderID: 1, VoterFingerprint: voterFingerprint, ApplicationFingerprint: task5ApplicationFingerprint},
			wantHex: "00020001010000000000000000000000000000000000000000000000000000000000000010a44b7bf119aec85037e343680323c220ee02b09a627298dc8965fba4ae021b",
		},
		{
			name: "ack", message: wire.MessageRaftHandshakeAck,
			rpc:     HandshakeAck{ResponderID: 2, VoterFingerprint: voterFingerprint, ApplicationFingerprint: task5ApplicationFingerprint},
			wantHex: "00020002010000000000000000000000000000000000000000000000000000000000000010a44b7bf119aec85037e343680323c220ee02b09a627298dc8965fba4ae021b",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, encoded, err := EncodeRPC(test.rpc, DefaultCodecLimits())
			if err != nil {
				t.Fatalf("EncodeRPC: %v", err)
			}
			if message != test.message || hex.EncodeToString(encoded) != test.wantHex {
				t.Fatalf("encoded (%d, %x), want (%d, %s)", message, encoded, test.message, test.wantHex)
			}
			decoded, err := DecodeRPC(message, encoded, DefaultCodecLimits())
			if err != nil || decoded != test.rpc {
				t.Fatalf("DecodeRPC = %#v, %v; want %#v", decoded, err, test.rpc)
			}
			for length := 0; length < len(encoded); length++ {
				if _, err := DecodeRPC(message, encoded[:length], DefaultCodecLimits()); err == nil {
					t.Fatalf("accepted truncated v2 handshake prefix %d/%d", length, len(encoded))
				}
			}
		})
	}
}

func TestHandshakeApplicationFingerprintRejectsOldZeroAndMutatedBytes(t *testing.T) {
	voterFingerprint := VoterFingerprint{1}
	oldRequest, err := hex.DecodeString("000100010100000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	oldAck := append([]byte(nil), oldRequest...)
	oldAck[3] = 2
	for _, test := range []struct {
		name    string
		message wire.MessageType
		payload []byte
	}{
		{name: "old request", message: wire.MessageRaftHandshake, payload: oldRequest},
		{name: "old ack", message: wire.MessageRaftHandshakeAck, payload: oldAck},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeRPC(test.message, test.payload, DefaultCodecLimits()); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("DecodeRPC error = %v, want ErrUnsupportedSchema", err)
			}
		})
	}
	for _, rpc := range []RPC{
		Handshake{SenderID: 1, VoterFingerprint: voterFingerprint},
		HandshakeAck{ResponderID: 2, VoterFingerprint: voterFingerprint},
	} {
		if _, _, err := EncodeRPC(rpc, DefaultCodecLimits()); !errors.Is(err, ErrInvalidRPC) {
			t.Fatalf("EncodeRPC(%T) error = %v, want ErrInvalidRPC", rpc, err)
		}
	}
	_, encoded, err := EncodeRPC(Handshake{SenderID: 1, VoterFingerprint: voterFingerprint, ApplicationFingerprint: task5ApplicationFingerprint}, DefaultCodecLimits())
	if err != nil {
		t.Fatal(err)
	}
	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] ^= 0x80
	decoded, err := DecodeRPC(wire.MessageRaftHandshake, mutated, DefaultCodecLimits())
	if err != nil {
		t.Fatalf("structurally valid mutation should decode for transport comparison: %v", err)
	}
	if decoded.(Handshake).ApplicationFingerprint == task5ApplicationFingerprint {
		t.Fatal("mutated application fingerprint was silently normalized")
	}
}

func TestTCPApplicationFingerprintMismatchRejectedBeforeIngressOrReplayCommit(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	matching := task5ApplicationFingerprint
	transport.applicationFingerprint = matching
	mismatched := matching
	mismatched[0] ^= 0xff
	requestID := wire.RequestID{0x55}
	frame := transportFrame(t, transport, 2, requestID, Handshake{
		SenderID: 2, VoterFingerprint: transport.voters.Fingerprint(), ApplicationFingerprint: mismatched,
	})
	if rpc, ok := transport.validateInboundFrame(frame, 0, wire.MessageRaftHandshake); ok || rpc != nil {
		t.Fatalf("mismatched handshake accepted: %#v", rpc)
	}
	guard, err := transport.replayGuard(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Preflight(2, requestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		t.Fatalf("mismatched application handshake was credited to replay/contact state: %v", err)
	}
}

func TestTCPApplicationFingerprintMismatchAckCannotOpenRPCStream(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	transport.applicationFingerprint = task5ApplicationFingerprint
	requestID := wire.RequestID{0x66}
	mismatched := task5ApplicationFingerprint
	mismatched[31] ^= 1
	frame := transportFrame(t, transport, 2, requestID, HandshakeAck{
		ResponderID: 2, VoterFingerprint: transport.voters.Fingerprint(), ApplicationFingerprint: mismatched,
	})
	if err := transport.acceptHandshakeAck(frame, 2, requestID); !errors.Is(err, ErrApplicationFingerprint) {
		t.Fatalf("acceptHandshakeAck error = %v, want ErrApplicationFingerprint", err)
	}
	guard, err := transport.replayGuard(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Preflight(2, requestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		t.Fatalf("mismatched application ACK was credited to replay/contact state: %v", err)
	}
}

func TestServiceApplicationFingerprintRequiredOwnedAndForwardedBeforeEffects(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33900)
	options := ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	}
	if service, err := NewService(options); !errors.Is(err, ErrApplicationFingerprint) || service != nil {
		t.Fatalf("zero application fingerprint NewService = (%v, %v), want nil ErrApplicationFingerprint", service, err)
	}

	options.ApplicationFingerprint = task5ApplicationFingerprint
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	options.ApplicationFingerprint[0] ^= 0xff
	if service.options.ApplicationFingerprint != task5ApplicationFingerprint {
		t.Fatal("service aliases caller application fingerprint")
	}
	service.openStore = func(string, StorageIdentity, VoterSet, StoreOptions) (StableStore, error) {
		return nil, errors.New("stop after constructor ownership proof")
	}
	var captured TCPTransportOptions
	service.newTransport = func(options TCPTransportOptions) (serviceTransport, error) {
		captured = options
		return nil, errors.New("unexpected transport construction")
	}
	_ = service.Run(t.Context())
	if captured.ApplicationFingerprint != ([32]byte{}) {
		t.Fatal("transport was constructed after injected earlier storage failure")
	}

}

func task5Handshake(senderID uint16, fingerprint VoterFingerprint) Handshake {
	return Handshake{SenderID: senderID, VoterFingerprint: fingerprint, ApplicationFingerprint: task5ApplicationFingerprint}
}

func task5HandshakeAck(responderID uint16, fingerprint VoterFingerprint) HandshakeAck {
	return HandshakeAck{ResponderID: responderID, VoterFingerprint: fingerprint, ApplicationFingerprint: task5ApplicationFingerprint}
}
