package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestTCPHandshakeBindsConfiguredHeaderAndPayloadSender(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	ingress := task10Ingress{called: make(chan RPC, 1)}
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		transport.handleInboundConnection(context.Background(), server, ingress)
		close(done)
	}()
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	handshake := Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()}
	requestID := wire.RequestID{1}
	if err := writeFrameForTest(stream, transportFrame(t, transport, 2, requestID, handshake)); err != nil {
		t.Fatal(err)
	}
	ackFrame, err := readFrameOrError(stream)
	if err != nil {
		t.Fatal(err)
	}
	if ackFrame.Header.Message != wire.MessageRaftHandshakeAck || ackFrame.Header.SenderID != 1 || ackFrame.Header.RequestID != requestID {
		t.Fatalf("handshake ack header = %#v", ackFrame.Header)
	}
	ack, err := DecodeRPC(ackFrame.Header.Message, ackFrame.Payload, DefaultCodecLimits())
	if err != nil || ack.(HandshakeAck).ResponderID != 1 {
		t.Fatalf("handshake ack = (%#v, %v)", ack, err)
	}

	rpc := AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1}
	if err := writeFrameForTest(stream, transportFrame(t, transport, 2, wire.RequestID{2}, rpc)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ingress.called:
		if got.(AppendEntriesRequest).LeaderID != 2 {
			t.Fatalf("submitted RPC = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("valid bound RPC was not submitted")
	}
	_ = stream.Close()
	awaitClosed(t, done)
}

func TestTCPHandshakeRejectsPayloadMismatchAndReplayAcrossReconnect(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	bad := Handshake{SenderID: 3, VoterFingerprint: transport.voters.Fingerprint()}
	client, server := net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	if err := writeFrameForTest(stream, transportFrame(t, transport, 2, wire.RequestID{1}, bad)); err != nil {
		t.Fatal(err)
	}
	expectClosedStream(t, stream)

	valid := transportFrame(t, transport, 2, wire.RequestID{2}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
	for attempt := 0; attempt < 2; attempt++ {
		client, server = net.Pipe()
		go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
		stream = wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
		if err := writeFrameForTest(stream, valid); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			if _, err := readFrameOrError(stream); err != nil {
				t.Fatalf("first handshake: %v", err)
			}
			_ = stream.Close()
		} else {
			expectClosedStream(t, stream)
		}
	}
}

func TestTCPInvalidPayloadDoesNotConsumeAcceptedReplayCapacity(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	transport.replay = wire.NewReplayGuard(transport.clock, transport.replayWindow, TransportFutureSkew, 1)
	client, server := net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	invalid := wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageRaftHandshake), Payload: []byte{0}}
	if err := writeFrameForTest(stream, invalid); err != nil {
		t.Fatal(err)
	}
	expectClosedStream(t, stream)

	client, server = net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream = wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	valid := transportFrame(t, transport, 2, wire.RequestID{2}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
	if err := writeFrameForTest(stream, valid); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameOrError(stream); err != nil {
		t.Fatalf("valid handshake after invalid capacity entry: %v", err)
	}
}

func TestTCPBoundStreamRejectsSenderChangeAndGob(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame func(*TCPTransport) wire.Frame
	}{
		{name: "changed sender", frame: func(transport *TCPTransport) wire.Frame {
			return transportFrame(t, transport, 3, wire.RequestID{2}, AppendEntriesRequest{LeaderID: 3, Term: 1, Generation: 1})
		}},
		{name: "gob codec", frame: func(transport *TCPTransport) wire.Frame {
			frame := transportFrame(t, transport, 2, wire.RequestID{2}, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1})
			frame.Header.Codec = wire.CodecGob
			return frame
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := newTask10Transport(t, task10TransportOptions{})
			client, server := net.Pipe()
			go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
			stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
			handshake := transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
			if err := writeFrameForTest(stream, handshake); err != nil {
				t.Fatal(err)
			}
			if _, err := readFrameOrError(stream); err != nil {
				t.Fatal(err)
			}
			_ = writeFrameForTest(stream, test.frame(transport))
			expectClosedStream(t, stream)
		})
	}
}

func TestTCPHandshakeRejectsUnauthenticatedUnboundAndInvalidPeers(t *testing.T) {
	for _, test := range []struct {
		name  string
		build func(*TCPTransport) (*wire.TCPFrameStream, wire.Frame)
	}{
		{name: "wrong cluster", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
			frame.Header.ClusterID[0]++
			return nil, frame
		}},
		{name: "wrong mac", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
			return wire.NewTCPFrameStream(nil, wire.NewHMACAuthenticator([]byte("different-authentication-key-0000")), wire.DefaultLimits(), time.Second), frame
		}},
		{name: "rpc before handshake", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 2, wire.RequestID{1}, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1})
		}},
		{name: "self sender", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 1, wire.RequestID{1}, Handshake{SenderID: 1, VoterFingerprint: transport.voters.Fingerprint()})
		}},
		{name: "nonvoter", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 4, wire.RequestID{1}, Handshake{SenderID: 4, VoterFingerprint: transport.voters.Fingerprint()})
		}},
		{name: "fingerprint mismatch", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			fingerprint := transport.voters.Fingerprint()
			fingerprint[0]++
			return nil, transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: fingerprint})
		}},
		{name: "unknown type", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageType(999)), Payload: []byte{0, 1}}
		}},
		{name: "malformed binary", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageRaftHandshake), Payload: []byte{0}}
		}},
		{name: "future timestamp", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
			frame.Header.TimestampMillis = transport.clock.Now().Add(TransportFutureSkew + time.Millisecond).UnixMilli()
			return nil, frame
		}},
		{name: "expired timestamp", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, Handshake{SenderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
			frame.Header.TimestampMillis = transport.clock.Now().Add(-transport.replayWindow).UnixMilli()
			return nil, frame
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := newTask10Transport(t, task10TransportOptions{})
			client, server := net.Pipe()
			go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
			provided, frame := test.build(transport)
			stream := wire.NewTCPFrameStream(client, transport.authenticator, wire.DefaultLimits(), time.Second)
			if provided != nil {
				stream = wire.NewTCPFrameStream(client, wire.NewHMACAuthenticator([]byte("different-authentication-key-0000")), wire.DefaultLimits(), time.Second)
			}
			_ = writeFrameForTest(stream, frame)
			expectClosedStream(t, stream)
		})
	}
}

func TestTCPHandshakeReadDeadlineCoversOversizedPrefixAndSlowloris(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(net.Conn)
	}{
		{name: "oversized prefix", write: func(connection net.Conn) {
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], uint32(wire.DefaultLimits().MaxFrameSize+1))
			_, _ = connection.Write(prefix[:])
		}},
		{name: "slowloris header", write: func(connection net.Conn) {
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], uint32(wire.FixedHeaderSize+wire.MACSize))
			_, _ = connection.Write(prefix[:])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := newTask10Transport(t, task10TransportOptions{})
			client, server := net.Pipe()
			done := make(chan struct{})
			go func() {
				transport.handleInboundConnection(context.Background(), server, task10Ingress{})
				close(done)
			}()
			test.write(client)
			awaitClosed(t, done)
			_ = client.Close()
		})
	}
}

func TestTCPRetryUsesFreshWireRequestIDForSameSemanticRPC(t *testing.T) {
	requestIDs := &task10RequestIDs{}
	var dialMu sync.Mutex
	connections := 0
	received := make(chan wire.Frame, 1)
	var transport *TCPTransport
	transport = newTask10Transport(t, task10TransportOptions{
		requestIDs: requestIDs,
		dial: func(context.Context, string, string) (net.Conn, error) {
			dialMu.Lock()
			connections++
			attempt := connections
			dialMu.Unlock()
			client, server := net.Pipe()
			if attempt == 1 {
				client = &failWriteConn{Conn: client, writesRemaining: 2}
			}
			go serveTask10OutboundPeer(t, transport, server, received)
			return client, nil
		},
		backoff: func(context.Context, time.Duration) error { return nil },
	})
	listener := newBlockingListener()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, listener, task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	rpc := AppendEntriesRequest{LeaderID: 1, Term: 4, Generation: 7, LeaderCommit: 3}
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: rpc}); err != nil || got != TransportAccepted {
		t.Fatalf("Handoff = (%d, %v)", got, err)
	}
	select {
	case frame := <-received:
		decoded, err := DecodeRPC(frame.Header.Message, frame.Payload, DefaultCodecLimits())
		if err != nil || decoded.(AppendEntriesRequest).Generation != rpc.Generation {
			t.Fatalf("retried RPC = (%#v, %v)", decoded, err)
		}
		if frame.Header.RequestID != (wire.RequestID{4}) {
			t.Fatalf("retry request ID = %x, want fourth fresh ID", frame.Header.RequestID)
		}
	case <-time.After(time.Second):
		t.Fatal("semantic RPC was not delivered after reconnect")
	}
	activeConnections := 0
	transport.connections.Range(func(_, _ any) bool {
		activeConnections++
		return true
	})
	if activeConnections != 1 {
		t.Fatalf("tracked connections after reconnect = %d, want one active stream", activeConnections)
	}
	requestIDs.mu.Lock()
	calls := append([]wire.RequestID(nil), requestIDs.calls...)
	requestIDs.mu.Unlock()
	if len(calls) < 4 || calls[1] == calls[3] {
		t.Fatalf("request ID sequence = %x, want fresh ID on retry", calls)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func transportFrame(t *testing.T, transport *TCPTransport, sender uint16, requestID wire.RequestID, rpc RPC) wire.Frame {
	t.Helper()
	messageType, payload, err := EncodeRPC(rpc, transport.codecLimits)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Frame{Header: transportHeader(transport, sender, requestID, messageType), Payload: payload}
}

func transportHeader(transport *TCPTransport, sender uint16, requestID wire.RequestID, message wire.MessageType) wire.Header {
	return wire.Header{
		Version: wire.Version1, Message: message, ClusterID: transport.clusterID,
		SenderID: sender, RequestID: requestID, TimestampMillis: transport.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}
}

type failWriteConn struct {
	net.Conn
	mu              sync.Mutex
	writesRemaining int
}

func (connection *failWriteConn) Write(payload []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.writesRemaining == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	connection.writesRemaining--
	return connection.Conn.Write(payload)
}

func serveTask10OutboundPeer(t *testing.T, transport *TCPTransport, connection net.Conn, received chan<- wire.Frame) {
	t.Helper()
	defer connection.Close()
	stream := wire.NewTCPFrameStream(connection, transport.authenticator, transport.limits, time.Second)
	handshake, err := readFrameOrError(stream)
	if err != nil {
		return
	}
	ack := transportFrame(t, transport, 2, handshake.Header.RequestID, HandshakeAck{ResponderID: 2, VoterFingerprint: transport.voters.Fingerprint()})
	if err := writeFrameForTest(stream, ack); err != nil {
		return
	}
	frame, err := readFrameOrError(stream)
	if err == nil {
		received <- frame
	}
}

func TestTCPRejectsZeroOrReusedGeneratedRequestID(t *testing.T) {
	for _, ids := range []RequestIDSource{
		&fixedTask10RequestIDs{ids: []wire.RequestID{{}}},
		&fixedTask10RequestIDs{ids: []wire.RequestID{{1}, {1}}},
	} {
		transport := newTask10Transport(t, task10TransportOptions{requestIDs: ids})
		first, err := transport.nextRequestID()
		if first == (wire.RequestID{}) && !errors.Is(err, ErrRequestIDExhausted) {
			t.Fatalf("zero request ID error = %v", err)
		}
		if err == nil {
			if _, err = transport.nextRequestID(); !errors.Is(err, ErrRequestIDExhausted) {
				t.Fatalf("reused request ID error = %v", err)
			}
		}
	}
}

type fixedTask10RequestIDs struct {
	ids []wire.RequestID
	mu  sync.Mutex
}

func (source *fixedTask10RequestIDs) NextRequestID() (wire.RequestID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.ids) == 0 {
		return wire.RequestID{}, ErrRequestIDExhausted
	}
	id := source.ids[0]
	source.ids = source.ids[1:]
	return id, nil
}
