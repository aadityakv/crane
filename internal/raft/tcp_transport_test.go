package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/wire"
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
	handshake := task5Handshake(2, transport.voters.Fingerprint())
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

func TestTCPHandshakeAckReservesCorrelatedRequestIDForOutboundReplayWindow(t *testing.T) {
	id := wire.RequestID{0x51}
	transport := newTask10Transport(t, task10TransportOptions{requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{id}}})
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		transport.handleInboundConnection(context.Background(), server, task10Ingress{})
		close(done)
	}()
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	handshake := transportFrame(t, transport, 2, id, task5Handshake(2, transport.voters.Fingerprint()))
	if err := writeFrameForTest(stream, handshake); err != nil {
		t.Fatal(err)
	}
	ack, err := readFrameOrError(stream)
	if err != nil {
		t.Fatal(err)
	}
	ackTime := time.UnixMilli(ack.Header.TimestampMillis)
	if got, want := task10TrackedRequestExpiry(transport, 2, id), ackTime.Add(transport.replayRetention); !got.Equal(want) {
		t.Fatalf("correlated ACK expiry = %s, want exact ACK timestamp retention %s", got, want)
	}
	if got := task10TrackedRequestCount(transport, 2); got != 1 {
		t.Fatalf("correlated ACK request map entries = %d, want 1", got)
	}
	if got := task10TrackedExpiryCount(transport, 2); got != 1 {
		t.Fatalf("correlated ACK request heap entries = %d, want 1", got)
	}
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatalf("generated reuse after correlated ACK error = %v, want ErrRequestIDExhausted", err)
	}
	_ = stream.Close()
	awaitClosed(t, done)
}

func TestTCPHandshakeAckRejectsRequestIDAlreadyUsedOutboundToPeer(t *testing.T) {
	id := wire.RequestID{0x52}
	transport := newTask10Transport(t, task10TransportOptions{requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{id}}})
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	handshake := transportFrame(t, transport, 2, id, task5Handshake(2, transport.voters.Fingerprint()))
	writeRejectedFrameForTest(t, stream, handshake)
	expectClosedStream(t, stream)
}

func TestTCPHandshakeRejectsPayloadMismatchAndReplayAcrossReconnect(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	bad := task5Handshake(3, transport.voters.Fingerprint())
	client, server := net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	writeRejectedFrameForTest(t, stream, transportFrame(t, transport, 2, wire.RequestID{1}, bad))
	expectClosedStream(t, stream)

	valid := transportFrame(t, transport, 2, wire.RequestID{2}, task5Handshake(2, transport.voters.Fingerprint()))
	for attempt := 0; attempt < 2; attempt++ {
		client, server = net.Pipe()
		go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
		stream = wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
		if attempt == 0 {
			if err := writeFrameForTest(stream, valid); err != nil {
				t.Fatal(err)
			}
			if _, err := readFrameOrError(stream); err != nil {
				t.Fatalf("first handshake: %v", err)
			}
			_ = stream.Close()
		} else {
			writeRejectedFrameForTest(t, stream, valid)
			expectClosedStream(t, stream)
		}
	}
}

func writeRejectedFrameForTest(t *testing.T, stream *wire.TCPFrameStream, frame wire.Frame) {
	t.Helper()
	err := writeFrameForTest(stream, frame)
	if err == nil {
		return
	}
	// The rejecting peer may close after consuming the frame but before the
	// writer clears its deadline. Errors from encoding or the write itself stay fatal.
	if errors.Is(err, io.ErrClosedPipe) && strings.HasPrefix(err.Error(), "clear TCP write deadline:") {
		return
	}
	t.Fatalf("write rejected frame: %v", err)
}

func TestTCPInvalidPayloadDoesNotConsumeAcceptedReplayCapacity(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	transport.replayGuards[2] = wire.NewReplayGuard(transport.clock, transport.replayWindow, TransportFutureSkew, 1)
	client, server := net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	invalid := wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageRaftHandshake), Payload: []byte{0}}
	_ = writeFrameForTest(stream, invalid)
	expectClosedStream(t, stream)

	client, server = net.Pipe()
	go transport.handleInboundConnection(context.Background(), server, task10Ingress{})
	stream = wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
	valid := transportFrame(t, transport, 2, wire.RequestID{2}, task5Handshake(2, transport.voters.Fingerprint()))
	if err := writeFrameForTest(stream, valid); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameOrError(stream); err != nil {
		t.Fatalf("valid handshake after invalid capacity entry: %v", err)
	}
}

func TestTCPInboundReplayCapacityIsIndependentPerConfiguredRemoteSender(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(1500, 0))
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock})
	heartbeats := int((transport.replayWindow+TransportFutureSkew)/(20*time.Millisecond)) + 1
	for heartbeat := 0; heartbeat < heartbeats; heartbeat++ {
		requestID := task10InboundRequestID(uint64(heartbeat + 1))
		for _, senderID := range []uint16{2, 3} {
			frame := wire.Frame{Header: transportHeader(transport, senderID, requestID, wire.MessageRaftAppendEntriesResponse)}
			if preflighted, err := transport.preflightFrame(frame); err != nil || !preflighted {
				t.Fatalf("heartbeat %d sender %d preflight = (%t, %v)", heartbeat, senderID, preflighted, err)
			}
			if err := transport.commitFrame(frame); err != nil {
				t.Fatalf("heartbeat %d sender %d commit: %v", heartbeat, senderID, err)
			}
		}
		manualClock.Advance(20 * time.Millisecond)
	}
	if got := task10InboundReplayGuardCount(t, transport); got != 2 {
		t.Fatalf("inbound replay guards = %d, want one for each of two remote voters", got)
	}
	for _, senderID := range []uint16{2, 3} {
		accepted, acceptedHeap, invalid, invalidHeap := task10InboundReplayCounts(t, transport, senderID)
		if accepted > TransportReplayEntries || acceptedHeap > TransportReplayEntries || invalid > TransportReplayEntries || invalidHeap > TransportReplayEntries {
			t.Fatalf("sender %d replay bounds = accepted %d/%d invalid %d/%d, cap %d", senderID, accepted, acceptedHeap, invalid, invalidHeap, TransportReplayEntries)
		}
	}
}

func TestTCPInboundInvalidReplayCacheIsIndependentPerConfiguredRemoteSender(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	firstSenderID := wire.RequestID{0xff}
	firstFrame := wire.Frame{Header: transportHeader(transport, 2, firstSenderID, wire.MessageRaftAppendEntriesResponse)}
	if preflighted, err := transport.preflightFrame(firstFrame); err != nil || !preflighted {
		t.Fatalf("sender 2 invalid candidate preflight = (%t, %v)", preflighted, err)
	}
	transport.recordInvalidFrame(firstFrame)
	for index := 1; index <= TransportReplayEntries; index++ {
		frame := wire.Frame{Header: transportHeader(transport, 3, task10InboundRequestID(uint64(index)), wire.MessageRaftAppendEntriesResponse)}
		if preflighted, err := transport.preflightFrame(frame); err != nil || !preflighted {
			t.Fatalf("sender 3 invalid candidate %d preflight = (%t, %v)", index, preflighted, err)
		}
		transport.recordInvalidFrame(frame)
	}
	if _, err := transport.preflightFrame(firstFrame); !errors.Is(err, wire.ErrReplay) {
		t.Fatalf("sender 2 invalid replay after sender 3 pressure = %v, want ErrReplay", err)
	}
	for _, senderID := range []uint16{2, 3} {
		_, _, invalid, invalidHeap := task10InboundReplayCounts(t, transport, senderID)
		if invalid > TransportReplayEntries || invalidHeap > TransportReplayEntries {
			t.Fatalf("sender %d invalid replay bounds = map %d heap %d, cap %d", senderID, invalid, invalidHeap, TransportReplayEntries)
		}
	}
}

func TestTCPInboundReplayGuardFailsClosedAtPerSenderCapacityAndRecoversAtExpiry(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(1600, 0))
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock})
	for index := 1; index <= TransportReplayEntries; index++ {
		frame := wire.Frame{Header: transportHeader(transport, 2, task10InboundRequestID(uint64(index)), wire.MessageRaftAppendEntriesResponse)}
		if err := transport.commitFrame(frame); err != nil {
			t.Fatalf("sender 2 commit %d/%d: %v", index, TransportReplayEntries, err)
		}
	}
	full := wire.Frame{Header: transportHeader(transport, 2, task10InboundRequestID(TransportReplayEntries+1), wire.MessageRaftAppendEntriesResponse)}
	if _, err := transport.preflightFrame(full); !errors.Is(err, wire.ErrReplayCacheFull) {
		t.Fatalf("sender 2 capacity+1 preflight = %v, want ErrReplayCacheFull", err)
	}
	independent := wire.Frame{Header: transportHeader(transport, 3, task10InboundRequestID(1), wire.MessageRaftAppendEntriesResponse)}
	if err := transport.commitFrame(independent); err != nil {
		t.Fatalf("sender 3 first commit while sender 2 is full: %v", err)
	}
	manualClock.Advance(transport.replayWindow)
	reused := wire.Frame{Header: transportHeader(transport, 2, task10InboundRequestID(1), wire.MessageRaftAppendEntriesResponse)}
	if err := transport.commitFrame(reused); err != nil {
		t.Fatalf("sender 2 reuse at exact expiry: %v", err)
	}
	accepted, acceptedHeap, _, _ := task10InboundReplayCounts(t, transport, 2)
	if accepted != 1 || acceptedHeap != 1 {
		t.Fatalf("sender 2 post-expiry replay state = map %d heap %d, want 1/1", accepted, acceptedHeap)
	}
}

func TestTCPInboundReplayRejectsBeforeAllocatingForUnconfiguredSender(t *testing.T) {
	transport := newTask10Transport(t, task10TransportOptions{})
	for _, senderID := range []uint16{0, 1, 4} {
		frame := wire.Frame{Header: transportHeader(transport, senderID, wire.RequestID{1}, wire.MessageRaftAppendEntriesResponse)}
		if preflighted, err := transport.preflightFrame(frame); err == nil || preflighted {
			t.Fatalf("sender %d preflight = (%t, %v), want rejected before replay guard", senderID, preflighted, err)
		}
	}
	if got := task10InboundReplayGuardCount(t, transport); got != 2 {
		t.Fatalf("replay guard map grew after invalid senders: %d", got)
	}
}

func task10InboundRequestID(value uint64) wire.RequestID {
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[len(requestID)-8:], value)
	return requestID
}

func task10InboundReplayGuardCount(t *testing.T, transport *TCPTransport) int {
	t.Helper()
	guards := reflect.ValueOf(transport).Elem().FieldByName("replayGuards")
	if !guards.IsValid() {
		t.Fatal("TCPTransport has no per-sender inbound replay guard map")
	}
	return guards.Len()
}

func task10InboundReplayCounts(t *testing.T, transport *TCPTransport, senderID uint16) (int, int, int, int) {
	t.Helper()
	guards := reflect.ValueOf(transport).Elem().FieldByName("replayGuards")
	if !guards.IsValid() {
		t.Fatal("TCPTransport has no per-sender inbound replay guard map")
	}
	guard := guards.MapIndex(reflect.ValueOf(senderID))
	if !guard.IsValid() || guard.IsNil() {
		t.Fatalf("TCPTransport has no inbound replay guard for sender %d", senderID)
	}
	state := guard.Elem()
	return state.FieldByName("seen").Len(), state.FieldByName("expirations").Len(), state.FieldByName("invalid").Len(), state.FieldByName("invalidExpiry").Len()
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
			done := make(chan struct{})
			called := make(chan RPC, 1)
			go func() {
				transport.handleInboundConnection(context.Background(), server, task10Ingress{called: called})
				close(done)
			}()
			stream := wire.NewTCPFrameStream(client, transport.authenticator, transport.limits, time.Second)
			handshake := transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, transport.voters.Fingerprint()))
			if err := writeFrameForTest(stream, handshake); err != nil {
				t.Fatal(err)
			}
			if _, err := readFrameOrError(stream); err != nil {
				t.Fatal(err)
			}
			_ = writeFrameForTest(stream, test.frame(transport))
			select {
			case rpc := <-called:
				t.Fatalf("rejected stream submitted %#v", rpc)
			case <-time.After(20 * time.Millisecond):
			}
			awaitClosed(t, done)
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
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, transport.voters.Fingerprint()))
			frame.Header.ClusterID[0]++
			return nil, frame
		}},
		{name: "wrong mac", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, transport.voters.Fingerprint()))
			return wire.NewTCPFrameStream(nil, wire.NewHMACAuthenticator([]byte("different-authentication-key-0000")), wire.DefaultLimits(), time.Second), frame
		}},
		{name: "rpc before handshake", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 2, wire.RequestID{1}, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1})
		}},
		{name: "self sender", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 1, wire.RequestID{1}, task5Handshake(1, transport.voters.Fingerprint()))
		}},
		{name: "nonvoter", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, transportFrame(t, transport, 4, wire.RequestID{1}, task5Handshake(4, transport.voters.Fingerprint()))
		}},
		{name: "fingerprint mismatch", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			fingerprint := transport.voters.Fingerprint()
			fingerprint[0]++
			return nil, transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, fingerprint))
		}},
		{name: "unknown type", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageType(999)), Payload: []byte{0, 1}}
		}},
		{name: "malformed binary", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			return nil, wire.Frame{Header: transportHeader(transport, 2, wire.RequestID{1}, wire.MessageRaftHandshake), Payload: []byte{0}}
		}},
		{name: "future timestamp", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, transport.voters.Fingerprint()))
			frame.Header.TimestampMillis = transport.clock.Now().Add(TransportFutureSkew + time.Millisecond).UnixMilli()
			return nil, frame
		}},
		{name: "expired timestamp", build: func(transport *TCPTransport) (*wire.TCPFrameStream, wire.Frame) {
			frame := transportFrame(t, transport, 2, wire.RequestID{1}, task5Handshake(2, transport.voters.Fingerprint()))
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

func TestTCPOutboundHandshakeRejectsEveryUncorrelatedAck(t *testing.T) {
	for _, test := range []struct {
		name       string
		respond    bool
		seedReplay bool
		build      func(*TCPTransport, wire.Frame) wire.Frame
	}{
		{name: "wrong sender", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			return transportFrame(t, transport, 3, handshake.Header.RequestID, task5HandshakeAck(3, transport.voters.Fingerprint()))
		}},
		{name: "wrong cluster header", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			frame := transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
			frame.Header.ClusterID[0]++
			return frame
		}},
		{name: "wrong payload responder", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			return transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(3, transport.voters.Fingerprint()))
		}},
		{name: "wrong fingerprint", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			fingerprint := transport.voters.Fingerprint()
			fingerprint[0]++
			return transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, fingerprint))
		}},
		{name: "wrong application fingerprint", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			fingerprint := task5ApplicationFingerprint
			fingerprint[0]++
			return transportFrame(t, transport, 2, handshake.Header.RequestID, HandshakeAck{ResponderID: 2, VoterFingerprint: transport.voters.Fingerprint(), ApplicationFingerprint: fingerprint})
		}},
		{name: "wrong request ID", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			requestID := handshake.Header.RequestID
			requestID[0]++
			return transportFrame(t, transport, 2, requestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
		}},
		{name: "wrong message", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			return transportFrame(t, transport, 2, handshake.Header.RequestID, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1})
		}},
		{name: "wrong codec", respond: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			frame := transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
			frame.Header.Codec = wire.CodecGob
			return frame
		}},
		{name: "replayed ack", respond: true, seedReplay: true, build: func(transport *TCPTransport, handshake wire.Frame) wire.Frame {
			return transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
		}},
		{name: "ack timeout", respond: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverDone := make(chan error, 1)
			var transport *TCPTransport
			transport = newTask10Transport(t, task10TransportOptions{
				requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{{1}}},
				dial: func(context.Context, string, string) (net.Conn, error) {
					client, server := net.Pipe()
					go func() {
						defer server.Close()
						stream := wire.NewTCPFrameStream(server, transport.authenticator, transport.limits, time.Second)
						handshake, err := readFrameOrError(stream)
						if err != nil {
							serverDone <- err
							return
						}
						if !test.respond {
							_, err = readFrameOrError(stream)
							serverDone <- err
							return
						}
						if test.seedReplay {
							err = transport.replayGuards[2].Commit(2, handshake.Header.RequestID, transport.clock.Now())
						}
						if err == nil {
							err = writeFrameForTest(stream, test.build(transport, handshake))
						}
						serverDone <- err
					}()
					return client, nil
				},
			})
			voter, _ := transport.voters.Voter(2)
			stream, _, err := transport.dialPeer(context.Background(), voter)
			if stream != nil {
				_ = stream.Close()
			}
			if err == nil {
				t.Fatal("uncorrelated handshake acknowledgement was accepted")
			}
			select {
			case <-serverDone:
			case <-time.After(time.Second):
				t.Fatal("outbound peer did not exit")
			}
		})
	}
}

func TestTCPDialAndHandshakeShareOneAggregateRPCTimeout(t *testing.T) {
	const rpcTimeout = 120 * time.Millisecond
	const phaseDelay = 80 * time.Millisecond
	serverDone := make(chan error, 1)
	var transport *TCPTransport
	transport = newTask10Transport(t, task10TransportOptions{
		requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{{1}}},
		rpcTimeout: rpcTimeout,
		dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			timer := time.NewTimer(phaseDelay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			client, server := net.Pipe()
			go func() {
				defer server.Close()
				stream := wire.NewTCPFrameStream(server, transport.authenticator, transport.limits, time.Second)
				handshake, err := readFrameOrError(stream)
				if err == nil {
					time.Sleep(phaseDelay)
					ack := transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
					err = writeFrameForTest(stream, ack)
				}
				serverDone <- err
			}()
			return client, nil
		},
	})
	voter, _ := transport.voters.Voter(2)
	started := time.Now()
	stream, _, err := transport.dialPeer(context.Background(), voter)
	if stream != nil {
		_ = stream.Close()
	}
	if err == nil {
		t.Fatal("dial plus handshake exceeded aggregate RPCTimeout without failing")
	}
	if elapsed := time.Since(started); elapsed >= 2*phaseDelay {
		t.Fatalf("aggregate timeout elapsed %s, want before two %s phases", elapsed, phaseDelay)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("delayed peer did not exit")
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
	ack := transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
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
		&fixedTask10RequestIDs{},
	} {
		transport := newTask10Transport(t, task10TransportOptions{requestIDs: ids})
		first, _, err := task10AllocateRequestID(context.Background(), transport, 2)
		if first == (wire.RequestID{}) && !errors.Is(err, ErrRequestIDExhausted) {
			t.Fatalf("zero request ID error = %v", err)
		}
		if err == nil {
			if _, _, err = task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
				t.Fatalf("reused request ID error = %v", err)
			}
		}
	}
}

func TestTCPReplayRetentionRejectsDurationOverflowAtConstruction(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	maxReplayWindow := maxDuration - TransportFutureSkew
	base := TCPTransportOptions{
		LocalID:                1,
		Voters:                 task10Voters(t),
		ClusterID:              [16]byte{1},
		ApplicationFingerprint: task5ApplicationFingerprint,
		Authenticator:          wire.NewHMACAuthenticator([]byte("01234567890123456789012345678901")),
		Clock:                  &task10PanicClock{},
		ReplayWindow:           maxReplayWindow + time.Nanosecond,
		RPCTimeout:             time.Second,
	}
	if _, err := NewTCPTransport(base); !errors.Is(err, ErrTransportInvariant) {
		t.Fatalf("overflowing replay retention constructor error = %v, want ErrTransportInvariant", err)
	}

	manualClock := clock.NewManual(time.Unix(2000, 0))
	id := wire.RequestID{0x44}
	base.Clock = manualClock
	base.ReplayWindow = maxReplayWindow
	base.RequestIDs = &fixedTask10RequestIDs{ids: []wire.RequestID{id, id}}
	transport, err := NewTCPTransport(base)
	if err != nil {
		t.Fatalf("max-safe replay window rejected: %v", err)
	}
	if transport.replayRetention != maxDuration {
		t.Fatalf("stored replay retention = %s, want %s", transport.replayRetention, maxDuration)
	}
	first, timestamp, err := task10AllocateRequestID(context.Background(), transport, 2)
	if err != nil || first != id {
		t.Fatalf("max-safe first allocation = (%x, %v), want %x", first, err, id)
	}
	expiresAt := task10TrackedRequestExpiry(transport, 2, id)
	if !expiresAt.After(timestamp) || expiresAt.Sub(timestamp) != maxDuration {
		t.Fatalf("max-safe retention = timestamp %s expiry %s duration %s, want positive %s", timestamp, expiresAt, expiresAt.Sub(timestamp), maxDuration)
	}
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatalf("immediate near-max reuse error = %v, want ErrRequestIDExhausted", err)
	}
}

func TestTCPOutgoingRequestIDsAreReplayWindowBounded(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(2000, 0))
	ids := make([]wire.RequestID, 0, TransportReplayEntries)
	for index := 1; index <= TransportReplayEntries; index++ {
		ids = append(ids, wire.RequestID{byte(index >> 8), byte(index)})
	}
	transport := newTask10Transport(t, task10TransportOptions{requestIDs: &fixedTask10RequestIDs{ids: ids}, clock: manualClock})
	for index := 0; index < TransportReplayEntries; index++ {
		if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
			t.Fatalf("request ID %d/%d: %v", index+1, TransportReplayEntries, err)
		}
	}
	if got := task10TrackedRequestCount(transport, 2); got != TransportReplayEntries {
		t.Fatalf("tracked request IDs = %d, want fixed cap %d", got, TransportReplayEntries)
	}
}

func TestTCPOutgoingRequestIDReuseExpiresWithReplayWindow(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(3000, 0))
	id := wire.RequestID{9}
	transport := newTask10Transport(t, task10TransportOptions{
		requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{id, id, id}},
		clock:      manualClock,
	})
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatalf("within-window reuse error = %v, want ErrRequestIDExhausted", err)
	}
	manualClock.Advance(transport.replayWindow + TransportFutureSkew)
	if got, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil || got != id {
		t.Fatalf("expired reuse = (%x, %v), want accepted %x", got, err, id)
	}
}

func TestTCPOutgoingRequestIDTrackingStaysBoundedAcrossLongHeartbeatRun(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(4000, 0))
	transport := newTask10Transport(t, task10TransportOptions{requestIDs: &task10RequestIDs{}, clock: manualClock})
	for window := 0; window < 4; window++ {
		for index := 0; index < TransportReplayEntries; index++ {
			if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
				t.Fatalf("window %d request %d: %v", window, index, err)
			}
		}
		if got := task10TrackedRequestCount(transport, 2); got > TransportReplayEntries {
			t.Fatalf("window %d tracked %d request IDs, cap %d", window, got, TransportReplayEntries)
		}
		manualClock.Advance(transport.replayWindow + TransportFutureSkew)
	}
}

func TestTCPRequestIDRetentionUsesExactFrameTimestamp(t *testing.T) {
	advancingClock := &task10AdvancingClock{now: time.Unix(5000, 750*int64(time.Microsecond)), step: time.Millisecond}
	handshakeReceived := make(chan wire.Frame, 1)
	releaseServer := make(chan struct{})
	var transport *TCPTransport
	transport = newTask10Transport(t, task10TransportOptions{
		clock: advancingClock, requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{{1}}},
		dial: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer server.Close()
				stream := wire.NewTCPFrameStream(server, transport.authenticator, transport.limits, time.Second)
				handshake, err := readFrameOrError(stream)
				if err != nil {
					return
				}
				handshakeReceived <- handshake
				ack := transportFrame(t, transport, 2, handshake.Header.RequestID, task5HandshakeAck(2, transport.voters.Fingerprint()))
				_ = writeFrameForTest(stream, ack)
				<-releaseServer
			}()
			return client, nil
		},
	})
	voter, _ := transport.voters.Voter(2)
	stream, _, err := transport.dialPeer(context.Background(), voter)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stream.Close()
		close(releaseServer)
	}()
	handshake := <-handshakeReceived
	frameTime := time.UnixMilli(handshake.Header.TimestampMillis)
	wantExpiry := frameTime.Add(transport.replayWindow + TransportFutureSkew)
	if got := task10TrackedRequestExpiry(transport, 2, handshake.Header.RequestID); !got.Equal(wantExpiry) {
		t.Fatalf("tracked expiry = %s, want frame timestamp retention %s", got, wantExpiry)
	}
}

func TestTCPRequestIDReuseWaitsForReplayWindowAndReceiverSkew(t *testing.T) {
	senderClock := clock.NewManual(time.Unix(6000, 0))
	receiverClock := clock.NewManual(senderClock.Now().Add(-TransportFutureSkew))
	id := wire.RequestID{7}
	transport := newTask10Transport(t, task10TransportOptions{
		clock: senderClock, requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{id, id, id, id}},
	})
	receiver := wire.NewReplayGuard(receiverClock, transport.replayWindow, TransportFutureSkew, TransportReplayEntries)
	first, firstTime, err := task10AllocateRequestID(context.Background(), transport, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Commit(1, first, firstTime); err != nil {
		t.Fatalf("receiver rejected first ID: %v", err)
	}
	senderClock.Advance(transport.replayWindow)
	receiverClock.Advance(transport.replayWindow)
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatalf("reuse before future-skew retention error = %v, want ErrRequestIDExhausted", err)
	}
	senderClock.Advance(TransportFutureSkew - time.Millisecond)
	receiverClock.Advance(TransportFutureSkew - time.Millisecond)
	if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); !errors.Is(err, ErrRequestIDExhausted) {
		t.Fatalf("reuse one millisecond before safe boundary error = %v, want ErrRequestIDExhausted", err)
	}
	senderClock.Advance(time.Millisecond)
	receiverClock.Advance(time.Millisecond)
	reused, reusedTime, err := task10AllocateRequestID(context.Background(), transport, 2)
	if err != nil || reused != id {
		t.Fatalf("safe post-expiry reuse = (%x, %v), want %x", reused, err, id)
	}
	if err := receiver.Commit(1, reused, reusedTime); err != nil {
		t.Fatalf("receiver 30s behind rejected safe reuse: %v", err)
	}
}

func TestTCPRequestIDTrackingIsBoundedPerDestinationPeer(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(7000, 0))
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock, requestIDs: &task10RequestIDs{}})
	const perPeer = 5000
	for index := 0; index < perPeer; index++ {
		for _, peerID := range []uint16{2, 3} {
			if _, _, err := task10AllocateRequestID(context.Background(), transport, peerID); err != nil {
				t.Fatalf("peer %d allocation %d: %v", peerID, index, err)
			}
		}
	}
	for _, peerID := range []uint16{2, 3} {
		if got := task10TrackedRequestCount(transport, peerID); got != perPeer {
			t.Fatalf("peer %d tracked IDs = %d, want %d", peerID, got, perPeer)
		}
	}
}

func TestTCPRequestIDMayRepeatAcrossIndependentDestinationPeers(t *testing.T) {
	id := wire.RequestID{33}
	transport := newTask10Transport(t, task10TransportOptions{requestIDs: &fixedTask10RequestIDs{ids: []wire.RequestID{id, id}}})
	for _, peerID := range []uint16{2, 3} {
		got, _, err := task10AllocateRequestID(context.Background(), transport, peerID)
		if err != nil || got != id {
			t.Fatalf("peer %d allocation = (%x, %v), want safe shared ID %x", peerID, got, err, id)
		}
	}
}

func TestTCPRequestIDCapacityWaitsAndRecoversAfterSafeExpiry(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(8000, 0))
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock, requestIDs: &task10RequestIDs{}})
	for index := 0; index < TransportReplayEntries; index++ {
		if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
			t.Fatalf("fill allocation %d: %v", index, err)
		}
	}
	result := make(chan error, 1)
	go func() {
		_, _, err := task10AllocateRequestID(context.Background(), transport, 2)
		result <- err
	}()
	requireTask10RequestCapacityWaiting(t, manualClock, result)
	manualClock.Advance(transport.replayWindow + TransportFutureSkew)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("post-expiry allocation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("capacity waiter did not resume after safe expiry")
	}
	if got := task10TrackedRequestCount(transport, 2); got != 1 {
		t.Fatalf("post-expiry tracked IDs = %d, want 1", got)
	}
	if got := task10TrackedExpiryCount(transport, 2); got != 1 {
		t.Fatalf("post-expiry heap entries = %d, want 1", got)
	}
}

func TestTCPRequestIDCapacityWaitIsCancellationAware(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(9000, 0))
	source := &task10RequestIDs{}
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock, requestIDs: source})
	for index := 0; index < TransportReplayEntries; index++ {
		if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, err := task10AllocateRequestID(ctx, transport, 2)
		result <- err
	}()
	requireTask10RequestCapacityWaiting(t, manualClock, result)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("capacity cancellation error = %v, want context.Canceled", err)
	}
	source.mu.Lock()
	calls := len(source.calls)
	source.mu.Unlock()
	if calls != TransportReplayEntries {
		t.Fatalf("ID source calls = %d, want no allocation while capacity-blocked", calls)
	}
}

func TestTCPWorkerCapacityWaitDoesNotFatallyStopTransport(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(9500, 0))
	source := &task10RequestIDs{}
	transport := newTask10Transport(t, task10TransportOptions{
		clock: manualClock, requestIDs: source,
		dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				<-ctx.Done()
				_ = server.Close()
			}()
			return client, nil
		},
	})
	for index := 0; index < TransportReplayEntries; index++ {
		if _, _, err := task10AllocateRequestID(context.Background(), transport, 2); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx, newBlockingListener(), task10Ingress{}) }()
	awaitClosed(t, transport.Ready())
	if got, err := transport.Handoff(PeerMessage{To: 2, RPC: RequestVoteRequest{CandidateID: 1, Term: 1}}); err != nil || got != TransportAccepted {
		t.Fatalf("capacity Handoff = (%d, %v)", got, err)
	}
	for attempt := 0; attempt < 1_000_000 && manualClock.PendingTimers() == 0; attempt++ {
		runtime.Gosched()
	}
	if manualClock.PendingTimers() == 0 {
		t.Fatal("worker did not wait on safe request-ID expiry")
	}
	select {
	case err := <-done:
		t.Fatalf("capacity wait fatally stopped transport: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("capacity-wait cancellation: %v", err)
	}
}

func TestTCPRequestIDTrackingSustainsTwentyMillisecondHeartbeats(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(10000, 0))
	transport := newTask10Transport(t, task10TransportOptions{clock: manualClock, requestIDs: &task10RequestIDs{}})
	for heartbeat := 0; heartbeat < 9000; heartbeat++ {
		for _, peerID := range []uint16{2, 3} {
			if _, _, err := task10AllocateRequestID(context.Background(), transport, peerID); err != nil {
				t.Fatalf("heartbeat %d peer %d: %v", heartbeat, peerID, err)
			}
		}
		manualClock.Advance(20 * time.Millisecond)
	}
	for _, peerID := range []uint16{2, 3} {
		if got := task10TrackedRequestCount(transport, peerID); got > TransportReplayEntries {
			t.Fatalf("peer %d map grew to %d", peerID, got)
		}
		if got := task10TrackedExpiryCount(transport, peerID); got > TransportReplayEntries {
			t.Fatalf("peer %d heap grew to %d", peerID, got)
		}
	}
}

func task10AllocateRequestID(ctx context.Context, transport *TCPTransport, peerID uint16) (wire.RequestID, time.Time, error) {
	return transport.nextRequestIDForPeer(ctx, peerID)
}

func task10TrackedRequestExpiry(transport *TCPTransport, peerID uint16, id wire.RequestID) time.Time {
	transport.requestMu.Lock()
	defer transport.requestMu.Unlock()
	return transport.requestTrackers[peerID].issued[id]
}

func task10TrackedRequestCount(transport *TCPTransport, peerID uint16) int {
	transport.requestMu.Lock()
	defer transport.requestMu.Unlock()
	return len(transport.requestTrackers[peerID].issued)
}

func task10TrackedExpiryCount(transport *TCPTransport, peerID uint16) int {
	transport.requestMu.Lock()
	defer transport.requestMu.Unlock()
	return transport.requestTrackers[peerID].expires.Len()
}

func requireTask10RequestCapacityWaiting(t *testing.T, manualClock *clock.Manual, result <-chan error) {
	t.Helper()
	for attempt := 0; attempt < 1_000_000; attempt++ {
		select {
		case err := <-result:
			t.Fatalf("capacity allocation returned before release: %v", err)
		default:
		}
		if manualClock.PendingTimers() != 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("capacity allocation neither returned nor registered its clock wait")
}

type task10AdvancingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

type task10PanicClock struct{}

func (*task10PanicClock) Now() time.Time { panic("overflowing constructor read clock") }
func (*task10PanicClock) NewTimer(time.Duration) clock.Timer {
	panic("overflowing constructor started timer")
}

func (source *task10AdvancingClock) Now() time.Time {
	source.mu.Lock()
	defer source.mu.Unlock()
	now := source.now
	source.now = source.now.Add(source.step)
	return now
}

func (*task10AdvancingClock) NewTimer(duration time.Duration) clock.Timer {
	return clock.NewReal().NewTimer(duration)
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
