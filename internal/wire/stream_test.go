package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestTCPFrameRoundTripAndSequentialFrames(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	auth := NewHMACAuthenticator(testKey)
	limits := DefaultLimits()
	clientStream := NewTCPFrameStream(client, auth, limits, time.Second)
	serverStream := NewTCPFrameStream(server, auth, limits, time.Second)
	frames := []Frame{
		{Header: testHeader(), Payload: []byte("first")},
		{Header: testHeader(), Payload: []byte("second")},
	}
	frames[1].Header.RequestID = RequestID{2}

	writeErrors := make(chan error, 1)
	go func() {
		for _, frame := range frames {
			if err := clientStream.WriteFrame(context.Background(), frame); err != nil {
				writeErrors <- err
				return
			}
		}
		writeErrors <- nil
	}()

	for index, want := range frames {
		got, err := serverStream.ReadFrame(context.Background())
		if err != nil {
			t.Fatalf("read frame %d: %v", index, err)
		}
		if got.Header != want.Header || string(got.Payload) != string(want.Payload) {
			t.Fatalf("frame %d = %#v, want %#v", index, got, want)
		}
	}
	if err := <-writeErrors; err != nil {
		t.Fatalf("write frames: %v", err)
	}
}

func TestTCPReadFrameRejectsTruncatedPrefixes(t *testing.T) {
	for _, size := range []int{0, 1, 3} {
		t.Run(string(rune('0'+size))+"_bytes", func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			go func() {
				if size > 0 {
					_, _ = client.Write(make([]byte, size))
				}
				_ = client.Close()
			}()

			_, err := ReadTCPFrame(context.Background(), server, NewHMACAuthenticator(testKey), DefaultLimits(), 0)
			want := io.EOF
			if size > 0 {
				want = io.ErrUnexpectedEOF
			}
			if !errors.Is(err, want) {
				t.Fatalf("ReadTCPFrame error = %v, want %v", err, want)
			}
		})
	}
}

func TestTCPReadFrameRejectsDeclaredBodyAboveLimit(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	limits := DefaultLimits()
	limits.MaxFrameSize = FixedHeaderSize + MACSize
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(limits.MaxFrameSize+1))
	go func() {
		_, _ = client.Write(prefix)
		_ = client.Close()
	}()

	if _, err := ReadTCPFrame(context.Background(), server, NewHMACAuthenticator(testKey), limits, time.Second); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized declared body error = %v", err)
	}
}

func TestTCPReadFrameRejectsOversizedCraneTupleAfterOnlyPrefixAndFixedHeader(t *testing.T) {
	const bodyLength = 1201
	header := testHeader()
	header.Message = MessageCraneTupleDelivery
	header.Codec = CodecBinary
	fixedHeader := make([]byte, FixedHeaderSize)
	encodeHeader(fixedHeader, header, bodyLength-FixedHeaderSize-MACSize)
	prefix := make([]byte, tcpLengthPrefixSize)
	binary.BigEndian.PutUint32(prefix, bodyLength)
	conn := newReadOnlyConn(append(prefix, fixedHeader...))

	if _, err := ReadTCPFrame(context.Background(), conn, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized Crane TCP frame error = %v, want ErrTooLarge", err)
	}
	if conn.maximumReadRequest > FixedHeaderSize {
		t.Fatalf("largest body read request = %d bytes, want at most fixed header size %d", conn.maximumReadRequest, FixedHeaderSize)
	}
	if conn.Len() != 0 {
		t.Fatalf("unread prefix/header bytes = %d, want 0", conn.Len())
	}
}

func TestTCPReadFrameAcceptsExactCraneTupleFrameLimit(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	header := testHeader()
	header.Message = MessageCraneTupleDeliveryAck
	header.Codec = CodecBinary
	payload := make([]byte, 1200-FixedHeaderSize-MACSize)
	body, err := Encode(header, payload, auth, DefaultLimits())
	if err != nil {
		t.Fatalf("Encode exact Crane TCP frame: %v", err)
	}
	input := append(tcpPrefix(body), body...)
	frame, err := ReadTCPFrame(context.Background(), newReadOnlyConn(input), auth, DefaultLimits(), time.Second)
	if err != nil {
		t.Fatalf("ReadTCPFrame exact Crane frame: %v", err)
	}
	if frame.Header != header || !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("decoded exact Crane frame = %#v", frame)
	}
}

func TestTCPWriteFrameRejectsConfiguredCraneLimitAboveCompiledMaximumBeforeWriting(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxCraneDatagramSize = 1201
	header := testHeader()
	header.Message = MessageCraneTupleDeliveryNack
	header.Codec = CodecBinary
	conn := &countingWriteConn{}
	frame := Frame{Header: header, Payload: make([]byte, 1201-FixedHeaderSize-MACSize)}

	if err := WriteTCPFrame(context.Background(), conn, frame, NewHMACAuthenticator(testKey), limits, time.Second); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("WriteTCPFrame with 1201-byte Crane limit error = %v, want ErrTooLarge", err)
	}
	if conn.writeCalls != 0 {
		t.Fatalf("WriteTCPFrame performed %d writes before rejecting invalid Crane limit", conn.writeCalls)
	}
}

func TestTCPReadFrameRejectsBodyShorterThanFixedHeaderAndMAC(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	prefix := make([]byte, tcpLengthPrefixSize)
	binary.BigEndian.PutUint32(prefix, FixedHeaderSize+MACSize-1)
	written := make(chan error, 1)
	go func() {
		_, err := client.Write(prefix)
		written <- err
	}()

	if _, err := ReadTCPFrame(context.Background(), server, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, ErrMalformed) {
		t.Fatalf("short declared body error = %v, want ErrMalformed", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("write prefix: %v", err)
	}
}

func TestTCPReadFrameRejectsTruncatedFixedHeader(t *testing.T) {
	prefix := make([]byte, tcpLengthPrefixSize)
	binary.BigEndian.PutUint32(prefix, FixedHeaderSize+MACSize)
	input := append(prefix, make([]byte, FixedHeaderSize-1)...)
	conn := newReadOnlyConn(input)

	if _, err := ReadTCPFrame(context.Background(), conn, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated fixed header error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestTCPReadFrameValidatesHeaderBeforeDeclaredBodyAllocation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{
			name: "malformed_magic",
			mutate: func(header []byte) {
				header[magicOffset] ^= 0xff
			},
			want: ErrMalformed,
		},
		{
			name: "unsupported_version",
			mutate: func(header []byte) {
				binary.BigEndian.PutUint16(header[versionOffset:messageOffset], Version1+1)
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unsupported_codec",
			mutate: func(header []byte) {
				header[codecOffset] = 0xff
			},
			want: ErrUnsupportedCodec,
		},
		{
			name: "wrong_cluster",
			mutate: func(header []byte) {
				header[clusterIDOffset] ^= 0xff
			},
			want: ErrAuthentication,
		},
		{
			name: "embedded_length_disagrees_with_outer_length",
			mutate: func(header []byte) {
				binary.BigEndian.PutUint32(header[payloadLengthOffset:FixedHeaderSize], 0)
			},
			want: ErrMalformed,
		},
	}

	const declaredBodyLength = defaultMaxFrameSize
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := make([]byte, FixedHeaderSize)
			encodeHeader(header, testHeader(), uint32(declaredBodyLength-FixedHeaderSize-MACSize))
			tt.mutate(header)
			prefix := make([]byte, tcpLengthPrefixSize)
			binary.BigEndian.PutUint32(prefix, declaredBodyLength)
			conn := newReadOnlyConn(append(prefix, header...))
			limits := DefaultLimits()
			expectedClusterID := testHeader().ClusterID
			limits.ExpectedClusterID = &expectedClusterID

			if _, err := ReadTCPFrame(context.Background(), conn, NewHMACAuthenticator(testKey), limits, time.Second); !errors.Is(err, tt.want) {
				t.Fatalf("ReadTCPFrame error = %v, want %v", err, tt.want)
			}
			if conn.maximumReadRequest > FixedHeaderSize {
				t.Fatalf("largest body read request = %d bytes, want at most fixed header size %d", conn.maximumReadRequest, FixedHeaderSize)
			}
		})
	}
}

func TestTCPReadFrameRejectsBadMACAfterBoundedRead(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	header := testHeader()
	header.Message = MessageRaftAppendEntriesRequest
	header.Codec = CodecBinary
	body, err := Encode(header, []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)-1] ^= 0xff
	input := append(tcpPrefix(body), body...)

	if _, err := ReadTCPFrame(context.Background(), newReadOnlyConn(input), auth, DefaultLimits(), time.Second); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("bad MAC error = %v, want ErrAuthentication", err)
	}
}

func TestTCPReadFramePreservesTruncatedBodyError(t *testing.T) {
	auth := NewHMACAuthenticator(testKey)
	body, err := Encode(testHeader(), []byte("payload"), auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(len(body)))
	client, server := net.Pipe()
	defer server.Close()
	go func() {
		_, _ = client.Write(prefix)
		_, _ = client.Write(body[:len(body)-1])
		_ = client.Close()
	}()

	if _, err := ReadTCPFrame(context.Background(), server, auth, DefaultLimits(), time.Second); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body error = %v", err)
	}
}

func TestTCPReadFrameHonorsContextDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	if _, err := ReadTCPFrame(ctx, server, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context deadline error = %v", err)
	}
}

func TestTCPReadFrameHonorsContextCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := ReadTCPFrame(ctx, server, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation error = %v, want context.Canceled", err)
	}
}

func TestTCPReadFrameHonorsConfiguredIOTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_, err := ReadTCPFrame(context.Background(), server, NewHMACAuthenticator(testKey), DefaultLimits(), 25*time.Millisecond)
	var netError net.Error
	if !errors.As(err, &netError) || !netError.Timeout() {
		t.Fatalf("configured timeout error = %v", err)
	}
}

func TestTCPWriteFrameHandlesShortWrites(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	auth := NewHMACAuthenticator(testKey)
	want := Frame{Header: testHeader(), Payload: []byte("partial-write")}

	writeErrors := make(chan error, 1)
	go func() {
		writeErrors <- WriteTCPFrame(context.Background(), &limitedWriteConn{Conn: client, maximum: 2}, want, auth, DefaultLimits(), time.Second)
	}()
	got, err := ReadTCPFrame(context.Background(), server, auth, DefaultLimits(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header != want.Header || string(got.Payload) != string(want.Payload) {
		t.Fatalf("decoded frame = %#v, want %#v", got, want)
	}
	if err := <-writeErrors; err != nil {
		t.Fatalf("short-write loop error = %v", err)
	}
}

func TestTCPWriteFramePreservesConnectionError(t *testing.T) {
	client, server := net.Pipe()
	_ = server.Close()
	defer client.Close()
	frame := Frame{Header: testHeader(), Payload: []byte("payload")}

	if err := WriteTCPFrame(context.Background(), client, frame, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed connection error = %v", err)
	}
}

func TestTCPWriteFrameReturnsDeadlineClearError(t *testing.T) {
	clearError := errors.New("clear deadline")
	conn := &deadlineFailureConn{clearError: clearError}
	frame := Frame{Header: testHeader(), Payload: []byte("payload")}

	if err := WriteTCPFrame(context.Background(), conn, frame, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second); !errors.Is(err, clearError) {
		t.Fatalf("deadline-clear error = %v", err)
	}
}

func TestTCPWriteFrameJoinsPrimaryAndDeadlineClearErrors(t *testing.T) {
	writeError := errors.New("write failed")
	clearError := errors.New("clear deadline")
	conn := &deadlineFailureConn{writeError: writeError, clearError: clearError}
	frame := Frame{Header: testHeader(), Payload: []byte("payload")}

	err := WriteTCPFrame(context.Background(), conn, frame, NewHMACAuthenticator(testKey), DefaultLimits(), time.Second)
	if !errors.Is(err, writeError) || !errors.Is(err, clearError) {
		t.Fatalf("write and deadline-clear error = %v", err)
	}
}

func TestTCPFrameStreamSerializesWritesAndDeadlines(t *testing.T) {
	conn := newSerializedWriteConn()
	released := false
	t.Cleanup(func() {
		if !released {
			close(conn.releaseFirstWrite)
		}
	})
	auth := NewHMACAuthenticator(testKey)
	stream := NewTCPFrameStream(conn, auth, DefaultLimits(), time.Second)
	first := Frame{Header: testHeader(), Payload: []byte("first")}
	second := Frame{Header: testHeader(), Payload: []byte("second")}
	second.Header.RequestID = RequestID{2}

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- stream.WriteFrame(context.Background(), first)
	}()
	<-conn.firstWriteEntered
	if stream.writeMu.TryLock() {
		stream.writeMu.Unlock()
		t.Fatal("stream did not hold its write lock across connection I/O")
	}
	if !stream.readMu.TryLock() {
		t.Fatal("blocked write also blocked the independent read direction")
	}
	stream.readMu.Unlock()

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondResult <- stream.WriteFrame(context.Background(), second)
	}()
	<-secondStarted
	close(conn.releaseFirstWrite)
	released = true
	if err := <-firstResult; err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second write: %v", err)
	}

	firstBody, err := Encode(first.Header, first.Payload, auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	secondBody, err := Encode(second.Header, second.Payload, auth, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	wantWrites := [][]byte{tcpPrefix(firstBody), firstBody, tcpPrefix(secondBody), secondBody}
	events := conn.snapshotEvents()
	if len(events) != 8 {
		t.Fatalf("connection events = %#v", events)
	}
	wantKinds := []string{"set", "write", "write", "clear", "set", "write", "write", "clear"}
	writeIndex := 0
	for index, event := range events {
		if event.kind != wantKinds[index] {
			t.Fatalf("event %d = %q, want %q; all events = %#v", index, event.kind, wantKinds[index], events)
		}
		if event.kind == "write" {
			if !bytes.Equal(event.payload, wantWrites[writeIndex]) {
				t.Fatalf("write %d = %x, want %x", writeIndex, event.payload, wantWrites[writeIndex])
			}
			writeIndex++
		}
	}
}

type limitedWriteConn struct {
	net.Conn
	maximum int
}

type readOnlyConn struct {
	*bytes.Reader
	maximumReadRequest int
}

func newReadOnlyConn(payload []byte) *readOnlyConn {
	return &readOnlyConn{Reader: bytes.NewReader(payload)}
}

func (c *readOnlyConn) Read(payload []byte) (int, error) {
	if len(payload) > c.maximumReadRequest {
		c.maximumReadRequest = len(payload)
	}
	return c.Reader.Read(payload)
}

func (*readOnlyConn) Write([]byte) (int, error)       { return 0, io.ErrClosedPipe }
func (*readOnlyConn) Close() error                    { return nil }
func (*readOnlyConn) LocalAddr() net.Addr             { return nil }
func (*readOnlyConn) RemoteAddr() net.Addr            { return nil }
func (*readOnlyConn) SetDeadline(time.Time) error     { return nil }
func (*readOnlyConn) SetReadDeadline(time.Time) error { return nil }
func (*readOnlyConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *limitedWriteConn) Write(payload []byte) (int, error) {
	if len(payload) > c.maximum {
		payload = payload[:c.maximum]
	}
	return c.Conn.Write(payload)
}

type deadlineFailureConn struct {
	writeError error
	clearError error
}

type countingWriteConn struct {
	writeCalls int
}

func (*countingWriteConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *countingWriteConn) Write(payload []byte) (int, error) {
	c.writeCalls++
	return len(payload), nil
}
func (*countingWriteConn) Close() error                     { return nil }
func (*countingWriteConn) LocalAddr() net.Addr              { return nil }
func (*countingWriteConn) RemoteAddr() net.Addr             { return nil }
func (*countingWriteConn) SetDeadline(time.Time) error      { return nil }
func (*countingWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (*countingWriteConn) SetWriteDeadline(time.Time) error { return nil }

func (*deadlineFailureConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *deadlineFailureConn) Write(payload []byte) (int, error) {
	if c.writeError != nil {
		return 0, c.writeError
	}
	return len(payload), nil
}

func (*deadlineFailureConn) Close() error                    { return nil }
func (*deadlineFailureConn) LocalAddr() net.Addr             { return nil }
func (*deadlineFailureConn) RemoteAddr() net.Addr            { return nil }
func (*deadlineFailureConn) SetDeadline(time.Time) error     { return nil }
func (*deadlineFailureConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineFailureConn) SetWriteDeadline(value time.Time) error {
	if value.IsZero() {
		return c.clearError
	}
	return nil
}

type serializedWriteConn struct {
	mu                sync.Mutex
	events            []serializedConnEvent
	writeCalls        int
	firstWriteEntered chan struct{}
	releaseFirstWrite chan struct{}
}

type serializedConnEvent struct {
	kind    string
	payload []byte
}

func newSerializedWriteConn() *serializedWriteConn {
	return &serializedWriteConn{
		firstWriteEntered: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
}

func (*serializedWriteConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *serializedWriteConn) Write(payload []byte) (int, error) {
	c.mu.Lock()
	c.writeCalls++
	call := c.writeCalls
	c.events = append(c.events, serializedConnEvent{kind: "write", payload: append([]byte(nil), payload...)})
	if call == 1 {
		close(c.firstWriteEntered)
	}
	c.mu.Unlock()
	if call == 1 {
		<-c.releaseFirstWrite
	}
	return len(payload), nil
}

func (*serializedWriteConn) Close() error                { return nil }
func (*serializedWriteConn) LocalAddr() net.Addr         { return nil }
func (*serializedWriteConn) RemoteAddr() net.Addr        { return nil }
func (*serializedWriteConn) SetDeadline(time.Time) error { return nil }
func (*serializedWriteConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *serializedWriteConn) SetWriteDeadline(value time.Time) error {
	kind := "set"
	if value.IsZero() {
		kind = "clear"
	}
	c.mu.Lock()
	c.events = append(c.events, serializedConnEvent{kind: kind})
	c.mu.Unlock()
	return nil
}

func (c *serializedWriteConn) snapshotEvents() []serializedConnEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]serializedConnEvent, len(c.events))
	copy(events, c.events)
	return events
}

func tcpPrefix(body []byte) []byte {
	prefix := make([]byte, tcpLengthPrefixSize)
	binary.BigEndian.PutUint32(prefix, uint32(len(body)))
	return prefix
}
