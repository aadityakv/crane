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
