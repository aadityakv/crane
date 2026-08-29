package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestTCPFrameRoundTripAndSequentialFrames(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	auth := NewHMACAuthenticator(testKey)
	limits := DefaultLimits()
	frames := []Frame{
		{Header: testHeader(), Payload: []byte("first")},
		{Header: testHeader(), Payload: []byte("second")},
	}
	frames[1].Header.RequestID = RequestID{2}

	writeErrors := make(chan error, 1)
	go func() {
		for _, frame := range frames {
			if err := WriteTCPFrame(context.Background(), client, frame, auth, limits, time.Second); err != nil {
				writeErrors <- err
				return
			}
		}
		writeErrors <- nil
	}()

	for index, want := range frames {
		got, err := ReadTCPFrame(context.Background(), server, auth, limits, time.Second)
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
