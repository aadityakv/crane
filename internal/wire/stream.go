package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const tcpLengthPrefixSize = 4

// TCPFrameStream owns one connection and serializes complete framed operations
// independently in each direction. Callers must not perform direct I/O or call
// the free frame helpers on its connection.
type TCPFrameStream struct {
	conn      net.Conn
	auth      Authenticator
	limits    Limits
	ioTimeout time.Duration
	readMu    sync.Mutex
	writeMu   sync.Mutex
}

// NewTCPFrameStream returns the concurrency-safe framed operational path for conn.
func NewTCPFrameStream(conn net.Conn, auth Authenticator, limits Limits, ioTimeout time.Duration) *TCPFrameStream {
	if limits.ExpectedClusterID != nil {
		expectedClusterID := *limits.ExpectedClusterID
		limits.ExpectedClusterID = &expectedClusterID
	}
	return &TCPFrameStream{conn: conn, auth: auth, limits: limits, ioTimeout: ioTimeout}
}

// ReadFrame reads one complete frame while excluding other reads on the connection.
func (s *TCPFrameStream) ReadFrame(ctx context.Context) (Frame, error) {
	if s == nil {
		return Frame{}, errors.New("read TCP frame stream: nil stream")
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	return ReadTCPFrame(ctx, s.conn, s.auth, s.limits, s.ioTimeout)
}

// WriteFrame writes one complete frame while excluding other writes on the connection.
func (s *TCPFrameStream) WriteFrame(ctx context.Context, frame Frame) error {
	if s == nil {
		return errors.New("write TCP frame stream: nil stream")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return WriteTCPFrame(ctx, s.conn, frame, s.auth, s.limits, s.ioTimeout)
}

// Close closes the owned connection.
func (s *TCPFrameStream) Close() error {
	if s == nil || s.conn == nil {
		return errors.New("close TCP frame stream: nil connection")
	}
	return s.conn.Close()
}

// ReadTCPFrame reads one uint32-length-prefixed authenticated frame body. It is
// a single-operation primitive; use TCPFrameStream to serialize repeated or
// concurrent reads and their connection deadlines.
func ReadTCPFrame(ctx context.Context, conn net.Conn, auth Authenticator, limits Limits, ioTimeout time.Duration) (_ Frame, err error) {
	if conn == nil {
		return Frame{}, errors.New("read TCP frame: nil connection")
	}
	resolved, err := resolveLimits(limits)
	if err != nil {
		return Frame{}, err
	}
	clearDeadline, err := applyOperationDeadline(ctx, ioTimeout, conn.SetReadDeadline)
	if err != nil {
		return Frame{}, fmt.Errorf("set TCP read deadline: %w", err)
	}
	defer func() {
		if clearError := clearDeadline(); clearError != nil {
			err = errors.Join(err, fmt.Errorf("clear TCP read deadline: %w", clearError))
		}
	}()

	var prefix [tcpLengthPrefixSize]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return Frame{}, fmt.Errorf("read TCP frame prefix: %w", contextualIOError(ctx, err))
	}
	bodyLength := binary.BigEndian.Uint32(prefix[:])
	minimumBodyLength := uint32(FixedHeaderSize + MACSize)
	if bodyLength < minimumBodyLength {
		return Frame{}, fmt.Errorf("%w: declared TCP body is shorter than fixed header and MAC", ErrMalformed)
	}
	if uint64(bodyLength) > uint64(resolved.MaxFrameSize) {
		return Frame{}, fmt.Errorf("%w: declared TCP body is %d bytes, maximum is %d", ErrTooLarge, bodyLength, resolved.MaxFrameSize)
	}

	var fixedHeader [FixedHeaderSize]byte
	if _, err := io.ReadFull(conn, fixedHeader[:]); err != nil {
		return Frame{}, fmt.Errorf("read TCP frame fixed header: %w", contextualIOError(ctx, err))
	}
	if string(fixedHeader[magicOffset:versionOffset]) != frameMagic {
		return Frame{}, fmt.Errorf("%w: invalid magic", ErrMalformed)
	}
	header := decodeHeader(fixedHeader[:])
	if err := validateHeader(header, resolved); err != nil {
		return Frame{}, err
	}

	payloadLength := binary.BigEndian.Uint32(fixedHeader[payloadLengthOffset:FixedHeaderSize])
	declaredLength := uint64(FixedHeaderSize) + uint64(payloadLength) + uint64(MACSize)
	limit := effectiveLimit(header.Message, resolved)
	if declaredLength > uint64(limit) {
		return Frame{}, fmt.Errorf("%w: declared body is %d bytes, maximum is %d", ErrTooLarge, declaredLength, limit)
	}
	if declaredLength != uint64(bodyLength) {
		return Frame{}, fmt.Errorf("%w: embedded body is %d bytes, outer prefix declares %d", ErrMalformed, declaredLength, bodyLength)
	}

	body := make([]byte, int(bodyLength))
	copy(body[:FixedHeaderSize], fixedHeader[:])
	if _, err := io.ReadFull(conn, body[FixedHeaderSize:]); err != nil {
		return Frame{}, fmt.Errorf("read TCP frame payload and MAC: %w", contextualIOError(ctx, err))
	}
	frame, err := Decode(body, auth, resolved)
	if err != nil {
		return Frame{}, fmt.Errorf("decode TCP frame: %w", err)
	}
	return frame, nil
}

// WriteTCPFrame encodes and completely writes one uint32-length-prefixed frame
// body. It is a single-operation primitive; use TCPFrameStream to serialize
// repeated or concurrent writes and their connection deadlines.
func WriteTCPFrame(ctx context.Context, conn net.Conn, frame Frame, auth Authenticator, limits Limits, ioTimeout time.Duration) (err error) {
	if conn == nil {
		return errors.New("write TCP frame: nil connection")
	}
	body, err := Encode(frame.Header, frame.Payload, auth, limits)
	if err != nil {
		return fmt.Errorf("encode TCP frame: %w", err)
	}
	if uint64(len(body)) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: TCP body cannot be represented by its uint32 prefix", ErrTooLarge)
	}

	clearDeadline, err := applyOperationDeadline(ctx, ioTimeout, conn.SetWriteDeadline)
	if err != nil {
		return fmt.Errorf("set TCP write deadline: %w", err)
	}
	defer func() {
		if clearError := clearDeadline(); clearError != nil {
			err = errors.Join(err, fmt.Errorf("clear TCP write deadline: %w", clearError))
		}
	}()

	var prefix [tcpLengthPrefixSize]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
	if err := writeAll(conn, prefix[:]); err != nil {
		return fmt.Errorf("write TCP frame prefix: %w", contextualIOError(ctx, err))
	}
	if err := writeAll(conn, body); err != nil {
		return fmt.Errorf("write TCP frame body: %w", contextualIOError(ctx, err))
	}
	return nil
}

func applyOperationDeadline(ctx context.Context, ioTimeout time.Duration, setDeadline func(time.Time) error) (func() error, error) {
	if ctx == nil {
		return func() error { return nil }, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return func() error { return nil }, err
	}

	var deadline time.Time
	if ioTimeout > 0 {
		deadline = time.Now().Add(ioTimeout)
	}
	if contextDeadline, exists := ctx.Deadline(); exists && (deadline.IsZero() || contextDeadline.Before(deadline)) {
		deadline = contextDeadline
	}
	if deadline.IsZero() {
		return func() error { return nil }, nil
	}
	if err := setDeadline(deadline); err != nil {
		return func() error { return nil }, err
	}
	return func() error {
		return setDeadline(time.Time{})
	}, nil
}

func contextualIOError(ctx context.Context, ioError error) error {
	if ctx == nil {
		return ioError
	}
	if contextError := ctx.Err(); contextError != nil {
		return errors.Join(contextError, ioError)
	}
	if deadline, exists := ctx.Deadline(); exists && !time.Now().Before(deadline) {
		return errors.Join(context.DeadlineExceeded, ioError)
	}
	return ioError
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if written < 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
