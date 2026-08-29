package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const tcpLengthPrefixSize = 4

// ReadTCPFrame reads one uint32-length-prefixed authenticated frame body.
func ReadTCPFrame(ctx context.Context, conn net.Conn, auth Authenticator, limits Limits, ioTimeout time.Duration) (Frame, error) {
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
	defer clearDeadline()

	var prefix [tcpLengthPrefixSize]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return Frame{}, fmt.Errorf("read TCP frame prefix: %w", contextualIOError(ctx, err))
	}
	bodyLength := binary.BigEndian.Uint32(prefix[:])
	if uint64(bodyLength) > uint64(resolved.MaxFrameSize) {
		return Frame{}, fmt.Errorf("%w: declared TCP body is %d bytes, maximum is %d", ErrTooLarge, bodyLength, resolved.MaxFrameSize)
	}

	body := make([]byte, int(bodyLength))
	if _, err := io.ReadFull(conn, body); err != nil {
		return Frame{}, fmt.Errorf("read TCP frame body: %w", contextualIOError(ctx, err))
	}
	frame, err := Decode(body, auth, resolved)
	if err != nil {
		return Frame{}, fmt.Errorf("decode TCP frame: %w", err)
	}
	return frame, nil
}

// WriteTCPFrame encodes and completely writes one uint32-length-prefixed frame body.
func WriteTCPFrame(ctx context.Context, conn net.Conn, frame Frame, auth Authenticator, limits Limits, ioTimeout time.Duration) error {
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
	defer clearDeadline()

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

func applyOperationDeadline(ctx context.Context, ioTimeout time.Duration, setDeadline func(time.Time) error) (func(), error) {
	if ctx == nil {
		return func() {}, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return func() {}, err
	}

	var deadline time.Time
	if ioTimeout > 0 {
		deadline = time.Now().Add(ioTimeout)
	}
	if contextDeadline, exists := ctx.Deadline(); exists && (deadline.IsZero() || contextDeadline.Before(deadline)) {
		deadline = contextDeadline
	}
	if deadline.IsZero() {
		return func() {}, nil
	}
	if err := setDeadline(deadline); err != nil {
		return func() {}, err
	}
	return func() {
		_ = setDeadline(time.Time{})
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
