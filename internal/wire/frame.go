package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	frameMagic          = "CS42"
	magicOffset         = 0
	versionOffset       = 4
	messageOffset       = 6
	clusterIDOffset     = 8
	senderIDOffset      = 24
	requestIDOffset     = 26
	timestampOffset     = 42
	codecOffset         = 50
	payloadLengthOffset = 51
)

// Encode returns the canonical header, payload, and trailing MAC as one owned frame body.
func Encode(header Header, payload []byte, auth Authenticator, limits Limits) ([]byte, error) {
	resolved, err := resolveLimits(limits)
	if err != nil {
		return nil, err
	}
	if err := validateHeader(header, resolved); err != nil {
		return nil, err
	}
	if auth == nil {
		return nil, ErrAuthentication
	}

	limit := effectiveLimit(header.Message, resolved)
	overhead := FixedHeaderSize + MACSize
	if len(payload) > limit-overhead {
		return nil, fmt.Errorf("%w: encoded body exceeds %d bytes", ErrTooLarge, limit)
	}
	totalLength := overhead + len(payload)
	encoded := make([]byte, totalLength)
	encodeHeader(encoded[:FixedHeaderSize], header, uint32(len(payload)))
	copy(encoded[FixedHeaderSize:], payload)

	signedLength := FixedHeaderSize + len(payload)
	mac := auth.Sign(encoded[:signedLength])
	if len(mac) != MACSize {
		return nil, fmt.Errorf("%w: authenticator returned %d-byte MAC", ErrAuthentication, len(mac))
	}
	copy(encoded[signedLength:], mac)
	return encoded, nil
}

// Decode validates bounds and fixed fields, authenticates canonical bytes, and
// only then allocates and returns an independently owned payload.
func Decode(encoded []byte, auth Authenticator, limits Limits) (Frame, error) {
	resolved, err := resolveLimits(limits)
	if err != nil {
		return Frame{}, err
	}
	if len(encoded) > resolved.MaxFrameSize {
		return Frame{}, fmt.Errorf("%w: body is %d bytes, maximum is %d", ErrTooLarge, len(encoded), resolved.MaxFrameSize)
	}
	if len(encoded) < FixedHeaderSize+MACSize {
		return Frame{}, fmt.Errorf("%w: body is shorter than fixed header and MAC", ErrMalformed)
	}
	if string(encoded[magicOffset:versionOffset]) != frameMagic {
		return Frame{}, fmt.Errorf("%w: invalid magic", ErrMalformed)
	}

	header := decodeHeader(encoded[:FixedHeaderSize])
	if err := validateHeader(header, resolved); err != nil {
		return Frame{}, err
	}

	payloadLength := binary.BigEndian.Uint32(encoded[payloadLengthOffset:FixedHeaderSize])
	declaredLength := uint64(FixedHeaderSize) + uint64(payloadLength) + uint64(MACSize)
	limit := effectiveLimit(header.Message, resolved)
	if declaredLength > uint64(limit) {
		return Frame{}, fmt.Errorf("%w: declared body is %d bytes, maximum is %d", ErrTooLarge, declaredLength, limit)
	}
	if declaredLength != uint64(len(encoded)) {
		return Frame{}, fmt.Errorf("%w: declared body is %d bytes, received %d", ErrMalformed, declaredLength, len(encoded))
	}
	if auth == nil {
		return Frame{}, ErrAuthentication
	}

	signedLength := FixedHeaderSize + int(payloadLength)
	if !auth.Verify(encoded[:signedLength], encoded[signedLength:]) {
		return Frame{}, ErrAuthentication
	}
	payload := make([]byte, int(payloadLength))
	copy(payload, encoded[FixedHeaderSize:signedLength])
	return Frame{Header: header, Payload: payload}, nil
}

func resolveLimits(limits Limits) (Limits, error) {
	defaults := DefaultLimits()
	if limits.MaxFrameSize == 0 {
		limits.MaxFrameSize = defaults.MaxFrameSize
	}
	if limits.MaxSWIMDatagramSize == 0 {
		limits.MaxSWIMDatagramSize = defaults.MaxSWIMDatagramSize
	}
	minimum := FixedHeaderSize + MACSize
	if limits.MaxFrameSize < minimum || limits.MaxSWIMDatagramSize < minimum {
		return Limits{}, fmt.Errorf("%w: limits must permit the fixed header and MAC", ErrTooLarge)
	}
	if uint64(limits.MaxFrameSize) > uint64(math.MaxUint32) {
		return Limits{}, fmt.Errorf("%w: frame limit exceeds the canonical uint32 length domain", ErrTooLarge)
	}
	return limits, nil
}

func effectiveLimit(message MessageType, limits Limits) int {
	limit := limits.MaxFrameSize
	if message == MessageSWIMPing && limits.MaxSWIMDatagramSize < limit {
		limit = limits.MaxSWIMDatagramSize
	}
	return limit
}

func validateHeader(header Header, limits Limits) error {
	if header.Version != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, header.Version)
	}
	if header.Codec != CodecGob {
		return fmt.Errorf("%w: %d", ErrUnsupportedCodec, header.Codec)
	}
	if limits.ExpectedClusterID != nil && header.ClusterID != *limits.ExpectedClusterID {
		return ErrAuthentication
	}
	return nil
}

func encodeHeader(destination []byte, header Header, payloadLength uint32) {
	copy(destination[magicOffset:versionOffset], frameMagic)
	binary.BigEndian.PutUint16(destination[versionOffset:messageOffset], header.Version)
	binary.BigEndian.PutUint16(destination[messageOffset:clusterIDOffset], uint16(header.Message))
	copy(destination[clusterIDOffset:senderIDOffset], header.ClusterID[:])
	binary.BigEndian.PutUint16(destination[senderIDOffset:requestIDOffset], header.SenderID)
	copy(destination[requestIDOffset:timestampOffset], header.RequestID[:])
	binary.BigEndian.PutUint64(destination[timestampOffset:codecOffset], uint64(header.TimestampMillis))
	destination[codecOffset] = byte(header.Codec)
	binary.BigEndian.PutUint32(destination[payloadLengthOffset:FixedHeaderSize], payloadLength)
}

func decodeHeader(source []byte) Header {
	var header Header
	header.Version = binary.BigEndian.Uint16(source[versionOffset:messageOffset])
	header.Message = MessageType(binary.BigEndian.Uint16(source[messageOffset:clusterIDOffset]))
	copy(header.ClusterID[:], source[clusterIDOffset:senderIDOffset])
	header.SenderID = binary.BigEndian.Uint16(source[senderIDOffset:requestIDOffset])
	copy(header.RequestID[:], source[requestIDOffset:timestampOffset])
	header.TimestampMillis = int64(binary.BigEndian.Uint64(source[timestampOffset:codecOffset]))
	header.Codec = Codec(source[codecOffset])
	return header
}
