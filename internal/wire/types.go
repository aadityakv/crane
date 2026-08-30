// Package wire provides authenticated, fixed-width framing independent of payload schemas.
package wire

import "errors"

const (
	// Version1 is the initial canonical wire protocol version.
	Version1 uint16 = 1
	// FixedHeaderSize is the byte length of every canonical frame header.
	FixedHeaderSize = 55
	// MACSize is the byte length of the trailing HMAC-SHA256 authentication code.
	MACSize = 32

	defaultMaxFrameSize        = 8 << 20
	defaultMaxSWIMDatagramSize = 1200
)

var (
	// ErrMalformed classifies frames whose fixed layout or declared length is invalid.
	ErrMalformed = errors.New("malformed wire frame")
	// ErrTooLarge classifies frames that exceed a configured allocation or datagram bound.
	ErrTooLarge = errors.New("wire frame too large")
	// ErrUnsupportedVersion classifies frames using a protocol version this package cannot decode.
	ErrUnsupportedVersion = errors.New("unsupported wire version")
	// ErrUnsupportedCodec classifies frames using an unknown payload codec.
	ErrUnsupportedCodec = errors.New("unsupported wire codec")
	// ErrAuthentication classifies frames that fail authentication or target another cluster.
	ErrAuthentication = errors.New("wire authentication failed")
)

// MessageType identifies the semantic payload type without coupling framing to its Go representation.
type MessageType uint16

const (
	// MessageSWIMPing carries a SWIM direct-probe datagram payload.
	MessageSWIMPing MessageType = 1
	// MessageSWIMAck carries a SWIM direct-probe acknowledgement datagram payload.
	MessageSWIMAck MessageType = 2
	// MessageSWIMPingReq carries a SWIM indirect-probe request datagram payload.
	MessageSWIMPingReq MessageType = 3
	// MessageSWIMIndirectAck carries a relayed SWIM acknowledgement datagram payload.
	MessageSWIMIndirectAck MessageType = 4
	// MessageSWIMGossip carries membership updates without a probe payload.
	MessageSWIMGossip MessageType = 5
	// MessageSWIMDigest asks an active peer to resynchronize over TCP.
	MessageSWIMDigest MessageType = 6
	// MessageSWIMJoinRequest starts the authenticated two-step admission exchange.
	MessageSWIMJoinRequest MessageType = 7
	// MessageSWIMJoinSnapshot returns the seed's membership view to a joining node.
	MessageSWIMJoinSnapshot MessageType = 8
	// MessageSWIMJoinAnnounce proposes a durably prepared Alive incarnation.
	MessageSWIMJoinAnnounce MessageType = 9
	// MessageSWIMJoinAccepted acknowledges the exact admitted incarnation.
	MessageSWIMJoinAccepted MessageType = 10
	// MessageSWIMSnapshotRequest asks an active peer for a complete membership view.
	MessageSWIMSnapshotRequest MessageType = 11
	// MessageSWIMSnapshotResponse returns a complete membership view to an active peer.
	MessageSWIMSnapshotResponse MessageType = 12
	// MessageSWIMError carries an authentication-safe TCP protocol failure.
	MessageSWIMError MessageType = 13
	// MessageSWIMSnapshotApplied acknowledges owner-validated snapshot application.
	MessageSWIMSnapshotApplied MessageType = 14
	// MessageRaftHandshake authenticates and binds a voter stream to its sender.
	MessageRaftHandshake MessageType = 100
	// MessageRaftHandshakeAck acknowledges an authenticated voter stream handshake.
	MessageRaftHandshakeAck MessageType = 101
	// MessageRaftPreVoteRequest asks whether a prospective election could succeed.
	MessageRaftPreVoteRequest MessageType = 102
	// MessageRaftPreVoteResponse reports one voter's pre-vote decision.
	MessageRaftPreVoteResponse MessageType = 103
	// MessageRaftRequestVoteRequest asks for a vote in an active election term.
	MessageRaftRequestVoteRequest MessageType = 104
	// MessageRaftRequestVoteResponse reports one voter's election decision.
	MessageRaftRequestVoteResponse MessageType = 105
	// MessageRaftAppendEntriesRequest replicates log entries or carries a heartbeat.
	MessageRaftAppendEntriesRequest MessageType = 106
	// MessageRaftAppendEntriesResponse reports the result of a replication request.
	MessageRaftAppendEntriesResponse MessageType = 107
	// MessageRaftInstallSnapshotRequest carries one bounded snapshot chunk.
	MessageRaftInstallSnapshotRequest MessageType = 108
	// MessageRaftInstallSnapshotResponse reports durable snapshot chunk progress.
	MessageRaftInstallSnapshotResponse MessageType = 109
	// MessageRaftError carries a typed authentication-safe protocol failure.
	MessageRaftError MessageType = 110

	// MessageSWIMACK preserves the conventional all-caps ACK spelling.
	MessageSWIMACK = MessageSWIMAck
	// MessageSWIMIndirectACK preserves the conventional all-caps ACK spelling.
	MessageSWIMIndirectACK = MessageSWIMIndirectAck
)

// Codec identifies the concrete payload serialization used inside a frame.
type Codec uint8

const (
	// CodecGob identifies a payload encoded as one concrete gob value.
	CodecGob Codec = 1
	// CodecBinary identifies an explicitly encoded canonical binary payload.
	CodecBinary Codec = 2
)

// RequestID is the fixed-width identifier used for request correlation and replay defense.
type RequestID [16]byte

// Header contains the authenticated routing and replay metadata of a frame.
type Header struct {
	Version         uint16
	Message         MessageType
	ClusterID       [16]byte
	SenderID        uint16
	RequestID       RequestID
	TimestampMillis int64
	Codec           Codec
}

// Frame is an authenticated header and an independently owned payload byte slice.
type Frame struct {
	Header  Header
	Payload []byte
}

// Limits bounds complete frame bodies and SWIM UDP datagrams. ExpectedClusterID,
// when non-nil, rejects otherwise authenticated frames for another cluster.
type Limits struct {
	MaxFrameSize        int
	MaxSWIMDatagramSize int
	ExpectedClusterID   *[16]byte
}

// DefaultLimits returns the initial 8 MiB frame and 1200-byte SWIM datagram bounds.
func DefaultLimits() Limits {
	return Limits{
		MaxFrameSize:        defaultMaxFrameSize,
		MaxSWIMDatagramSize: defaultMaxSWIMDatagramSize,
	}
}
