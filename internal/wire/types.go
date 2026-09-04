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
	// MaxCraneDatagramBytesV1 is the absolute complete-frame ceiling for Crane v1 tuple traffic.
	MaxCraneDatagramBytesV1 = 1200

	defaultMaxFrameSize         = 8 << 20
	defaultMaxSWIMDatagramSize  = 1200
	defaultMaxCraneDatagramSize = MaxCraneDatagramBytesV1
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
	// ErrUnsupportedMessage classifies reserved or unknown messages in an owned protocol range.
	ErrUnsupportedMessage = errors.New("unsupported wire message")
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
	// MessageCraneWorkerHandshake binds a worker-control stream to its sender.
	MessageCraneWorkerHandshake MessageType = 200
	// MessageCraneWorkerHandshakeAck acknowledges a worker-control stream handshake.
	MessageCraneWorkerHandshakeAck MessageType = 201
	// MessageCraneWorkerFenceRequest asks a worker to adopt a coordinator epoch.
	MessageCraneWorkerFenceRequest MessageType = 202
	// MessageCraneWorkerFenceResponse reports worker fencing progress.
	MessageCraneWorkerFenceResponse MessageType = 203
	// MessageCraneWorkerRegisterRequest registers a fenced worker epoch.
	MessageCraneWorkerRegisterRequest MessageType = 204
	// MessageCraneWorkerRegisterResponse reports worker registration results.
	MessageCraneWorkerRegisterResponse MessageType = 205
	// MessageCraneAssignmentSetInstall replaces one job's complete worker assignment set.
	MessageCraneAssignmentSetInstall MessageType = 206
	// MessageCraneAssignmentSetInstallAck acknowledges an assignment-set installation.
	MessageCraneAssignmentSetInstallAck MessageType = 207
	// MessageCraneWorkerStatusRequest requests a bounded worker inventory.
	MessageCraneWorkerStatusRequest MessageType = 208
	// MessageCraneWorkerStatusReport returns a bounded worker inventory.
	MessageCraneWorkerStatusReport MessageType = 209
	// MessageCraneCheckpointNotice reports checkpoint progress.
	MessageCraneCheckpointNotice MessageType = 210
	// MessageCraneCheckpointAck acknowledges committed checkpoint progress.
	MessageCraneCheckpointAck MessageType = 211
	// MessageCraneResultRecordChunk transfers bounded result-record data.
	MessageCraneResultRecordChunk MessageType = 212
	// MessageCraneResultRecordAck acknowledges result-record transfer progress.
	MessageCraneResultRecordAck MessageType = 213
	// MessageCraneResultArtifactChunk transfers bounded result-artifact data.
	MessageCraneResultArtifactChunk MessageType = 214
	// MessageCraneResultArtifactAck acknowledges result-artifact transfer progress.
	MessageCraneResultArtifactAck MessageType = 215
	// MessageCraneResultFetchRequest requests a committed result artifact.
	MessageCraneResultFetchRequest MessageType = 216
	// MessageCraneResultFetchChunk returns bounded result-artifact data.
	MessageCraneResultFetchChunk MessageType = 217
	// MessageCraneWorkerError carries a typed worker-control protocol failure.
	MessageCraneWorkerError MessageType = 218
	// MessageCraneWorkerReserved pins the intentionally unusable worker-control ID 219.
	MessageCraneWorkerReserved MessageType = 219
	// MessageCraneSubmitRequest submits a validated job specification.
	MessageCraneSubmitRequest MessageType = 240
	// MessageCraneSubmitResponse reports the result of job submission.
	MessageCraneSubmitResponse MessageType = 241
	// MessageCraneCancelRequest requests cancellation of one job.
	MessageCraneCancelRequest MessageType = 242
	// MessageCraneCancelResponse reports the result of job cancellation.
	MessageCraneCancelResponse MessageType = 243
	// MessageCraneStatusRequest requests one linearizable job status.
	MessageCraneStatusRequest MessageType = 244
	// MessageCraneStatusResponse returns one linearizable job status.
	MessageCraneStatusResponse MessageType = 245
	// MessageCraneResultPageRequest requests one bounded result page.
	MessageCraneResultPageRequest MessageType = 246
	// MessageCraneResultPageResponse returns one bounded result page.
	MessageCraneResultPageResponse MessageType = 247
	// MessageCraneLeaderRedirect identifies the current control-plane leader.
	MessageCraneLeaderRedirect MessageType = 248
	// MessageCraneControlError carries a typed client-control protocol failure.
	MessageCraneControlError MessageType = 249
	// MessageCraneJobListRequest requests every retained-job summary.
	MessageCraneJobListRequest MessageType = 250
	// MessageCraneJobListResponse returns every retained-job summary.
	MessageCraneJobListResponse MessageType = 251
	// MessageCraneTupleDelivery carries one attempt-fenced tuple delivery.
	MessageCraneTupleDelivery MessageType = 280
	// MessageCraneTupleDeliveryAck acknowledges durable tuple acceptance.
	MessageCraneTupleDeliveryAck MessageType = 281
	// MessageCraneTupleDeliveryNack rejects a tuple delivery with a typed reason.
	MessageCraneTupleDeliveryNack MessageType = 282

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

// Limits bounds complete frame bodies and service-specific UDP datagrams.
// ExpectedClusterID, when non-nil, rejects otherwise authenticated frames for
// another cluster.
type Limits struct {
	MaxFrameSize         int
	MaxSWIMDatagramSize  int
	MaxCraneDatagramSize int
	ExpectedClusterID    *[16]byte
}

// DefaultLimits returns the initial 8 MiB frame and 1200-byte SWIM and Crane datagram bounds.
func DefaultLimits() Limits {
	return Limits{
		MaxFrameSize:         defaultMaxFrameSize,
		MaxSWIMDatagramSize:  defaultMaxSWIMDatagramSize,
		MaxCraneDatagramSize: defaultMaxCraneDatagramSize,
	}
}
