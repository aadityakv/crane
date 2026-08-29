package swim

import "time"

// PingMessage is the concrete authenticated payload for a direct probe.
type PingMessage struct {
	Ping    Ping
	Updates []Update
}

// AckMessage is the concrete authenticated payload for a direct acknowledgement.
type AckMessage struct {
	Ack     Ack
	Updates []Update
}

// PingReqMessage is the concrete authenticated payload for an indirect probe request.
type PingReqMessage struct {
	PingReq PingReq
	Updates []Update
}

// IndirectAckMessage is the concrete authenticated payload for a relayed acknowledgement.
type IndirectAckMessage struct {
	IndirectAck IndirectAck
	Updates     []Update
}

// GossipMessage carries only bounded piggyback membership updates.
type GossipMessage struct {
	Updates []Update
}

// DigestMessage asks an active peer to fetch a complete TCP snapshot.
type DigestMessage struct {
	Updates []Update
}

// SnapshotRequest asks an active member for a complete immutable membership view.
type SnapshotRequest struct{}

// SnapshotResponse returns the responder's complete membership value set.
type SnapshotResponse struct {
	Members []Member
}

// SnapshotApplied acknowledges that the correlated snapshot was validated and
// applied by the requester's membership owner.
type SnapshotApplied struct{}

// ProtocolErrorMessage is an authentication-safe TCP failure response.
type ProtocolErrorMessage struct {
	Code    string
	Message string
}

// Ping asks one member to acknowledge one origin-scoped probe sequence.
type Ping struct {
	OriginID uint16
	Sequence uint64
}

// Ack confirms receipt of a Ping for its origin-scoped sequence.
type Ack struct {
	OriginID uint16
	Sequence uint64
}

// PingReq asks a relay to probe Target on behalf of OriginID while preserving
// the origin's sequence generation.
type PingReq struct {
	OriginID uint16
	Target   Member
	Sequence uint64
}

// IndirectAck tells an origin that one authorized relay reached Target.
type IndirectAck struct {
	OriginID uint16
	Target   Member
	Sequence uint64
}

// Outbound is one protocol message addressed to a membership endpoint.
type Outbound struct {
	To      Member
	Message any
}

// TimerKind identifies the engine callback required at one deadline.
type TimerKind uint8

const (
	TimerDirectProbe TimerKind = iota
	TimerIndirectProbe
	TimerRelayProbe
	TimerSuspicion
	TimerTombstone
	// TimerLeaveDeadline bounds graceful dissemination; it has no engine
	// callback because the service owner uses it to close its cleanup context.
	TimerLeaveDeadline
)

// TimerRequest asks the event-loop owner to invoke a generation-specific
// engine timeout handler at Deadline.
type TimerRequest struct {
	Kind     TimerKind
	OriginID uint16
	NodeID   uint16
	Sequence uint64
	// Incarnation makes membership timers generation-specific.
	Incarnation uint64
	Status      Status
	Deadline    time.Time
}

// Effects contains only work the single-owner engine asks its caller to
// perform. Protocol handlers never perform network, timer, persistence, or
// subscriber I/O directly. When PersistIncarnation is non-nil, the caller must
// durably persist it before executing Events or disseminating the queued local
// membership update.
type Effects struct {
	Outbound           []Outbound
	Timers             []TimerRequest
	Events             []MembershipEvent
	PersistIncarnation *uint64
	SnapshotRequired   bool
}
