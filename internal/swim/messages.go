package swim

import (
	"time"

	"github.com/aadityakv/crane/internal/wire"
)

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
	// Floors carries terminal generations whose visible tombstones expired.
	Floors []Member
}

// SnapshotApplied acknowledges that the correlated snapshot was validated and
// applied by the requester's membership owner.
type SnapshotApplied struct{}

// ProtocolErrorMessage is an authentication-safe TCP failure response.
type ProtocolErrorMessage struct {
	Code    string
	Message string
}

// ProbeID is the complete origin-scoped correlation identity. Sequence keeps
// deterministic ordering while RequestID prevents a colliding sequence from
// matching a different probe generation.
type ProbeID struct {
	Sequence  uint64
	RequestID wire.RequestID
}

// Ping asks one member to acknowledge one origin-scoped probe sequence.
type Ping struct {
	OriginID uint16
	Sequence uint64
	// RequestID is the probe UUID and must match the authenticated frame.
	RequestID wire.RequestID
}

// ID returns the complete probe correlation identity.
func (p Ping) ID() ProbeID { return ProbeID{Sequence: p.Sequence, RequestID: p.RequestID} }

// Ack confirms receipt of a Ping for its origin-scoped sequence.
type Ack struct {
	OriginID uint16
	Sequence uint64
	// RequestID echoes the Ping probe UUID.
	RequestID wire.RequestID
}

// ID returns the complete probe correlation identity.
func (a Ack) ID() ProbeID { return ProbeID{Sequence: a.Sequence, RequestID: a.RequestID} }

// PingReq asks a relay to probe Target on behalf of OriginID while preserving
// the origin's sequence generation.
type PingReq struct {
	OriginID uint16
	Target   Member
	Sequence uint64
	// RequestID preserves the origin's probe UUID through the relay.
	RequestID wire.RequestID
}

// ID returns the complete probe correlation identity.
func (p PingReq) ID() ProbeID { return ProbeID{Sequence: p.Sequence, RequestID: p.RequestID} }

// IndirectAck tells an origin that one authorized relay reached Target.
type IndirectAck struct {
	OriginID uint16
	Target   Member
	Sequence uint64
	// RequestID echoes the origin's probe UUID through the relay.
	RequestID wire.RequestID
}

// ID returns the complete probe correlation identity.
func (a IndirectAck) ID() ProbeID {
	return ProbeID{Sequence: a.Sequence, RequestID: a.RequestID}
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
	// RequestID makes relay cleanup exact when sequences collide.
	RequestID wire.RequestID
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
