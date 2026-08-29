package swim

import "time"

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
