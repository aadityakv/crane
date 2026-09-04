// Package swim implements the SWIM membership state machine.
package swim

// Status is a member's current SWIM state. Its numeric order is the
// equal-incarnation merge precedence.
type Status uint8

const (
	// Alive means the member is believed healthy.
	Alive Status = iota
	// Suspect means a probe failed and the member has until the suspicion timeout to refute it.
	Suspect
	// Dead means the suspicion timeout elapsed without refutation.
	Dead
	// Left means the member announced a graceful departure.
	Left
)

// Member is the immutable value recorded for one node identity.
type Member struct {
	NodeID      uint16
	Host        string
	BasePort    uint16
	Incarnation uint64
	Status      Status
}

// Update reports a membership record and the node that observed or produced it.
type Update struct {
	Member     Member
	ReporterID uint16
}

// EventCause identifies why a membership event was delivered.
type EventCause uint8

const (
	// EventMemberChanged is a successfully merged membership transition.
	EventMemberChanged EventCause = iota
	// EventResyncRequired tells a slow subscriber to fetch a fresh snapshot.
	EventResyncRequired
)

// MembershipEvent is an immutable membership transition. Previous is zero for
// the first record of a node. ReporterID is metadata and never affects merge
// precedence.
type MembershipEvent struct {
	Previous   Member
	Current    Member
	Cause      EventCause
	ReporterID uint16
}

func (s Status) valid() bool {
	return s <= Left
}
