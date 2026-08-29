package swim

import (
	"fmt"
	"sort"
)

// Table is the event-loop-owned membership view. It deliberately has no
// internal synchronization; callers must confine it to one owner goroutine.
type Table struct {
	members map[uint16]Member
}

// NewTable returns an empty membership table.
func NewTable() *Table {
	return &Table{members: make(map[uint16]Member)}
}

// Merge applies update when it is newer by incarnation or, at an equal
// incarnation, has a more severe status. Equal-incarnation updates cannot
// change an address.
func (t *Table) Merge(update Update) (bool, MembershipEvent) {
	incoming := update.Member
	if incoming.NodeID == 0 || update.ReporterID == 0 || !incoming.Status.valid() {
		return false, MembershipEvent{}
	}

	previous, exists := t.members[incoming.NodeID]
	if exists {
		switch {
		case incoming.Incarnation < previous.Incarnation:
			return false, MembershipEvent{}
		case incoming.Incarnation == previous.Incarnation:
			if incoming.Status <= previous.Status {
				return false, MembershipEvent{}
			}
			incoming = previous
			incoming.Status = update.Member.Status
		}
	}

	t.members[incoming.NodeID] = incoming
	return true, MembershipEvent{
		Previous:   previous,
		Current:    incoming,
		Cause:      EventMemberChanged,
		ReporterID: update.ReporterID,
	}
}

// Get returns a value copy of nodeID's membership record.
func (t *Table) Get(nodeID uint16) (Member, bool) {
	member, ok := t.members[nodeID]
	return member, ok
}

// MustGet returns nodeID's membership record or panics when it is absent.
func (t *Table) MustGet(nodeID uint16) Member {
	member, ok := t.Get(nodeID)
	if !ok {
		panic(fmt.Sprintf("swim: member %d is absent", nodeID))
	}
	return member
}

// Snapshot returns value copies sorted by node ID.
func (t *Table) Snapshot() []Member {
	members := make([]Member, 0, len(t.members))
	for _, member := range t.members {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}
