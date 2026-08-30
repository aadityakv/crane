package swim

import (
	"fmt"
	"sort"
)

// Table is the event-loop-owned membership view. It deliberately has no
// internal synchronization; callers must confine it to one owner goroutine.
type Table struct {
	members map[uint16]Member
	// floors retains at most one terminal generation per nonzero uint16 node
	// identity. The key domain bounds this map to 65,535 entries without an
	// eviction policy that could permit an old generation to resurrect.
	floors map[uint16]Member
}

// NewTable returns an empty membership table.
func NewTable() *Table {
	return &Table{
		members: make(map[uint16]Member),
		floors:  make(map[uint16]Member),
	}
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
	reference, hasReference := previous, exists
	if floor, hasFloor := t.floors[incoming.NodeID]; hasFloor && (!hasReference || compareMemberVersion(floor, reference) > 0) {
		reference, hasReference = floor, true
	}
	if hasReference {
		switch {
		case incoming.Incarnation < reference.Incarnation:
			return false, MembershipEvent{}
		case incoming.Incarnation == reference.Incarnation:
			if incoming.Status <= reference.Status {
				return false, MembershipEvent{}
			}
			incoming = reference
			incoming.Status = update.Member.Status
		}
	}

	t.members[incoming.NodeID] = incoming
	t.retainIncarnationFloor(incoming)
	return true, MembershipEvent{
		Previous:   previous,
		Current:    incoming,
		Cause:      EventMemberChanged,
		ReporterID: update.ReporterID,
	}
}

// MergeIncarnationFloor retains terminal generation knowledge received in a
// join or repair snapshot. A floor that advances a visible member is surfaced
// as the corresponding terminal transition; a floor for an already-expired
// identity remains invisible while still rejecting stale future updates.
func (t *Table) MergeIncarnationFloor(floor Member, reporterID uint16) (bool, MembershipEvent) {
	if floor.NodeID == 0 || floor.Incarnation == 0 || reporterID == 0 || (floor.Status != Dead && floor.Status != Left) {
		return false, MembershipEvent{}
	}
	if retained, exists := t.floors[floor.NodeID]; exists {
		comparison := compareMemberVersion(floor, retained)
		if comparison <= 0 {
			return false, MembershipEvent{}
		}
		if floor.Incarnation == retained.Incarnation {
			canonical := retained
			canonical.Status = floor.Status
			floor = canonical
		}
	}
	if current, exists := t.members[floor.NodeID]; exists {
		comparison := compareMemberVersion(floor, current)
		if comparison > 0 {
			if floor.Incarnation == current.Incarnation {
				canonical := current
				canonical.Status = floor.Status
				floor = canonical
			}
			return t.Merge(Update{Member: floor, ReporterID: reporterID})
		}
	}
	t.floors[floor.NodeID] = floor
	return true, MembershipEvent{}
}

// ExpireTerminal removes only the exact visible Dead or Left record while
// retaining its generation floor for future merges and snapshot exchange.
func (t *Table) ExpireTerminal(member Member) bool {
	if member.Status != Dead && member.Status != Left {
		return false
	}
	current, exists := t.members[member.NodeID]
	if !exists || current != member {
		return false
	}
	t.retainIncarnationFloor(current)
	delete(t.members, member.NodeID)
	return true
}

// IncarnationFloor returns the retained terminal generation for nodeID.
func (t *Table) IncarnationFloor(nodeID uint16) (Member, bool) {
	floor, exists := t.floors[nodeID]
	return floor, exists
}

// IncarnationFloors returns copied hidden terminal floors sorted by node ID.
// A floor dominated by a currently visible generation is omitted because the
// visible member already carries an equal or newer recovery bound.
func (t *Table) IncarnationFloors() []Member {
	floors := make([]Member, 0, len(t.floors))
	for nodeID, floor := range t.floors {
		if current, exists := t.members[nodeID]; exists && compareMemberVersion(current, floor) >= 0 {
			continue
		}
		floors = append(floors, floor)
	}
	sort.Slice(floors, func(i, j int) bool {
		return floors[i].NodeID < floors[j].NodeID
	})
	return floors
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

func (t *Table) retainIncarnationFloor(member Member) {
	if member.Status != Dead && member.Status != Left {
		return
	}
	retained, exists := t.floors[member.NodeID]
	if !exists || compareMemberVersion(member, retained) > 0 {
		if exists && member.Incarnation == retained.Incarnation {
			canonical := retained
			canonical.Status = member.Status
			member = canonical
		}
		t.floors[member.NodeID] = member
	}
}

func compareMemberVersion(left, right Member) int {
	switch {
	case left.Incarnation < right.Incarnation:
		return -1
	case left.Incarnation > right.Incarnation:
		return 1
	case left.Status < right.Status:
		return -1
	case left.Status > right.Status:
		return 1
	default:
		return 0
	}
}
