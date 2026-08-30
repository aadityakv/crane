package swim

import (
	"errors"
	"fmt"
	"math"

	"github.com/aaditya/cs425mp3/internal/config"
)

var (
	// ErrIncarnationRecoveryRequired means neither local durable state nor the
	// seed snapshot contains a trustworthy generation for this node identity.
	ErrIncarnationRecoveryRequired = errors.New("incarnation recovery required")
	// ErrIncarnationOverflow means the recovered generation cannot be advanced.
	ErrIncarnationOverflow = errors.New("incarnation overflow")
	// ErrDuplicateNodeID means a live or suspect process already owns the
	// announced stable identity.
	ErrDuplicateNodeID = errors.New("duplicate live node ID")
	// ErrInvalidJoinAnnouncement means the joining identity or endpoint cannot
	// be safely admitted.
	ErrInvalidJoinAnnouncement = errors.New("invalid join announcement")
	// ErrStaleJoinIncarnation means a terminal membership record is at least as
	// new as the proposed rejoin.
	ErrStaleJoinIncarnation = errors.New("stale join incarnation")
)

// JoinRequest asks a seed for a snapshot relevant to one stable identity.
type JoinRequest struct {
	NodeID uint16
}

// JoinSnapshot is the seed's immutable membership value set at request time.
type JoinSnapshot struct {
	Members []Member
	// Floors carries terminal generations retained after visible expiry so a
	// joining identity must advance past them.
	Floors []Member
}

// JoinAnnounce proposes the durably prepared Alive generation for admission.
type JoinAnnounce struct {
	Member Member
}

// JoinAccepted acknowledges the exact generation admitted by the seed.
type JoinAccepted struct {
	Member Member
}

// PrepareJoin recovers the highest trustworthy incarnation, durably advances
// it, and only then returns an Alive membership value.
func PrepareJoin(store IncarnationStore, snapshot []Member, self Member) (Member, error) {
	return PrepareJoinWithFloors(store, snapshot, nil, self)
}

// PrepareJoinWithFloors is PrepareJoin with the retained terminal floors from
// the seed's join snapshot included in incarnation recovery.
func PrepareJoinWithFloors(store IncarnationStore, snapshot, floors []Member, self Member) (Member, error) {
	if store == nil {
		return Member{}, fmt.Errorf("%w: incarnation store is nil", ErrInvalidJoinAnnouncement)
	}
	if self.NodeID == 0 {
		return Member{}, fmt.Errorf("%w: node ID must be nonzero", ErrInvalidJoinAnnouncement)
	}
	if err := validateAdvertisedEndpoint(self); err != nil {
		return Member{}, err
	}

	persisted, err := store.Load()
	if err != nil {
		return Member{}, fmt.Errorf("load incarnation: %w", err)
	}
	highest := persisted
	for _, member := range snapshot {
		if member.NodeID == self.NodeID && member.Incarnation > highest {
			highest = member.Incarnation
		}
	}
	for _, floor := range floors {
		if floor.NodeID == self.NodeID && (floor.Status == Dead || floor.Status == Left) && floor.Incarnation > highest {
			highest = floor.Incarnation
		}
	}
	if highest == 0 {
		return Member{}, fmt.Errorf("%w for node %d", ErrIncarnationRecoveryRequired, self.NodeID)
	}
	if highest == math.MaxUint64 {
		return Member{}, fmt.Errorf("%w for node %d", ErrIncarnationOverflow, self.NodeID)
	}

	next := highest + 1
	if err := store.Store(next); err != nil {
		return Member{}, fmt.Errorf("persist joining incarnation %d: %w", next, err)
	}
	self.Incarnation = next
	self.Status = Alive
	return self, nil
}

// ValidateJoinAnnouncement checks admission without mutating the membership
// table. The caller owns merging and dissemination after accepting the join.
func ValidateJoinAnnouncement(table *Table, announce JoinAnnounce) error {
	if table == nil {
		return fmt.Errorf("%w: membership table is nil", ErrInvalidJoinAnnouncement)
	}
	joining := announce.Member
	if joining.NodeID == 0 {
		return fmt.Errorf("%w: node ID must be nonzero", ErrInvalidJoinAnnouncement)
	}
	if joining.Incarnation == 0 {
		return fmt.Errorf("%w: incarnation must be nonzero", ErrInvalidJoinAnnouncement)
	}
	if joining.Status != Alive {
		return fmt.Errorf("%w: status must be Alive", ErrInvalidJoinAnnouncement)
	}
	if err := validateAdvertisedEndpoint(joining); err != nil {
		return err
	}

	current, exists := table.Get(joining.NodeID)
	if !exists {
		if floor, retained := table.IncarnationFloor(joining.NodeID); retained && joining.Incarnation <= floor.Incarnation {
			return fmt.Errorf("%w: node %d floor is %d, proposed %d", ErrStaleJoinIncarnation, joining.NodeID, floor.Incarnation, joining.Incarnation)
		}
		return nil
	}
	switch current.Status {
	case Alive:
		if current == joining {
			return nil
		}
		return fmt.Errorf("%w: node %d is currently %v", ErrDuplicateNodeID, joining.NodeID, current.Status)
	case Suspect:
		return fmt.Errorf("%w: node %d is currently %v", ErrDuplicateNodeID, joining.NodeID, current.Status)
	case Dead, Left:
		if joining.Incarnation <= current.Incarnation {
			return fmt.Errorf("%w: node %d has %d, proposed %d", ErrStaleJoinIncarnation, joining.NodeID, current.Incarnation, joining.Incarnation)
		}
		return nil
	default:
		return fmt.Errorf("%w: existing status %d", ErrInvalidJoinAnnouncement, current.Status)
	}
}

func validateAdvertisedEndpoint(member Member) error {
	candidate := config.NodeConfig{
		AdvertiseHost: member.Host,
		BasePort:      member.BasePort,
	}
	for _, service := range config.Services() {
		if _, err := candidate.AdvertiseEndpoint(service.Service); err != nil {
			return fmt.Errorf("%w: advertised endpoint: %v", ErrInvalidJoinAnnouncement, err)
		}
	}
	return nil
}
