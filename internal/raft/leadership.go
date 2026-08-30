package raft

import "sync"

const (
	// MinLeadershipSubscriptionCapacity is the smallest permitted delta buffer.
	MinLeadershipSubscriptionCapacity = 1
	// MaxLeadershipSubscriptionCapacity is the largest permitted delta buffer.
	MaxLeadershipSubscriptionCapacity = 1024
)

// LeadershipEvent is one sequenced local leadership observation.
type LeadershipEvent struct {
	// Sequence is checked and monotonically increasing within one node run.
	Sequence uint64
	// Term is the nondecreasing durable Raft term.
	Term uint64
	// Role is the local protocol role.
	Role Role
	// LeaderID is the best-effort known leader, or zero.
	LeaderID uint16
	// LocalID is the exact configured voter represented by this event.
	LocalID uint16
}

// LeadershipSubscription is an owner-linearized snapshot followed by bounded deltas.
type LeadershipSubscription struct {
	snapshot    LeadershipEvent
	events      <-chan LeadershipEvent
	done        <-chan struct{}
	unsubscribe func()

	mu  *sync.Mutex
	err *error
}

// Snapshot returns the owner-linearized current leadership observation.
func (subscription *LeadershipSubscription) Snapshot() LeadershipEvent {
	if subscription == nil {
		return LeadershipEvent{}
	}
	return subscription.snapshot
}

// Events returns the bounded stream of leadership deltas after Snapshot.
func (subscription *LeadershipSubscription) Events() <-chan LeadershipEvent {
	if subscription == nil {
		return nil
	}
	return subscription.events
}

// Done closes when the subscription ends for any reason.
func (subscription *LeadershipSubscription) Done() <-chan struct{} {
	if subscription == nil {
		return nil
	}
	return subscription.done
}

// Err reports the terminal subscription classification after Done closes.
func (subscription *LeadershipSubscription) Err() error {
	if subscription == nil || subscription.mu == nil || subscription.err == nil {
		return nil
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	return *subscription.err
}

// Unsubscribe requests idempotent nonblocking removal from the owner loop.
func (subscription *LeadershipSubscription) Unsubscribe() {
	if subscription != nil && subscription.unsubscribe != nil {
		subscription.unsubscribe()
	}
}
