package swim

import "sync"

type subscription struct {
	events            chan MembershipEvent
	resynchronization bool
}

// Subscriptions fans membership transitions out to bounded subscriber
// channels. It is safe for concurrent lifecycle calls from service clients.
type Subscriptions struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[uint64]*subscription
	closed      bool
}

// NewSubscriptions returns an open, empty subscription set.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{subscribers: make(map[uint64]*subscription)}
}

// Subscribe registers a bounded event channel. Nonpositive capacities are
// normalized to one so an overflow marker can always be delivered. After
// Close it returns ID zero and an already-closed channel.
func (s *Subscriptions) Subscribe(capacity int) (uint64, <-chan MembershipEvent) {
	if capacity < 1 {
		capacity = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		events := make(chan MembershipEvent)
		close(events)
		return 0, events
	}

	s.nextID++
	if s.nextID == 0 {
		s.nextID++
	}
	id := s.nextID
	events := make(chan MembershipEvent, capacity)
	s.subscribers[id] = &subscription{events: events}
	return id, events
}

// Publish offers event to every current subscriber without waiting for a
// channel receiver. A full subscriber is switched to resynchronization mode.
func (s *Subscriptions) Publish(event MembershipEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	for _, subscriber := range s.subscribers {
		if subscriber.resynchronization {
			continue
		}
		select {
		case subscriber.events <- event:
		default:
			drainEvents(subscriber.events)
			subscriber.events <- MembershipEvent{Cause: EventResyncRequired}
			subscriber.resynchronization = true
		}
	}
}

// MarkResynchronized resumes delta delivery after the subscriber has fetched
// and installed a complete membership snapshot. It reports whether id still
// names a live subscription.
func (s *Subscriptions) MarkResynchronized(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if subscriber, ok := s.subscribers[id]; ok {
		subscriber.resynchronization = false
		return true
	}
	return false
}

// Unsubscribe removes id and closes its event channel. It is idempotent.
func (s *Subscriptions) Unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscriber, ok := s.subscribers[id]
	if !ok {
		return
	}
	delete(s.subscribers, id)
	close(subscriber.events)
}

// Close closes every subscriber and rejects future subscriptions. It is
// idempotent and safe to race with Publish, MarkResynchronized, or Unsubscribe.
func (s *Subscriptions) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for id, subscriber := range s.subscribers {
		delete(s.subscribers, id)
		close(subscriber.events)
	}
}

func drainEvents(events chan MembershipEvent) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}
