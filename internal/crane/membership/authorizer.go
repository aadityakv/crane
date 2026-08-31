// Package membership derives fail-closed Crane peer authorization from SWIM.
package membership

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/endpointauth"
	"github.com/aaditya/cs425mp3/internal/swim"
)

const (
	authorizerStateNew uint32 = iota
	authorizerStateRunning
	authorizerStateStopped
)

const upstreamSubscriptionCapacity = 256
const maximumSubscriberCapacity = 1024

var (
	// ErrInvalidOptions reports an unusable authorizer dependency or node configuration.
	ErrInvalidOptions = errors.New("invalid membership authorizer options")
	// ErrNotRunning reports an operation that requires an installed membership view.
	ErrNotRunning = errors.New("membership authorizer not running")
	// ErrUnauthorized reports a source that is not an active member's advertised endpoint.
	ErrUnauthorized = errors.New("unauthorized crane peer")
	// ErrSubscriptionClosed reports recovery attempted after subscription cancellation.
	ErrSubscriptionClosed = errors.New("membership subscription closed")
)

// EventCause identifies why an authorizer subscriber must update its view.
type EventCause uint8

const (
	// MemberChanged is one accepted monotonic SWIM membership transition.
	MemberChanged EventCause = iota
	// ResyncRequired tells a subscriber to fetch its scoped complete Snapshot.
	ResyncRequired
)

// Event is an immutable revision transition delivered to one subscriber.
type Event struct {
	Revision uint64      // Revision is the complete authorizer view revision after this event.
	Previous swim.Member // Previous is the prior visible member, or zero for a new identity.
	Current  swim.Member // Current is the accepted monotonic member value.
	Cause    EventCause  // Cause distinguishes a delta from mandatory snapshot recovery.
}

// View is an independently owned complete membership authorization view.
type View struct {
	Revision uint64        // Revision monotonically identifies this complete view.
	Members  []swim.Member // Members is sorted by NodeID and owned by the caller.
}

type membershipSubscription interface {
	Events() <-chan swim.MembershipEvent
	Snapshot(context.Context) ([]swim.Member, error)
}

type membershipSource interface {
	Ready() <-chan struct{}
	Subscribe(context.Context, int) (membershipSubscription, error)
}

type swimSource struct{ service *swim.Service }

func (source swimSource) Ready() <-chan struct{} { return source.service.Ready() }
func (source swimSource) Subscribe(ctx context.Context, capacity int) (membershipSubscription, error) {
	return source.service.Subscribe(ctx, capacity)
}

type subscriber struct {
	events chan Event
	resync bool
}

// Authorizer owns a monotonic membership projection and bounded subscribers.
type Authorizer struct {
	configuration config.NodeConfig
	source        membershipSource
	addresses     *endpointauth.Matcher

	state atomic.Uint32
	ready chan struct{}
	done  chan struct{}

	mu       sync.RWMutex
	revision uint64
	members  map[uint16]swim.Member
	// floors retains the greatest observed version for every identity, even
	// when an exact replacement snapshot omits that identity.
	floors      map[uint16]swim.Member
	blocked     map[uint16]struct{}
	subscribers map[uint64]*subscriber
	nextID      uint64
}

// NewAuthorizer constructs a side-effect-free SWIM-backed authorizer.
func NewAuthorizer(configuration config.NodeConfig, service *swim.Service, resolver endpointauth.Resolver, sourceClock clock.Clock) (*Authorizer, error) {
	if service == nil {
		return nil, fmt.Errorf("%w: SWIM service is nil", ErrInvalidOptions)
	}
	return newAuthorizerWithSource(configuration, swimSource{service: service}, resolver, sourceClock)
}

func newAuthorizerWithSource(configuration config.NodeConfig, source membershipSource, resolver endpointauth.Resolver, sourceClock clock.Clock) (*Authorizer, error) {
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidOptions, err)
	}
	if source == nil {
		return nil, fmt.Errorf("%w: membership source is nil", ErrInvalidOptions)
	}
	if sourceClock == nil {
		return nil, fmt.Errorf("%w: clock is nil", ErrInvalidOptions)
	}
	return &Authorizer{
		configuration: configuration,
		source:        source,
		addresses:     endpointauth.NewMatcher(resolver, sourceClock, endpointauth.Options{}),
		ready:         make(chan struct{}),
		done:          make(chan struct{}),
		members:       make(map[uint16]swim.Member),
		floors:        make(map[uint16]swim.Member),
		blocked:       make(map[uint16]struct{}),
		subscribers:   make(map[uint64]*subscriber),
	}, nil
}

// Name returns the stable supervisor registration name.
func (authorizer *Authorizer) Name() string { return "crane-membership-authorizer" }

// Ready closes after a scoped upstream subscription has installed its first
// exact snapshot.
func (authorizer *Authorizer) Ready() <-chan struct{} {
	if authorizer == nil || authorizer.ready == nil {
		return closedSignal()
	}
	return authorizer.ready
}

// Run installs the initial exact view and then serially applies SWIM deltas.
func (authorizer *Authorizer) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run membership authorizer: nil context")
	}
	if authorizer == nil || !authorizer.state.CompareAndSwap(authorizerStateNew, authorizerStateRunning) {
		return ErrNotRunning
	}
	defer authorizer.stop()

	select {
	case <-authorizer.source.Ready():
	case <-ctx.Done():
		return nil
	}

	subscriptionContext, cancelSubscription := context.WithCancel(ctx)
	defer cancelSubscription()
	subscription, err := authorizer.source.Subscribe(subscriptionContext, upstreamSubscriptionCapacity)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("subscribe to SWIM membership: %w", err)
	}
	if subscription == nil {
		return errors.New("subscribe to SWIM membership: nil subscription")
	}
	snapshot, err := subscription.Snapshot(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("capture initial SWIM membership: %w", err)
	}
	if err := authorizer.installInitial(snapshot); err != nil {
		return fmt.Errorf("install initial SWIM membership: %w", err)
	}
	close(authorizer.ready)

	for {
		select {
		case event, open := <-subscription.Events():
			if !open {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("SWIM membership subscription closed")
			}
			if event.Cause == swim.EventResyncRequired {
				snapshot, err := subscription.Snapshot(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("resynchronize SWIM membership: %w", err)
				}
				if err := authorizer.replaceSnapshot(snapshot); err != nil {
					return fmt.Errorf("replace SWIM membership: %w", err)
				}
				continue
			}
			authorizer.apply(event)
		case <-ctx.Done():
			return nil
		}
	}
}

// View returns an owned, sorted membership snapshot.
func (authorizer *Authorizer) View() View {
	if authorizer == nil {
		return View{}
	}
	authorizer.mu.RLock()
	defer authorizer.mu.RUnlock()
	return authorizer.viewLocked()
}

// AuthorizeTCP accepts only the current Alive or Suspect member's numeric IP.
// The remote TCP source port is intentionally ephemeral and ignored.
func (authorizer *Authorizer) AuthorizeTCP(nodeID uint16, remote net.Addr) error {
	member, revision, ok := authorizer.activeMember(nodeID)
	if !ok || !authorizer.addresses.MatchTCP(context.Background(), remote, config.Endpoint{Host: member.Host, Port: member.BasePort}) {
		return ErrUnauthorized
	}
	if !authorizer.stillActive(nodeID, member, revision) {
		return ErrUnauthorized
	}
	return nil
}

// AuthorizeUDP accepts only the current Alive or Suspect member's numeric IP
// and exact derived UDP service source port.
func (authorizer *Authorizer) AuthorizeUDP(nodeID uint16, remote net.Addr, service config.Service) error {
	member, revision, ok := authorizer.activeMember(nodeID)
	if !ok {
		return ErrUnauthorized
	}
	spec, registered := config.LookupService(service)
	port := uint32(member.BasePort) + uint32(spec.Offset)
	if !registered || spec.Transport != config.TransportUDP || port == 0 || port > 65535 {
		return ErrUnauthorized
	}
	advertised := config.Endpoint{Host: member.Host, Port: uint16(port)}
	if !authorizer.addresses.MatchUDP(context.Background(), remote, advertised) || !authorizer.stillActive(nodeID, member, revision) {
		return ErrUnauthorized
	}
	return nil
}

// Subscribe returns a bounded delta stream with subscription-scoped recovery.
func (authorizer *Authorizer) Subscribe(ctx context.Context, capacity int) (*Subscription, error) {
	if ctx == nil {
		return nil, errors.New("subscribe to membership authorizer: nil context")
	}
	if capacity < 1 {
		capacity = 1
	}
	if capacity > maximumSubscriberCapacity {
		capacity = maximumSubscriberCapacity
	}
	if authorizer == nil {
		return nil, ErrNotRunning
	}
	authorizer.mu.Lock()
	if authorizer.state.Load() != authorizerStateRunning || authorizer.revision == 0 {
		authorizer.mu.Unlock()
		return nil, ErrNotRunning
	}
	authorizer.nextID++
	if authorizer.nextID == 0 {
		authorizer.nextID++
	}
	id := authorizer.nextID
	entry := &subscriber{events: make(chan Event, capacity)}
	authorizer.subscribers[id] = entry
	authorizer.mu.Unlock()

	subscription := &Subscription{authorizer: authorizer, id: id, events: entry.events}
	go func() {
		select {
		case <-ctx.Done():
		case <-authorizer.done:
		}
		authorizer.unsubscribe(id)
	}()
	return subscription, nil
}

// Subscription is one bounded authorizer stream and scoped recovery handle.
type Subscription struct {
	authorizer *Authorizer
	id         uint64
	events     <-chan Event
}

// Events returns the bounded stream, which closes on cancellation or stop.
func (subscription *Subscription) Events() <-chan Event {
	if subscription == nil || subscription.events == nil {
		events := make(chan Event)
		close(events)
		return events
	}
	return subscription.events
}

// Snapshot returns a complete owned view and acknowledges recovery only for
// this subscription.
func (subscription *Subscription) Snapshot(ctx context.Context) (View, error) {
	if ctx == nil {
		return View{}, errors.New("membership snapshot: nil context")
	}
	if err := ctx.Err(); err != nil {
		return View{}, err
	}
	if subscription == nil || subscription.authorizer == nil || subscription.id == 0 {
		return View{}, ErrSubscriptionClosed
	}
	return subscription.authorizer.subscriptionSnapshot(ctx, subscription.id)
}

func (authorizer *Authorizer) installInitial(snapshot []swim.Member) error {
	members, floors, err := validatedSnapshot(snapshot, nil)
	if err != nil {
		return err
	}
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	authorizer.members = members
	authorizer.floors = floors
	authorizer.revision = 1
	return nil
}

func (authorizer *Authorizer) replaceSnapshot(snapshot []swim.Member) error {
	authorizer.mu.RLock()
	retained := cloneMembers(authorizer.floors)
	oldMembers := cloneMembers(authorizer.members)
	authorizer.mu.RUnlock()

	members, floors, err := validatedSnapshot(snapshot, retained)
	if err != nil {
		return err
	}
	blocked := make(map[uint16]struct{}, len(oldMembers)+len(members))
	hosts := make(map[string]struct{}, len(oldMembers)+len(members))
	for nodeID, member := range oldMembers {
		blocked[nodeID] = struct{}{}
		hosts[member.Host] = struct{}{}
	}
	for nodeID, member := range members {
		blocked[nodeID] = struct{}{}
		hosts[member.Host] = struct{}{}
	}
	authorizer.mu.Lock()
	for nodeID := range blocked {
		authorizer.blocked[nodeID] = struct{}{}
	}
	authorizer.mu.Unlock()
	for host := range hosts {
		authorizer.addresses.Invalidate(host)
	}

	authorizer.mu.Lock()
	authorizer.members = members
	authorizer.floors = floors
	for nodeID := range blocked {
		delete(authorizer.blocked, nodeID)
	}
	authorizer.advanceRevisionLocked()
	authorizer.forceResyncLocked()
	authorizer.mu.Unlock()
	return nil
}

func (authorizer *Authorizer) apply(event swim.MembershipEvent) {
	incoming := event.Current
	if event.Cause != swim.EventMemberChanged || event.ReporterID == 0 || validateMember(incoming) != nil {
		return
	}
	authorizer.mu.Lock()
	previous, visible := authorizer.members[incoming.NodeID]
	reference, referenced := previous, visible
	if floor, exists := authorizer.floors[incoming.NodeID]; exists && (!referenced || compareMember(floor, reference) > 0) {
		reference, referenced = floor, true
	}
	if referenced {
		switch {
		case incoming.Incarnation < reference.Incarnation:
			authorizer.mu.Unlock()
			return
		case incoming.Incarnation == reference.Incarnation:
			if incoming.Status <= reference.Status {
				authorizer.mu.Unlock()
				return
			}
			incoming = reference
			incoming.Status = event.Current.Status
		}
	}
	authorizer.blocked[incoming.NodeID] = struct{}{}
	authorizer.mu.Unlock()

	if visible {
		authorizer.addresses.Invalidate(previous.Host)
	}
	authorizer.addresses.Invalidate(incoming.Host)

	authorizer.mu.Lock()
	authorizer.members[incoming.NodeID] = incoming
	if floor, exists := authorizer.floors[incoming.NodeID]; !exists || compareMember(incoming, floor) > 0 {
		authorizer.floors[incoming.NodeID] = incoming
	}
	delete(authorizer.blocked, incoming.NodeID)
	authorizer.advanceRevisionLocked()
	authorizer.publishLocked(Event{Revision: authorizer.revision, Previous: previous, Current: incoming, Cause: MemberChanged})
	authorizer.mu.Unlock()
}

func validatedSnapshot(snapshot []swim.Member, retained map[uint16]swim.Member) (map[uint16]swim.Member, map[uint16]swim.Member, error) {
	members := make(map[uint16]swim.Member, len(snapshot))
	floors := cloneMembers(retained)
	seen := make(map[uint16]struct{}, len(snapshot))
	for _, member := range snapshot {
		if err := validateMember(member); err != nil {
			return nil, nil, err
		}
		if _, duplicate := seen[member.NodeID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate member node ID %d", member.NodeID)
		}
		seen[member.NodeID] = struct{}{}
		if floor, exists := floors[member.NodeID]; exists {
			switch {
			case member.Incarnation < floor.Incarnation:
				continue
			case member.Incarnation == floor.Incarnation:
				if member.Status < floor.Status {
					continue
				}
				canonical := floor
				canonical.Status = member.Status
				member = canonical
			}
			if compareMember(member, floor) < 0 {
				continue
			}
		}
		members[member.NodeID] = member
		if floor, exists := floors[member.NodeID]; !exists || compareMember(member, floor) > 0 {
			floors[member.NodeID] = member
		}
	}
	return members, floors, nil
}

func validateMember(member swim.Member) error {
	if member.NodeID == 0 || member.Incarnation == 0 {
		return errors.New("member identity and incarnation must be nonzero")
	}
	if member.Status != swim.Alive && member.Status != swim.Suspect && member.Status != swim.Dead && member.Status != swim.Left {
		return fmt.Errorf("member %d has invalid status", member.NodeID)
	}
	last, ok := config.LookupService(config.ServiceRaftRPC)
	if !ok || uint32(member.BasePort)+uint32(last.Offset) > 65535 {
		return fmt.Errorf("member %d base port cannot derive all services", member.NodeID)
	}
	endpoint := config.Endpoint{Host: member.Host, Port: member.BasePort}
	if _, err := config.ParseRoutableEndpoint(endpoint.String()); err != nil {
		return fmt.Errorf("member %d endpoint: %w", member.NodeID, err)
	}
	return nil
}

func (authorizer *Authorizer) activeMember(nodeID uint16) (swim.Member, uint64, bool) {
	if authorizer == nil || authorizer.state.Load() != authorizerStateRunning {
		return swim.Member{}, 0, false
	}
	authorizer.mu.RLock()
	defer authorizer.mu.RUnlock()
	member, exists := authorizer.members[nodeID]
	_, blocked := authorizer.blocked[nodeID]
	return member, authorizer.revision, exists && !blocked && authorizer.revision != 0 && active(member.Status)
}

func (authorizer *Authorizer) stillActive(nodeID uint16, expected swim.Member, revision uint64) bool {
	authorizer.mu.RLock()
	defer authorizer.mu.RUnlock()
	current, exists := authorizer.members[nodeID]
	_, blocked := authorizer.blocked[nodeID]
	return exists && !blocked && authorizer.revision == revision && current == expected && active(current.Status)
}

func (authorizer *Authorizer) viewLocked() View {
	view := View{Revision: authorizer.revision, Members: make([]swim.Member, 0, len(authorizer.members))}
	for _, member := range authorizer.members {
		view.Members = append(view.Members, member)
	}
	sort.Slice(view.Members, func(left, right int) bool { return view.Members[left].NodeID < view.Members[right].NodeID })
	return view
}

func (authorizer *Authorizer) subscriptionSnapshot(ctx context.Context, id uint64) (View, error) {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return View{}, err
	}
	subscriber, exists := authorizer.subscribers[id]
	if !exists {
		return View{}, ErrSubscriptionClosed
	}
	drain(subscriber.events)
	view := authorizer.viewLocked()
	subscriber.resync = false
	return view, nil
}

func (authorizer *Authorizer) advanceRevisionLocked() {
	authorizer.revision++
	if authorizer.revision == 0 {
		authorizer.revision++
	}
}

func (authorizer *Authorizer) publishLocked(event Event) {
	for _, subscriber := range authorizer.subscribers {
		if subscriber.resync {
			continue
		}
		select {
		case subscriber.events <- event:
		default:
			drain(subscriber.events)
			subscriber.events <- Event{Revision: authorizer.revision, Cause: ResyncRequired}
			subscriber.resync = true
		}
	}
}

func (authorizer *Authorizer) forceResyncLocked() {
	for _, subscriber := range authorizer.subscribers {
		drain(subscriber.events)
		subscriber.events <- Event{Revision: authorizer.revision, Cause: ResyncRequired}
		subscriber.resync = true
	}
}

func (authorizer *Authorizer) unsubscribe(id uint64) {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	if subscriber, exists := authorizer.subscribers[id]; exists {
		delete(authorizer.subscribers, id)
		close(subscriber.events)
	}
}

func (authorizer *Authorizer) stop() {
	authorizer.state.Store(authorizerStateStopped)
	close(authorizer.done)
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	for id, subscriber := range authorizer.subscribers {
		delete(authorizer.subscribers, id)
		close(subscriber.events)
	}
}

func cloneMembers(source map[uint16]swim.Member) map[uint16]swim.Member {
	result := make(map[uint16]swim.Member, len(source))
	for nodeID, member := range source {
		result[nodeID] = member
	}
	return result
}

func compareMember(left, right swim.Member) int {
	if left.Incarnation < right.Incarnation {
		return -1
	}
	if left.Incarnation > right.Incarnation {
		return 1
	}
	if left.Status < right.Status {
		return -1
	}
	if left.Status > right.Status {
		return 1
	}
	return 0
}

func active(status swim.Status) bool { return status == swim.Alive || status == swim.Suspect }
func drain(events chan Event) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}

func closedSignal() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
