package membership

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/endpointauth"
	"github.com/aaditya/cs425mp3/internal/swim"
)

func TestAuthorizerRequiresExactInitialSnapshotAndPublishesOwnedMonotonicViews(t *testing.T) {
	configuration := authorizerTestConfig(t)
	ready := make(chan struct{})
	close(ready)
	initialGate := make(chan struct{})
	subscription := &fakeMembershipSubscription{
		events: make(chan swim.MembershipEvent, 32), gate: initialGate,
		snapshot: []swim.Member{
			{NodeID: 4, Host: "left.test", BasePort: 9400, Incarnation: 2, Status: swim.Left},
			{NodeID: 2, Host: "suspect.test", BasePort: 9200, Incarnation: 3, Status: swim.Suspect},
			{NodeID: 1, Host: "peer.test", BasePort: 9100, Incarnation: 5, Status: swim.Alive},
			{NodeID: 3, Host: "dead.test", BasePort: 9300, Incarnation: 7, Status: swim.Dead},
		},
	}
	source := &fakeMembershipSource{ready: ready, subscription: subscription}
	resolver := &staticResolver{answers: map[string][]netip.Addr{
		"peer.test":    {netip.MustParseAddr("127.0.0.2")},
		"suspect.test": {netip.MustParseAddr("127.0.0.3")},
		"revived.test": {netip.MustParseAddr("127.0.0.4")},
	}}
	authorizer, err := newAuthorizerWithSource(configuration, source, resolver, clock.NewManual(time.Unix(3000, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if authorizer.Name() != "crane-membership-authorizer" {
		t.Fatalf("Name=%q", authorizer.Name())
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- authorizer.Run(ctx) }()

	select {
	case <-authorizer.Ready():
		t.Fatal("authorizer became ready before exact initial snapshot")
	default:
	}
	if err := authorizer.AuthorizeTCP(1, &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 40000}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("pre-ready authorization error=%v", err)
	}
	close(initialGate)
	waitAuthorizerReady(t, authorizer)

	view := authorizer.View()
	if view.Revision != 1 || len(view.Members) != 4 {
		t.Fatalf("initial view=%#v", view)
	}
	for index, member := range view.Members {
		if member.NodeID != uint16(index+1) {
			t.Fatalf("view not sorted: %#v", view.Members)
		}
	}
	view.Members[0].Host = "mutated"
	if authorizer.View().Members[0].Host != "peer.test" {
		t.Fatal("View exposed mutable membership")
	}

	if err := authorizer.AuthorizeTCP(1, &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 65500}); err != nil {
		t.Fatalf("Alive TCP authorization: %v", err)
	}
	if err := authorizer.AuthorizeTCP(2, &net.TCPAddr{IP: net.ParseIP("127.0.0.3"), Port: 1}); err != nil {
		t.Fatalf("Suspect TCP authorization: %v", err)
	}
	for _, nodeID := range []uint16{3, 4, 99} {
		if err := authorizer.AuthorizeTCP(nodeID, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("terminal/unknown node %d error=%v", nodeID, err)
		}
	}
	tuplePort := uint16(9100 + 7)
	if err := authorizer.AuthorizeUDP(1, &net.UDPAddr{IP: net.ParseIP("127.0.0.2"), Port: int(tuplePort)}, config.ServiceCraneTupleACK); err != nil {
		t.Fatalf("exact UDP source: %v", err)
	}
	if err := authorizer.AuthorizeUDP(1, &net.UDPAddr{IP: net.ParseIP("127.0.0.2"), Port: int(tuplePort + 1)}, config.ServiceCraneTupleACK); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong UDP source port error=%v", err)
	}

	previousDead := swim.Member{NodeID: 3, Host: "dead.test", BasePort: 9300, Incarnation: 7, Status: swim.Dead}
	revived := swim.Member{NodeID: 3, Host: "revived.test", BasePort: 9300, Incarnation: 8, Status: swim.Alive}
	subscription.events <- swim.MembershipEvent{Previous: previousDead, Current: revived, Cause: swim.EventMemberChanged, ReporterID: 1}
	waitAuthorizerRevision(t, authorizer, 2)
	if err := authorizer.AuthorizeTCP(3, &net.TCPAddr{IP: net.ParseIP("127.0.0.4"), Port: 12}); err != nil {
		t.Fatalf("higher-incarnation revival denied: %v", err)
	}
	regressed := revived
	regressed.Incarnation = 7
	subscription.events <- swim.MembershipEvent{Previous: revived, Current: regressed, Cause: swim.EventMemberChanged, ReporterID: 1}
	revivedSuspect := revived
	revivedSuspect.Status = swim.Suspect
	subscription.events <- swim.MembershipEvent{Previous: revived, Current: revivedSuspect, Cause: swim.EventMemberChanged, ReporterID: 1}
	waitAuthorizerRevision(t, authorizer, 3)
	if got := authorizer.View(); got.Revision != 3 || got.Members[2] != revivedSuspect {
		t.Fatalf("regressed event changed monotonic progression: %#v", got)
	}
	terminal := authorizer.View().Members[0]
	terminal.Status = swim.Dead
	subscription.events <- swim.MembershipEvent{Previous: authorizer.View().Members[0], Current: terminal, Cause: swim.EventMemberChanged, ReporterID: 2}
	waitAuthorizerRevision(t, authorizer, 4)
	if err := authorizer.AuthorizeTCP(1, &net.TCPAddr{IP: net.ParseIP("127.0.0.2")}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("terminal member authorization error=%v", err)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run cancellation: %v", err)
	}
}

func TestAuthorizerBoundedSubscriptionsRequireScopedSnapshotAfterOverflowAndUpstreamResync(t *testing.T) {
	configuration := authorizerTestConfig(t)
	ready := make(chan struct{})
	close(ready)
	upstream := &fakeMembershipSubscription{
		events:   make(chan swim.MembershipEvent, 32),
		gate:     closedChannel(),
		snapshot: []swim.Member{{NodeID: 1, Host: "127.0.0.1", BasePort: 9100, Incarnation: 1, Status: swim.Alive}},
	}
	authorizer, err := newAuthorizerWithSource(configuration, &fakeMembershipSource{ready: ready, subscription: upstream}, &staticResolver{}, clock.NewManual(time.Unix(4000, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- authorizer.Run(ctx) }()
	waitAuthorizerReady(t, authorizer)

	subCtx, subCancel := context.WithCancel(context.Background())
	subscription, err := authorizer.Subscribe(subCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	second := swim.Member{NodeID: 2, Host: "127.0.0.2", BasePort: 9200, Incarnation: 1, Status: swim.Alive}
	third := swim.Member{NodeID: 3, Host: "127.0.0.3", BasePort: 9300, Incarnation: 1, Status: swim.Alive}
	upstream.events <- swim.MembershipEvent{Current: second, Cause: swim.EventMemberChanged, ReporterID: 1}
	upstream.events <- swim.MembershipEvent{Current: third, Cause: swim.EventMemberChanged, ReporterID: 1}
	waitAuthorizerRevision(t, authorizer, 3)
	event := receiveAuthorizerEvent(t, subscription.Events())
	if event.Cause != ResyncRequired || event.Revision != 3 {
		t.Fatalf("overflow event=%#v", event)
	}
	view, err := subscription.Snapshot(context.Background())
	if err != nil || view.Revision != 3 || len(view.Members) != 3 {
		t.Fatalf("scoped snapshot=%#v err=%v", view, err)
	}
	fourth := swim.Member{NodeID: 4, Host: "127.0.0.4", BasePort: 9400, Incarnation: 1, Status: swim.Suspect}
	upstream.events <- swim.MembershipEvent{Current: fourth, Cause: swim.EventMemberChanged, ReporterID: 1}
	if event := receiveAuthorizerEvent(t, subscription.Events()); event.Cause != MemberChanged || event.Current != fourth || event.Revision != 4 {
		t.Fatalf("post-resync delta=%#v", event)
	}

	upstream.setSnapshot([]swim.Member{{NodeID: 5, Host: "127.0.0.5", BasePort: 9500, Incarnation: 9, Status: swim.Alive}})
	upstream.events <- swim.MembershipEvent{Cause: swim.EventResyncRequired}
	waitAuthorizerRevision(t, authorizer, 5)
	if event := receiveAuthorizerEvent(t, subscription.Events()); event.Cause != ResyncRequired || event.Revision != 5 {
		t.Fatalf("upstream resync event=%#v", event)
	}
	resynced, err := subscription.Snapshot(context.Background())
	if err != nil || len(resynced.Members) != 1 || resynced.Members[0].NodeID != 5 {
		t.Fatalf("upstream replacement snapshot=%#v err=%v", resynced, err)
	}
	staleFirst := swim.Member{NodeID: 1, Host: "127.0.0.10", BasePort: 9100, Incarnation: 1, Status: swim.Alive}
	freshFirst := staleFirst
	freshFirst.Incarnation = 2
	upstream.events <- swim.MembershipEvent{Current: staleFirst, Cause: swim.EventMemberChanged, ReporterID: 5}
	upstream.events <- swim.MembershipEvent{Current: freshFirst, Cause: swim.EventMemberChanged, ReporterID: 5}
	if event := receiveAuthorizerEvent(t, subscription.Events()); event.Cause != MemberChanged || event.Current != freshFirst || event.Revision != 6 {
		t.Fatalf("post-resync monotonic delta=%#v", event)
	}
	if view := authorizer.View(); view.Revision != 6 || len(view.Members) != 2 {
		t.Fatalf("regressed omitted generation changed view: %#v", view)
	}

	subCancel()
	select {
	case _, open := <-subscription.Events():
		if open {
			t.Fatal("subscription channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription cancellation did not close events")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run cancellation: %v", err)
	}
}

func TestAuthorizerRetainsTerminalFloorAcrossTombstoneResync(t *testing.T) {
	configuration := authorizerTestConfig(t)
	upstream := &fakeMembershipSubscription{
		events: make(chan swim.MembershipEvent, 8), gate: closedChannel(),
		snapshot: []swim.Member{{NodeID: 3, Host: "127.0.0.3", BasePort: 9300, Incarnation: 7, Status: swim.Dead}},
	}
	authorizer, err := newAuthorizerWithSource(configuration, &fakeMembershipSource{ready: closedChannel(), subscription: upstream}, &staticResolver{}, clock.NewManual(time.Unix(5000, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- authorizer.Run(ctx) }()
	waitAuthorizerReady(t, authorizer)

	upstream.setSnapshot(nil)
	upstream.events <- swim.MembershipEvent{Cause: swim.EventResyncRequired}
	waitAuthorizerRevision(t, authorizer, 2)
	stale := swim.Member{NodeID: 3, Host: "127.0.0.30", BasePort: 9300, Incarnation: 7, Status: swim.Alive}
	fresh := stale
	fresh.Incarnation = 8
	upstream.events <- swim.MembershipEvent{Current: stale, Cause: swim.EventMemberChanged, ReporterID: 1}
	upstream.events <- swim.MembershipEvent{Current: fresh, Cause: swim.EventMemberChanged, ReporterID: 1}
	waitAuthorizerRevision(t, authorizer, 3)
	view := authorizer.View()
	if view.Revision != 3 || len(view.Members) != 1 || view.Members[0] != fresh {
		t.Fatalf("terminal floor did not reject stale resurrection: %#v", view)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run cancellation: %v", err)
	}
}

func TestSubscriptionSnapshotProactivelyDrainsStaleRecoveryMarker(t *testing.T) {
	configuration := authorizerTestConfig(t)
	upstream := &fakeMembershipSubscription{
		events: make(chan swim.MembershipEvent, 8), gate: closedChannel(),
		snapshot: []swim.Member{{NodeID: 1, Host: "127.0.0.1", BasePort: 9100, Incarnation: 1, Status: swim.Alive}},
	}
	authorizer, err := newAuthorizerWithSource(configuration, &fakeMembershipSource{ready: closedChannel(), subscription: upstream}, &staticResolver{}, clock.NewManual(time.Unix(5500, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- authorizer.Run(ctx) }()
	waitAuthorizerReady(t, authorizer)
	subscription, err := authorizer.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for nodeID := uint16(2); nodeID <= 3; nodeID++ {
		member := swim.Member{NodeID: nodeID, Host: "127.0.0." + string(rune('0'+nodeID)), BasePort: 9100 + nodeID*100, Incarnation: 1, Status: swim.Alive}
		upstream.events <- swim.MembershipEvent{Current: member, Cause: swim.EventMemberChanged, ReporterID: 1}
	}
	waitAuthorizerRevision(t, authorizer, 3)
	view, err := subscription.Snapshot(context.Background())
	if err != nil || view.Revision != 3 {
		t.Fatalf("proactive snapshot=%#v err=%v", view, err)
	}
	select {
	case event := <-subscription.Events():
		t.Fatalf("stale queued event remained after proactive snapshot: %#v", event)
	default:
	}
	fourth := swim.Member{NodeID: 4, Host: "127.0.0.4", BasePort: 9500, Incarnation: 1, Status: swim.Alive}
	upstream.events <- swim.MembershipEvent{Current: fourth, Cause: swim.EventMemberChanged, ReporterID: 1}
	if event := receiveAuthorizerEvent(t, subscription.Events()); event.Revision != 4 || event.Cause != MemberChanged || event.Current != fourth {
		t.Fatalf("post-proactive-snapshot event=%#v", event)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run cancellation: %v", err)
	}
}

func TestAuthorizerRejectsInvalidExactInitialSnapshotWithoutReadiness(t *testing.T) {
	var nilAuthorizer *Authorizer
	if _, err := nilAuthorizer.Subscribe(context.Background(), 1); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("nil authorizer Subscribe error=%v, want ErrNotRunning", err)
	}

	configuration := authorizerTestConfig(t)
	upstream := &fakeMembershipSubscription{
		events: make(chan swim.MembershipEvent), gate: closedChannel(),
		snapshot: []swim.Member{{NodeID: 2, Host: "127.0.0.2", BasePort: 9200, Incarnation: 1, Status: swim.Status(255)}},
	}
	authorizer, err := newAuthorizerWithSource(configuration, &fakeMembershipSource{ready: closedChannel(), subscription: upstream}, &staticResolver{}, clock.NewManual(time.Unix(6000, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Run(context.Background()); err == nil {
		t.Fatal("invalid exact snapshot unexpectedly started authorizer")
	}
	select {
	case <-authorizer.Ready():
		t.Fatal("invalid exact snapshot closed readiness")
	default:
	}
}

func TestAuthorizerConcurrentViewsAuthorizationAndMembershipChanges(t *testing.T) {
	configuration := authorizerTestConfig(t)
	upstream := &fakeMembershipSubscription{
		events: make(chan swim.MembershipEvent, 128), gate: closedChannel(),
		snapshot: []swim.Member{{NodeID: 2, Host: "127.0.0.2", BasePort: 9200, Incarnation: 1, Status: swim.Alive}},
	}
	authorizer, err := newAuthorizerWithSource(configuration, &fakeMembershipSource{ready: closedChannel(), subscription: upstream}, &staticResolver{}, clock.NewManual(time.Unix(6500, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- authorizer.Run(ctx) }()
	waitAuthorizerReady(t, authorizer)

	failures := make(chan error, 8)
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 500 {
				view := authorizer.View()
				if len(view.Members) != 1 {
					failures <- fmt.Errorf("concurrent view has %d members", len(view.Members))
					return
				}
				view.Members[0].Host = "caller-owned"
				err := authorizer.AuthorizeTCP(2, &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 49152})
				if err != nil && !errors.Is(err, ErrUnauthorized) {
					failures <- err
					return
				}
			}
		}()
	}
	for incarnation := uint64(2); incarnation <= 101; incarnation++ {
		status := swim.Alive
		if incarnation%2 == 0 {
			status = swim.Suspect
		}
		upstream.events <- swim.MembershipEvent{
			Current:    swim.Member{NodeID: 2, Host: "127.0.0.2", BasePort: 9200, Incarnation: incarnation, Status: status},
			Cause:      swim.EventMemberChanged,
			ReporterID: 1,
		}
	}
	waitAuthorizerRevision(t, authorizer, 101)
	readers.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run cancellation: %v", err)
	}
}

type fakeMembershipSource struct {
	ready        <-chan struct{}
	subscription *fakeMembershipSubscription
}

func (s *fakeMembershipSource) Ready() <-chan struct{} { return s.ready }
func (s *fakeMembershipSource) Subscribe(context.Context, int) (membershipSubscription, error) {
	return s.subscription, nil
}

type fakeMembershipSubscription struct {
	mu       sync.Mutex
	events   chan swim.MembershipEvent
	gate     <-chan struct{}
	snapshot []swim.Member
}

func (s *fakeMembershipSubscription) Events() <-chan swim.MembershipEvent { return s.events }
func (s *fakeMembershipSubscription) Snapshot(ctx context.Context) ([]swim.Member, error) {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]swim.Member(nil), s.snapshot...), nil
}
func (s *fakeMembershipSubscription) setSnapshot(members []swim.Member) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = append([]swim.Member(nil), members...)
}

type staticResolver struct{ answers map[string][]netip.Addr }

func (r *staticResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), r.answers[host]...), nil
}

func authorizerTestConfig(t *testing.T) config.NodeConfig {
	t.Helper()
	secret := t.TempDir() + "/cluster.secret"
	if err := os.WriteFile(secret, bytes.Repeat([]byte{1}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration := config.NodeConfig{
		NodeID: 1, ClusterID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1",
		BasePort: 9100, Introducer: "127.0.0.1:9102", StorageDir: t.TempDir(), ClusterSecretFile: secret,
		RaftVoters: []config.RaftVoter{{NodeID: 1, Endpoint: "127.0.0.1:9108"}, {NodeID: 2, Endpoint: "127.0.0.2:9208"}, {NodeID: 3, Endpoint: "127.0.0.3:9308"}},
		Raft:       config.DefaultRaftConfig(), Crane: config.DefaultCraneConfig(), Timing: config.DefaultTimingConfig(),
	}
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	return configuration
}

func closedChannel() <-chan struct{} {
	result := make(chan struct{})
	close(result)
	return result
}

func waitAuthorizerReady(t *testing.T, authorizer *Authorizer) {
	t.Helper()
	select {
	case <-authorizer.Ready():
	case <-time.After(time.Second):
		t.Fatal("authorizer did not become ready")
	}
}

func waitAuthorizerRevision(t *testing.T, authorizer *Authorizer, revision uint64) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if authorizer.View().Revision >= revision {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("authorizer revision=%d want >=%d", authorizer.View().Revision, revision)
		default:
		}
	}
}

func receiveAuthorizerEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authorizer event")
		return Event{}
	}
}

var _ endpointauth.Resolver = (*staticResolver)(nil)
