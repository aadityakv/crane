package swim

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
)

func TestAddressMatcherExpiresPositiveAndNegativeDNSCacheEntries(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(9000, 0))
	resolver := &mutableAddressResolver{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.1")}}
	matcher := newAddressMatcherWithClock(resolver, manualClock)
	advertised := config.Endpoint{Host: "peer.test", Port: 9000}

	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.1", Port: 9000}, advertised) {
		t.Fatal("initial DNS answer did not match")
	}
	resolver.set([]netip.Addr{netip.MustParseAddr("192.0.2.2")}, nil)
	if matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.2", Port: 9000}, advertised) {
		t.Fatal("positive cache refreshed before its TTL")
	}
	manualClock.Advance(30 * time.Second)
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.2", Port: 9000}, advertised) {
		t.Fatal("positive cache did not refresh after its TTL")
	}

	resolver.set(nil, errors.New("temporary DNS failure"))
	matcher.invalidate("peer.test")
	if matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.2", Port: 9000}, advertised) {
		t.Fatal("negative DNS answer unexpectedly matched")
	}
	firstNegativeCalls := resolver.callCount()
	if matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.2", Port: 9000}, advertised) {
		t.Fatal("cached negative DNS answer unexpectedly matched")
	}
	if got := resolver.callCount(); got != firstNegativeCalls {
		t.Fatalf("negative cache lookup calls = %d, want %d", got, firstNegativeCalls)
	}
	resolver.set([]netip.Addr{netip.MustParseAddr("192.0.2.3")}, nil)
	manualClock.Advance(time.Second)
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.3", Port: 9000}, advertised) {
		t.Fatal("negative cache did not retry after its bounded TTL")
	}
}

func TestServiceHigherIncarnationInvalidatesDNSGeneration(t *testing.T) {
	manualClock := clock.NewManual(time.Unix(9010, 0))
	resolver := &mutableAddressResolver{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.10")}}
	matcher := newAddressMatcherWithClock(resolver, manualClock)
	advertised := config.Endpoint{Host: "peer.test", Port: 12000}
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.10", Port: 12000}, advertised) {
		t.Fatal("initial generation DNS answer did not match")
	}
	resolver.set([]netip.Addr{netip.MustParseAddr("192.0.2.11")}, nil)

	previous := Member{NodeID: 2, Host: "peer.test", BasePort: 12000, Incarnation: 4, Status: Alive}
	current := previous
	current.Incarnation = 5
	service := &Service{options: ServiceOptions{Clock: manualClock}, addresses: matcher}
	loop := &serviceLoop{
		service:       service,
		engine:        &Engine{table: NewTable()},
		dissemination: NewDisseminator(8, 1),
		subscriptions: NewSubscriptions(),
		runContext:    context.Background(),
	}
	defer loop.subscriptions.Close()
	if err := loop.executeEffects(context.Background(), Effects{Events: []MembershipEvent{{Previous: previous, Current: current, Cause: EventMemberChanged}}}); err != nil {
		t.Fatal(err)
	}
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.11", Port: 12000}, advertised) {
		t.Fatal("higher-incarnation rejoin retained the prior DNS generation")
	}
}

func TestAddressMatcherReusesResolvedAddressCache(t *testing.T) {
	resolver := &staticAddressResolver{addresses: map[string][]netip.Addr{
		"one.test":   {netip.MustParseAddr("192.0.2.1")},
		"two.test":   {netip.MustParseAddr("192.0.2.2")},
		"three.test": {netip.MustParseAddr("192.0.2.3")},
	}}
	matcher := newAddressMatcher(resolver)
	for index, host := range []string{"one.test", "two.test", "three.test"} {
		source := config.Endpoint{Host: "192.0.2." + string(rune('1'+index)), Port: 9000}
		advertised := config.Endpoint{Host: host, Port: 9000}
		if !matcher.matchesSource(context.Background(), source, advertised) {
			t.Fatalf("resolved source %s did not match %s", source, advertised)
		}
	}
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.1", Port: 9000}, config.Endpoint{Host: "one.test", Port: 9000}) {
		t.Fatal("cached address did not match again")
	}
	if calls := resolver.callCount("one.test"); calls != 1 {
		t.Fatalf("cached host lookup calls = %d, want 1", calls)
	}
}

func TestAddressMatcherBoundsResolverLatency(t *testing.T) {
	matcher := newAddressMatcher(blockingAddressResolver{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	matched := matcher.matchesSource(ctx,
		config.Endpoint{Host: "192.0.2.1", Port: 9000},
		config.Endpoint{Host: "blocked.test", Port: 9000},
	)
	if matched {
		t.Fatal("blocked resolver unexpectedly matched")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("resolver cancellation took %v, want bounded latency", elapsed)
	}
}

type blockingAddressResolver struct{}

func (blockingAddressResolver) LookupNetIP(ctx context.Context, _, _ string) ([]netip.Addr, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type mutableAddressResolver struct {
	mu        sync.Mutex
	addresses []netip.Addr
	err       error
	calls     int
}

func (r *mutableAddressResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return append([]netip.Addr(nil), r.addresses...), r.err
}

func (r *mutableAddressResolver) set(addresses []netip.Addr, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addresses = append([]netip.Addr(nil), addresses...)
	r.err = err
}

func (r *mutableAddressResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}
