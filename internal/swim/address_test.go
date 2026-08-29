package swim

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
)

func TestAddressMatcherBoundsAndReusesResolvedAddressCache(t *testing.T) {
	resolver := &staticAddressResolver{addresses: map[string][]netip.Addr{
		"one.test":   {netip.MustParseAddr("192.0.2.1")},
		"two.test":   {netip.MustParseAddr("192.0.2.2")},
		"three.test": {netip.MustParseAddr("192.0.2.3")},
	}}
	matcher := newAddressMatcher(resolver)
	matcher.capacity = 2
	for index, host := range []string{"one.test", "two.test", "three.test"} {
		source := config.Endpoint{Host: "192.0.2." + string(rune('1'+index)), Port: 9000}
		advertised := config.Endpoint{Host: host, Port: 9000}
		if !matcher.matchesSource(context.Background(), source, advertised) {
			t.Fatalf("resolved source %s did not match %s", source, advertised)
		}
	}
	matcher.mu.Lock()
	cacheEntries := len(matcher.cache)
	matcher.mu.Unlock()
	if cacheEntries != 2 {
		t.Fatalf("resolved cache entries = %d, want capacity 2", cacheEntries)
	}
	if !matcher.matchesSource(context.Background(), config.Endpoint{Host: "192.0.2.1", Port: 9000}, config.Endpoint{Host: "one.test", Port: 9000}) {
		t.Fatal("evicted address did not resolve again")
	}
	if calls := resolver.callCount("one.test"); calls != 2 {
		t.Fatalf("evicted host lookup calls = %d, want 2", calls)
	}
}

func TestAddressMatcherBoundsResolverLatency(t *testing.T) {
	matcher := newAddressMatcher(blockingAddressResolver{})
	matcher.timeout = 10 * time.Millisecond
	started := time.Now()
	matched := matcher.matchesSource(context.Background(),
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
