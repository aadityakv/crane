package endpointauth

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
)

func TestAddressMatcherCanonicalAliasesMultipleAnswersAndTransportPorts(t *testing.T) {
	manual := clock.NewManual(time.Unix(1000, 0))
	resolver := &mutableResolver{answers: map[string][]netip.Addr{
		"Peer.Example.": {netip.MustParseAddr("2001:db8::2"), netip.MustParseAddr("::ffff:192.0.2.7"), netip.MustParseAddr("192.0.2.7")},
	}}
	matcher := NewMatcher(resolver, manual, Options{})
	advertised := config.Endpoint{Host: "Peer.Example.", Port: 9007}

	if !matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: 49152}, advertised) {
		t.Fatal("TCP did not match one of multiple DNS answers while ignoring ephemeral source port")
	}
	if !matcher.MatchUDP(context.Background(), &net.UDPAddr{IP: net.ParseIP("2001:db8::2"), Port: 9007}, advertised) {
		t.Fatal("UDP did not match IPv6 answer and exact service port")
	}
	if matcher.MatchUDP(context.Background(), &net.UDPAddr{IP: net.ParseIP("192.0.2.7"), Port: 9008}, advertised) {
		t.Fatal("UDP accepted a non-service source port")
	}
	if !matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.7"), Port: 1}, config.Endpoint{Host: "peer.example", Port: 9007}) {
		t.Fatal("case/root-dot alias did not reuse canonical DNS identity or IPv4 unmapping")
	}
	if calls := resolver.Calls(); calls != 1 {
		t.Fatalf("canonical aliases caused %d DNS calls, want 1", calls)
	}
	if matcher.MatchTCP(context.Background(), hostnameAddr("peer.example:4000"), advertised) {
		t.Fatal("matcher used a hostname/reverse-DNS source identity")
	}
}

func TestAddressMatcherCacheExpiresOnlyOnInjectedClockAndInvalidation(t *testing.T) {
	manual := clock.NewManual(time.Unix(2000, 0))
	resolver := &mutableResolver{answers: map[string][]netip.Addr{"peer.test": {netip.MustParseAddr("192.0.2.1")}}}
	matcher := NewMatcher(resolver, manual, Options{PositiveTTL: time.Second, NegativeTTL: 100 * time.Millisecond, CacheEntries: 2})
	advertised := config.Endpoint{Host: "peer.test", Port: 7000}
	match := func(ip string) bool {
		return matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP(ip), Port: 1}, advertised)
	}
	if !match("192.0.2.1") {
		t.Fatal("initial address did not match")
	}
	resolver.Set("peer.test", []netip.Addr{netip.MustParseAddr("192.0.2.2")}, nil)
	if match("192.0.2.2") {
		t.Fatal("cache expired without injected-clock advancement")
	}
	manual.Advance(time.Second)
	if !match("192.0.2.2") {
		t.Fatal("cache did not expire at injected deadline")
	}

	resolver.Set("peer.test", nil, errors.New("dns unavailable"))
	matcher.Invalidate("PEER.TEST.")
	if match("192.0.2.2") {
		t.Fatal("negative lookup matched")
	}
	calls := resolver.Calls()
	if match("192.0.2.2") || resolver.Calls() != calls {
		t.Fatal("negative cache was not reused")
	}
	resolver.Set("peer.test", []netip.Addr{netip.MustParseAddr("192.0.2.3")}, nil)
	manual.Advance(100 * time.Millisecond)
	if !match("192.0.2.3") {
		t.Fatal("negative cache did not expire on injected clock")
	}
}

func TestAddressMatcherBoundsCacheByCanonicalIdentity(t *testing.T) {
	resolver := &mutableResolver{answers: map[string][]netip.Addr{
		"one.test":   {netip.MustParseAddr("192.0.2.1")},
		"two.test":   {netip.MustParseAddr("192.0.2.2")},
		"three.test": {netip.MustParseAddr("192.0.2.3")},
	}}
	matcher := NewMatcher(resolver, clock.NewManual(time.Unix(3000, 0)), Options{CacheEntries: 2})
	for index, host := range []string{"one.test", "two.test", "three.test"} {
		remote := &net.TCPAddr{IP: net.ParseIP("192.0.2." + string(rune('1'+index))), Port: 1}
		if !matcher.MatchTCP(context.Background(), remote, config.Endpoint{Host: host, Port: 7000}) {
			t.Fatalf("host %s did not resolve", host)
		}
	}
	if !matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}, config.Endpoint{Host: "ONE.TEST.", Port: 7000}) {
		t.Fatal("evicted canonical identity did not resolve again")
	}
	if calls := resolver.Calls(); calls != 4 {
		t.Fatalf("resolver calls = %d, want 4 after LRU eviction", calls)
	}
}

func TestAddressMatcherInvalidationFencesConcurrentLookupCaching(t *testing.T) {
	tests := []struct {
		name      string
		first     []netip.Addr
		firstErr  error
		firstWant bool
	}{
		{name: "positive", first: []netip.Addr{netip.MustParseAddr("192.0.2.1")}, firstWant: true},
		{name: "negative", firstErr: errors.New("stale DNS failure")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &delayedResolver{
				first: test.first, firstErr: test.firstErr,
				next:    []netip.Addr{netip.MustParseAddr("192.0.2.2")},
				started: make(chan struct{}), release: make(chan struct{}),
			}
			matcher := NewMatcher(resolver, clock.NewManual(time.Unix(3500, 0)), Options{})
			advertised := config.Endpoint{Host: "peer.test", Port: 7000}
			result := make(chan bool, 1)
			go func() {
				result <- matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}, advertised)
			}()
			select {
			case <-resolver.started:
			case <-time.After(time.Second):
				t.Fatal("first DNS lookup did not start")
			}
			matcher.Invalidate("PEER.TEST.")
			close(resolver.release)
			if got := <-result; got != test.firstWant {
				t.Fatalf("in-flight caller result=%t want %t", got, test.firstWant)
			}
			if !matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("192.0.2.2"), Port: 1}, advertised) {
				t.Fatal("post-invalidation caller reused the stale in-flight result")
			}
			if calls := resolver.Calls(); calls != 2 {
				t.Fatalf("DNS calls=%d want 2", calls)
			}
		})
	}
}

func TestAddressMatcherInvalidationGenerationWrapIsDeterministic(t *testing.T) {
	resolver := &mutableResolver{answers: map[string][]netip.Addr{"peer.test": {netip.MustParseAddr("192.0.2.1")}}}
	matcher := NewMatcher(resolver, clock.NewManual(time.Unix(3600, 0)), Options{})
	if !matcher.MatchTCP(context.Background(), &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}, config.Endpoint{Host: "peer.test", Port: 7000}) {
		t.Fatal("failed to populate cache")
	}
	matcher.mu.Lock()
	matcher.generation = ^uint64(0)
	matcher.mu.Unlock()
	matcher.Invalidate("uncached.test")
	matcher.mu.Lock()
	generation := matcher.generation
	cacheEntries := len(matcher.cache)
	matcher.mu.Unlock()
	if generation != 1 || cacheEntries != 0 {
		t.Fatalf("wrapped invalidation generation=%d cache entries=%d, want 1/0", generation, cacheEntries)
	}
}

func FuzzAddressMatcherLiteralIPCanonicalization(f *testing.F) {
	f.Add("192.0.2.1", "::ffff:192.0.2.1")
	f.Add("2001:db8::1", "2001:0db8:0:0:0:0:0:1")
	f.Fuzz(func(t *testing.T, advertisedText, remoteText string) {
		advertised, advertisedError := netip.ParseAddr(advertisedText)
		remote, remoteError := netip.ParseAddr(remoteText)
		if advertisedError != nil || remoteError != nil {
			return
		}
		matcher := NewMatcher(nil, clock.NewManual(time.Unix(7000, 0)), Options{})
		got := matcher.MatchTCP(context.Background(), net.TCPAddrFromAddrPort(netip.AddrPortFrom(remote, 49152)), config.Endpoint{Host: advertised.String(), Port: 9000})
		want := advertised.Unmap() == remote.Unmap() && !advertised.IsUnspecified() && !remote.IsUnspecified() && remote.Zone() == ""
		if got != want {
			t.Fatalf("MatchTCP(%s,%s)=%t want %t", advertised, remote, got, want)
		}
	})
}

type hostnameAddr string

func (hostnameAddr) Network() string  { return "tcp" }
func (a hostnameAddr) String() string { return string(a) }

type mutableResolver struct {
	mu      sync.Mutex
	answers map[string][]netip.Addr
	errors  map[string]error
	calls   int
}

type delayedResolver struct {
	mu       sync.Mutex
	first    []netip.Addr
	firstErr error
	next     []netip.Addr
	started  chan struct{}
	release  chan struct{}
	calls    int
}

func (resolver *delayedResolver) LookupNetIP(ctx context.Context, _, _ string) ([]netip.Addr, error) {
	resolver.mu.Lock()
	resolver.calls++
	call := resolver.calls
	first := append([]netip.Addr(nil), resolver.first...)
	firstErr := resolver.firstErr
	next := append([]netip.Addr(nil), resolver.next...)
	resolver.mu.Unlock()
	if call == 1 {
		close(resolver.started)
		select {
		case <-resolver.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return first, firstErr
	}
	return next, nil
}

func (resolver *delayedResolver) Calls() int {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	return resolver.calls
}

func (r *mutableResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	key := canonicalHost(host)
	for candidate, answers := range r.answers {
		if canonicalHost(candidate) == key {
			return append([]netip.Addr(nil), answers...), r.errors[key]
		}
	}
	return nil, r.errors[key]
}

func (r *mutableResolver) Set(host string, answers []netip.Addr, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.answers == nil {
		r.answers = make(map[string][]netip.Addr)
	}
	if r.errors == nil {
		r.errors = make(map[string]error)
	}
	key := canonicalHost(host)
	r.answers[key] = append([]netip.Addr(nil), answers...)
	r.errors[key] = err
}

func (r *mutableResolver) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}
