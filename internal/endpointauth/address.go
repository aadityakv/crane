// Package endpointauth matches authenticated logical identities to numeric
// network sources without reverse DNS.
package endpointauth

import (
	"container/list"
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	defaultCacheEntries  = 4096
	defaultLookupTimeout = time.Second
	defaultPositiveTTL   = 30 * time.Second
	defaultNegativeTTL   = time.Second
)

// Resolver is the bounded DNS seam used for advertised endpoint identities.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Options controls bounded DNS lookup and cache behavior. Zero fields use
// conservative defaults.
type Options struct {
	LookupTimeout time.Duration // LookupTimeout bounds one resolver call.
	PositiveTTL   time.Duration // PositiveTTL retains successful answer sets.
	NegativeTTL   time.Duration // NegativeTTL retains resolver failures.
	CacheEntries  int           // CacheEntries bounds canonical DNS identities.
}

// Matcher owns a bounded, injected-clock DNS cache for source authorization.
type Matcher struct {
	resolver Resolver
	clock    clock.Clock
	options  Options

	mu    sync.Mutex
	cache map[string]*cacheEntry
	order list.List
}

type cacheEntry struct {
	addresses []netip.Addr
	err       error
	expires   time.Time
	element   *list.Element
}

// NewMatcher constructs a side-effect-free address matcher.
func NewMatcher(resolver Resolver, sourceClock clock.Clock, options Options) *Matcher {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if sourceClock == nil {
		sourceClock = clock.NewReal()
	}
	if options.LookupTimeout <= 0 {
		options.LookupTimeout = defaultLookupTimeout
	}
	if options.PositiveTTL <= 0 {
		options.PositiveTTL = defaultPositiveTTL
	}
	if options.NegativeTTL <= 0 {
		options.NegativeTTL = defaultNegativeTTL
	}
	if options.CacheEntries <= 0 {
		options.CacheEntries = defaultCacheEntries
	}
	return &Matcher{resolver: resolver, clock: sourceClock, options: options, cache: make(map[string]*cacheEntry)}
}

// MatchTCP reports whether remote is a numeric TCP address whose canonical IP
// is advertised. Its ephemeral source port is deliberately ignored.
func (matcher *Matcher) MatchTCP(ctx context.Context, remote net.Addr, advertised config.Endpoint) bool {
	address, ok := tcpAddress(remote)
	return ok && matcher.matches(ctx, address, advertised)
}

// MatchUDP reports whether remote is a numeric UDP address whose canonical IP
// and source port exactly match the advertised service endpoint.
func (matcher *Matcher) MatchUDP(ctx context.Context, remote net.Addr, advertised config.Endpoint) bool {
	address, port, ok := udpAddress(remote)
	return ok && port == advertised.Port && matcher.matches(ctx, address, advertised)
}

func (matcher *Matcher) matches(ctx context.Context, source netip.Addr, advertised config.Endpoint) bool {
	if matcher == nil || !source.IsValid() {
		return false
	}
	canonical, err := config.CanonicalEndpoint(advertised)
	if err != nil {
		return false
	}
	addresses, err := matcher.resolve(ctx, canonical.Host)
	if err != nil {
		return false
	}
	source = source.Unmap()
	for _, address := range addresses {
		if source == address {
			return true
		}
	}
	return false
}

// Invalidate removes one canonical DNS identity from the cache. Literal IPs
// have no cached resolver generation.
func (matcher *Matcher) Invalidate(host string) {
	if matcher == nil {
		return
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return
	}
	key := canonicalHost(host)
	matcher.mu.Lock()
	defer matcher.mu.Unlock()
	if entry := matcher.cache[key]; entry != nil {
		matcher.removeLocked(key, entry)
	}
}

func (matcher *Matcher) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{address.Unmap()}, nil
	}
	key := canonicalHost(host)
	if addresses, err, ok := matcher.cached(key); ok {
		return addresses, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookupContext, cancel := context.WithTimeout(ctx, matcher.options.LookupTimeout)
	defer cancel()
	addresses, err := matcher.resolver.LookupNetIP(lookupContext, "ip", key)
	if err != nil {
		if ctx.Err() == nil {
			matcher.store(key, nil, err, matcher.options.NegativeTTL)
		}
		return nil, err
	}
	canonical := make([]netip.Addr, 0, len(addresses))
	seen := make(map[netip.Addr]struct{}, len(addresses))
	for _, address := range addresses {
		if !address.IsValid() {
			continue
		}
		address = address.Unmap()
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		canonical = append(canonical, address)
	}
	if len(canonical) == 0 {
		err = &net.DNSError{Name: key, Err: "no addresses"}
		matcher.store(key, nil, err, matcher.options.NegativeTTL)
		return nil, err
	}
	matcher.store(key, canonical, nil, matcher.options.PositiveTTL)
	return append([]netip.Addr(nil), canonical...), nil
}

func (matcher *Matcher) cached(key string) ([]netip.Addr, error, bool) {
	matcher.mu.Lock()
	defer matcher.mu.Unlock()
	entry := matcher.cache[key]
	if entry == nil {
		return nil, nil, false
	}
	if !matcher.clock.Now().Before(entry.expires) {
		matcher.removeLocked(key, entry)
		return nil, nil, false
	}
	matcher.order.MoveToBack(entry.element)
	return append([]netip.Addr(nil), entry.addresses...), entry.err, true
}

func (matcher *Matcher) store(key string, addresses []netip.Addr, err error, ttl time.Duration) {
	matcher.mu.Lock()
	defer matcher.mu.Unlock()
	if entry := matcher.cache[key]; entry != nil {
		matcher.removeLocked(key, entry)
	}
	for len(matcher.cache) >= matcher.options.CacheEntries {
		oldest := matcher.order.Front()
		if oldest == nil {
			break
		}
		oldestKey := oldest.Value.(string)
		matcher.removeLocked(oldestKey, matcher.cache[oldestKey])
	}
	element := matcher.order.PushBack(key)
	matcher.cache[key] = &cacheEntry{addresses: append([]netip.Addr(nil), addresses...), err: err, expires: matcher.clock.Now().Add(ttl), element: element}
}

func (matcher *Matcher) removeLocked(key string, entry *cacheEntry) {
	delete(matcher.cache, key)
	matcher.order.Remove(entry.element)
}

func canonicalHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func tcpAddress(remote net.Addr) (netip.Addr, bool) {
	address, ok := remote.(*net.TCPAddr)
	if !ok || address == nil {
		return netip.Addr{}, false
	}
	return canonicalIP(address.IP)
}

func udpAddress(remote net.Addr) (netip.Addr, uint16, bool) {
	address, ok := remote.(*net.UDPAddr)
	if !ok || address == nil || address.Port <= 0 || address.Port > 65535 {
		return netip.Addr{}, 0, false
	}
	canonical, ok := canonicalIP(address.IP)
	return canonical, uint16(address.Port), ok
}

func canonicalIP(input net.IP) (netip.Addr, bool) {
	address, ok := netip.AddrFromSlice(input)
	return address.Unmap(), ok && address.IsValid()
}
