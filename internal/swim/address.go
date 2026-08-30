package swim

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
	serviceAddressCacheEntries  = 4096
	serviceAddressLookupTimeout = time.Second
	serviceAddressPositiveTTL   = 30 * time.Second
	serviceAddressNegativeTTL   = time.Second
)

// AddressResolver is the bounded DNS seam used to authenticate advertised
// endpoint addresses. *net.Resolver implements this interface.
type AddressResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type addressMatcher struct {
	resolver    AddressResolver
	clock       clock.Clock
	timeout     time.Duration
	positiveTTL time.Duration
	negativeTTL time.Duration
	capacity    int

	mu    sync.Mutex
	cache map[string]*addressCacheEntry
	order list.List
}

type addressCacheEntry struct {
	addresses []netip.Addr
	err       error
	expires   time.Time
	element   *list.Element
}

func newAddressMatcher(resolver AddressResolver) *addressMatcher {
	return newAddressMatcherWithClock(resolver, clock.NewReal())
}

func newAddressMatcherWithClock(resolver AddressResolver, sourceClock clock.Clock) *addressMatcher {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if sourceClock == nil {
		sourceClock = clock.NewReal()
	}
	return &addressMatcher{
		resolver:    resolver,
		clock:       sourceClock,
		timeout:     serviceAddressLookupTimeout,
		positiveTTL: serviceAddressPositiveTTL,
		negativeTTL: serviceAddressNegativeTTL,
		capacity:    serviceAddressCacheEntries,
		cache:       make(map[string]*addressCacheEntry),
	}
}

// matchesSource requires an exact typed port and a numeric packet source whose
// canonical address is one of the advertised literal or resolved addresses.
func (m *addressMatcher) matchesSource(ctx context.Context, source, advertised config.Endpoint) bool {
	if m == nil || source.Port != advertised.Port {
		return false
	}
	sourceAddress, err := netip.ParseAddr(source.Host)
	if err != nil {
		return false
	}
	advertisedAddresses, err := m.resolve(ctx, advertised.Host)
	if err != nil {
		return false
	}
	sourceAddress = sourceAddress.Unmap()
	for _, address := range advertisedAddresses {
		if sourceAddress == address {
			return true
		}
	}
	return false
}

func (m *addressMatcher) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{address.Unmap()}, nil
	}
	key := strings.ToLower(strings.TrimSuffix(host, "."))
	if addresses, err, ok := m.cached(key); ok {
		return addresses, err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	lookupContext, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	addresses, err := m.resolver.LookupNetIP(lookupContext, "ip", host)
	if err != nil {
		if ctx.Err() == nil {
			m.store(key, nil, err, m.negativeTTL)
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
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		canonical = append(canonical, address)
	}
	if len(canonical) == 0 {
		err := &net.DNSError{Name: host, Err: "no addresses"}
		if ctx.Err() == nil {
			m.store(key, nil, err, m.negativeTTL)
		}
		return nil, err
	}
	m.store(key, canonical, nil, m.positiveTTL)
	return append([]netip.Addr(nil), canonical...), nil
}

func (m *addressMatcher) cached(key string) ([]netip.Addr, error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.cache[key]
	if !exists {
		return nil, nil, false
	}
	if !m.clock.Now().Before(entry.expires) {
		m.removeLocked(key, entry)
		return nil, nil, false
	}
	m.order.MoveToBack(entry.element)
	return append([]netip.Addr(nil), entry.addresses...), entry.err, true
}

func (m *addressMatcher) store(key string, addresses []netip.Addr, err error, ttl time.Duration) {
	if ttl <= 0 || m.capacity <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.cache[key]; ok {
		m.removeLocked(key, existing)
	}
	for len(m.cache) >= m.capacity {
		oldest := m.order.Front()
		if oldest == nil {
			break
		}
		oldestKey := oldest.Value.(string)
		m.removeLocked(oldestKey, m.cache[oldestKey])
	}
	element := m.order.PushBack(key)
	m.cache[key] = &addressCacheEntry{
		addresses: append([]netip.Addr(nil), addresses...),
		err:       err,
		expires:   m.clock.Now().Add(ttl),
		element:   element,
	}
}

func (m *addressMatcher) invalidate(host string) {
	if m == nil {
		return
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return
	}
	key := strings.ToLower(strings.TrimSuffix(host, "."))
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, exists := m.cache[key]; exists {
		m.removeLocked(key, entry)
	}
}

func (m *addressMatcher) removeLocked(key string, entry *addressCacheEntry) {
	if entry == nil {
		return
	}
	delete(m.cache, key)
	m.order.Remove(entry.element)
}
