package swim

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	serviceAddressCacheEntries  = 4096
	serviceAddressLookupTimeout = time.Second
)

// AddressResolver is the bounded DNS seam used to authenticate advertised
// endpoint addresses. *net.Resolver implements this interface.
type AddressResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type addressMatcher struct {
	resolver AddressResolver
	timeout  time.Duration
	capacity int

	mu    sync.Mutex
	cache map[string][]netip.Addr
	order []string
}

func newAddressMatcher(resolver AddressResolver) *addressMatcher {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &addressMatcher{
		resolver: resolver,
		timeout:  serviceAddressLookupTimeout,
		capacity: serviceAddressCacheEntries,
		cache:    make(map[string][]netip.Addr),
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
	m.mu.Lock()
	if cached, ok := m.cache[key]; ok {
		result := append([]netip.Addr(nil), cached...)
		m.mu.Unlock()
		return result, nil
	}
	m.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	lookupContext, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	addresses, err := m.resolver.LookupNetIP(lookupContext, "ip", host)
	if err != nil {
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
		return nil, &net.DNSError{Name: host, Err: "no addresses"}
	}

	m.mu.Lock()
	if cached, ok := m.cache[key]; ok {
		canonical = append([]netip.Addr(nil), cached...)
	} else {
		if len(m.cache) >= m.capacity && len(m.order) != 0 {
			delete(m.cache, m.order[0])
			m.order = m.order[1:]
		}
		m.cache[key] = append([]netip.Addr(nil), canonical...)
		m.order = append(m.order, key)
	}
	m.mu.Unlock()
	return canonical, nil
}
