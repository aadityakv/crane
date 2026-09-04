package swim

import (
	"context"
	"net"
	"net/netip"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/endpointauth"
)

// AddressResolver is the bounded DNS seam used to authenticate advertised
// endpoint addresses. *net.Resolver implements this interface.
type AddressResolver = endpointauth.Resolver

type addressMatcher struct {
	shared *endpointauth.Matcher
}

func newAddressMatcher(resolver AddressResolver) *addressMatcher {
	return newAddressMatcherWithClock(resolver, clock.NewReal())
}

func newAddressMatcherWithClock(resolver AddressResolver, sourceClock clock.Clock) *addressMatcher {
	return &addressMatcher{shared: endpointauth.NewMatcher(resolver, sourceClock, endpointauth.Options{})}
}

// matchesSource requires an exact typed port and a numeric packet source whose
// canonical address is one of the advertised literal or resolved addresses.
func (matcher *addressMatcher) matchesSource(ctx context.Context, source, advertised config.Endpoint) bool {
	if matcher == nil || matcher.shared == nil {
		return false
	}
	address, err := netip.ParseAddr(source.Host)
	if err != nil {
		return false
	}
	remote := net.UDPAddrFromAddrPort(netip.AddrPortFrom(address.Unmap(), source.Port))
	return matcher.shared.MatchUDP(ctx, remote, advertised)
}

func (matcher *addressMatcher) invalidate(host string) {
	if matcher == nil || matcher.shared == nil {
		return
	}
	matcher.shared.Invalidate(host)
}
