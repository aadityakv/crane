package config

import (
	"fmt"
	"net"
	"strconv"
)

// Endpoint identifies a host and nonzero TCP or UDP port without embedding transport policy.
type Endpoint struct {
	// Host is an IP address or DNS name used to reach the endpoint.
	Host string
	// Port is the nonzero network port used to reach Host.
	Port uint16
}

// ParseEndpoint parses a host and nonzero port, including bracketed IPv6 addresses.
func ParseEndpoint(value string) (Endpoint, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return Endpoint{}, fmt.Errorf("split endpoint %q: %w", value, err)
	}
	if host == "" {
		return Endpoint{}, fmt.Errorf("endpoint host is empty")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return Endpoint{}, fmt.Errorf("parse endpoint port %q: %w", portText, err)
	}
	if port == 0 {
		return Endpoint{}, fmt.Errorf("endpoint port must be nonzero")
	}
	return Endpoint{Host: host, Port: uint16(port)}, nil
}

// String returns the endpoint with IPv6 hosts bracketed for unambiguous host/port parsing.
func (e Endpoint) String() string {
	return net.JoinHostPort(e.Host, strconv.FormatUint(uint64(e.Port), 10))
}

// BindEndpoint derives this node's listener endpoint for a registered service.
func (c NodeConfig) BindEndpoint(service Service) (Endpoint, error) {
	return c.deriveEndpoint(c.BindHost, service)
}

// AdvertiseEndpoint derives the routable endpoint peers use for a registered service.
func (c NodeConfig) AdvertiseEndpoint(service Service) (Endpoint, error) {
	if err := validateAdvertiseHost(c.AdvertiseHost); err != nil {
		return Endpoint{}, err
	}
	return c.deriveEndpoint(c.AdvertiseHost, service)
}

func (c NodeConfig) deriveEndpoint(host string, service Service) (Endpoint, error) {
	if host == "" {
		return Endpoint{}, fmt.Errorf("endpoint host is empty")
	}
	spec, ok := LookupService(service)
	if !ok {
		return Endpoint{}, fmt.Errorf("unknown service %d", service)
	}
	port := uint32(c.BasePort) + uint32(spec.Offset)
	if port > 65535 {
		return Endpoint{}, fmt.Errorf("base port %d plus service offset %d exceeds 65535", c.BasePort, spec.Offset)
	}
	if port == 0 {
		return Endpoint{}, fmt.Errorf("derived endpoint port must be nonzero")
	}
	return Endpoint{Host: host, Port: uint16(port)}, nil
}
