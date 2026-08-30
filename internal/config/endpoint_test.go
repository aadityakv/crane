package config

import "testing"

func TestParseEndpointAndStringIPv6(t *testing.T) {
	endpoint, err := ParseEndpoint("[2001:db8::1]:8008")
	if err != nil {
		t.Fatalf("ParseEndpoint: %v", err)
	}
	if endpoint.Host != "2001:db8::1" || endpoint.Port != 8008 {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	if got := endpoint.String(); got != "[2001:db8::1]:8008" {
		t.Fatalf("String() = %q", got)
	}
}

func TestParseEndpointRejectsInvalidPort(t *testing.T) {
	if _, err := ParseEndpoint("127.0.0.1:0"); err == nil {
		t.Fatal("ParseEndpoint accepted port zero")
	}
}

func TestCanonicalEndpointComparisonNormalizesDNSAndIPSpelling(t *testing.T) {
	dnsAbsolute, err := ParseEndpoint("Node.Example.Test.:8008")
	if err != nil {
		t.Fatal(err)
	}
	dnsLower, err := ParseEndpoint("node.example.test:8008")
	if err != nil {
		t.Fatal(err)
	}
	if !SameEndpoint(dnsAbsolute, dnsLower) {
		t.Fatalf("DNS endpoints are not semantically equal: %#v %#v", dnsAbsolute, dnsLower)
	}
	ipExpanded, err := ParseEndpoint("[2001:0db8:0:0:0:0:0:1]:8008")
	if err != nil {
		t.Fatal(err)
	}
	ipCompressed, err := ParseEndpoint("[2001:db8::1]:8008")
	if err != nil {
		t.Fatal(err)
	}
	if !SameEndpoint(ipExpanded, ipCompressed) {
		t.Fatalf("IPv6 endpoints are not semantically equal: %#v %#v", ipExpanded, ipCompressed)
	}
	if dnsAbsolute.Host != "node.example.test" || ipExpanded.Host != "2001:db8::1" {
		t.Fatalf("canonical hosts = %q and %q", dnsAbsolute.Host, ipExpanded.Host)
	}
}
