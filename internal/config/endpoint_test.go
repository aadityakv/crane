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
