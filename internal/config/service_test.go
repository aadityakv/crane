package config

import "testing"

func TestServiceRegistryMatchesApprovedLayout(t *testing.T) {
	want := []ServiceSpec{
		{Service: ServiceSWIMPing, Name: "swim-ping", Offset: 0, Transport: TransportUDP},
		{Service: ServiceSWIMACK, Name: "swim-ack", Offset: 1, Transport: TransportUDP},
		{Service: ServiceSWIMSnapshot, Name: "swim-snapshot", Offset: 2, Transport: TransportTCP},
		{Service: ServiceFileRPC, Name: "file-rpc", Offset: 3, Transport: TransportTCP},
		{Service: ServiceGrepRPC, Name: "grep-rpc", Offset: 4, Transport: TransportTCP},
		{Service: ServiceCraneWorker, Name: "crane-worker", Offset: 5, Transport: TransportTCP},
		{Service: ServiceTopologyControl, Name: "topology-control", Offset: 6, Transport: TransportTCP},
		{Service: ServiceCraneTupleACK, Name: "crane-tuple-ack", Offset: 7, Transport: TransportUDP},
		{Service: ServiceRaftRPC, Name: "raft-rpc", Offset: 8, Transport: TransportTCP},
	}
	got := Services()
	if len(got) != len(want) {
		t.Fatalf("Services() length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Services()[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
	got[0].Offset = 99
	again := Services()
	if again[0].Offset != 0 {
		t.Fatal("Services exposed mutable registry storage")
	}
}

func TestServiceRegistryHasUniqueFields(t *testing.T) {
	services := Services()
	checks := []struct {
		name  string
		check func(ServiceSpec, map[Service]bool, map[string]bool, map[int]bool) bool
	}{
		{name: "unique IDs", check: func(spec ServiceSpec, ids map[Service]bool, _ map[string]bool, _ map[int]bool) bool {
			if ids[spec.Service] {
				return false
			}
			ids[spec.Service] = true
			return true
		}},
		{name: "unique names", check: func(spec ServiceSpec, _ map[Service]bool, names map[string]bool, _ map[int]bool) bool {
			if names[spec.Name] {
				return false
			}
			names[spec.Name] = true
			return true
		}},
		{name: "unique offsets", check: func(spec ServiceSpec, _ map[Service]bool, _ map[string]bool, offsets map[int]bool) bool {
			if offsets[spec.Offset] {
				return false
			}
			offsets[spec.Offset] = true
			return true
		}},
		{name: "offset range", check: func(spec ServiceSpec, _ map[Service]bool, _ map[string]bool, _ map[int]bool) bool {
			return spec.Offset >= 0 && spec.Offset <= 8
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			ids := make(map[Service]bool)
			names := make(map[string]bool)
			offsets := make(map[int]bool)
			for _, spec := range services {
				if !check.check(spec, ids, names, offsets) {
					t.Errorf("invalid registry entry: %#v", spec)
				}
			}
		})
	}
}

func TestLookupServiceRejectsUnknownIDs(t *testing.T) {
	tests := []struct {
		name    string
		service Service
		found   bool
	}{
		{name: "registered", service: ServiceFileRPC, found: true},
		{name: "negative", service: Service(^uint8(0)), found: false},
		{name: "after registry", service: Service(9), found: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := LookupService(tt.service)
			if got != tt.found {
				t.Fatalf("LookupService(%d) found = %v, want %v", tt.service, got, tt.found)
			}
		})
	}
}
