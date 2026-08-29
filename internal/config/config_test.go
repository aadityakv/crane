package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func createSecret(t *testing.T, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(path, []byte("test-cluster-secret"), mode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	return path
}

func validConfig(secretPath string) NodeConfig {
	return NodeConfig{
		NodeID:            1,
		ClusterID:         "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		BindHost:          "127.0.0.1",
		AdvertiseHost:     "127.0.0.1",
		BasePort:          8000,
		Introducer:        "127.0.0.1:8002",
		StorageDir:        "data/node-1",
		ClusterSecretFile: secretPath,
		RaftVoters: []RaftVoter{
			{NodeID: 1, Endpoint: "127.0.0.1:8008"},
			{NodeID: 2, Endpoint: "127.0.0.1:8108"},
			{NodeID: 3, Endpoint: "127.0.0.1:8208"},
		},
		Timing: DefaultTimingConfig(),
	}
}

func TestNodeConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NodeConfig)
		secret os.FileMode
	}{
		{name: "zero_node_id", mutate: func(c *NodeConfig) { c.NodeID = 0 }},
		{name: "wildcard_advertise_host", mutate: func(c *NodeConfig) { c.AdvertiseHost = "0.0.0.0" }},
		{name: "base_port_overflow", mutate: func(c *NodeConfig) { c.BasePort = 65535 }},
		{name: "duplicate_voter_id", mutate: func(c *NodeConfig) { c.RaftVoters[1].NodeID = 1 }},
		{name: "duplicate_voter_endpoint", mutate: func(c *NodeConfig) { c.RaftVoters[1].Endpoint = "127.0.0.1:8008" }},
		{name: "two_voters", mutate: func(c *NodeConfig) { c.RaftVoters = c.RaftVoters[:2] }},
		{name: "insecure_secret_permissions", secret: 0o644, mutate: func(*NodeConfig) {}},
		{name: "unsafe_storage_root", mutate: func(c *NodeConfig) { c.StorageDir = "/" }},
		{name: "direct_plus_indirect_exceeds_interval", mutate: func(c *NodeConfig) { c.Timing.IndirectProbeTimeout = Duration(800000000) }},
		{name: "local_voter_endpoint_mismatch", mutate: func(c *NodeConfig) { c.RaftVoters[0].Endpoint = "127.0.0.1:9008" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode := tt.secret
			if mode == 0 {
				mode = 0o600
			}
			cfg := validConfig(createSecret(t, mode))
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate succeeded")
			}
		})
	}
}

func TestDecode(t *testing.T) {
	t.Run("unknown_json_field", func(t *testing.T) {
		secret := createSecret(t, 0o600)
		input := fmt.Sprintf(`{
"node_id":1,"cluster_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
"bind_host":"127.0.0.1","advertise_host":"127.0.0.1","base_port":8000,
"introducer":"127.0.0.1:8002","storage_dir":"data/node-1","cluster_secret_file":%q,
"raft_voters":[{"node_id":1,"endpoint":"127.0.0.1:8008"},{"node_id":2,"endpoint":"127.0.0.1:8108"},{"node_id":3,"endpoint":"127.0.0.1:8208"}],
"unknown_json_field":true
}`, secret)
		if _, err := Decode(bytes.NewBufferString(input)); err == nil {
			t.Fatal("Decode accepted an unknown JSON field")
		}
	})
}

func TestDecodeAppliesTimingDefaults(t *testing.T) {
	secret := createSecret(t, 0o600)
	input := fmt.Sprintf(`{
"node_id":1,"cluster_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
"bind_host":"127.0.0.1","advertise_host":"127.0.0.1","base_port":8000,
"introducer":"127.0.0.1:8002","storage_dir":"data/node-1","cluster_secret_file":%q,
"raft_voters":[{"node_id":1,"endpoint":"127.0.0.1:8008"},{"node_id":2,"endpoint":"127.0.0.1:8108"},{"node_id":3,"endpoint":"127.0.0.1:8208"}]
}`, secret)
	cfg, err := Decode(bytes.NewBufferString(input))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cfg.Timing != DefaultTimingConfig() {
		t.Fatalf("Timing = %#v, want defaults %#v", cfg.Timing, DefaultTimingConfig())
	}
}

func TestAdvertiseEndpointsUseRegistry(t *testing.T) {
	cfg := validConfig(createSecret(t, 0o600))
	for _, spec := range Services() {
		endpoint, err := cfg.AdvertiseEndpoint(spec.Service)
		if err != nil {
			t.Fatalf("AdvertiseEndpoint(%v): %v", spec.Service, err)
		}
		if endpoint.Host != "127.0.0.1" || int(endpoint.Port) != 8000+spec.Offset {
			t.Fatalf("endpoint = %#v for %#v", endpoint, spec)
		}
	}
}
