package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateTimingRejectsOverflowingTimeoutSum(t *testing.T) {
	timing := DefaultTimingConfig()
	timing.ProbeInterval = Duration(math.MaxInt64)
	timing.DirectProbeTimeout = Duration(math.MaxInt64)
	timing.IndirectProbeTimeout = 1

	if err := validateTiming(timing); err == nil {
		t.Fatal("validateTiming accepted direct+indirect duration overflow")
	}
}

func TestReplayWindowRetentionBoundaryValidation(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	const modeledFutureSkew = 30 * time.Second
	maxReplayWindow := maxDuration - modeledFutureSkew
	secret := createSecret(t, 0o600)

	configuration := validConfig(secret)
	configuration.Timing.ReplayWindow = Duration(maxReplayWindow)
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate rejected exact max-safe replay window: %v", err)
	}

	configuration.Timing.ReplayWindow = Duration(maxReplayWindow + time.Nanosecond)
	if err := configuration.Validate(); err == nil {
		t.Fatal("Validate accepted replay window whose modeled retention overflows")
	}
}

func TestDecodeReplayWindowRetentionBoundary(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	const modeledFutureSkew = 30 * time.Second
	maxReplayWindow := maxDuration - modeledFutureSkew
	secret := createSecret(t, 0o600)

	for _, test := range []struct {
		name         string
		replayWindow time.Duration
		wantErr      bool
	}{
		{name: "exact max-safe", replayWindow: maxReplayWindow},
		{name: "one nanosecond over", replayWindow: maxReplayWindow + time.Nanosecond, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := validConfig(secret)
			configuration.Timing.ReplayWindow = Duration(test.replayWindow)
			encoded, err := json.Marshal(configuration)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			decoded, err := Decode(bytes.NewReader(encoded))
			if test.wantErr {
				if err == nil {
					t.Fatal("Decode accepted replay window whose modeled retention overflows")
				}
				return
			}
			if err != nil {
				t.Fatalf("Decode rejected exact max-safe replay window: %v", err)
			}
			if got := time.Duration(decoded.Timing.ReplayWindow); got != test.replayWindow {
				t.Fatalf("decoded replay window = %s, want %s", got, test.replayWindow)
			}
		})
	}
}

func createSecret(t *testing.T, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(path, bytes.Repeat([]byte("s"), 32), mode); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	return path
}

func TestLoadClusterSecretRejectsUnsafeOrWeakKeyMaterial(t *testing.T) {
	tests := []struct {
		name     string
		contents []byte
		wantErr  bool
	}{
		{name: "empty", contents: nil, wantErr: true},
		{name: "short", contents: bytes.Repeat([]byte("s"), 31), wantErr: true},
		{name: "minimum_256_bit_key", contents: bytes.Repeat([]byte("s"), 32)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cluster.secret")
			if err := os.WriteFile(path, test.contents, 0o600); err != nil {
				t.Fatal(err)
			}
			secret, err := LoadClusterSecret(path)
			if test.wantErr {
				if err == nil {
					t.Fatal("LoadClusterSecret accepted weak key material")
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadClusterSecret: %v", err)
			}
			if !bytes.Equal(secret, test.contents) {
				t.Fatalf("LoadClusterSecret returned %d bytes, want original key material", len(secret))
			}
		})
	}
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
		Raft:   DefaultRaftConfig(),
		Crane:  DefaultCraneConfig(),
	}
}

func TestNodeConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NodeConfig)
		secret os.FileMode
	}{
		{name: "zero_node_id", mutate: func(c *NodeConfig) { c.NodeID = 0 }},
		{name: "base_port_zero_for_service_offset_zero", mutate: func(c *NodeConfig) {
			c.BasePort = 0
			c.RaftVoters[0].Endpoint = "127.0.0.1:8"
		}},
		{name: "wildcard_advertise_host", mutate: func(c *NodeConfig) { c.AdvertiseHost = "0.0.0.0" }},
		{name: "malformed_bind_host", mutate: func(c *NodeConfig) { c.BindHost = "bad host" }},
		{name: "malformed_advertise_host", mutate: func(c *NodeConfig) {
			c.AdvertiseHost = "bad host"
			c.RaftVoters[0].Endpoint = "bad host:8008"
		}},
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

func TestNodeConfigRejectsWildcardIntroducerAndVoters(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*NodeConfig)
	}{
		{name: "IPv4 introducer", mutate: func(c *NodeConfig) { c.Introducer = "0.0.0.0:8002" }},
		{name: "IPv6 introducer", mutate: func(c *NodeConfig) { c.Introducer = "[::]:8002" }},
		{name: "IPv4 voter", mutate: func(c *NodeConfig) { c.RaftVoters[1].Endpoint = "0.0.0.0:8108" }},
		{name: "IPv6 voter", mutate: func(c *NodeConfig) { c.RaftVoters[1].Endpoint = "[::]:8108" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig(createSecret(t, 0o600))
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate accepted a wildcard routable endpoint")
			}
		})
	}
}

func TestNodeConfigRejectsCanonicalDuplicateVoterEndpoints(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  string
		second string
	}{
		{name: "DNS case and root dot", first: "Node.Example.Test.:8108", second: "node.example.test:8108"},
		{name: "equivalent IPv6", first: "[2001:0db8:0:0:0:0:0:1]:8108", second: "[2001:db8::1]:8108"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig(createSecret(t, 0o600))
			cfg.RaftVoters[1].Endpoint = test.first
			cfg.RaftVoters[2].Endpoint = test.second
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate accepted semantically duplicate voter endpoints")
			}
		})
	}
}

func TestNodeConfigRejectsUnreadableSecret(t *testing.T) {
	secret := createSecret(t, 0o000)
	if file, err := os.Open(secret); err == nil {
		file.Close()
		t.Skip("current user can read a mode 000 file on this filesystem")
	}
	cfg := validConfig(secret)
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted an unreadable cluster secret")
	}
}

func TestNodeConfigAcceptsDNSHost(t *testing.T) {
	cfg := validConfig(createSecret(t, 0o600))
	cfg.BindHost = "localhost"
	cfg.AdvertiseHost = "node-1.example.test"
	cfg.RaftVoters[0].Endpoint = "node-1.example.test:8008"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestNodeConfigAcceptsNonVoterWithSharedFixedVoterMap(t *testing.T) {
	cfg := validConfig(createSecret(t, 0o600))
	cfg.NodeID = 4
	cfg.BasePort = 8300
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate non-voter: %v", err)
	}
}

func TestNodeConfigAcceptsAbsoluteDNSHosts(t *testing.T) {
	tests := []struct {
		name      string
		bindHost  string
		advertise string
	}{
		{name: "absolute_fqdn", bindHost: "localhost.", advertise: "node-1.example.test."},
		{name: "absolute_localhost", bindHost: "localhost.", advertise: "localhost."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig(createSecret(t, 0o600))
			cfg.BindHost = tt.bindHost
			cfg.AdvertiseHost = tt.advertise
			cfg.RaftVoters[0].Endpoint = strings.TrimSuffix(strings.ToLower(tt.advertise), ".") + ":8008"
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestNodeConfigRejectsInvalidTrailingDNSDots(t *testing.T) {
	for _, host := range []string{".", "localhost.."} {
		t.Run(host, func(t *testing.T) {
			cfg := validConfig(createSecret(t, 0o600))
			cfg.BindHost = host
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate accepted invalid DNS root-dot syntax")
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
"raft_voters":[{"node_id":1,"endpoint":"127.0.0.1:8008"},{"node_id":2,"endpoint":"127.0.0.1:8108"},{"node_id":3,"endpoint":"127.0.0.1:8208"}],
"crane":{"worker_slots":4,"worker_control_timeout":"2s","tuple_retry_interval":"200ms","tuple_completion_retry_interval":"1s","failure_grace_period":"5s","max_worker_store_bytes":1073741824,"consensus_fingerprint":"9bbf4290ec5af75345d86578c196058b4a2e49175daf9cbe352e492ff8739412"}
}`, secret)
	cfg, err := Decode(bytes.NewBufferString(input))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cfg.Timing != DefaultTimingConfig() {
		t.Fatalf("Timing = %#v, want defaults %#v", cfg.Timing, DefaultTimingConfig())
	}
}

func TestDecodeAppliesAndMergesRaftDefaults(t *testing.T) {
	secret := createSecret(t, 0o600)
	base := fmt.Sprintf(`{
"node_id":1,"cluster_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
"bind_host":"127.0.0.1","advertise_host":"127.0.0.1","base_port":8000,
"introducer":"127.0.0.1:8002","storage_dir":"data/node-1","cluster_secret_file":%q,
"raft_voters":[{"node_id":1,"endpoint":"127.0.0.1:8008"},{"node_id":2,"endpoint":"127.0.0.1:8108"},{"node_id":3,"endpoint":"127.0.0.1:8208"}],
"crane":{"worker_slots":4,"worker_control_timeout":"2s","tuple_retry_interval":"200ms","tuple_completion_retry_interval":"1s","failure_grace_period":"5s","max_worker_store_bytes":1073741824,"consensus_fingerprint":"9bbf4290ec5af75345d86578c196058b4a2e49175daf9cbe352e492ff8739412"}`, secret)
	t.Run("omitted", func(t *testing.T) {
		cfg, err := Decode(bytes.NewBufferString(base + `}`))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if cfg.Raft != DefaultRaftConfig() {
			t.Fatalf("Raft = %#v, want defaults %#v", cfg.Raft, DefaultRaftConfig())
		}
	})
	t.Run("partial", func(t *testing.T) {
		cfg, err := Decode(bytes.NewBufferString(base + `,"raft":{"heartbeat_interval":"200ms"}}`))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		want := DefaultRaftConfig()
		want.HeartbeatInterval = Duration(200 * time.Millisecond)
		if cfg.Raft != want {
			t.Fatalf("Raft = %#v, want %#v", cfg.Raft, want)
		}
	})
	t.Run("nested_unknown_field", func(t *testing.T) {
		if _, err := Decode(bytes.NewBufferString(base + `,"raft":{"unknown":true}}`)); err == nil {
			t.Fatal("Decode accepted an unknown raft field")
		}
	})
}

func TestAdvertiseEndpointsUseRegistry(t *testing.T) {
	cfg := validConfig(createSecret(t, 0o600))
	for _, spec := range Services() {
		endpoint, err := cfg.AdvertiseEndpoint(spec.Service)
		if err != nil {
			t.Fatalf("AdvertiseEndpoint(%v): %v", spec.Service, err)
		}
		if endpoint.Host != "127.0.0.1" || int(endpoint.Port) != 8000+int(spec.Offset) {
			t.Fatalf("endpoint = %#v for %#v", endpoint, spec)
		}
	}
}
