package config

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestDefaultRaftConfigUsesApprovedProtocolBounds(t *testing.T) {
	// This fails if the protocol defaults drift from the approved safety envelope.
	want := RaftConfig{
		ElectionTimeoutMin:     Duration(750 * time.Millisecond),
		ElectionTimeoutMax:     Duration(1500 * time.Millisecond),
		HeartbeatInterval:      Duration(150 * time.Millisecond),
		RPCTimeout:             Duration(500 * time.Millisecond),
		MaxAppendEntries:       64,
		SnapshotEntryThreshold: 4096,
		SnapshotByteThreshold:  64 << 20,
		MaxSnapshotBytes:       16 << 20,
	}
	if got := DefaultRaftConfig(); !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultRaftConfig() = %#v, want %#v", got, want)
	}
}

func TestValidateRaftRejectsUnsafeTimingRelationships(t *testing.T) {
	valid := DefaultRaftConfig()
	tests := []struct {
		name   string
		mutate func(*RaftConfig)
	}{
		{name: "zero_election_minimum", mutate: func(c *RaftConfig) { c.ElectionTimeoutMin = 0 }},
		{name: "zero_election_maximum", mutate: func(c *RaftConfig) { c.ElectionTimeoutMax = 0 }},
		{name: "zero_heartbeat", mutate: func(c *RaftConfig) { c.HeartbeatInterval = 0 }},
		{name: "zero_rpc_timeout", mutate: func(c *RaftConfig) { c.RPCTimeout = 0 }},
		{name: "heartbeat_equals_election_minimum", mutate: func(c *RaftConfig) { c.HeartbeatInterval = c.ElectionTimeoutMin }},
		{name: "heartbeat_exceeds_election_minimum", mutate: func(c *RaftConfig) { c.HeartbeatInterval = c.ElectionTimeoutMin + 1 }},
		{name: "minimum_equals_maximum", mutate: func(c *RaftConfig) { c.ElectionTimeoutMax = c.ElectionTimeoutMin }},
		{name: "minimum_exceeds_maximum", mutate: func(c *RaftConfig) { c.ElectionTimeoutMin = c.ElectionTimeoutMax + 1 }},
		{name: "minimum_below_three_heartbeats", mutate: func(c *RaftConfig) { c.ElectionTimeoutMin = Duration(449 * time.Millisecond) }},
		{name: "rpc_timeout_equals_election_minimum", mutate: func(c *RaftConfig) { c.RPCTimeout = c.ElectionTimeoutMin }},
		{name: "rpc_timeout_exceeds_election_minimum", mutate: func(c *RaftConfig) { c.RPCTimeout = c.ElectionTimeoutMin + 1 }},
		{name: "three_heartbeats_would_overflow", mutate: func(c *RaftConfig) {
			c.ElectionTimeoutMin = Duration(math.MaxInt64 - 1)
			c.ElectionTimeoutMax = Duration(math.MaxInt64)
			c.HeartbeatInterval = Duration(math.MaxInt64/3 + 1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid
			tt.mutate(&got)
			if err := validateRaft(got); err == nil {
				t.Fatal("validateRaft accepted an unsafe timing relationship")
			}
		})
	}
}

func TestValidateRaftRejectsZeroThresholdsAndOversizedSnapshots(t *testing.T) {
	valid := DefaultRaftConfig()
	tests := []struct {
		name   string
		mutate func(*RaftConfig)
	}{
		{name: "zero_append_entries", mutate: func(c *RaftConfig) { c.MaxAppendEntries = 0 }},
		{name: "zero_snapshot_entries", mutate: func(c *RaftConfig) { c.SnapshotEntryThreshold = 0 }},
		{name: "zero_snapshot_bytes", mutate: func(c *RaftConfig) { c.SnapshotByteThreshold = 0 }},
		{name: "zero_max_snapshot", mutate: func(c *RaftConfig) { c.MaxSnapshotBytes = 0 }},
		{name: "snapshot_larger_than_one_gib", mutate: func(c *RaftConfig) { c.MaxSnapshotBytes = 1<<30 + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valid
			tt.mutate(&got)
			if err := validateRaft(got); err == nil {
				t.Fatal("validateRaft accepted an invalid bound")
			}
		})
	}
	valid.MaxSnapshotBytes = 1 << 30
	if err := validateRaft(valid); err != nil {
		t.Fatalf("validateRaft rejected one GiB snapshot: %v", err)
	}
}

func TestNodeConfigRaftVoterByIDReturnsOwnedValue(t *testing.T) {
	configuration := validConfig(createSecret(t, 0o600))
	voter, ok := configuration.RaftVoterByID(2)
	if !ok || voter != (RaftVoter{NodeID: 2, Endpoint: "127.0.0.1:8108"}) {
		t.Fatalf("RaftVoterByID(2) = %#v, %t", voter, ok)
	}
	voter.Endpoint = "changed.example.test:8008"
	if configuration.RaftVoters[1].Endpoint != "127.0.0.1:8108" {
		t.Fatalf("RaftVoterByID exposed configured voters: %#v", configuration.RaftVoters)
	}
	if voter, ok := configuration.RaftVoterByID(4); ok || voter != (RaftVoter{}) {
		t.Fatalf("RaftVoterByID(4) = %#v, %t, want no voter", voter, ok)
	}
}
