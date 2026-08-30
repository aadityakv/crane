package config

import "fmt"

const (
	// MaxRaftCommandBytes bounds one Raft command payload on the network.
	MaxRaftCommandBytes uint64 = 1 << 20
	// MaxRaftSnapshotChunkBytes bounds one Raft snapshot transfer chunk.
	MaxRaftSnapshotChunkBytes uint64 = 256 << 10
	// MaxRaftSnapshotBytes bounds a configured state-machine snapshot.
	MaxRaftSnapshotBytes uint64 = 1 << 30
)

// RaftConfig contains fixed-voter Raft timing and bounded persistence settings.
type RaftConfig struct {
	// ElectionTimeoutMin is the inclusive lower bound for randomized elections.
	ElectionTimeoutMin Duration `json:"election_timeout_min"`
	// ElectionTimeoutMax is the exclusive upper bound for randomized elections.
	ElectionTimeoutMax Duration `json:"election_timeout_max"`
	// HeartbeatInterval controls empty AppendEntries while a leader is healthy.
	HeartbeatInterval Duration `json:"heartbeat_interval"`
	// RPCTimeout bounds one peer exchange before reconnect or retry.
	RPCTimeout Duration `json:"rpc_timeout"`
	// MaxAppendEntries bounds entries carried by one AppendEntries request.
	MaxAppendEntries uint16 `json:"max_append_entries"`
	// SnapshotEntryThreshold triggers compaction after this many applied entries.
	SnapshotEntryThreshold uint64 `json:"snapshot_entry_threshold"`
	// SnapshotByteThreshold triggers compaction after this many retained WAL bytes.
	SnapshotByteThreshold uint64 `json:"snapshot_byte_threshold"`
	// MaxSnapshotBytes bounds one state-machine snapshot before persistence.
	MaxSnapshotBytes uint64 `json:"max_snapshot_bytes"`
}

// DefaultRaftConfig returns the conservative fixed-voter Raft configuration.
func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		ElectionTimeoutMin:     750_000_000,
		ElectionTimeoutMax:     1_500_000_000,
		HeartbeatInterval:      150_000_000,
		RPCTimeout:             500_000_000,
		MaxAppendEntries:       64,
		SnapshotEntryThreshold: 4096,
		SnapshotByteThreshold:  64 << 20,
		MaxSnapshotBytes:       16 << 20,
	}
}

func validateRaft(raft RaftConfig) error {
	if raft.ElectionTimeoutMin <= 0 || raft.ElectionTimeoutMax <= 0 || raft.HeartbeatInterval <= 0 || raft.RPCTimeout <= 0 {
		return fmt.Errorf("all raft durations must be greater than zero")
	}
	if raft.HeartbeatInterval >= raft.ElectionTimeoutMin || raft.ElectionTimeoutMin >= raft.ElectionTimeoutMax {
		return fmt.Errorf("raft heartbeat interval must be less than election timeout minimum, which must be less than election timeout maximum")
	}
	if raft.ElectionTimeoutMin/raft.HeartbeatInterval < 3 {
		return fmt.Errorf("raft election timeout minimum must be at least three heartbeat intervals")
	}
	if raft.RPCTimeout >= raft.ElectionTimeoutMin {
		return fmt.Errorf("raft RPC timeout must be less than election timeout minimum")
	}
	if raft.MaxAppendEntries == 0 || raft.SnapshotEntryThreshold == 0 || raft.SnapshotByteThreshold == 0 || raft.MaxSnapshotBytes == 0 {
		return fmt.Errorf("raft append and snapshot thresholds must be nonzero")
	}
	if raft.MaxSnapshotBytes > MaxRaftSnapshotBytes {
		return fmt.Errorf("raft maximum snapshot bytes must not exceed %d", MaxRaftSnapshotBytes)
	}
	return nil
}
