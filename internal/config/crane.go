package config

import (
	"fmt"
	"strings"
	"time"

	"crane/internal/crane/model"
)

const (
	minCraneWorkerSlots uint16 = 1
	minWorkerStoreBytes uint64 = 1 << 20
	maxWorkerStoreBytes uint64 = 1 << 40
)

// CraneConfig contains local operational limits and the compiled consensus contract.
type CraneConfig struct {
	// WorkerSlots is the number of task slots this worker offers the cluster
	// (1..MaxWorkerSlots); placement never exceeds the cluster-wide total.
	WorkerSlots uint16 `json:"worker_slots"`
	// WorkerControlTimeout bounds every +5 worker-control and +6
	// public-control request exchange, including dial and handshake.
	WorkerControlTimeout Duration `json:"worker_control_timeout"`
	// TupleRetryInterval is the resend interval for +7 tuple deliveries that
	// have not been acknowledged by the destination.
	TupleRetryInterval Duration `json:"tuple_retry_interval"`
	// TupleCompletionRetryInterval is the resend interval for completion
	// acknowledgments that await durable downstream custody.
	TupleCompletionRetryInterval Duration `json:"tuple_completion_retry_interval"`
	// FailureGracePeriod is how long a worker must remain continuously
	// Dead/Left and unreachable before its tasks and replicas are reassigned;
	// Suspect alone never reassigns.
	FailureGracePeriod Duration `json:"failure_grace_period"`
	// MaxWorkerStoreBytes is the durable budget for the worker's write-ahead
	// log plus snapshots; exhaustion fails closed with a retryable capacity
	// error rather than accepting work that cannot be stored.
	MaxWorkerStoreBytes uint64 `json:"max_worker_store_bytes"`
	// ConsensusFingerprint is the hex SHA-256 of the compiled consensus
	// contract; startup rejects a value that differs from the binary's own,
	// and peers advertising a different fingerprint fail the handshake.
	ConsensusFingerprint string `json:"consensus_fingerprint"`
}

// DefaultCraneConfig returns bounded operational defaults and the compiled v1 fingerprint.
func DefaultCraneConfig() CraneConfig {
	return CraneConfig{
		WorkerSlots:                  4,
		WorkerControlTimeout:         Duration(2 * time.Second),
		TupleRetryInterval:           Duration(200 * time.Millisecond),
		TupleCompletionRetryInterval: Duration(time.Second),
		FailureGracePeriod:           Duration(5 * time.Second),
		MaxWorkerStoreBytes:          1 << 30,
		ConsensusFingerprint:         model.ConsensusFingerprintHex(),
	}
}

// Validate checks every local operational bound and the required compiled contract.
func (c CraneConfig) Validate() error {
	maxWorkerSlots := uint16(model.LimitsV1().MaxWorkerSlots)
	if c.WorkerSlots < minCraneWorkerSlots || c.WorkerSlots > maxWorkerSlots {
		return fmt.Errorf("crane worker slots must be between %d and %d", minCraneWorkerSlots, maxWorkerSlots)
	}
	if err := validateCraneDuration("worker control timeout", c.WorkerControlTimeout, 100*time.Millisecond, 30*time.Second); err != nil {
		return err
	}
	if err := validateCraneDuration("tuple retry interval", c.TupleRetryInterval, 10*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := validateCraneDuration("tuple completion retry interval", c.TupleCompletionRetryInterval, 10*time.Millisecond, 30*time.Second); err != nil {
		return err
	}
	if err := validateCraneDuration("failure grace period", c.FailureGracePeriod, 200*time.Millisecond, 5*time.Minute); err != nil {
		return err
	}
	if c.TupleCompletionRetryInterval < c.TupleRetryInterval {
		return fmt.Errorf("crane tuple completion retry interval must not be shorter than tuple retry interval")
	}
	control := time.Duration(c.WorkerControlTimeout)
	if control > time.Duration((1<<63-1)/2) || time.Duration(c.FailureGracePeriod) < control*2 {
		return fmt.Errorf("crane failure grace period must be at least two worker control timeouts")
	}
	if c.MaxWorkerStoreBytes < minWorkerStoreBytes || c.MaxWorkerStoreBytes > maxWorkerStoreBytes {
		return fmt.Errorf("crane worker store bytes must be between %d and %d", minWorkerStoreBytes, maxWorkerStoreBytes)
	}
	if len(c.ConsensusFingerprint) != 64 || strings.ToLower(c.ConsensusFingerprint) != c.ConsensusFingerprint {
		return fmt.Errorf("crane consensus fingerprint must be 64 lowercase hexadecimal characters")
	}
	for _, value := range c.ConsensusFingerprint {
		if value < '0' || value > '9' && value < 'a' || value > 'f' {
			return fmt.Errorf("crane consensus fingerprint must be 64 lowercase hexadecimal characters")
		}
	}
	if c.ConsensusFingerprint != model.ConsensusFingerprintHex() {
		return fmt.Errorf("crane consensus fingerprint does not match this binary")
	}
	return nil
}

func validateCraneDuration(name string, value Duration, minimum, maximum time.Duration) error {
	duration := time.Duration(value)
	if duration < minimum || duration > maximum {
		return fmt.Errorf("crane %s must be between %s and %s", name, minimum, maximum)
	}
	return nil
}

func requiredCraneSnapshotBytes() uint64 {
	return model.LimitsV1().MaxSnapshotBytes
}

// CraneControlEndpointFromRaft derives a canonical +6 control endpoint from a canonical +8 voter endpoint.
func CraneControlEndpointFromRaft(raft Endpoint) (Endpoint, error) {
	canonical, err := CanonicalEndpoint(raft)
	if err != nil {
		return Endpoint{}, fmt.Errorf("invalid raft endpoint: %w", err)
	}
	if canonical != raft {
		return Endpoint{}, fmt.Errorf("raft endpoint must use canonical host and port spelling")
	}
	if err := validateRoutableEndpoint(canonical); err != nil {
		return Endpoint{}, fmt.Errorf("invalid raft endpoint: %w", err)
	}
	raftSpec, raftRegistered := LookupService(ServiceRaftRPC)
	controlSpec, controlRegistered := LookupService(ServiceTopologyControl)
	if !raftRegistered || !controlRegistered || raftSpec.Offset <= controlSpec.Offset {
		return Endpoint{}, fmt.Errorf("invalid service registry for Crane control derivation")
	}
	if canonical.Port < raftSpec.Offset {
		return Endpoint{}, fmt.Errorf("raft endpoint port %d is below required +8 to +6 offset", canonical.Port)
	}
	return Endpoint{Host: canonical.Host, Port: canonical.Port - (raftSpec.Offset - controlSpec.Offset)}, nil
}
