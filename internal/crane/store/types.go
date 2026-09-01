package store

import (
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	// DefaultMaxBytes is the default complete worker WAL ceiling.
	DefaultMaxBytes uint64 = 1 << 30
	// MaxRecordPayloadBytes bounds one future domain record before allocation.
	MaxRecordPayloadBytes = 1 << 20
	// MaxTransactionRecords bounds one atomic WAL transaction.
	MaxTransactionRecords = 1024
)

// Identity binds a worker store to one configured cluster member.
type Identity struct {
	// ClusterID prevents a copied store from joining another cluster.
	ClusterID [16]byte
	// NodeID prevents a copied store from impersonating another member.
	NodeID uint16
}

// Validate rejects zero cluster or node identities.
func (identity Identity) Validate() error {
	if identity.ClusterID == ([16]byte{}) || identity.NodeID == 0 {
		return ErrInvalidIdentity
	}
	return nil
}

// FaultPoint identifies a durable boundary available to deterministic tests.
type FaultPoint uint8

const (
	// FaultBeforeAppend occurs after validation/preflight and before WAL writes.
	FaultBeforeAppend FaultPoint = 1
	// FaultBeforeSync occurs after a complete append and before fsync.
	FaultBeforeSync FaultPoint = 2
)

// FaultInjector injects failures at explicit durable boundaries.
type FaultInjector interface{ Inject(FaultPoint) error }

// Options fixes store bounds and new-store epoch creation for one lifetime.
type Options struct {
	// MaxBytes bounds the complete WAL including framing; zero selects DefaultMaxBytes.
	MaxBytes uint64
	// Faults optionally injects deterministic durable-boundary failures.
	Faults FaultInjector
	// NewWorkerEpoch supplies a nonzero epoch only for a truly empty new store.
	NewWorkerEpoch func() (model.WorkerEpoch, error)
}

// RecordType identifies one Task14+ application record inside a transaction.
type RecordType uint16

// Record is one bounded future domain mutation with owned canonical payload.
type Record struct {
	// Type selects the concrete Task14+ record schema and must be nonzero.
	Type RecordType
	// Payload is that record's complete canonical bytes.
	Payload []byte
}

// Clone returns an independently owned record.
func (record Record) Clone() Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}

// Transaction is one crash-atomic ordered group of future domain records.
type Transaction struct {
	// Records are applied in order only after the matching commit boundary is durable.
	Records []Record
}

// Clone returns an independently owned transaction.
func (transaction Transaction) Clone() Transaction {
	result := Transaction{Records: make([]Record, len(transaction.Records))}
	for i := range transaction.Records {
		result.Records[i] = transaction.Records[i].Clone()
	}
	return result
}

// Validate rejects an empty, oversized, unknown-type, or oversized-payload transaction.
func (transaction Transaction) Validate() error {
	if len(transaction.Records) == 0 || len(transaction.Records) > MaxTransactionRecords {
		return fmt.Errorf("%w: record count %d", ErrInvalidTransaction, len(transaction.Records))
	}
	for i, record := range transaction.Records {
		if record.Type == 0 || len(record.Payload) > MaxRecordPayloadBytes {
			return fmt.Errorf("%w: record %d type=%d bytes=%d", ErrInvalidTransaction, i, record.Type, len(record.Payload))
		}
	}
	return nil
}

// RecoveredState is a complete owned replay foundation for Task14+.
type RecoveredState struct {
	// Identity is the exact on-disk cluster/member binding.
	Identity Identity
	// WorkerEpoch is the persisted nonzero worker incarnation fence.
	WorkerEpoch model.WorkerEpoch
	// LastSequence is the last complete WAL record sequence.
	LastSequence uint64
	// WALBytes is the exact durable WAL length after safe tail truncation.
	WALBytes uint64
	// Transactions are the committed, ordered, independently owned mutations.
	Transactions []Transaction
}

// Clone returns a completely independently owned recovered state.
func (state RecoveredState) Clone() RecoveredState {
	source := state.Transactions
	state.Transactions = make([]Transaction, len(source))
	for i := range source {
		state.Transactions[i] = source[i].Clone()
	}
	return state
}

func validateEpoch(epoch model.WorkerEpoch) error {
	if err := epoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidIdentity, err)
	}
	return nil
}
