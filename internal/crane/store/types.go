package store

import (
	"fmt"
	"github.com/aadityakv/crane/internal/crane/integrationhook"

	"github.com/aadityakv/crane/internal/crane/model"
)

const (
	// DefaultMaxBytes is the default worker store byte budget: the complete
	// WAL including framing plus every snapshot file and outstanding custody
	// reservation charged against the same ceiling.
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
	// FaultSnapshotTempCreate occurs before creating a snapshot temporary file.
	FaultSnapshotTempCreate FaultPoint = 3
	// FaultSnapshotTempWrite occurs before writing snapshot bytes.
	FaultSnapshotTempWrite FaultPoint = 4
	// FaultSnapshotTempSync occurs before syncing snapshot bytes.
	FaultSnapshotTempSync FaultPoint = 5
	// FaultSnapshotTempClose occurs after closing the snapshot temporary file.
	FaultSnapshotTempClose FaultPoint = 6
	// FaultSnapshotRename occurs before publishing the immutable snapshot file.
	FaultSnapshotRename FaultPoint = 7
	// FaultSnapshotDirectorySync occurs before the snapshot rename directory barrier.
	FaultSnapshotDirectorySync FaultPoint = 8
	// FaultReplacementWALCreate occurs before creating the replacement WAL temporary file.
	FaultReplacementWALCreate FaultPoint = 9
	// FaultReplacementWALWrite occurs before writing the replacement WAL anchor.
	FaultReplacementWALWrite FaultPoint = 10
	// FaultReplacementWALSync occurs before syncing the replacement WAL anchor.
	FaultReplacementWALSync FaultPoint = 11
	// FaultReplacementWALClose occurs after closing the replacement WAL temporary file.
	FaultReplacementWALClose FaultPoint = 12
	// FaultReplacementWALRename occurs before publishing the immutable replacement WAL.
	FaultReplacementWALRename FaultPoint = 13
	// FaultReplacementWALDirectorySync occurs before the replacement WAL rename directory barrier.
	FaultReplacementWALDirectorySync FaultPoint = 14
	// FaultCurrentTempCreate occurs before creating the current-generation marker temporary file.
	FaultCurrentTempCreate FaultPoint = 15
	// FaultCurrentTempWrite occurs before writing the current-generation marker.
	FaultCurrentTempWrite FaultPoint = 16
	// FaultCurrentTempSync occurs before syncing the current-generation marker.
	FaultCurrentTempSync FaultPoint = 17
	// FaultCurrentTempClose occurs after closing the current-generation marker temporary file.
	FaultCurrentTempClose FaultPoint = 18
	// FaultCurrentRename occurs before atomically replacing the current-generation marker.
	FaultCurrentRename FaultPoint = 19
	// FaultCurrentDirectorySync occurs before the current-generation commit directory barrier.
	FaultCurrentDirectorySync FaultPoint = 20
	// FaultPreviousWALClose occurs after closing the previous generation WAL.
	FaultPreviousWALClose FaultPoint = 21
	// FaultObsoleteCleanup occurs before removing obsolete durable generations.
	FaultObsoleteCleanup FaultPoint = 22
	// FaultObsoleteDirectorySync occurs before syncing obsolete-generation cleanup.
	FaultObsoleteDirectorySync FaultPoint = 23
	// FaultCloseWAL occurs after closing the active WAL during Store.Close.
	FaultCloseWAL FaultPoint = 24
	// FaultCloseLock occurs after unlocking and closing the lock file during Store.Close.
	FaultCloseLock FaultPoint = 25
	// FaultCloseRoot occurs after closing the anchored root during Store.Close.
	FaultCloseRoot FaultPoint = 26
	// FaultCloseDirectory occurs after unlocking and closing the directory during Store.Close.
	FaultCloseDirectory FaultPoint = 27
)

// FaultInjector injects failures at explicit durable boundaries.
type FaultInjector interface{ Inject(FaultPoint) error }

// NoopFaultInjector is the production-safe default fault injector.
type NoopFaultInjector struct{}

// Inject never fails a durable boundary.
func (NoopFaultInjector) Inject(FaultPoint) error { return nil }

// String returns the stable diagnostic name of a durable boundary.
func (point FaultPoint) String() string {
	names := [...]string{
		"", "before-append", "before-sync", "snapshot-temp-create", "snapshot-temp-write",
		"snapshot-temp-sync", "snapshot-temp-close", "snapshot-rename", "snapshot-directory-sync",
		"replacement-wal-create", "replacement-wal-write", "replacement-wal-sync", "replacement-wal-close",
		"replacement-wal-rename", "replacement-wal-directory-sync", "current-temp-create", "current-temp-write",
		"current-temp-sync", "current-temp-close", "current-rename", "current-directory-sync", "previous-wal-close",
		"obsolete-cleanup", "obsolete-directory-sync", "close-wal", "close-lock", "close-root", "close-directory",
	}
	if int(point) >= len(names) {
		return fmt.Sprintf("fault-%d", point)
	}
	return names[point]
}

func allFaultPoints() []FaultPoint {
	result := make([]FaultPoint, FaultCloseDirectory)
	for i := range result {
		result[i] = FaultPoint(i + 1)
	}
	return result
}

// Options fixes store bounds and new-store epoch creation for one lifetime.
type Options struct {
	// MaxBytes bounds the complete WAL including framing together with the
	// snapshot files and custody reservations charged against the same
	// budget; zero selects DefaultMaxBytes.
	MaxBytes uint64
	// Faults optionally injects deterministic durable-boundary failures.
	Faults FaultInjector
	// NewWorkerEpoch supplies a nonzero epoch only for a truly empty new store.
	NewWorkerEpoch func() (model.WorkerEpoch, error)
	// Hook observes named durable boundaries strictly after each
	// transaction's fsync succeeded and before the mutation returns to its
	// caller; nil selects the production no-op hook.
	Hook integrationhook.Hook
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

// RecoveredState is bounded WAL metadata published only after complete validation.
type RecoveredState struct {
	// Identity is the exact on-disk cluster/member binding.
	Identity Identity
	// WorkerEpoch is the persisted nonzero worker incarnation fence.
	WorkerEpoch model.WorkerEpoch
	// LastSequence is the last complete WAL record sequence.
	LastSequence uint64
	// WALBytes is the exact durable WAL length after safe tail truncation.
	WALBytes uint64
	// TransactionCount is the number of complete validated transactions.
	TransactionCount uint64
	// SnapshotGeneration is the current committed snapshot generation, or zero for a legacy WAL-only store.
	SnapshotGeneration uint64
	// SnapshotBytes is the exact current committed snapshot length.
	SnapshotBytes uint64
}

// Clone returns an independently owned recovered metadata value.
func (state RecoveredState) Clone() RecoveredState {
	return state
}

func validateEpoch(epoch model.WorkerEpoch) error {
	if err := epoch.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidIdentity, err)
	}
	return nil
}
