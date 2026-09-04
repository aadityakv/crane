// Package store is the crash-safe durable state of one Crane worker. Store
// exclusively owns a worker's write-ahead log and the snapshot files beside
// it, applying every mutation (deliveries, outboxes, checkpoints, results,
// events, repairs, assignments, and fences) as one crash-atomic Transaction
// of typed Records. RecoverWork rebuilds the validated high-level
// RecoveredWork from the log on open, and Snapshot compacts it under the
// same byte budget. The package guards the invariant that no mutation is
// visible to the worker before it is fsynced, and that a store can never be
// opened by two processes at once or by a member other than the one whose
// Identity it was created under.
package store
