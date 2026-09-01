package store

import "errors"

var (
	// ErrInvalidIdentity rejects a zero cluster or node identity.
	ErrInvalidIdentity = errors.New("invalid worker store identity")
	// ErrIdentityMismatch rejects a store copied from another cluster member.
	ErrIdentityMismatch = errors.New("worker store identity mismatch")
	// ErrInvalidOptions rejects unsafe paths or impossible configured bounds.
	ErrInvalidOptions = errors.New("invalid worker store options")
	// ErrInvalidTransaction rejects malformed or arithmetic-invalid transactions.
	ErrInvalidTransaction = errors.New("invalid worker store transaction")
	// ErrCorrupt reports invalid durable bytes or unsafe filesystem metadata.
	ErrCorrupt = errors.New("worker store corrupt")
	// ErrLocked reports another live owner of the anchored worker directory.
	ErrLocked = errors.New("worker store locked")
	// ErrCapacity reports a prospective transaction exceeding the WAL byte limit.
	ErrCapacity = errors.New("worker store capacity exhausted")
	// ErrClosed reports an operation attempted after Store.Close.
	ErrClosed = errors.New("worker store closed")
	// ErrUnavailable reports a Store poisoned by an ambiguous persistence failure.
	ErrUnavailable = errors.New("worker store unavailable after failed persistence")
	// ErrHistoricalAuthorityUnavailable reports a checkpoint-compacted delivery
	// whose old assignment authority was intentionally retired after the causal
	// safe frontier and therefore cannot be authenticated against a replacement.
	ErrHistoricalAuthorityUnavailable = errors.New("compacted delivery historical authority unavailable")
)
