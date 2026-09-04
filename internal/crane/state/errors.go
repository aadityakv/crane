// Package state implements Crane's deterministic Raft-applied command state.
package state

import "errors"

var (
	// ErrMalformedCommand reports bytes that do not decode as a canonical command.
	ErrMalformedCommand = errors.New("malformed crane command")
	// ErrUnsupportedCommandSchema reports a command stamped with a schema version this build does not apply.
	ErrUnsupportedCommandSchema = errors.New("unsupported crane command schema")
	// ErrConsensusFingerprintMismatch reports a command produced under a different consensus contract.
	ErrConsensusFingerprintMismatch = errors.New("crane command consensus fingerprint mismatch")
	// ErrUnknownCommandKind reports a command kind outside the replicated set.
	ErrUnknownCommandKind = errors.New("unknown crane command kind")
	// ErrCommandDigestMismatch reports a command whose digest does not cover its actual content.
	ErrCommandDigestMismatch = errors.New("crane command digest mismatch")
	// ErrInvalidCommandSubject reports a command whose subject key does not match its kind or target.
	ErrInvalidCommandSubject = errors.New("invalid crane command subject")
	// ErrInvalidCommand reports a decodable command whose fields violate the state contract.
	ErrInvalidCommand = errors.New("invalid crane command")
	// ErrMalformedCommandResult reports bytes that do not decode as a canonical command result.
	ErrMalformedCommandResult = errors.New("malformed crane command result")
	// ErrInvalidSnapshot reports a snapshot that fails structural validation.
	ErrInvalidSnapshot = errors.New("invalid crane state snapshot")
	// ErrSnapshotTooLarge identifies a snapshot that exceeds the consensus size limit.
	ErrSnapshotTooLarge = errors.New("crane state snapshot exceeds size limit")
	// ErrSnapshotOrder identifies a duplicate or non-canonical snapshot collection order.
	ErrSnapshotOrder = errors.New("invalid crane snapshot collection order")
	// ErrSnapshotCrossReference identifies inconsistent state between snapshot collections.
	ErrSnapshotCrossReference = errors.New("invalid crane snapshot cross-reference")
	// ErrInvalidApplyIndex reports an apply index that does not advance the machine's last applied index.
	ErrInvalidApplyIndex = errors.New("invalid crane apply index")
)
