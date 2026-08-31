// Package state implements Crane's deterministic Raft-applied command state.
package state

import "errors"

var (
	ErrMalformedCommand             = errors.New("malformed crane command")
	ErrUnsupportedCommandSchema     = errors.New("unsupported crane command schema")
	ErrConsensusFingerprintMismatch = errors.New("crane command consensus fingerprint mismatch")
	ErrUnknownCommandKind           = errors.New("unknown crane command kind")
	ErrCommandDigestMismatch        = errors.New("crane command digest mismatch")
	ErrInvalidCommandSubject        = errors.New("invalid crane command subject")
	ErrInvalidCommand               = errors.New("invalid crane command")
	ErrMalformedCommandResult       = errors.New("malformed crane command result")
	ErrInvalidSnapshot              = errors.New("invalid crane state snapshot")
	// ErrSnapshotTooLarge identifies a snapshot that exceeds the consensus size limit.
	ErrSnapshotTooLarge = errors.New("crane state snapshot exceeds size limit")
	// ErrSnapshotOrder identifies a duplicate or non-canonical snapshot collection order.
	ErrSnapshotOrder = errors.New("invalid crane snapshot collection order")
	// ErrSnapshotCrossReference identifies inconsistent state between snapshot collections.
	ErrSnapshotCrossReference = errors.New("invalid crane snapshot cross-reference")
	ErrInvalidApplyIndex      = errors.New("invalid crane apply index")
)
