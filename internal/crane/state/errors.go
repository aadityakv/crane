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
	ErrInvalidApplyIndex            = errors.New("invalid crane apply index")
)
