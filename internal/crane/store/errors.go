package store

import "errors"

var (
	ErrInvalidIdentity    = errors.New("invalid worker store identity")
	ErrIdentityMismatch   = errors.New("worker store identity mismatch")
	ErrInvalidOptions     = errors.New("invalid worker store options")
	ErrInvalidTransaction = errors.New("invalid worker store transaction")
	ErrCorrupt            = errors.New("worker store corrupt")
	ErrLocked             = errors.New("worker store locked")
	ErrCapacity           = errors.New("worker store capacity exhausted")
	ErrClosed             = errors.New("worker store closed")
	ErrUnavailable        = errors.New("worker store unavailable after failed persistence")
)
