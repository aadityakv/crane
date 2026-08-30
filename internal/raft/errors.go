package raft

import "errors"

var (
	// ErrInvalidVoterSet classifies malformed fixed voter configurations.
	ErrInvalidVoterSet = errors.New("invalid raft voter set")
	// ErrNotVoter classifies a node ID outside the fixed voter set.
	ErrNotVoter = errors.New("raft node is not a configured voter")
	// ErrInvalidStorageIdentity classifies incomplete or inconsistent persisted identity fields.
	ErrInvalidStorageIdentity = errors.New("invalid raft storage identity")
	// ErrInvalidEntry classifies an entry outside the canonical Raft log domain.
	ErrInvalidEntry = errors.New("invalid raft entry")
	// ErrMalformedRPC classifies a payload whose binary layout is incomplete or non-canonical.
	ErrMalformedRPC = errors.New("malformed raft RPC")
	// ErrUnsupportedSchema classifies a payload encoded with an unknown schema version.
	ErrUnsupportedSchema = errors.New("unsupported raft RPC schema")
	// ErrRPCTooLarge classifies a payload or nested byte string above an explicit codec bound.
	ErrRPCTooLarge = errors.New("raft RPC too large")
	// ErrInvalidRPC classifies a structurally complete payload with invalid Raft semantics.
	ErrInvalidRPC = errors.New("invalid raft RPC")
	// ErrUnknownRPC classifies a wire message that has no v1 Raft payload schema.
	ErrUnknownRPC = errors.New("unknown raft RPC")
	// ErrVoterFingerprint classifies a handshake for another configured voter set.
	ErrVoterFingerprint = errors.New("raft voter fingerprint mismatch")
)
