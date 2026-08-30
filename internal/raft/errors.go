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
	// ErrLogCompacted classifies a log index hidden by the current snapshot base.
	ErrLogCompacted = errors.New("raft log index compacted")
	// ErrLogUnavailable classifies a log index beyond the last available entry.
	ErrLogUnavailable = errors.New("raft log index unavailable")
	// ErrLogGap classifies a non-contiguous append or recovered retained suffix.
	ErrLogGap = errors.New("raft log contains an index gap")
	// ErrLogMismatch classifies a previous-index term mismatch during log repair.
	ErrLogMismatch = errors.New("raft log term mismatch")
	// ErrCommittedConflict classifies an attempt to replace committed log state.
	ErrCommittedConflict = errors.New("raft committed log conflict")
	// ErrLogRegression classifies an attempt to move a protected log index backward.
	ErrLogRegression = errors.New("raft protected log index regression")
	// ErrLogOverflow classifies checked Raft log index arithmetic that cannot be represented.
	ErrLogOverflow = errors.New("raft log index overflow")
	// ErrLogInvariant classifies an operation or recovered state that violates a Raft log invariant.
	ErrLogInvariant = errors.New("raft log invariant violation")
)
