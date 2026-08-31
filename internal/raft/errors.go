package raft

import "errors"

var (
	// ErrInvalidVoterSet classifies malformed fixed voter configurations.
	ErrInvalidVoterSet = errors.New("invalid raft voter set")
	// ErrNotVoter classifies a node ID outside the fixed voter set.
	ErrNotVoter = errors.New("raft node is not a configured voter")
	// ErrInvalidStorageIdentity classifies incomplete or inconsistent persisted identity fields.
	ErrInvalidStorageIdentity = errors.New("invalid raft storage identity")
	// ErrInvalidStorageState classifies a recovered or prospective state that violates durable Raft invariants.
	ErrInvalidStorageState = errors.New("invalid raft storage state")
	// ErrStorageIdentityMismatch classifies durable state belonging to another format, cluster, voter, or voter set.
	ErrStorageIdentityMismatch = errors.New("raft storage identity mismatch")
	// ErrStoreClosed classifies use of a stable store after its one successful close.
	ErrStoreClosed = errors.New("raft stable store is closed")
	// ErrStorageCorrupt classifies complete durable bytes that cannot safely be interpreted.
	ErrStorageCorrupt = errors.New("raft storage is corrupt")
	// ErrStorageLocked classifies a storage directory already owned by another live store.
	ErrStorageLocked = errors.New("raft storage is locked")
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
	// ErrUnknownRPC classifies a wire message that has no supported Raft payload schema.
	ErrUnknownRPC = errors.New("unknown raft RPC")
	// ErrVoterFingerprint classifies a handshake for another configured voter set.
	ErrVoterFingerprint = errors.New("raft voter fingerprint mismatch")
	// ErrApplicationFingerprint classifies a peer running incompatible application rules.
	ErrApplicationFingerprint = errors.New("raft application fingerprint mismatch")
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
	// ErrInvalidCoreState classifies inconsistent recovered state or deterministic core options.
	ErrInvalidCoreState = errors.New("invalid raft core state")
	// ErrReadyOutstanding classifies input attempted before the current protocol batch advances.
	ErrReadyOutstanding = errors.New("raft Ready batch is outstanding")
	// ErrAdvanceToken classifies a missing, stale, or mismatched Ready token.
	ErrAdvanceToken = errors.New("invalid raft Ready advance token")
	// ErrReadyTokenExhausted classifies terminal exhaustion of unique Ready tokens.
	ErrReadyTokenExhausted = errors.New("raft Ready token space exhausted")
	// ErrDeadlineOverflow classifies election deadline arithmetic outside uint64.
	ErrDeadlineOverflow = errors.New("raft election deadline overflow")
	// ErrTickRegression classifies a logical tick earlier than the last accepted tick.
	ErrTickRegression = errors.New("raft logical tick regressed")
	// ErrTermOverflow classifies an election that cannot increment MaxUint64.
	ErrTermOverflow = errors.New("raft term overflow")
	// ErrReplicationGenerationOverflow classifies a peer request generation that cannot advance.
	ErrReplicationGenerationOverflow = errors.New("raft replication generation overflow")
	// ErrNotLeader classifies a proposal submitted while the local voter is not leader.
	ErrNotLeader = errors.New("raft voter is not leader")
	// ErrLeadershipNotAuthorized classifies a leader fenced until a current-term commit.
	ErrLeadershipNotAuthorized = errors.New("raft leadership is not authorized by a current-term commit")
	// ErrProposalFailed classifies an exact pending proposal lost before committed handoff.
	ErrProposalFailed = errors.New("raft proposal failed before exact committed handoff")
	// ErrProposalIdentityOverflow classifies a leader-local proposal identity that cannot advance.
	ErrProposalIdentityOverflow = errors.New("raft proposal identity overflow")
	// ErrUnsupportedCoreRPC classifies a validated payload outside the current core task boundary.
	ErrUnsupportedCoreRPC = errors.New("unsupported raft core RPC")
	// ErrNotRunning classifies a local API call made before Run becomes ready.
	ErrNotRunning = errors.New("raft node is not running")
	// ErrStopped classifies a local API call made after terminal node shutdown.
	ErrStopped = errors.New("raft node is stopped")
	// ErrOverloaded classifies a bounded local or inbound queue at capacity.
	ErrOverloaded = errors.New("raft node is overloaded")
	// ErrLeadershipResyncRequired classifies a leadership stream that overflowed.
	ErrLeadershipResyncRequired = errors.New("raft leadership resynchronization required")
	// ErrInvalidLeadershipCapacity classifies a subscription capacity outside its bounded domain.
	ErrInvalidLeadershipCapacity = errors.New("invalid raft leadership subscription capacity")
	// ErrLeadershipSequenceOverflow classifies terminal exhaustion of leadership event sequencing.
	ErrLeadershipSequenceOverflow = errors.New("raft leadership sequence exhausted")
	// ErrSnapshotUnavailable classifies recovered snapshot metadata without Task 9 snapshot bytes.
	ErrSnapshotUnavailable = errors.New("raft snapshot bytes unavailable")
	// ErrInvalidSnapshot classifies malformed, corrupt, oversized, or identity-mismatched snapshot bytes.
	ErrInvalidSnapshot = errors.New("invalid raft snapshot")
	// ErrSnapshotRejected classifies a peer snapshot chunk that cannot continue the active transfer safely.
	ErrSnapshotRejected = errors.New("raft snapshot chunk rejected")
	// ErrTransferIDExhausted classifies inability to allocate a nonzero peer-local snapshot transfer identity.
	ErrTransferIDExhausted = errors.New("raft snapshot transfer identity exhausted")
	// ErrTransportInvariant classifies an invalid result from the bounded transport handoff seam.
	ErrTransportInvariant = errors.New("invalid raft transport handoff result")
	// ErrTransportStopped classifies a repeated transport run or terminal transport use.
	ErrTransportStopped = errors.New("raft transport is stopped")
	// ErrTransportProtocol classifies a rejected authenticated peer stream.
	ErrTransportProtocol = errors.New("invalid raft transport protocol")
	// ErrRequestIDExhausted classifies zero, reused, or unavailable wire request identity.
	ErrRequestIDExhausted = errors.New("raft wire request identity exhausted")
)

// NotLeaderError carries a best-effort, non-authoritative leader hint.
type NotLeaderError struct {
	// LeaderID is the last leader identity observed by the local voter, or zero.
	LeaderID uint16
}

// Error returns a command-free diagnostic string.
func (err *NotLeaderError) Error() string {
	if err == nil || err.LeaderID == 0 {
		return ErrNotLeader.Error()
	}
	return ErrNotLeader.Error() + "; best-effort leader hint is available"
}

// Unwrap preserves errors.Is classification as ErrNotLeader.
func (*NotLeaderError) Unwrap() error { return ErrNotLeader }
