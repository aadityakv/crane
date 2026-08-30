package raft

// Progress is an owned diagnostic view of one leader's replication state for a voter.
type Progress struct {
	// MatchIndex is the greatest index proven stored by the voter.
	MatchIndex uint64
	// NextIndex is the first index to send and never falls below MatchIndex plus one.
	NextIndex uint64
	// Generation is the greatest peer-local request generation issued.
	Generation RequestGeneration
	// ActiveGeneration is the one live response generation, or zero when none is live.
	ActiveGeneration RequestGeneration
	// SnapshotNeeded reports that log replication cannot cross the compacted base.
	SnapshotNeeded bool
	// ActiveTransferID is the live peer-local snapshot transfer, or zero.
	ActiveTransferID TransferID
	// ActiveSnapshotID is the immutable snapshot bound to ActiveTransferID.
	ActiveSnapshotID SnapshotID
	// SnapshotNextOffset is the next unacknowledged byte in the active transfer.
	SnapshotNextOffset uint64

	activeMatchIndex uint64
}
