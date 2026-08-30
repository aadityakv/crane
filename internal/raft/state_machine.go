package raft

// ProposalResult is the exact applied log position and owned application result.
type ProposalResult struct {
	// Index is the exact applied log index.
	Index uint64
	// Term is the term that created the applied entry.
	Term uint64
	// Result contains an owned copy of the deterministic application result.
	Result []byte
}

// SnapshotCapture is an immutable application snapshot at one exact applied position.
type SnapshotCapture interface {
	// SchemaVersion identifies the application snapshot encoding.
	SchemaVersion() uint32
	// MarshalBinary returns independently owned deterministic snapshot bytes.
	MarshalBinary() ([]byte, error)
}

// StateMachine is the deterministic application boundary owned by one Raft node.
type StateMachine interface {
	// Apply applies one committed command at its exact log position.
	Apply(index, term uint64, command []byte) ([]byte, error)
	// Capture freezes application state at one exact applied log position.
	Capture(index, term uint64) (SnapshotCapture, error)
	// Restore replaces application state from one schema-versioned snapshot.
	Restore(schemaVersion uint32, snapshot []byte) error
}
