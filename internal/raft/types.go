package raft

import "fmt"

// Role identifies one deterministic Raft protocol role.
type Role uint8

const (
	// RoleFollower receives leader replication and may begin a pre-vote.
	RoleFollower Role = iota + 1
	// RolePreCandidate is gathering pre-votes without changing durable term state.
	RolePreCandidate
	// RoleCandidate is requesting votes in its current durable term.
	RoleCandidate
	// RoleLeader replicates entries for its current term.
	RoleLeader
)

// EntryKind distinguishes application commands from internal leadership barriers.
type EntryKind uint8

const (
	// EntryCommand carries an opaque application command.
	EntryCommand EntryKind = iota + 1
	// EntryNoOp advances commitment without invoking the application.
	EntryNoOp
)

// Entry is one immutable-by-convention canonical Raft log value.
type Entry struct {
	// Index is the nonzero logical log position.
	Index uint64
	// Term is the nonzero election term that created the entry.
	Term uint64
	// Kind determines whether the entry carries an application command.
	Kind    EntryKind
	command []byte
}

// NewEntry validates an entry and takes an owned copy of command.
func NewEntry(index, term uint64, kind EntryKind, command []byte) (Entry, error) {
	if index == 0 || term == 0 {
		return Entry{}, fmt.Errorf("%w: index and term must be nonzero", ErrInvalidEntry)
	}
	if kind != EntryCommand && kind != EntryNoOp {
		return Entry{}, fmt.Errorf("%w: unknown kind %d", ErrInvalidEntry, kind)
	}
	if kind == EntryNoOp && len(command) != 0 {
		return Entry{}, fmt.Errorf("%w: no-op entry carries command bytes", ErrInvalidEntry)
	}
	return Entry{Index: index, Term: term, Kind: kind, command: cloneBytes(command)}, nil
}

// CommandBytes returns an owned copy of the opaque command.
func (e Entry) CommandBytes() []byte {
	return cloneBytes(e.command)
}

// Clone returns an independently owned entry.
func (e Entry) Clone() Entry {
	e.command = cloneBytes(e.command)
	return e
}

// HardState contains safety-critical state that must survive restart.
type HardState struct {
	// Term is the greatest durable term observed by the voter.
	Term uint64
	// VotedFor is the voter granted a vote in Term, or zero when none was granted.
	VotedFor uint16
	// CommitIndex is the greatest log index known committed.
	CommitIndex uint64
}

// SnapshotMetadata identifies the application state covered by a snapshot.
type SnapshotMetadata struct {
	// LastIncludedIndex is the final log index represented by the snapshot.
	LastIncludedIndex uint64
	// LastIncludedTerm is the term at LastIncludedIndex.
	LastIncludedTerm uint64
	// StateMachineSchemaVersion identifies the application snapshot schema.
	StateMachineSchemaVersion uint32
}

// Status is a point-in-time, non-linearizable diagnostic view of a voter.
type Status struct {
	// Role is the voter's current protocol role.
	Role Role
	// Term is the current term.
	Term uint64
	// LeaderID is the best-effort known leader, or zero when unknown.
	LeaderID uint16
	// CommitIndex is the greatest committed index.
	CommitIndex uint64
	// AppliedIndex is the greatest applied index.
	AppliedIndex uint64
	// LastIndex is the greatest available log or snapshot index.
	LastIndex uint64
}

// RequestGeneration identifies one leader-local replication attempt.
type RequestGeneration uint64

// TransferID identifies one snapshot transfer attempt independently of wire retries.
type TransferID [16]byte

// IsZero reports whether the transfer identifier is unset.
func (id TransferID) IsZero() bool {
	return id == TransferID{}
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned
}
