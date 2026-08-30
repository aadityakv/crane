package raft

// ReadyToken identifies one exact, live protocol Ready batch.
type ReadyToken uint64

// DurabilityPrerequisite describes state that must be durable before a message
// may be handed to a peer worker. EntriesThrough is zero when no entry
// durability is required.
type DurabilityPrerequisite struct {
	// HardState requires the HardState in the same Ready batch to be durable.
	HardState bool
	// EntriesThrough requires unstable entries through this inclusive index to be durable.
	EntriesThrough uint64
}

// PeerMessage is one owned outbound RPC and its durability handoff gate.
type PeerMessage struct {
	// To is the configured destination voter.
	To uint16
	// RPC is an owned canonical Task 3 protocol value.
	RPC RPC
	// Requires states what the owner must persist before bounded peer handoff.
	Requires DurabilityPrerequisite
}

// ProposalID identifies one leader-local proposal independently of command bytes.
type ProposalID uint64

// CommittedProposal hands off one exact committed local proposal for later application completion.
type CommittedProposal struct {
	// ID is the unique leader-local proposal identity.
	ID ProposalID
	// Entry is the exact term, index, kind, and content identity that committed.
	Entry Entry
}

// FailedProposal reports one exact local proposal that can no longer commit as submitted.
type FailedProposal struct {
	// ID is the unique leader-local proposal identity.
	ID ProposalID
	// Entry is the exact entry originally bound to the proposal.
	Entry Entry
	// Err classifies why the pending proposal can no longer complete.
	Err error
}

// SnapshotActionKind identifies one owner-executed snapshot storage operation.
type SnapshotActionKind uint8

const (
	// SnapshotActionStage durably stages one exact InstallSnapshot chunk.
	SnapshotActionStage SnapshotActionKind = iota + 1
	// SnapshotActionAbort removes any partially staged incoming snapshot.
	SnapshotActionAbort
)

// SnapshotAction separates protocol validation in Core from snapshot bytes and
// filesystem ownership in Node and StableStore.
type SnapshotAction struct {
	Kind SnapshotActionKind
	// Request is an owned canonical request identifying the operation.
	Request InstallSnapshotRequest
	// Reset requires an older active staging file to be discarded first.
	Reset bool
}

// SnapshotActionResult reports the durable result of one exact action.
type SnapshotActionResult struct {
	NextOffset uint64
	Done       bool
	State      RecoveredState
	Rejected   bool
}

// Ready is one immutable-by-convention owned protocol output batch.
type Ready struct {
	// Token must be supplied exactly once to Core.Advance.
	Token ReadyToken
	// HardState is the latest safety-critical state to persist, or nil when unchanged.
	HardState *HardState
	// Entries are owned unstable log entries to persist in ascending order.
	Entries []Entry
	// Messages are owned outbound peer RPCs with explicit durability gates.
	Messages []PeerMessage
	// CommittedEntries are newly committed owned entries to process in ascending order.
	CommittedEntries []Entry
	// CommittedProposals are exact local proposal identities handed to the later apply owner.
	CommittedProposals []CommittedProposal
	// FailedProposals are exact local proposals invalidated before committed handoff.
	FailedProposals []FailedProposal
	// SnapshotActions are owner-executed storage operations. A batch containing
	// one cannot advance until CompleteSnapshotAction accepts its durable result.
	SnapshotActions []SnapshotAction
}

// Clone returns an independently owned copy of the complete batch.
func (ready Ready) Clone() Ready {
	owned := Ready{Token: ready.Token}
	if ready.HardState != nil {
		hardState := *ready.HardState
		owned.HardState = &hardState
	}
	owned.Entries = cloneEntries(ready.Entries)
	owned.CommittedEntries = cloneEntries(ready.CommittedEntries)
	if ready.CommittedProposals != nil {
		owned.CommittedProposals = make([]CommittedProposal, len(ready.CommittedProposals))
		for index, proposal := range ready.CommittedProposals {
			owned.CommittedProposals[index] = CommittedProposal{ID: proposal.ID, Entry: proposal.Entry.Clone()}
		}
	}
	if ready.FailedProposals != nil {
		owned.FailedProposals = make([]FailedProposal, len(ready.FailedProposals))
		for index, proposal := range ready.FailedProposals {
			owned.FailedProposals[index] = FailedProposal{ID: proposal.ID, Entry: proposal.Entry.Clone(), Err: proposal.Err}
		}
	}
	if ready.Messages != nil {
		owned.Messages = make([]PeerMessage, len(ready.Messages))
		for index, message := range ready.Messages {
			owned.Messages[index] = PeerMessage{
				To:       message.To,
				RPC:      CloneRPC(message.RPC),
				Requires: message.Requires,
			}
		}
	}
	if ready.SnapshotActions != nil {
		owned.SnapshotActions = make([]SnapshotAction, len(ready.SnapshotActions))
		for index, action := range ready.SnapshotActions {
			owned.SnapshotActions[index] = SnapshotAction{
				Kind:    action.Kind,
				Request: CloneRPC(action.Request).(InstallSnapshotRequest),
				Reset:   action.Reset,
			}
		}
	}
	return owned
}
