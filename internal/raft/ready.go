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
}

// Clone returns an independently owned copy of the complete batch.
func (ready Ready) Clone() Ready {
	owned := Ready{Token: ready.Token}
	if ready.HardState != nil {
		hardState := *ready.HardState
		owned.HardState = &hardState
	}
	owned.Entries = cloneEntries(ready.Entries)
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
	return owned
}
