// Package raft implements the fixed-voter Raft consensus that replicates
// Crane's coordinator state. Core is the synchronous, deterministic protocol
// state machine for one voter; Node serializes a Core with its StableStore,
// StateMachine, and transport into one event loop that surfaces committed
// proposals and leadership changes; Service wires a Node to durable recovery
// and the authenticated TCP transport as a supervised node service. VoterSet
// fixes the quorum and trust boundary, and Snapshot carries portable
// application state between peers. The package guards the invariant that no
// term, vote, or log entry is ever handed to a peer before the state it
// depends on is durable in the StableStore.
package raft
