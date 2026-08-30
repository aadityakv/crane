package raft

// TransportHandoff classifies one bounded local peer-worker handoff attempt.
type TransportHandoff uint8

const (
	// TransportAccepted means a peer worker accepted ownership of the message.
	TransportAccepted TransportHandoff = iota + 1
	// TransportUnavailable means the peer worker was unavailable or full.
	TransportUnavailable
)

// Transport is the minimal bounded Task 8 peer-message handoff seam.
type Transport interface {
	// Handoff transfers ownership without waiting for network delivery.
	Handoff(PeerMessage) (TransportHandoff, error)
}
