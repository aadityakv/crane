package raft

import (
	"context"
	"sync"
)

const (
	// PeerQueueCapacity is the fixed number of unsent semantic intents retained per remote voter.
	PeerQueueCapacity = 64
)

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

type peerIntentQueue struct {
	mu       sync.Mutex
	items    []PeerMessage
	capacity int
	notify   chan struct{}
	closed   bool
}

func newPeerIntentQueue(capacity int) *peerIntentQueue {
	return &peerIntentQueue{capacity: capacity, notify: make(chan struct{}, 1)}
}

func (queue *peerIntentQueue) offer(message PeerMessage) TransportHandoff {
	if queue == nil {
		return TransportUnavailable
	}
	owned := PeerMessage{To: message.To, RPC: CloneRPC(message.RPC), Requires: message.Requires}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed || owned.RPC == nil {
		return TransportUnavailable
	}
	if len(queue.items) != 0 && canCoalesceAppend(queue.items[len(queue.items)-1], owned) {
		queue.items[len(queue.items)-1] = owned
		queue.signalLocked()
		return TransportAccepted
	}
	if len(queue.items) >= queue.capacity {
		return TransportUnavailable
	}
	queue.items = append(queue.items, owned)
	queue.signalLocked()
	return TransportAccepted
}

func canCoalesceAppend(current, replacement PeerMessage) bool {
	left, leftOK := normalizeRPC(current.RPC).(AppendEntriesRequest)
	right, rightOK := normalizeRPC(replacement.RPC).(AppendEntriesRequest)
	if !leftOK || !rightOK || current.To != replacement.To || left.Term != right.Term || right.Generation < left.Generation {
		return false
	}
	return len(left.Entries) == 0 || len(right.Entries) != 0
}

func (queue *peerIntentQueue) take(ctx context.Context) (PeerMessage, bool) {
	for {
		if message, ok := queue.takeNow(); ok {
			return message, true
		}
		queue.mu.Lock()
		closed := queue.closed
		queue.mu.Unlock()
		if closed {
			return PeerMessage{}, false
		}
		select {
		case <-queue.notify:
		case <-ctx.Done():
			return PeerMessage{}, false
		}
	}
}

func (queue *peerIntentQueue) takeNow() (PeerMessage, bool) {
	if queue == nil {
		return PeerMessage{}, false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items) == 0 {
		return PeerMessage{}, false
	}
	message := queue.items[0]
	copy(queue.items, queue.items[1:])
	queue.items[len(queue.items)-1] = PeerMessage{}
	queue.items = queue.items[:len(queue.items)-1]
	if len(queue.items) != 0 {
		queue.signalLocked()
	}
	return message, true
}

func (queue *peerIntentQueue) length() int {
	if queue == nil {
		return 0
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.items)
}

func (queue *peerIntentQueue) close() {
	if queue == nil {
		return
	}
	queue.mu.Lock()
	queue.closed = true
	for index := range queue.items {
		queue.items[index] = PeerMessage{}
	}
	queue.items = nil
	queue.signalLocked()
	queue.mu.Unlock()
}

func (queue *peerIntentQueue) signalLocked() {
	select {
	case queue.notify <- struct{}{}:
	default:
	}
}
