// Package integrationhook is the production-negative seam that lets the
// real-process integration harness observe exact durable boundaries and
// schedule bounded datagram faults inside a live node.
//
// The ordinary build of every binary contains only the no-op Hook: no event
// registry, no activation parser, no command reader, no listener, and no
// environment or descriptor inspection. Only a binary built with the
// `craneintegration` tag can activate the seam, and it does so exclusively
// through one inherited test-owned file descriptor.
package integrationhook

import (
	"strconv"

	"github.com/aaditya/cs425mp3/internal/wire"
)

// Direction names the side of the local +7 socket a datagram crosses.
type Direction uint8

const (
	// Send is a datagram leaving the node's bound +7 socket.
	Send Direction = iota + 1
	// Receive is an authenticated datagram accepted from the +7 socket.
	Receive
)

// String renders the direction in the activation protocol vocabulary.
func (direction Direction) String() string {
	switch direction {
	case Send:
		return "send"
	case Receive:
		return "recv"
	default:
		return "unknown"
	}
}

// Action is the bounded one-shot fate of one datagram.
type Action uint8

const (
	// Pass leaves the datagram untouched.
	Pass Action = iota
	// Drop suppresses the datagram; the durable sender remains responsible
	// for retrying it.
	Drop
	// Duplicate transmits the exact datagram twice from the same socket.
	Duplicate
)

// String renders the action in the activation protocol vocabulary.
func (action Action) String() string {
	switch action {
	case Pass:
		return "pass"
	case Drop:
		return "drop"
	case Duplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// Hook observes named durable boundaries and decides datagram fates.
//
// DurableBoundary is called by the owning store strictly after the
// transaction's fsync has succeeded and before any Accepted/Completed/status
// acknowledgement is written; it is never called for failed durability.
// DatagramAction is consulted once per datagram on the real send path and
// once per authenticated inbound datagram.
type Hook interface {
	DurableBoundary(name string)
	DatagramAction(direction Direction, message wire.MessageType) Action
}

// Noop is the production Hook: it reads nothing, records nothing, never
// blocks, and passes every datagram.
type Noop struct{}

// DurableBoundary returns immediately.
func (Noop) DurableBoundary(string) {}

// DatagramAction always passes.
func (Noop) DatagramAction(Direction, wire.MessageType) Action { return Pass }

// MessageName renders a +7 message type in the protocol vocabulary.
func MessageName(message wire.MessageType) string {
	switch message {
	case wire.MessageCraneTupleDelivery:
		return "delivery"
	case wire.MessageCraneTupleDeliveryAck:
		return "ack"
	case wire.MessageCraneTupleDeliveryNack:
		return "nack"
	default:
		return strconv.Itoa(int(message))
	}
}

// DatagramHolder is the optional extension a Hook may implement to capture
// an exact outbound datagram and re-send it later from the same socket. The
// production Noop never implements it. HoldDatagram reports whether the
// datagram was taken; when it was, the caller must not transmit it now and
// must let the hook invoke resend from the node's own bound socket later.
type DatagramHolder interface {
	HoldDatagram(message wire.MessageType, destination uint16, resend func()) bool
}
