package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aadityakv/crane/internal/config"
)

const (
	defaultMemoryReceiveQueue = 1024
	defaultMemoryPendingLimit = 65536
)

var (
	// ErrMemoryEndpointInUse reports an address already owned by another endpoint.
	ErrMemoryEndpointInUse = errors.New("memory datagram endpoint already in use")
	// ErrMemoryNetworkFull reports that deterministic pending-packet state reached its bound.
	ErrMemoryNetworkFull = errors.New("memory datagram network full")
)

type memoryLink struct {
	from config.Endpoint
	to   config.Endpoint
}

type memoryRule uint8

const (
	ruleDeliver memoryRule = iota
	ruleDrop
	ruleDelay
	ruleDuplicate
)

type scheduledPacket struct {
	from      config.Endpoint
	to        config.Endpoint
	data      []byte
	readyStep uint64
	copies    int
}

// MemoryNetwork is a deterministic, bounded datagram switch. Send only queues;
// Advance is the sole operation that delivers scheduled packets.
type MemoryNetwork struct {
	mu           sync.Mutex
	step         uint64
	endpoints    map[config.Endpoint]*MemoryDatagram
	rules        map[memoryLink]memoryRule
	pending      []scheduledPacket
	pendingLimit int
}

// NewMemoryNetwork returns an empty deterministic network with bounded queues.
func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		endpoints:    make(map[config.Endpoint]*MemoryDatagram),
		rules:        make(map[memoryLink]memoryRule),
		pendingLimit: defaultMemoryPendingLimit,
	}
}

// MemoryDatagram owns one receive queue and one or more local address aliases.
type MemoryDatagram struct {
	network   *MemoryNetwork
	addresses []config.Endpoint
	primary   config.Endpoint
	packets   chan Packet
	done      chan struct{}
	closeOnce sync.Once
}

// Endpoint registers one datagram handle for all supplied local addresses.
func (n *MemoryNetwork) Endpoint(addresses ...config.Endpoint) (*MemoryDatagram, error) {
	if n == nil || len(addresses) == 0 {
		return nil, fmt.Errorf("%w: no memory addresses", ErrInvalidDatagramEndpoint)
	}
	seen := make(map[config.Endpoint]struct{}, len(addresses))
	for _, address := range addresses {
		if address.Host == "" || address.Port == 0 {
			return nil, fmt.Errorf("%w: %s", ErrInvalidDatagramEndpoint, address)
		}
		if _, duplicate := seen[address]; duplicate {
			return nil, fmt.Errorf("%w: duplicate address %s", ErrInvalidDatagramEndpoint, address)
		}
		seen[address] = struct{}{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, address := range addresses {
		if _, exists := n.endpoints[address]; exists {
			return nil, fmt.Errorf("%w: %s", ErrMemoryEndpointInUse, address)
		}
	}
	datagram := &MemoryDatagram{
		network:   n,
		addresses: append([]config.Endpoint(nil), addresses...),
		primary:   addresses[0],
		packets:   make(chan Packet, defaultMemoryReceiveQueue),
		done:      make(chan struct{}),
	}
	for _, address := range addresses {
		n.endpoints[address] = datagram
	}
	return datagram, nil
}

// Send queues an owned packet according to the link rule active at send time.
func (d *MemoryDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	if d == nil {
		return ErrDatagramClosed
	}
	return d.SendFrom(ctx, d.primary, destination, payload)
}

// SendFrom queues a packet from one selected alias owned by this endpoint.
func (d *MemoryDatagram) SendFrom(ctx context.Context, source, destination config.Endpoint, payload []byte) error {
	if d == nil || d.network == nil {
		return ErrDatagramClosed
	}
	if ctx == nil {
		return errors.New("send memory datagram: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-d.done:
		return ErrDatagramClosed
	default:
	}

	network := d.network
	network.mu.Lock()
	defer network.mu.Unlock()
	if network.endpoints[source] != d {
		for _, address := range d.addresses {
			if address == source {
				return ErrDatagramClosed
			}
		}
		return fmt.Errorf("%w: source %s is not owned", ErrInvalidDatagramEndpoint, source)
	}
	if network.endpoints[d.primary] != d {
		return ErrDatagramClosed
	}
	if len(network.pending) >= network.pendingLimit {
		return ErrMemoryNetworkFull
	}
	rule := network.rules[memoryLink{from: source, to: destination}]
	if rule == ruleDrop {
		return nil
	}
	packet := scheduledPacket{
		from:      source,
		to:        destination,
		data:      append([]byte(nil), payload...),
		readyStep: network.step + 1,
		copies:    1,
	}
	if rule == ruleDelay {
		packet.readyStep++
	}
	if rule == ruleDuplicate {
		packet.copies = 2
	}
	network.pending = append(network.pending, packet)
	return nil
}

// Receive waits for one packet, endpoint closure, or cancellation.
func (d *MemoryDatagram) Receive(ctx context.Context) (Packet, error) {
	if d == nil {
		return Packet{}, ErrDatagramClosed
	}
	if ctx == nil {
		return Packet{}, errors.New("receive memory datagram: nil context")
	}
	select {
	case <-d.done:
		return Packet{}, ErrDatagramClosed
	default:
	}
	select {
	case packet := <-d.packets:
		packet.Data = append([]byte(nil), packet.Data...)
		return packet, nil
	case <-d.done:
		return Packet{}, ErrDatagramClosed
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

// Close unregisters every alias and unblocks pending receivers.
func (d *MemoryDatagram) Close() error {
	if d == nil || d.network == nil {
		return ErrDatagramClosed
	}
	d.closeOnce.Do(func() {
		d.network.mu.Lock()
		for _, address := range d.addresses {
			if d.network.endpoints[address] == d {
				delete(d.network.endpoints, address)
			}
		}
		d.network.mu.Unlock()
		close(d.done)
	})
	return nil
}

// Advance delivers every packet whose deterministic step has arrived and
// returns the number accepted by destination receive queues.
func (n *MemoryNetwork) Advance() int {
	if n == nil {
		return 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.step++
	delivered := 0
	retained := n.pending[:0]
	for _, scheduled := range n.pending {
		if scheduled.readyStep > n.step {
			retained = append(retained, scheduled)
			continue
		}
		destination := n.endpoints[scheduled.to]
		if destination == nil {
			continue
		}
		for range scheduled.copies {
			packet := Packet{From: scheduled.from, Data: append([]byte(nil), scheduled.data...)}
			select {
			case destination.packets <- packet:
				delivered++
			default:
			}
		}
	}
	n.pending = retained
	return delivered
}

// Pending returns the bounded number of packets awaiting an Advance step.
func (n *MemoryNetwork) Pending() int {
	if n == nil {
		return 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.pending)
}

// Drop discards future packets on one directed link.
func (n *MemoryNetwork) Drop(from, to config.Endpoint) {
	n.setRule(from, to, ruleDrop)
}

// Delay holds future packets on one directed link for one extra Advance step.
func (n *MemoryNetwork) Delay(from, to config.Endpoint) {
	n.setRule(from, to, ruleDelay)
}

// Duplicate delivers two owned copies of future packets on one directed link.
func (n *MemoryNetwork) Duplicate(from, to config.Endpoint) {
	n.setRule(from, to, ruleDuplicate)
}

// Partition drops future packets in both directions between two addresses.
func (n *MemoryNetwork) Partition(left, right config.Endpoint) {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.rules[memoryLink{from: left, to: right}] = ruleDrop
	n.rules[memoryLink{from: right, to: left}] = ruleDrop
	n.mu.Unlock()
}

// Heal removes controls globally when called without addresses, or in both
// directions for one supplied address pair.
func (n *MemoryNetwork) Heal(addresses ...config.Endpoint) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	switch len(addresses) {
	case 0:
		clear(n.rules)
	case 2:
		delete(n.rules, memoryLink{from: addresses[0], to: addresses[1]})
		delete(n.rules, memoryLink{from: addresses[1], to: addresses[0]})
	}
}

func (n *MemoryNetwork) setRule(from, to config.Endpoint, rule memoryRule) {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.rules[memoryLink{from: from, to: to}] = rule
	n.mu.Unlock()
}
