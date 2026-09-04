package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/aadityakv/crane/internal/config"
)

func TestMemoryNetworkDeliversOnlyWhenAdvancedAndOwnsPayloads(t *testing.T) {
	network := NewMemoryNetwork()
	fromAddress := config.Endpoint{Host: "node-1", Port: 8000}
	toAddress := config.Endpoint{Host: "node-2", Port: 8100}
	from, err := network.Endpoint(fromAddress)
	if err != nil {
		t.Fatal(err)
	}
	to, err := network.Endpoint(toAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = from.Close()
		_ = to.Close()
	})

	payload := []byte("frame")
	if err := from.Send(context.Background(), toAddress, payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got := network.Pending(); got != 1 {
		t.Fatalf("pending packets = %d, want 1 before Advance", got)
	}
	if got := network.Advance(); got != 1 {
		t.Fatalf("Advance delivered = %d, want 1", got)
	}

	packet, err := to.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if packet.From != fromAddress || string(packet.Data) != "frame" {
		t.Fatalf("packet = %#v, want copied frame from %#v", packet, fromAddress)
	}
	packet.Data[0] = 'Y'
	if got := network.Pending(); got != 0 {
		t.Fatalf("pending packets after receive = %d, want zero", got)
	}
}

func TestMemoryNetworkDropDelayAndDuplicateAreDeterministic(t *testing.T) {
	fromAddress := config.Endpoint{Host: "node-1", Port: 8000}
	toAddress := config.Endpoint{Host: "node-2", Port: 8100}

	t.Run("drop", func(t *testing.T) {
		network, from, to := memoryPair(t, fromAddress, toAddress)
		network.Drop(fromAddress, toAddress)
		if err := from.Send(context.Background(), toAddress, []byte("drop")); err != nil {
			t.Fatal(err)
		}
		if got := network.Advance(); got != 0 {
			t.Fatalf("Advance delivered = %d, want zero", got)
		}
		assertReceiveCanceled(t, to)
	})

	t.Run("delay", func(t *testing.T) {
		network, from, to := memoryPair(t, fromAddress, toAddress)
		network.Delay(fromAddress, toAddress)
		if err := from.Send(context.Background(), toAddress, []byte("delay")); err != nil {
			t.Fatal(err)
		}
		if got := network.Advance(); got != 0 {
			t.Fatalf("first Advance delivered = %d, want zero", got)
		}
		if got := network.Advance(); got != 1 {
			t.Fatalf("second Advance delivered = %d, want one", got)
		}
		packet, err := to.Receive(context.Background())
		if err != nil || string(packet.Data) != "delay" {
			t.Fatalf("delayed packet = %#v, error = %v", packet, err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		network, from, to := memoryPair(t, fromAddress, toAddress)
		network.Duplicate(fromAddress, toAddress)
		if err := from.Send(context.Background(), toAddress, []byte("duplicate")); err != nil {
			t.Fatal(err)
		}
		if got := network.Advance(); got != 2 {
			t.Fatalf("Advance delivered = %d, want two", got)
		}
		for range 2 {
			packet, err := to.Receive(context.Background())
			if err != nil || string(packet.Data) != "duplicate" {
				t.Fatalf("duplicate packet = %#v, error = %v", packet, err)
			}
		}
	})
}

func TestMemoryNetworkPartitionAndSelectiveHeal(t *testing.T) {
	network := NewMemoryNetwork()
	leftAddress := config.Endpoint{Host: "left", Port: 8000}
	rightAddress := config.Endpoint{Host: "right", Port: 8100}
	left, err := network.Endpoint(leftAddress)
	if err != nil {
		t.Fatal(err)
	}
	right, err := network.Endpoint(rightAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	network.Partition(leftAddress, rightAddress)
	if err := left.Send(context.Background(), rightAddress, []byte("blocked-left")); err != nil {
		t.Fatal(err)
	}
	if err := right.Send(context.Background(), leftAddress, []byte("blocked-right")); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got != 0 {
		t.Fatalf("partition delivered = %d packets, want zero", got)
	}

	network.Heal(leftAddress, rightAddress)
	if err := left.Send(context.Background(), rightAddress, []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := right.Send(context.Background(), leftAddress, []byte("right")); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got != 2 {
		t.Fatalf("healed network delivered = %d packets, want two", got)
	}
	leftPacket, leftErr := left.Receive(context.Background())
	rightPacket, rightErr := right.Receive(context.Background())
	if leftErr != nil || rightErr != nil || string(leftPacket.Data) != "right" || string(rightPacket.Data) != "left" {
		t.Fatalf("healed packets left=%#v/%v right=%#v/%v", leftPacket, leftErr, rightPacket, rightErr)
	}
}

func TestMemoryNetworkEndpointMayOwnBothSWIMAddresses(t *testing.T) {
	network := NewMemoryNetwork()
	ping := config.Endpoint{Host: "node-1", Port: 8000}
	ack := config.Endpoint{Host: "node-1", Port: 8001}
	receiver, err := network.Endpoint(ping, ack)
	if err != nil {
		t.Fatal(err)
	}
	senderAddress := config.Endpoint{Host: "sender", Port: 9000}
	sender, err := network.Endpoint(senderAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = receiver.Close()
		_ = sender.Close()
	})

	for _, destination := range []config.Endpoint{ping, ack} {
		if err := sender.Send(context.Background(), destination, []byte(destination.String())); err != nil {
			t.Fatal(err)
		}
	}
	if got := network.Advance(); got != 2 {
		t.Fatalf("Advance delivered = %d, want two aliases", got)
	}
	seen := make(map[string]bool)
	for range 2 {
		packet, err := receiver.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		seen[string(packet.Data)] = true
	}
	if !seen[ping.String()] || !seen[ack.String()] {
		t.Fatalf("received aliases = %#v", seen)
	}
}

func TestMemoryDatagramSendFromUsesOwnedAlias(t *testing.T) {
	network := NewMemoryNetwork()
	ping := config.Endpoint{Host: "node-1", Port: 8000}
	ack := config.Endpoint{Host: "node-1", Port: 8001}
	destination := config.Endpoint{Host: "node-2", Port: 8100}
	sender, err := network.Endpoint(ping, ack)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := network.Endpoint(destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sender.Close()
		_ = receiver.Close()
	})

	if err := sender.SendFrom(context.Background(), ack, destination, []byte("ack")); err != nil {
		t.Fatal(err)
	}
	if got := network.Advance(); got != 1 {
		t.Fatalf("Advance delivered = %d, want 1", got)
	}
	packet, err := receiver.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if packet.From != ack || string(packet.Data) != "ack" {
		t.Fatalf("selected-alias packet = %#v, want ack alias", packet)
	}

	unknown := config.Endpoint{Host: ping.Host, Port: ping.Port + 10}
	if err := sender.SendFrom(context.Background(), unknown, destination, []byte("forged")); !errors.Is(err, ErrInvalidDatagramEndpoint) {
		t.Fatalf("unknown alias error = %v, want ErrInvalidDatagramEndpoint", err)
	}
}

func memoryPair(t *testing.T, fromAddress, toAddress config.Endpoint) (*MemoryNetwork, *MemoryDatagram, *MemoryDatagram) {
	t.Helper()
	network := NewMemoryNetwork()
	from, err := network.Endpoint(fromAddress)
	if err != nil {
		t.Fatal(err)
	}
	to, err := network.Endpoint(toAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = from.Close()
		_ = to.Close()
	})
	return network, from, to
}

func assertReceiveCanceled(t *testing.T, datagram Datagram) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := datagram.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive error = %v, want context.Canceled", err)
	}
}
