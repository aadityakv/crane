package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
)

func TestDatagramDNSResolutionHonorsSendCancellation(t *testing.T) {
	source := freeUDPEndpoint(t)
	resolver := &blockingIPResolver{started: make(chan struct{})}
	datagram, err := ListenUDPWithResolver(resolver, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = datagram.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- datagram.Send(ctx, config.Endpoint{Host: "blocked.test", Port: 12000}, []byte("frame"))
	}()
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("outbound DNS resolution did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled DNS send error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("outbound DNS resolution ignored cancellation")
	}
}

func TestDatagramListensOnEveryConfiguredEndpoint(t *testing.T) {
	first := freeUDPEndpoint(t)
	second := freeUDPEndpoint(t)
	datagram, err := ListenUDP(first, second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = datagram.Close() })

	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })

	for _, destination := range []config.Endpoint{first, second} {
		address, err := net.ResolveUDPAddr("udp", destination.String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sender.WriteToUDP([]byte(destination.String()), address); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want := map[string]bool{first.String(): false, second.String(): false}
	for range 2 {
		packet, err := datagram.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		payload := string(packet.Data)
		if _, exists := want[payload]; !exists {
			t.Fatalf("payload = %q, want one configured endpoint", payload)
		}
		want[payload] = true
	}
	for endpoint, received := range want {
		if !received {
			t.Fatalf("did not receive packet sent to %s", endpoint)
		}
	}
}

func TestDatagramSendCopiesPayloadAndCloseUnblocksReceive(t *testing.T) {
	sourceEndpoint := freeUDPEndpoint(t)
	datagram, err := ListenUDP(sourceEndpoint)
	if err != nil {
		t.Fatal(err)
	}

	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close() })
	receiverAddress := receiver.LocalAddr().(*net.UDPAddr)
	destination := config.Endpoint{Host: receiverAddress.IP.String(), Port: uint16(receiverAddress.Port)}

	payload := []byte("authenticated-frame")
	if err := datagram.Send(context.Background(), destination, payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	buffer := make([]byte, 64)
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err := receiver.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buffer[:count]); got != "authenticated-frame" {
		t.Fatalf("received payload = %q, want owned original", got)
	}

	result := make(chan error, 1)
	go func() {
		_, err := datagram.Receive(context.Background())
		result <- err
	}()
	if err := datagram.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrDatagramClosed) {
			t.Fatalf("Receive after Close error = %v, want ErrDatagramClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Receive")
	}
}

func TestDatagramSendFromUsesSelectedBoundEndpoint(t *testing.T) {
	first := freeUDPEndpoint(t)
	second := freeUDPEndpoint(t)
	datagram, err := ListenUDP(first, second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = datagram.Close() })

	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close() })
	receiverAddress := receiver.LocalAddr().(*net.UDPAddr)
	destination := config.Endpoint{Host: receiverAddress.IP.String(), Port: uint16(receiverAddress.Port)}

	if err := datagram.SendFrom(context.Background(), second, destination, []byte("ack")); err != nil {
		t.Fatal(err)
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 16)
	count, source, err := receiver.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[:count]) != "ack" || source.Port != int(second.Port) {
		t.Fatalf("selected-source packet = %q from %s, want ack from port %d", buffer[:count], source, second.Port)
	}

	unknown := config.Endpoint{Host: first.Host, Port: first.Port + 10}
	if err := datagram.SendFrom(context.Background(), unknown, destination, []byte("forged")); !errors.Is(err, ErrInvalidDatagramEndpoint) {
		t.Fatalf("unknown source error = %v, want ErrInvalidDatagramEndpoint", err)
	}
}

func TestDatagramTruncationSignalUsesBoundedReadMsgUDP(t *testing.T) {
	endpoint := freeUDPEndpoint(t)
	datagram, err := ListenUDPBounded(1200, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = datagram.Close() })

	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	destination, _ := net.ResolveUDPAddr("udp", endpoint.String())
	if _, err := sender.WriteToUDP(make([]byte, 1201), destination); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	packet, err := datagram.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !packet.Truncated || len(packet.Data) != 1200 {
		t.Fatalf("oversized packet truncated=%t bytes=%d, want true/1200", packet.Truncated, len(packet.Data))
	}

	// Existing SWIM construction keeps the complete UDP payload behavior.
	unboundedEndpoint := freeUDPEndpoint(t)
	unbounded, err := ListenUDP(unboundedEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unbounded.Close() })
	unboundedDestination, _ := net.ResolveUDPAddr("udp", unboundedEndpoint.String())
	if _, err := sender.WriteToUDP(make([]byte, 1201), unboundedDestination); err != nil {
		t.Fatal(err)
	}
	full, err := unbounded.Receive(ctx)
	if err != nil || full.Truncated || len(full.Data) != 1201 {
		t.Fatalf("legacy receive truncated=%t bytes=%d err=%v", full.Truncated, len(full.Data), err)
	}
}

func freeUDPEndpoint(t *testing.T) config.Endpoint {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	address := listener.LocalAddr().(*net.UDPAddr)
	endpoint := config.Endpoint{Host: address.IP.String(), Port: uint16(address.Port)}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return endpoint
}

type blockingIPResolver struct {
	started chan struct{}
}

func (r *blockingIPResolver) LookupNetIP(ctx context.Context, _, _ string) ([]netip.Addr, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
