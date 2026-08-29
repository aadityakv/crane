package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
)

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
