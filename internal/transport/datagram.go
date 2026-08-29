// Package transport provides real and deterministic datagram seams for protocol services.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	udpReceiveQueueSize = 256
	maxUDPDatagramSize  = 65535
)

var (
	// ErrDatagramClosed reports an operation attempted after a datagram endpoint closed.
	ErrDatagramClosed = errors.New("datagram endpoint closed")
	// ErrInvalidDatagramEndpoint reports a missing or unusable endpoint address.
	ErrInvalidDatagramEndpoint = errors.New("invalid datagram endpoint")
)

// Packet is an independently owned datagram and its source address.
type Packet struct {
	From config.Endpoint
	Data []byte
}

// Datagram is the packet transport used by the SWIM service.
type Datagram interface {
	Send(context.Context, config.Endpoint, []byte) error
	Receive(context.Context) (Packet, error)
	Close() error
}

// SourceDatagram can select one of its bound local endpoints for a send. It is
// used by protocols whose authenticated message type determines the expected
// source port while preserving the minimal Datagram contract for test doubles.
type SourceDatagram interface {
	Datagram
	SendFrom(context.Context, config.Endpoint, config.Endpoint, []byte) error
}

// UDPDatagram aggregates packets from one or more bound UDP sockets.
type UDPDatagram struct {
	endpoints   []config.Endpoint
	connections []*net.UDPConn
	packets     chan Packet
	errors      chan error
	done        chan struct{}
	closeOnce   sync.Once
	closeError  error
	readers     sync.WaitGroup
	writeMu     sync.Mutex
}

// ListenUDP binds every endpoint and returns one aggregate datagram transport.
func ListenUDP(endpoints ...config.Endpoint) (*UDPDatagram, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: no bind endpoints", ErrInvalidDatagramEndpoint)
	}
	connections := make([]*net.UDPConn, 0, len(endpoints))
	for _, endpoint := range endpoints {
		address, err := resolveUDP(endpoint)
		if err != nil {
			closeUDPConnections(connections)
			return nil, err
		}
		connection, err := net.ListenUDP("udp", address)
		if err != nil {
			closeUDPConnections(connections)
			return nil, fmt.Errorf("listen UDP on %s: %w", endpoint, err)
		}
		connections = append(connections, connection)
	}

	datagram := &UDPDatagram{
		endpoints:   append([]config.Endpoint(nil), endpoints...),
		connections: connections,
		packets:     make(chan Packet, udpReceiveQueueSize),
		errors:      make(chan error, len(connections)),
		done:        make(chan struct{}),
	}
	for _, connection := range connections {
		datagram.readers.Add(1)
		go datagram.read(connection)
	}
	return datagram, nil
}

// Send writes one complete UDP datagram from the first bound socket.
func (d *UDPDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	if d == nil || len(d.connections) == 0 {
		return ErrDatagramClosed
	}
	return d.send(ctx, d.connections[0], destination, payload)
}

// SendFrom writes from the selected bound endpoint.
func (d *UDPDatagram) SendFrom(ctx context.Context, source, destination config.Endpoint, payload []byte) error {
	if d == nil || len(d.connections) == 0 {
		return ErrDatagramClosed
	}
	for index, endpoint := range d.endpoints {
		if endpoint == source {
			return d.send(ctx, d.connections[index], destination, payload)
		}
	}
	return fmt.Errorf("%w: source %s is not bound", ErrInvalidDatagramEndpoint, source)
}

func (d *UDPDatagram) send(ctx context.Context, connection *net.UDPConn, destination config.Endpoint, payload []byte) error {
	if ctx == nil {
		return errors.New("send datagram: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-d.done:
		return ErrDatagramClosed
	default:
	}
	address, err := resolveUDP(destination)
	if err != nil {
		return err
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := connection.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("set UDP write deadline: %w", err)
		}
		defer func() { _ = connection.SetWriteDeadline(time.Time{}) }()
	}
	written, err := connection.WriteToUDP(payload, address)
	if err != nil {
		if contextError := ctx.Err(); contextError != nil {
			return errors.Join(contextError, err)
		}
		select {
		case <-d.done:
			return errors.Join(ErrDatagramClosed, err)
		default:
		}
		return fmt.Errorf("send UDP datagram to %s: %w", destination, err)
	}
	if written != len(payload) {
		return fmt.Errorf("send UDP datagram to %s: wrote %d of %d bytes", destination, written, len(payload))
	}
	return nil
}

// Receive waits for one packet, a listener failure, closure, or cancellation.
func (d *UDPDatagram) Receive(ctx context.Context) (Packet, error) {
	if d == nil {
		return Packet{}, ErrDatagramClosed
	}
	if ctx == nil {
		return Packet{}, errors.New("receive datagram: nil context")
	}
	select {
	case <-d.done:
		return Packet{}, ErrDatagramClosed
	default:
	}
	select {
	case packet := <-d.packets:
		return packet, nil
	case err := <-d.errors:
		return Packet{}, err
	case <-d.done:
		return Packet{}, ErrDatagramClosed
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

// Close closes every bound socket and waits for its reader goroutine.
func (d *UDPDatagram) Close() error {
	if d == nil {
		return ErrDatagramClosed
	}
	d.closeOnce.Do(func() {
		close(d.done)
		var closeErrors []error
		for _, connection := range d.connections {
			if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErrors = append(closeErrors, err)
			}
		}
		d.readers.Wait()
		d.closeError = errors.Join(closeErrors...)
	})
	return d.closeError
}

func (d *UDPDatagram) read(connection *net.UDPConn) {
	defer d.readers.Done()
	buffer := make([]byte, maxUDPDatagramSize)
	for {
		count, source, err := connection.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-d.done:
				return
			default:
			}
			select {
			case d.errors <- fmt.Errorf("receive UDP datagram: %w", err):
			case <-d.done:
			}
			return
		}
		if source.Port <= 0 || source.Port > 65535 {
			continue
		}
		packet := Packet{
			From: config.Endpoint{Host: source.IP.String(), Port: uint16(source.Port)},
			Data: append([]byte(nil), buffer[:count]...),
		}
		select {
		case d.packets <- packet:
		case <-d.done:
			return
		}
	}
}

func resolveUDP(endpoint config.Endpoint) (*net.UDPAddr, error) {
	if endpoint.Host == "" || endpoint.Port == 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidDatagramEndpoint, endpoint)
	}
	address, err := net.ResolveUDPAddr("udp", endpoint.String())
	if err != nil {
		return nil, fmt.Errorf("%w %s: %v", ErrInvalidDatagramEndpoint, endpoint, err)
	}
	return address, nil
}

func closeUDPConnections(connections []*net.UDPConn) {
	for _, connection := range connections {
		_ = connection.Close()
	}
}
