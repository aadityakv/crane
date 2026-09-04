package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/transport"
	"github.com/aadityakv/crane/internal/wire"
)

const tupleIngressQueueSize = 4096

// TupleServiceOptions binds exactly one TupleEndpoint to the Engine that uses
// that same endpoint as its durable outbox Sender.
type TupleServiceOptions struct {
	// Endpoint is the single local owner of the +7 socket.
	Endpoint *TupleEndpoint
	// Engine receives validated deliveries and ACKs from this service.
	Engine *Engine
}

// TupleService activates and receives from exactly one +7 endpoint.
type TupleService struct {
	endpoint *TupleEndpoint
	engine   *Engine
	replay   *tupleReplay
	ready    chan struct{}
	started  atomic.Bool
}

// NewTupleService claims an endpoint for its exact Engine without opening its
// socket or starting receive work.
func NewTupleService(options TupleServiceOptions) (*TupleService, error) {
	if options.Endpoint == nil || options.Engine == nil {
		return nil, errors.New("crane tuple service requires endpoint and engine")
	}
	if options.Engine.sender != options.Endpoint {
		return nil, errors.New("crane tuple service endpoint is not the Engine sender")
	}
	if !options.Endpoint.claimed.CompareAndSwap(false, true) {
		return nil, ErrTupleEndpointInUse
	}
	return &TupleService{endpoint: options.Endpoint, engine: options.Engine, replay: newTupleReplay(options.Endpoint.clock, time.Duration(options.Endpoint.configuration.Timing.ReplayWindow), config.ReplayFutureSkewAllowance, tupleReplayEntries, tupleReplayEntriesPerSender), ready: make(chan struct{})}, nil
}

// Ready closes after the one +7 datagram has been activated for sends and receives.
func (service *TupleService) Ready() <-chan struct{} { return service.ready }

// Run activates the endpoint once and receives until cancellation or a fatal
// datagram failure, closing the owned socket before it returns.
func (service *TupleService) Run(ctx context.Context) (runErr error) {
	if ctx == nil {
		return errors.New("run Crane tuple service: nil context")
	}
	if !service.started.CompareAndSwap(false, true) {
		return errors.New("crane tuple service Run called more than once")
	}
	datagram, err := service.endpoint.activate()
	if err != nil {
		return err
	}
	receiveContext, cancelReceive := context.WithCancel(ctx)
	packets := make(chan transport.Packet, tupleIngressQueueSize)
	receiveErrors := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			packet, receiveErr := datagram.Receive(receiveContext)
			if receiveErr != nil {
				select {
				case receiveErrors <- receiveErr:
				case <-receiveContext.Done():
				}
				return
			}
			select {
			case packets <- packet:
			default:
				// An unaccepted UDP datagram may be dropped at the bounded
				// ingress boundary; its durable sender remains responsible.
			}
		}
	}()
	defer func() {
		cancelReceive()
		closeErr := service.endpoint.deactivate(datagram)
		readers.Wait()
		if closeErr != nil && !errors.Is(closeErr, transport.ErrDatagramClosed) {
			runErr = errors.Join(runErr, closeErr)
		}
	}()
	close(service.ready)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case receiveErr := <-receiveErrors:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(receiveErr, transport.ErrDatagramClosed) {
				return receiveErr
			}
			return fmt.Errorf("receive Crane +7 datagram: %w", receiveErr)
		case packet := <-packets:
			if processErr := service.process(ctx, datagram, packet); processErr != nil {
				return processErr
			}
		}
	}
}

func (service *TupleService) process(ctx context.Context, datagram transport.SourceDatagram, packet transport.Packet) error {
	if packet.Truncated || len(packet.Data) == 0 || len(packet.Data) > tupleDatagramMaximumBytes {
		return nil
	}
	frame, err := wire.Decode(packet.Data, service.endpoint.authenticator, wire.Limits{ExpectedClusterID: &service.endpoint.clusterID})
	if err != nil {
		return nil
	}
	timestamp := time.UnixMilli(frame.Header.TimestampMillis)
	// Every +7 handler is idempotent by construction (custody is deduplicated
	// by durable delivery identity; ACK/NACK are idempotent transitions), so
	// the replay guard is a cost bound, not a correctness gate. When an
	// authenticated peer's legitimate retry rate exhausts its bounded replay
	// cache the datagram is processed without being recorded instead of being
	// dropped — dropping it would suppress the very ACKs that stop the
	// retries (Task 24 defect #8).
	recorded := true
	if err = service.replay.preflight(frame.Header.SenderID, frame.Header.RequestID, timestamp); err != nil {
		if !errors.Is(err, wire.ErrReplayCacheFull) {
			return nil
		}
		recorded = false
	}
	replay := service.replay
	if !recorded {
		replay = nil
	}
	remoteIP := net.ParseIP(packet.From.Host)
	if remoteIP == nil || packet.From.Port == 0 || service.endpoint.peers.AuthorizeUDP(frame.Header.SenderID, &net.UDPAddr{IP: remoteIP, Port: int(packet.From.Port)}, config.ServiceCraneTupleACK) != nil {
		replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
		return nil
	}
	// An authenticated inbound datagram may be dropped by the integration
	// hook exactly as the network could have dropped it: before any replay
	// accounting or custody.
	if service.endpoint.hook.DatagramAction(integrationhook.Receive, frame.Header.Message) == integrationhook.Drop {
		return nil
	}

	switch frame.Header.Message {
	case wire.MessageCraneTupleDelivery:
		delivery, decodeErr := protocol.UnmarshalTupleDelivery(frame.Payload)
		if decodeErr != nil || delivery.Producer.WorkerID != frame.Header.SenderID || delivery.Destination.WorkerID != service.endpoint.configuration.NodeID {
			replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil
		}
		ack, handleErr := service.engine.HandleDelivery(ctx, delivery)
		if handleErr != nil {
			if fatalTupleStoreError(ctx, handleErr) {
				return fmt.Errorf("handle Crane tuple delivery: %w", handleErr)
			}
			if replay.commitInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp) != nil {
				return nil
			}
			_ = service.sendNACK(ctx, datagram, packet.From, delivery, tupleNACKCode(handleErr))
			return nil
		}
		if replay.commit(frame.Header.SenderID, frame.Header.RequestID, timestamp) != nil {
			return nil
		}
		_ = service.sendACK(ctx, datagram, packet.From, ack)
	case wire.MessageCraneTupleDeliveryAck:
		ack, decodeErr := protocol.UnmarshalTupleACK(frame.Payload)
		if decodeErr != nil || ack.Destination.WorkerID != frame.Header.SenderID {
			replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil
		}
		if handleErr := service.engine.HandleACK(ctx, ack); handleErr != nil {
			if fatalTupleStoreError(ctx, handleErr) {
				return fmt.Errorf("handle Crane tuple ACK: %w", handleErr)
			}
			_ = replay.commitInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil
		}
		_ = replay.commit(frame.Header.SenderID, frame.Header.RequestID, timestamp)
	case wire.MessageCraneTupleDeliveryNack:
		nack, decodeErr := protocol.UnmarshalTupleNACK(frame.Payload)
		if decodeErr != nil || nack.Destination.WorkerID != frame.Header.SenderID {
			replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
			return nil
		}
		_ = replay.commit(frame.Header.SenderID, frame.Header.RequestID, timestamp)
	default:
		replay.recordInvalid(frame.Header.SenderID, frame.Header.RequestID, timestamp)
	}
	return nil
}

func fatalTupleStoreError(ctx context.Context, err error) bool {
	return errors.Is(err, store.ErrUnavailable) || errors.Is(err, store.ErrClosed) && ctx.Err() == nil
}

func (service *TupleService) sendACK(ctx context.Context, datagram transport.SourceDatagram, destination config.Endpoint, ack protocol.TupleACK) error {
	payload, err := protocol.MarshalTupleACK(ack)
	if err != nil {
		return err
	}
	return service.endpoint.sendPayload(ctx, datagram, ack.Destination.WorkerID, destination, wire.MessageCraneTupleDeliveryAck, payload)
}

func (service *TupleService) sendNACK(ctx context.Context, datagram transport.SourceDatagram, destination config.Endpoint, delivery protocol.TupleDelivery, code protocol.TupleNACKCode) error {
	nack := protocol.TupleNACK{DeliveryID: delivery.DeliveryID, Destination: delivery.Destination, Assignment: delivery.Assignment, Coordinator: delivery.Coordinator, Code: code}
	payload, err := protocol.MarshalTupleNACK(nack)
	if err != nil {
		return err
	}
	return service.endpoint.sendPayload(ctx, datagram, delivery.Producer.WorkerID, destination, wire.MessageCraneTupleDeliveryNack, payload)
}

func tupleNACKCode(err error) protocol.TupleNACKCode {
	switch {
	case errors.Is(err, ErrNotReady):
		return protocol.TupleNACKNotReady
	case errors.Is(err, store.ErrCapacity):
		return protocol.TupleNACKCapacityExceeded
	case errors.Is(err, model.ErrIdentityReuse):
		return protocol.TupleNACKStaleAssignment
	case errors.Is(err, ErrNotRunning):
		return protocol.TupleNACKUnknownAssignment
	default:
		return protocol.TupleNACKWrongDestination
	}
}
