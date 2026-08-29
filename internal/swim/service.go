package swim

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	serviceEventQueueSize   = 4096
	serviceReplayEntries    = 65536
	serviceDisseminationMax = 4096
	serviceRetransmitFactor = 3
	serviceFutureSkew       = 30 * time.Second
)

var (
	// ErrInvalidServiceOptions reports a missing dependency or invalid node configuration.
	ErrInvalidServiceOptions = errors.New("swim: invalid service options")
	// ErrServiceAlreadyRun reports a second attempt to run one Service instance.
	ErrServiceAlreadyRun = errors.New("swim: service may only run once")
	// ErrServiceNotRunning reports a query made before Run starts or after it returns.
	ErrServiceNotRunning = errors.New("swim: service is not running")
	// ErrServiceNotAdmitted reports peer traffic received before local admission completes.
	ErrServiceNotAdmitted = errors.New("swim: service is not admitted")
)

// ServiceOptions supplies the validated identity and deterministic seams used
// by a SWIM service. A nil Datagram asks Run to bind both real UDP endpoints.
type ServiceOptions struct {
	Config        config.NodeConfig
	Authenticator wire.Authenticator
	Clock         clock.Clock
	Random        random.Source
	Store         IncarnationStore
	Datagram      transport.Datagram
}

const (
	serviceStateNew uint32 = iota
	serviceStateRunning
	serviceStateStopped
)

// Service connects the owner-confined SWIM engine to authenticated transports.
// Construction opens no listeners and launches no goroutines.
type Service struct {
	options   ServiceOptions
	clusterID [16]byte
	limits    wire.Limits
	replay    *wire.ReplayGuard

	ready     chan struct{}
	readyOnce sync.Once
	done      chan struct{}
	events    chan serviceEvent
	state     atomic.Uint32
	admitted  atomic.Bool
	active    atomic.Value
}

// NewService validates dependencies and returns a side-effect-free service.
func NewService(options ServiceOptions) (*Service, error) {
	if err := options.Config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidServiceOptions, err)
	}
	if options.Authenticator == nil {
		return nil, fmt.Errorf("%w: authenticator is nil", ErrInvalidServiceOptions)
	}
	if options.Clock == nil {
		return nil, fmt.Errorf("%w: clock is nil", ErrInvalidServiceOptions)
	}
	if options.Random == nil {
		return nil, fmt.Errorf("%w: random source is nil", ErrInvalidServiceOptions)
	}
	if options.Store == nil {
		return nil, fmt.Errorf("%w: incarnation store is nil", ErrInvalidServiceOptions)
	}
	clusterID, err := parseClusterID(options.Config.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidServiceOptions, err)
	}
	limits := wire.DefaultLimits()
	limits.ExpectedClusterID = &clusterID
	service := &Service{
		options:   options,
		clusterID: clusterID,
		limits:    limits,
		replay: wire.NewReplayGuard(
			options.Clock,
			time.Duration(options.Config.Timing.ReplayWindow),
			serviceFutureSkew,
			serviceReplayEntries,
		),
		ready:  make(chan struct{}),
		done:   make(chan struct{}),
		events: make(chan serviceEvent, serviceEventQueueSize),
	}
	service.active.Store(map[uint16]Member{})
	return service, nil
}

// Name returns the stable supervisor registration name.
func (s *Service) Name() string {
	return "swim"
}

// Ready closes only after both UDP endpoints and the TCP snapshot listener are live.
func (s *Service) Ready() <-chan struct{} {
	return s.ready
}

// Snapshot returns a copied, sorted membership view from the owner goroutine.
func (s *Service) Snapshot(ctx context.Context) ([]Member, error) {
	if ctx == nil {
		return nil, errors.New("swim snapshot: nil context")
	}
	if s == nil || s.state.Load() != serviceStateRunning {
		return nil, ErrServiceNotRunning
	}
	response := make(chan snapshotResult, 1)
	request := snapshotServiceEvent{response: response}
	select {
	case s.events <- request:
	case <-s.done:
		return nil, ErrServiceNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-response:
		return result.members, result.err
	case <-s.done:
		return nil, ErrServiceNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Subscribe registers a bounded event stream owned by the service loop. The
// channel closes on subscription cancellation or service shutdown.
func (s *Service) Subscribe(ctx context.Context, capacity int) (<-chan MembershipEvent, error) {
	if ctx == nil {
		return nil, errors.New("swim subscribe: nil context")
	}
	if s == nil || s.state.Load() != serviceStateRunning {
		return nil, ErrServiceNotRunning
	}
	response := make(chan subscriptionResult, 1)
	request := subscribeServiceEvent{capacity: capacity, response: response}
	select {
	case s.events <- request:
	case <-s.done:
		return nil, ErrServiceNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var result subscriptionResult
	select {
	case result = <-response:
	case <-s.done:
		return nil, ErrServiceNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	go func(id uint64) {
		select {
		case <-ctx.Done():
			select {
			case s.events <- unsubscribeServiceEvent{id: id}:
			case <-s.done:
			}
		case <-s.done:
		}
	}(result.id)
	return result.events, nil
}

// Run binds transports, starts bounded readers, and owns all SWIM state until
// cancellation or a fatal listener, persistence, or invariant error.
func (s *Service) Run(ctx context.Context) (runError error) {
	if s == nil {
		return fmt.Errorf("%w: nil service", ErrInvalidServiceOptions)
	}
	if ctx == nil {
		return errors.New("run swim service: nil context")
	}
	if !s.state.CompareAndSwap(serviceStateNew, serviceStateRunning) {
		return ErrServiceAlreadyRun
	}
	defer func() {
		s.state.Store(serviceStateStopped)
		close(s.done)
	}()

	workerContext, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	var workers sync.WaitGroup
	datagram := s.options.Datagram
	if datagram == nil {
		pingEndpoint, err := s.options.Config.BindEndpoint(config.ServiceSWIMPing)
		if err != nil {
			stopWorkers()
			return err
		}
		ackEndpoint, err := s.options.Config.BindEndpoint(config.ServiceSWIMACK)
		if err != nil {
			stopWorkers()
			return err
		}
		datagram, err = transport.ListenUDP(pingEndpoint, ackEndpoint)
		if err != nil {
			stopWorkers()
			return err
		}
	}
	defer func() {
		if err := datagram.Close(); err != nil && !errors.Is(err, transport.ErrDatagramClosed) {
			runError = errors.Join(runError, err)
		}
	}()

	snapshotEndpoint, err := s.options.Config.BindEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		stopWorkers()
		return err
	}
	listener, err := net.Listen("tcp", snapshotEndpoint.String())
	if err != nil {
		stopWorkers()
		return fmt.Errorf("listen SWIM snapshot TCP on %s: %w", snapshotEndpoint, err)
	}
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			runError = errors.Join(runError, err)
		}
	}()

	table := NewTable()
	dissemination := NewDisseminator(serviceDisseminationMax, serviceRetransmitFactor)
	engine, err := NewEngine(EngineConfig{
		SelfID:               s.options.Config.NodeID,
		ProbeInterval:        time.Duration(s.options.Config.Timing.ProbeInterval),
		DirectProbeTimeout:   time.Duration(s.options.Config.Timing.DirectProbeTimeout),
		IndirectProbeTimeout: time.Duration(s.options.Config.Timing.IndirectProbeTimeout),
		IndirectChecks:       s.options.Config.Timing.IndirectChecks,
		SuspicionMultiplier:  s.options.Config.Timing.SuspicionMultiplier,
	}, table, dissemination, s.options.Random)
	if err != nil {
		stopWorkers()
		return err
	}
	loop := &serviceLoop{
		service:       s,
		engine:        engine,
		dissemination: dissemination,
		subscriptions: NewSubscriptions(),
		datagram:      datagram,
		workerContext: workerContext,
		workers:       &workers,
		requestPrefix: s.options.Random.Uint64(),
		requestCount:  s.options.Random.Uint64(),
	}
	defer loop.subscriptions.Close()

	seed, err := config.ParseEndpoint(s.options.Config.Introducer)
	if err != nil {
		stopWorkers()
		return err
	}
	selfSnapshot, err := s.options.Config.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		stopWorkers()
		return err
	}
	if seed == selfSnapshot {
		self, err := PrepareJoin(s.options.Store, nil, Member{
			NodeID:   s.options.Config.NodeID,
			Host:     s.options.Config.AdvertiseHost,
			BasePort: s.options.Config.BasePort,
		})
		if err != nil {
			stopWorkers()
			return fmt.Errorf("bootstrap SWIM seed: %w", err)
		}
		if err := loop.executeEffects(workerContext, engine.ApplyUpdate(Update{Member: self, ReporterID: self.NodeID}, s.options.Clock.Now())); err != nil {
			stopWorkers()
			return err
		}
		loop.admitted = true
		s.admitted.Store(true)
		loop.refreshActiveMembership()
	}

	workers.Add(1)
	go s.receiveDatagrams(workerContext, datagram, &workers)
	s.readyOnce.Do(func() { close(s.ready) })
	if loop.admitted {
		loop.scheduleProbe()
	}

	runError = loop.run(ctx)
	stopWorkers()
	_ = listener.Close()
	_ = datagram.Close()
	workers.Wait()
	s.admitted.Store(false)
	s.active.Store(map[uint16]Member{})
	return runError
}

type serviceEvent interface{ serviceEvent() }

type snapshotResult struct {
	members []Member
	err     error
}

type snapshotServiceEvent struct{ response chan<- snapshotResult }

func (snapshotServiceEvent) serviceEvent() {}

type subscriptionResult struct {
	id     uint64
	events <-chan MembershipEvent
}

type subscribeServiceEvent struct {
	capacity int
	response chan<- subscriptionResult
}

func (subscribeServiceEvent) serviceEvent() {}

type unsubscribeServiceEvent struct{ id uint64 }

func (unsubscribeServiceEvent) serviceEvent() {}

type datagramServiceEvent struct {
	senderID uint16
	message  any
	updates  []Update
}

func (datagramServiceEvent) serviceEvent() {}

type timerServiceEvent struct {
	request TimerRequest
	probe   bool
}

func (timerServiceEvent) serviceEvent() {}

type fatalServiceEvent struct{ err error }

func (fatalServiceEvent) serviceEvent() {}

type serviceLoop struct {
	service       *Service
	engine        *Engine
	dissemination *Disseminator
	subscriptions *Subscriptions
	datagram      transport.Datagram
	workerContext context.Context
	workers       *sync.WaitGroup
	admitted      bool
	requestPrefix uint64
	requestCount  uint64
}

func (l *serviceLoop) run(parent context.Context) error {
	for {
		select {
		case event := <-l.service.events:
			switch event := event.(type) {
			case snapshotServiceEvent:
				event.response <- snapshotResult{members: l.engine.Snapshot()}
			case subscribeServiceEvent:
				id, events := l.subscriptions.Subscribe(event.capacity)
				event.response <- subscriptionResult{id: id, events: events}
			case unsubscribeServiceEvent:
				l.subscriptions.Unsubscribe(event.id)
			case datagramServiceEvent:
				if err := l.handleDatagram(event); err != nil {
					return err
				}
			case timerServiceEvent:
				if err := l.handleTimer(event); err != nil {
					return err
				}
			case fatalServiceEvent:
				return event.err
			}
		case <-parent.Done():
			if l.admitted {
				if err := l.executeEffects(l.workerContext, l.engine.Leave(l.service.options.Clock.Now())); err != nil {
					return err
				}
			}
			return nil
		case <-l.workerContext.Done():
			return nil
		}
	}
}

func (s *Service) receiveDatagrams(ctx context.Context, datagram transport.Datagram, workers *sync.WaitGroup) {
	defer workers.Done()
	for {
		packet, err := datagram.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, transport.ErrDatagramClosed) {
				return
			}
			s.enqueueWorkerEvent(ctx, fatalServiceEvent{err: fmt.Errorf("receive SWIM datagram: %w", err)})
			return
		}
		event, ok := s.decodeDatagram(packet)
		if !ok {
			continue
		}
		if !s.enqueueWorkerEvent(ctx, event) {
			return
		}
	}
}

func (s *Service) decodeDatagram(packet transport.Packet) (datagramServiceEvent, bool) {
	if len(packet.Data) > s.limits.MaxSWIMDatagramSize {
		return datagramServiceEvent{}, false
	}
	frame, err := wire.Decode(packet.Data, s.options.Authenticator, s.limits)
	if err != nil || frame.Header.SenderID == 0 || !knownDatagramType(frame.Header.Message) {
		return datagramServiceEvent{}, false
	}
	timestamp := time.UnixMilli(frame.Header.TimestampMillis)
	if err := s.replay.Accept(frame.Header.SenderID, frame.Header.RequestID, timestamp); err != nil {
		return datagramServiceEvent{}, false
	}
	if !s.admitted.Load() {
		return datagramServiceEvent{}, false
	}
	active := s.active.Load().(map[uint16]Member)
	if _, exists := active[frame.Header.SenderID]; !exists {
		return datagramServiceEvent{}, false
	}

	event := datagramServiceEvent{senderID: frame.Header.SenderID}
	switch frame.Header.Message {
	case wire.MessageSWIMPing:
		var message PingMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	case wire.MessageSWIMAck:
		var message AckMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	case wire.MessageSWIMPingReq:
		var message PingReqMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	case wire.MessageSWIMIndirectAck:
		var message IndirectAckMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	case wire.MessageSWIMGossip:
		var message GossipMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	case wire.MessageSWIMDigest:
		var message DigestMessage
		if wire.DecodeGob(frame.Payload, &message) != nil {
			return datagramServiceEvent{}, false
		}
		event.message, event.updates = message, message.Updates
	}
	for _, update := range event.updates {
		if !validUpdate(update) || validateAdvertisedEndpoint(update.Member) != nil {
			return datagramServiceEvent{}, false
		}
	}
	return event, true
}

func (s *Service) enqueueWorkerEvent(ctx context.Context, event serviceEvent) bool {
	select {
	case s.events <- event:
		return true
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	}
}

func (l *serviceLoop) handleDatagram(event datagramServiceEvent) error {
	if !l.admitted {
		return nil
	}
	sender, exists := l.engine.table.Get(event.senderID)
	if !exists || (sender.Status != Alive && sender.Status != Suspect) {
		return nil
	}
	for _, update := range event.updates {
		if err := l.executeEffects(l.workerContext, l.engine.ApplyUpdate(update, l.service.options.Clock.Now())); err != nil {
			return err
		}
	}
	sender, exists = l.engine.table.Get(event.senderID)
	if !exists || (sender.Status != Alive && sender.Status != Suspect) {
		return nil
	}

	var effects Effects
	now := l.service.options.Clock.Now()
	switch message := event.message.(type) {
	case PingMessage:
		effects = l.engine.HandlePing(sender, message.Ping, now)
	case AckMessage:
		effects = l.engine.HandleAck(sender, message.Ack, now)
	case PingReqMessage:
		effects = l.engine.HandlePingReq(sender, message.PingReq, now)
	case IndirectAckMessage:
		effects = l.engine.HandleIndirectAck(sender, message.IndirectAck, now)
	case GossipMessage, DigestMessage:
		return nil
	default:
		return nil
	}
	return l.executeEffects(l.workerContext, effects)
}

func (l *serviceLoop) handleTimer(event timerServiceEvent) error {
	now := l.service.options.Clock.Now()
	if event.probe {
		if l.admitted {
			if err := l.executeEffects(l.workerContext, l.engine.BeginProbe(now)); err != nil {
				return err
			}
			l.scheduleProbe()
		}
		return nil
	}
	request := event.request
	var effects Effects
	switch request.Kind {
	case TimerDirectProbe:
		effects = l.engine.HandleDirectTimeout(request.Sequence, now)
	case TimerIndirectProbe:
		effects = l.engine.HandleIndirectTimeout(request.Sequence, now)
	case TimerRelayProbe:
		effects = l.engine.HandleRelayTimeout(request.OriginID, request.Sequence, now)
	case TimerSuspicion:
		effects = l.engine.HandleSuspicionTimeout(request.NodeID, request.Incarnation, now)
	case TimerTombstone:
		effects = l.engine.ExpireTombstone(request.NodeID, request.Incarnation, request.Status, now)
	case TimerLeaveDeadline:
		return nil
	}
	return l.executeEffects(l.workerContext, effects)
}

func (l *serviceLoop) executeEffects(ctx context.Context, effects Effects) error {
	if effects.PersistIncarnation != nil {
		if err := l.service.options.Store.Store(*effects.PersistIncarnation); err != nil {
			return fmt.Errorf("persist SWIM incarnation %d: %w", *effects.PersistIncarnation, err)
		}
	}
	for _, timer := range effects.Timers {
		if timer.Kind != TimerLeaveDeadline {
			l.scheduleTimer(timer)
		}
	}
	if len(effects.Events) > 0 {
		l.refreshActiveMembership()
		for _, event := range effects.Events {
			l.subscriptions.Publish(event)
		}
	}
	for _, outbound := range effects.Outbound {
		if _, err := l.sendMessage(ctx, outbound.To, outbound.Message); err != nil {
			return err
		}
	}
	if len(effects.Events) > 0 && len(effects.Outbound) == 0 {
		if err := l.sendGossipRound(ctx); err != nil {
			return err
		}
	}
	if effects.SnapshotRequired || l.dissemination.DigestRequired {
		if err := l.sendDigestRound(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (l *serviceLoop) sendGossipRound(ctx context.Context) error {
	for _, member := range l.engine.Snapshot() {
		if member.NodeID == l.service.options.Config.NodeID || member.Status != Alive {
			continue
		}
		if _, err := l.sendMessage(ctx, member, GossipMessage{}); err != nil {
			return err
		}
	}
	return nil
}

func (l *serviceLoop) sendDigestRound(ctx context.Context) error {
	sent := false
	for _, member := range l.engine.Snapshot() {
		if member.NodeID == l.service.options.Config.NodeID || member.Status != Alive {
			continue
		}
		delivered, err := l.sendMessage(ctx, member, DigestMessage{})
		if err != nil {
			return err
		}
		sent = sent || delivered
	}
	if sent {
		l.dissemination.DigestRequired = false
	}
	return nil
}

func (l *serviceLoop) sendMessage(ctx context.Context, member Member, message any) (bool, error) {
	messageType, endpointService, payloadForUpdates, err := datagramMessageDescriptor(message)
	if err != nil {
		return false, err
	}
	destinationConfig := config.NodeConfig{AdvertiseHost: member.Host, BasePort: member.BasePort}
	destination, err := destinationConfig.AdvertiseEndpoint(endpointService)
	if err != nil {
		return false, fmt.Errorf("derive SWIM destination for node %d: %w", member.NodeID, err)
	}
	header := wire.Header{
		Version:         wire.Version1,
		Message:         messageType,
		ClusterID:       l.service.clusterID,
		SenderID:        l.service.options.Config.NodeID,
		RequestID:       l.nextRequestID(),
		TimestampMillis: l.service.options.Clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}
	encode := func(updates []Update) ([]byte, error) {
		payload, err := wire.EncodeGob(payloadForUpdates(append([]Update(nil), updates...)))
		if err != nil {
			return nil, err
		}
		return wire.Encode(header, payload, l.service.options.Authenticator, l.service.limits)
	}
	updates, err := l.dissemination.Take(l.service.limits.MaxSWIMDatagramSize, l.engine.aliveMembers(), encode)
	if err != nil {
		return false, err
	}
	encoded, err := encode(updates)
	if err != nil {
		return false, err
	}
	if err := l.datagram.Send(ctx, destination, encoded); err != nil {
		return false, nil
	}
	return true, nil
}

func datagramMessageDescriptor(message any) (wire.MessageType, config.Service, func([]Update) any, error) {
	switch message := message.(type) {
	case Ping:
		return wire.MessageSWIMPing, config.ServiceSWIMPing, func(updates []Update) any { return PingMessage{Ping: message, Updates: updates} }, nil
	case Ack:
		return wire.MessageSWIMAck, config.ServiceSWIMACK, func(updates []Update) any { return AckMessage{Ack: message, Updates: updates} }, nil
	case PingReq:
		return wire.MessageSWIMPingReq, config.ServiceSWIMPing, func(updates []Update) any { return PingReqMessage{PingReq: message, Updates: updates} }, nil
	case IndirectAck:
		return wire.MessageSWIMIndirectAck, config.ServiceSWIMACK, func(updates []Update) any { return IndirectAckMessage{IndirectAck: message, Updates: updates} }, nil
	case GossipMessage:
		return wire.MessageSWIMGossip, config.ServiceSWIMPing, func(updates []Update) any { return GossipMessage{Updates: updates} }, nil
	case DigestMessage:
		return wire.MessageSWIMDigest, config.ServiceSWIMPing, func(updates []Update) any { return DigestMessage{Updates: updates} }, nil
	default:
		return 0, 0, nil, fmt.Errorf("unsupported SWIM outbound message %T", message)
	}
}

func (l *serviceLoop) scheduleProbe() {
	deadline := l.service.options.Clock.Now().Add(time.Duration(l.service.options.Config.Timing.ProbeInterval))
	l.scheduleClockEvent(deadline, timerServiceEvent{probe: true})
}

func (l *serviceLoop) scheduleTimer(request TimerRequest) {
	l.scheduleClockEvent(request.Deadline, timerServiceEvent{request: request})
}

func (l *serviceLoop) scheduleClockEvent(deadline time.Time, event timerServiceEvent) {
	duration := deadline.Sub(l.service.options.Clock.Now())
	if duration < 0 {
		duration = 0
	}
	timer := l.service.options.Clock.NewTimer(duration)
	l.workers.Add(1)
	go func() {
		defer l.workers.Done()
		defer timer.Stop()
		select {
		case <-timer.C():
			l.service.enqueueWorkerEvent(l.workerContext, event)
		case <-l.workerContext.Done():
		}
	}()
}

func (l *serviceLoop) refreshActiveMembership() {
	active := make(map[uint16]Member)
	for _, member := range l.engine.Snapshot() {
		if member.Status == Alive || member.Status == Suspect {
			active[member.NodeID] = member
		}
	}
	l.service.active.Store(active)
}

func (l *serviceLoop) nextRequestID() wire.RequestID {
	l.requestCount++
	if l.requestCount == 0 {
		l.requestCount++
	}
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[:8], l.requestPrefix)
	binary.BigEndian.PutUint64(requestID[8:], l.requestCount)
	if requestID == (wire.RequestID{}) {
		requestID[15] = 1
	}
	return requestID
}

func knownDatagramType(message wire.MessageType) bool {
	switch message {
	case wire.MessageSWIMPing, wire.MessageSWIMAck, wire.MessageSWIMPingReq, wire.MessageSWIMIndirectAck, wire.MessageSWIMGossip, wire.MessageSWIMDigest:
		return true
	default:
		return false
	}
}

func parseClusterID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, fmt.Errorf("cluster ID must contain 16 UUID bytes")
	}
	var clusterID [16]byte
	copy(clusterID[:], decoded)
	return clusterID, nil
}
