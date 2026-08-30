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
	serviceTCPConnections   = 64
	serviceTCPIOTimeout     = 5 * time.Second
	serviceResyncWorkers    = 4
	serviceResyncQueueSize  = 64
	serviceSendWorkers      = 4
	serviceSendQueueSize    = 4096
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
	Resolver      AddressResolver
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
	addresses *addressMatcher

	ready       chan struct{}
	readyOnce   sync.Once
	done        chan struct{}
	events      chan serviceEvent
	state       atomic.Uint32
	admitted    atomic.Bool
	active      atomic.Value
	connections sync.Map
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
	if options.Datagram != nil {
		if _, ok := options.Datagram.(transport.SourceDatagram); !ok {
			return nil, fmt.Errorf("%w: injected datagram does not support source selection", ErrInvalidServiceOptions)
		}
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
		addresses: newAddressMatcherWithClock(options.Resolver, options.Clock),
		ready:     make(chan struct{}),
		done:      make(chan struct{}),
		events:    make(chan serviceEvent, serviceEventQueueSize),
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
		if result.err != nil {
			return nil, result.err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case s.events <- snapshotDeliveredServiceEvent{revision: result.revision}:
			return result.members, nil
		case <-s.done:
			return nil, ErrServiceNotRunning
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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
	requestState := new(subscribeRequestState)
	request := subscribeServiceEvent{capacity: capacity, response: response, state: requestState}
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
		if err := ctx.Err(); err != nil {
			s.enqueueUnsubscribe(result.id)
			return nil, err
		}
	case <-s.done:
		return nil, ErrServiceNotRunning
	case <-ctx.Done():
		if requestState.state.CompareAndSwap(subscribeRequestPending, subscribeRequestCanceled) {
			return nil, ctx.Err()
		}
		select {
		case result = <-response:
			s.enqueueUnsubscribe(result.id)
			return nil, ctx.Err()
		case <-s.done:
			return nil, ErrServiceNotRunning
		}
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

func (s *Service) enqueueUnsubscribe(id uint64) {
	if id == 0 {
		return
	}
	select {
	case s.events <- unsubscribeServiceEvent{id: id}:
	case <-s.done:
	}
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

	workerContext, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	var datagram transport.SourceDatagram
	if s.options.Datagram == nil {
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
		datagram, err = transport.ListenUDPWithResolver(s.options.Resolver, pingEndpoint, ackEndpoint)
		if err != nil {
			stopWorkers()
			return err
		}
	} else {
		datagram = s.options.Datagram.(transport.SourceDatagram)
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
	stopParentShutdown := context.AfterFunc(ctx, func() {
		_ = listener.Close()
		s.closeTCPConnections()
	})
	defer stopParentShutdown()

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
		runContext:    ctx,
		workerContext: workerContext,
		workers:       &workers,
		requestPrefix: s.options.Random.Uint64(),
		requestCount:  s.options.Random.Uint64(),
		resyncing:     make(map[uint16]bool),
	}
	loop.client, err = newProtocolClientWithAddressMatcher(s.options.Config, s.options.Authenticator, s.options.Clock, s.options.Random, serviceTCPIOTimeout, s.addresses)
	if err != nil {
		stopWorkers()
		return err
	}
	loop.beginSnapshot = loop.client.beginSnapshot
	loop.resyncJobs = make(chan snapshotResyncJob, serviceResyncQueueSize)
	loop.sendJobs = make(chan datagramSendJob, serviceSendQueueSize)
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
		if err := loop.executeEffects(ctx, engine.ApplyUpdate(Update{Member: self, ReporterID: self.NodeID}, s.options.Clock.Now())); err != nil {
			stopWorkers()
			return err
		}
		loop.admitted = true
		s.admitted.Store(true)
		loop.refreshActiveMembership()
	}

	loop.startSnapshotResyncWorkers()
	loop.startDatagramSendWorkers()
	workers.Add(1)
	go s.receiveDatagrams(workerContext, datagram, &workers)
	workers.Add(1)
	go s.acceptTCPConnections(workerContext, listener, &workers)
	s.readyOnce.Do(func() { close(s.ready) })
	if loop.admitted {
		loop.scheduleProbe()
	} else {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := loop.client.join(workerContext, seed, s.options.Store, Member{
				NodeID:   s.options.Config.NodeID,
				Host:     s.options.Config.AdvertiseHost,
				BasePort: s.options.Config.BasePort,
			})
			s.enqueueWorkerEvent(workerContext, joinCompletedServiceEvent{result: result, err: err})
		}()
	}

	runError = loop.run(ctx)
	stopWorkers()
	_ = listener.Close()
	s.closeTCPConnections()
	_ = datagram.Close()
	workers.Wait()
	s.admitted.Store(false)
	s.active.Store(map[uint16]Member{})
	return runError
}

type serviceEvent interface{ serviceEvent() }

type snapshotResult struct {
	members          []Member
	floors           []Member
	digestGeneration uint64
	revision         uint64
	err              error
}

type snapshotServiceEvent struct{ response chan<- snapshotResult }

func (snapshotServiceEvent) serviceEvent() {}

type snapshotDeliveredServiceEvent struct{ revision uint64 }

func (snapshotDeliveredServiceEvent) serviceEvent() {}

type subscriptionResult struct {
	id     uint64
	events <-chan MembershipEvent
}

const (
	subscribeRequestPending uint32 = iota
	subscribeRequestAccepted
	subscribeRequestCanceled
)

type subscribeRequestState struct{ state atomic.Uint32 }

type subscribeServiceEvent struct {
	capacity int
	response chan<- subscriptionResult
	state    *subscribeRequestState
}

func (subscribeServiceEvent) serviceEvent() {}

type unsubscribeServiceEvent struct{ id uint64 }

func (unsubscribeServiceEvent) serviceEvent() {}

type subscriptionCountServiceEvent struct{ response chan<- int }

func (subscriptionCountServiceEvent) serviceEvent() {}

type datagramServiceEvent struct {
	sender    Member
	senderID  uint16
	requestID wire.RequestID
	timestamp time.Time
	message   any
	updates   []Update
}

func (datagramServiceEvent) serviceEvent() {}

type datagramSendJob struct {
	source      config.Endpoint
	destination config.Endpoint
	payload     []byte
}

type timerServiceEvent struct {
	request TimerRequest
	probe   bool
}

func (timerServiceEvent) serviceEvent() {}

type fatalServiceEvent struct{ err error }

func (fatalServiceEvent) serviceEvent() {}

type tcpSnapshotServiceEvent struct{ response chan<- snapshotResult }

func (tcpSnapshotServiceEvent) serviceEvent() {}

type joinAdmissionResult struct {
	accepted Member
	err      error
}

type joinAdmissionServiceEvent struct {
	announce JoinAnnounce
	response chan<- joinAdmissionResult
}

func (joinAdmissionServiceEvent) serviceEvent() {}

type joinCompletedServiceEvent struct {
	result joinClientResult
	err    error
}

func (joinCompletedServiceEvent) serviceEvent() {}

type snapshotResyncServiceEvent struct {
	sender  Member
	members []Member
	floors  []Member
	applied chan<- error
	err     error
}

func (snapshotResyncServiceEvent) serviceEvent() {}

type snapshotResyncJob struct {
	sender   Member
	endpoint config.Endpoint
}

type beginSnapshotFunc func(context.Context, config.Endpoint, uint16) (*pendingSnapshot, error)

type snapshotServedServiceEvent struct {
	requester        Member
	digestGeneration uint64
}

func (snapshotServedServiceEvent) serviceEvent() {}

type serviceLoop struct {
	service            *Service
	engine             *Engine
	dissemination      *Disseminator
	subscriptions      *Subscriptions
	activeMembers      []Member
	datagram           transport.SourceDatagram
	runContext         context.Context
	workerContext      context.Context
	workers            *sync.WaitGroup
	client             *protocolClient
	admitted           bool
	requestPrefix      uint64
	requestCount       uint64
	membershipRevision uint64
	resyncing          map[uint16]bool
	resyncJobs         chan snapshotResyncJob
	beginSnapshot      beginSnapshotFunc
	sendJobs           chan datagramSendJob
	timerScheduler     serviceTimerScheduler
	clockTimer         clock.Timer
}

func (l *serviceLoop) run(parent context.Context) error {
	defer l.stopClockTimer()
	for {
		select {
		case event := <-l.service.events:
			switch event := event.(type) {
			case snapshotServiceEvent:
				event.response <- snapshotResult{members: l.engine.Snapshot(), revision: l.membershipRevision}
			case snapshotDeliveredServiceEvent:
				if event.revision == l.membershipRevision {
					l.subscriptions.markAllResynchronized()
				}
			case subscribeServiceEvent:
				if event.state == nil || !event.state.state.CompareAndSwap(subscribeRequestPending, subscribeRequestAccepted) {
					continue
				}
				id, events := l.subscriptions.Subscribe(event.capacity)
				event.response <- subscriptionResult{id: id, events: events}
			case unsubscribeServiceEvent:
				l.subscriptions.Unsubscribe(event.id)
			case subscriptionCountServiceEvent:
				event.response <- len(l.subscriptions.subscribers)
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
			case tcpSnapshotServiceEvent:
				event.response <- snapshotResult{members: l.engine.Snapshot(), floors: l.engine.IncarnationFloors(), digestGeneration: l.dissemination.digestGeneration, revision: l.membershipRevision}
			case joinAdmissionServiceEvent:
				if err := l.handleJoinAdmission(event); err != nil {
					return err
				}
			case joinCompletedServiceEvent:
				if err := l.handleJoinCompleted(event); err != nil {
					return err
				}
			case snapshotResyncServiceEvent:
				if err := l.handleSnapshotResync(event); err != nil {
					return err
				}
			case snapshotServedServiceEvent:
				l.handleSnapshotServed(event)
			}
		case <-l.timerChannel():
			if err := l.dispatchDueTimers(); err != nil {
				return err
			}
		case <-parent.Done():
			if l.admitted {
				if err := l.gracefulLeave(); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func (l *serviceLoop) gracefulLeave() error {
	aliveBefore := l.engine.aliveMembers()
	effects := l.engine.Leave(l.service.options.Clock.Now())
	if len(effects.Events) == 0 {
		return l.executeEffects(context.WithoutCancel(l.runContext), effects)
	}
	left := effects.Events[len(effects.Events)-1].Current
	if left.NodeID != l.service.options.Config.NodeID || left.Status != Left {
		return fmt.Errorf("swim: graceful leave produced invalid local transition %#v", left)
	}
	deadline := l.service.options.Clock.Now().Add(minimumSuspicionDuration)
	for _, timer := range effects.Timers {
		if timer.Kind == TimerLeaveDeadline {
			deadline = timer.Deadline
			break
		}
	}
	cleanupContext, cancelCleanup := context.WithCancel(context.WithoutCancel(l.runContext))
	duration := deadline.Sub(l.service.options.Clock.Now())
	if duration < 0 {
		duration = 0
	}
	deadlineTimer := l.service.options.Clock.NewTimer(duration)
	deadlineDone := make(chan struct{})
	go func() {
		defer close(deadlineDone)
		select {
		case <-deadlineTimer.C():
			cancelCleanup()
		case <-cleanupContext.Done():
		}
	}()
	defer func() {
		cancelCleanup()
		deadlineTimer.Stop()
		<-deadlineDone
	}()
	if err := l.executeEffects(cleanupContext, effects); err != nil {
		return err
	}
	budget := RetransmitBudget(serviceRetransmitFactor, aliveBefore)
	peers := make([]Member, 0)
	for _, member := range l.engine.Snapshot() {
		if member.NodeID != left.NodeID && (member.Status == Alive || member.Status == Suspect) {
			peers = append(peers, member)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	update := Update{Member: left, ReporterID: left.NodeID}
	sent := 0
	for sent < budget && cleanupContext.Err() == nil {
		progress := false
		for _, peer := range peers {
			if sent >= budget {
				break
			}
			delivered, err := l.sendDirectUpdate(cleanupContext, peer, update)
			if err != nil {
				return err
			}
			if delivered {
				sent++
				progress = true
			}
		}
		if !progress {
			break
		}
	}
	return nil
}

func (l *serviceLoop) sendDirectUpdate(ctx context.Context, member Member, update Update) (bool, error) {
	destinationConfig := config.NodeConfig{AdvertiseHost: member.Host, BasePort: member.BasePort}
	destination, err := destinationConfig.AdvertiseEndpoint(config.ServiceSWIMPing)
	if err != nil {
		return false, err
	}
	payload, err := wire.EncodeGob(GossipMessage{Updates: []Update{update}})
	if err != nil {
		return false, err
	}
	encoded, err := wire.Encode(wire.Header{
		Version:         wire.Version1,
		Message:         wire.MessageSWIMGossip,
		ClusterID:       l.service.clusterID,
		SenderID:        l.service.options.Config.NodeID,
		RequestID:       l.nextRequestID(),
		TimestampMillis: l.service.options.Clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}, payload, l.service.options.Authenticator, l.service.limits)
	if err != nil {
		return false, err
	}
	if err := l.sendDatagramDirect(ctx, config.ServiceSWIMPing, destination, encoded); err != nil {
		return false, nil
	}
	return true, nil
}

func (s *Service) receiveDatagrams(ctx context.Context, datagram transport.Datagram, workers *sync.WaitGroup) {
	defer workers.Done()
	for {
		packet, err := datagram.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			s.enqueueWorkerEvent(ctx, fatalServiceEvent{err: fmt.Errorf("receive SWIM datagram: %w", err)})
			return
		}
		event, ok := s.decodeDatagramContext(ctx, packet)
		if !ok {
			continue
		}
		if !s.enqueueWorkerEvent(ctx, event) {
			return
		}
	}
}

func (s *Service) acceptTCPConnections(ctx context.Context, listener net.Listener, workers *sync.WaitGroup) {
	defer workers.Done()
	capacity := make(chan struct{}, serviceTCPConnections)
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.enqueueWorkerEvent(ctx, fatalServiceEvent{err: fmt.Errorf("accept SWIM snapshot TCP: %w", err)})
			return
		}
		select {
		case capacity <- struct{}{}:
			s.connections.Store(connection, struct{}{})
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-capacity }()
				defer s.connections.Delete(connection)
				s.handleTCPConnection(ctx, connection)
			}()
		default:
			_ = connection.Close()
		}
	}
}

func (s *Service) handleTCPConnection(ctx context.Context, connection net.Conn) {
	stream := wire.NewTCPFrameStream(connection, s.options.Authenticator, s.limits, serviceTCPIOTimeout)
	defer stream.Close()
	frame, err := stream.ReadFrame(ctx)
	if err != nil {
		return
	}
	if frame.Header.SenderID == 0 {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, fmt.Errorf("%w: zero sender ID", ErrSnapshotProtocol))
		return
	}
	if !s.admitted.Load() {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, ErrServiceNotAdmitted)
		return
	}

	switch frame.Header.Message {
	case wire.MessageSWIMJoinRequest:
		s.handleTCPJoin(ctx, stream, frame)
	case wire.MessageSWIMSnapshotRequest:
		s.handleTCPSnapshot(ctx, stream, frame)
	default:
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, fmt.Errorf("%w: first message %d", ErrSnapshotProtocol, frame.Header.Message))
	}
}

func (s *Service) handleTCPJoin(ctx context.Context, stream *wire.TCPFrameStream, requestFrame wire.Frame) {
	var request JoinRequest
	if err := wire.DecodeGob(requestFrame.Payload, &request); err != nil || request.NodeID == 0 || request.NodeID != requestFrame.Header.SenderID {
		_ = s.writeTCPError(ctx, stream, requestFrame.Header.RequestID, fmt.Errorf("%w: invalid join request", ErrInvalidJoinAnnouncement))
		return
	}
	if err := s.acceptTCPReplay(requestFrame); err != nil {
		_ = s.writeTCPError(ctx, stream, requestFrame.Header.RequestID, err)
		return
	}
	snapshot := s.requestTCPSnapshotResult(ctx)
	if snapshot.err != nil {
		_ = s.writeTCPError(ctx, stream, requestFrame.Header.RequestID, snapshot.err)
		return
	}
	if err := s.writeTCPPayload(ctx, stream, wire.MessageSWIMJoinSnapshot, requestFrame.Header.RequestID, JoinSnapshot{Members: snapshot.members, Floors: snapshot.floors}); err != nil {
		return
	}

	announceFrame, err := stream.ReadFrame(ctx)
	if err != nil {
		return
	}
	if announceFrame.Header.SenderID == 0 || announceFrame.Header.SenderID != requestFrame.Header.SenderID || announceFrame.Header.Message != wire.MessageSWIMJoinAnnounce {
		_ = s.writeTCPError(ctx, stream, announceFrame.Header.RequestID, fmt.Errorf("%w: join announcement must follow snapshot", ErrSnapshotProtocol))
		return
	}
	var announce JoinAnnounce
	if err := wire.DecodeGob(announceFrame.Payload, &announce); err != nil || announce.Member.NodeID != announceFrame.Header.SenderID || announce.Member.Incarnation == 0 || announce.Member.Status != Alive || validateAdvertisedEndpoint(announce.Member) != nil {
		_ = s.writeTCPError(ctx, stream, announceFrame.Header.RequestID, fmt.Errorf("%w: invalid join announcement payload", ErrInvalidJoinAnnouncement))
		return
	}
	if err := s.acceptTCPReplay(announceFrame); err != nil {
		_ = s.writeTCPError(ctx, stream, announceFrame.Header.RequestID, err)
		return
	}
	result := s.requestJoinAdmission(ctx, announce)
	if result.err != nil {
		_ = s.writeTCPError(ctx, stream, announceFrame.Header.RequestID, result.err)
		return
	}
	_ = s.writeTCPPayload(ctx, stream, wire.MessageSWIMJoinAccepted, announceFrame.Header.RequestID, JoinAccepted{Member: result.accepted})
}

func (s *Service) handleTCPSnapshot(ctx context.Context, stream *wire.TCPFrameStream, frame wire.Frame) {
	active := s.active.Load().(map[uint16]Member)
	requester, exists := active[frame.Header.SenderID]
	if !exists {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, ErrServiceNotAdmitted)
		return
	}
	var request SnapshotRequest
	if err := wire.DecodeGob(frame.Payload, &request); err != nil {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, fmt.Errorf("%w: invalid snapshot request", ErrInvalidSnapshotPayload))
		return
	}
	if err := s.acceptTCPReplay(frame); err != nil {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, err)
		return
	}
	result := s.requestTCPSnapshotResult(ctx)
	if result.err != nil {
		_ = s.writeTCPError(ctx, stream, frame.Header.RequestID, result.err)
		return
	}
	if err := s.writeTCPPayload(ctx, stream, wire.MessageSWIMSnapshotResponse, frame.Header.RequestID, SnapshotResponse{Members: result.members, Floors: result.floors}); err != nil {
		return
	}
	appliedFrame, err := stream.ReadFrame(ctx)
	if err != nil {
		return
	}
	if appliedFrame.Header.SenderID != requester.NodeID || appliedFrame.Header.RequestID != frame.Header.RequestID || appliedFrame.Header.Message != wire.MessageSWIMSnapshotApplied {
		return
	}
	var applied SnapshotApplied
	if wire.DecodeGob(appliedFrame.Payload, &applied) != nil {
		return
	}
	s.enqueueWorkerEvent(ctx, snapshotServedServiceEvent{requester: requester, digestGeneration: result.digestGeneration})
}

func (s *Service) acceptTCPReplay(frame wire.Frame) error {
	if err := s.replay.Accept(frame.Header.SenderID, frame.Header.RequestID, time.UnixMilli(frame.Header.TimestampMillis)); err != nil {
		return err
	}
	return nil
}

func (s *Service) requestTCPSnapshot(ctx context.Context) ([]Member, error) {
	result := s.requestTCPSnapshotResult(ctx)
	return result.members, result.err
}

func (s *Service) requestTCPSnapshotResult(ctx context.Context) snapshotResult {
	response := make(chan snapshotResult, 1)
	select {
	case s.events <- tcpSnapshotServiceEvent{response: response}:
	case <-ctx.Done():
		return snapshotResult{err: ctx.Err()}
	case <-s.done:
		return snapshotResult{err: ErrServiceNotRunning}
	}
	select {
	case result := <-response:
		return result
	case <-ctx.Done():
		return snapshotResult{err: ctx.Err()}
	case <-s.done:
		return snapshotResult{err: ErrServiceNotRunning}
	}
}

func (s *Service) requestJoinAdmission(ctx context.Context, announce JoinAnnounce) joinAdmissionResult {
	response := make(chan joinAdmissionResult, 1)
	select {
	case s.events <- joinAdmissionServiceEvent{announce: announce, response: response}:
	case <-ctx.Done():
		return joinAdmissionResult{err: ctx.Err()}
	case <-s.done:
		return joinAdmissionResult{err: ErrServiceNotRunning}
	}
	select {
	case result := <-response:
		return result
	case <-ctx.Done():
		return joinAdmissionResult{err: ctx.Err()}
	case <-s.done:
		return joinAdmissionResult{err: ErrServiceNotRunning}
	}
}

func (s *Service) writeTCPPayload(ctx context.Context, stream *wire.TCPFrameStream, message wire.MessageType, requestID wire.RequestID, value any) error {
	payload, err := wire.EncodeGob(value)
	if err != nil {
		return err
	}
	return stream.WriteFrame(ctx, wire.Frame{Header: wire.Header{
		Version:         wire.Version1,
		Message:         message,
		ClusterID:       s.clusterID,
		SenderID:        s.options.Config.NodeID,
		RequestID:       requestID,
		TimestampMillis: s.options.Clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}, Payload: payload})
}

func (s *Service) writeTCPError(ctx context.Context, stream *wire.TCPFrameStream, requestID wire.RequestID, err error) error {
	return s.writeTCPPayload(ctx, stream, wire.MessageSWIMError, requestID, encodeProtocolError(err))
}

func (s *Service) closeTCPConnections() {
	s.connections.Range(func(connection, _ any) bool {
		_ = connection.(net.Conn).Close()
		return true
	})
}

func (s *Service) decodeDatagram(packet transport.Packet) (datagramServiceEvent, bool) {
	return s.decodeDatagramContext(context.Background(), packet)
}

func (s *Service) decodeDatagramContext(ctx context.Context, packet transport.Packet) (datagramServiceEvent, bool) {
	if len(packet.Data) > s.limits.MaxSWIMDatagramSize {
		return datagramServiceEvent{}, false
	}
	frame, err := wire.Decode(packet.Data, s.options.Authenticator, s.limits)
	if err != nil || frame.Header.SenderID == 0 || !knownDatagramType(frame.Header.Message) {
		return datagramServiceEvent{}, false
	}
	if !s.admitted.Load() {
		return datagramServiceEvent{}, false
	}
	active := s.active.Load().(map[uint16]Member)
	sender, exists := active[frame.Header.SenderID]
	if !exists || !s.matchesDatagramSource(ctx, packet.From, sender, frame.Header.Message) {
		return datagramServiceEvent{}, false
	}
	event := datagramServiceEvent{
		sender:    sender,
		senderID:  frame.Header.SenderID,
		requestID: frame.Header.RequestID,
		timestamp: time.UnixMilli(frame.Header.TimestampMillis),
	}
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
	var bound bool
	event.message, bound = bindDatagramProbeRequestID(event.message, frame.Header.RequestID)
	if !bound {
		return datagramServiceEvent{}, false
	}
	for _, update := range event.updates {
		if !validUpdate(update) || validateAdvertisedEndpoint(update.Member) != nil {
			return datagramServiceEvent{}, false
		}
	}
	if !validDatagramMessage(event.message, sender, active, s.options.Config.NodeID) {
		return datagramServiceEvent{}, false
	}
	switch frame.Header.Message {
	case wire.MessageSWIMAck, wire.MessageSWIMIndirectAck:
		// Exact probe/relay correlation is owner-confined and runs immediately
		// before replay acceptance and any state mutation.
	default:
		if err := s.replay.Accept(frame.Header.SenderID, frame.Header.RequestID, event.timestamp); err != nil {
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
	if !exists || sender != event.sender || (sender.Status != Alive && sender.Status != Suspect) {
		return nil
	}
	switch message := event.message.(type) {
	case AckMessage:
		if !l.engine.acceptsAck(sender, message.Ack) {
			return nil
		}
		if err := l.service.replay.Accept(event.senderID, event.requestID, event.timestamp); err != nil {
			return nil
		}
	case IndirectAckMessage:
		if !l.engine.acceptsIndirectAck(sender, message.IndirectAck) {
			return nil
		}
		if err := l.service.replay.Accept(event.senderID, event.requestID, event.timestamp); err != nil {
			return nil
		}
	}
	for _, update := range event.updates {
		if err := l.executeEffects(l.runContext, l.engine.ApplyUpdate(update, l.service.options.Clock.Now())); err != nil {
			return err
		}
	}
	sender, exists = l.engine.table.Get(event.senderID)
	if !exists || sender != event.sender || (sender.Status != Alive && sender.Status != Suspect) {
		return nil
	}

	var effects Effects
	now := l.service.options.Clock.Now()
	switch message := event.message.(type) {
	case PingMessage:
		effects = l.engine.HandlePing(sender, message.Ping, now)
	case AckMessage:
		effects = l.engine.HandleAck(sender, message.Ack, now)
		if message.Ack.OriginID == l.service.options.Config.NodeID {
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerDirectProbe, Sequence: message.Ack.Sequence, RequestID: message.Ack.RequestID}))
		} else {
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerRelayProbe, OriginID: message.Ack.OriginID, Sequence: message.Ack.Sequence, RequestID: message.Ack.RequestID}))
		}
	case PingReqMessage:
		effects = l.engine.HandlePingReq(sender, message.PingReq, now)
	case IndirectAckMessage:
		effects = l.engine.HandleIndirectAck(sender, message.IndirectAck, now)
		l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerDirectProbe, Sequence: message.IndirectAck.Sequence, RequestID: message.IndirectAck.RequestID}))
	case GossipMessage:
		return nil
	case DigestMessage:
		l.startSnapshotResync(sender)
		return nil
	default:
		return nil
	}
	return l.executeEffects(l.runContext, effects)
}

func (l *serviceLoop) handleJoinAdmission(event joinAdmissionServiceEvent) error {
	if !l.admitted {
		event.response <- joinAdmissionResult{err: ErrServiceNotAdmitted}
		return nil
	}
	if err := ValidateJoinAnnouncement(l.engine.table, event.announce); err != nil {
		event.response <- joinAdmissionResult{err: err}
		return nil
	}
	effects := l.engine.ApplyUpdate(Update{Member: event.announce.Member, ReporterID: event.announce.Member.NodeID}, l.service.options.Clock.Now())
	if err := l.executeEffects(l.runContext, effects); err != nil {
		event.response <- joinAdmissionResult{err: err}
		return err
	}
	event.response <- joinAdmissionResult{accepted: event.announce.Member}
	return nil
}

func (l *serviceLoop) handleJoinCompleted(event joinCompletedServiceEvent) error {
	if event.err != nil {
		return fmt.Errorf("join SWIM cluster: %w", event.err)
	}
	result := event.result
	if result.seedID == 0 || result.seedID == l.service.options.Config.NodeID {
		return fmt.Errorf("%w: invalid seed sender ID %d", ErrSnapshotProtocol, result.seedID)
	}
	if result.accepted.NodeID != l.service.options.Config.NodeID || result.accepted.Host != l.service.options.Config.AdvertiseHost || result.accepted.BasePort != l.service.options.Config.BasePort || result.accepted.Status != Alive || result.accepted.Incarnation == 0 {
		return fmt.Errorf("%w: seed accepted invalid local member %#v", ErrSnapshotProtocol, result.accepted)
	}
	seedPresent := false
	for _, floor := range result.floors {
		if err := l.executeEffects(l.runContext, l.engine.ApplyIncarnationFloor(floor, result.seedID, l.service.options.Clock.Now())); err != nil {
			return err
		}
	}
	for _, member := range result.snapshot {
		if member.NodeID == result.seedID && (member.Status == Alive || member.Status == Suspect) {
			seedPresent = true
		}
		if err := l.executeEffects(l.runContext, l.engine.ApplyUpdate(Update{Member: member, ReporterID: result.seedID}, l.service.options.Clock.Now())); err != nil {
			return err
		}
	}
	if !seedPresent {
		return fmt.Errorf("%w: join snapshot omitted active seed %d", ErrSnapshotProtocol, result.seedID)
	}
	if err := l.executeEffects(l.runContext, l.engine.ApplyUpdate(Update{Member: result.accepted, ReporterID: result.accepted.NodeID}, l.service.options.Clock.Now())); err != nil {
		return err
	}
	l.admitted = true
	l.service.admitted.Store(true)
	l.refreshActiveMembership()
	l.scheduleProbe()
	return nil
}

func (l *serviceLoop) startSnapshotResync(sender Member) {
	if l.resyncing[sender.NodeID] {
		return
	}
	endpointConfig := config.NodeConfig{AdvertiseHost: sender.Host, BasePort: sender.BasePort}
	endpoint, err := endpointConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		return
	}
	l.resyncing[sender.NodeID] = true
	job := snapshotResyncJob{sender: sender, endpoint: endpoint}
	select {
	case l.resyncJobs <- job:
	default:
		delete(l.resyncing, sender.NodeID)
	}
}

func (l *serviceLoop) startSnapshotResyncWorkers() {
	for range serviceResyncWorkers {
		l.workers.Add(1)
		go func() {
			defer l.workers.Done()
			for {
				select {
				case job := <-l.resyncJobs:
					l.runSnapshotResync(job)
				case <-l.workerContext.Done():
					return
				}
			}
		}()
	}
}

func (l *serviceLoop) runSnapshotResync(job snapshotResyncJob) {
	pending, err := l.beginSnapshot(l.workerContext, job.endpoint, job.sender.NodeID)
	if err != nil {
		l.service.enqueueWorkerEvent(l.workerContext, snapshotResyncServiceEvent{sender: job.sender, err: err})
		return
	}
	if pending == nil {
		l.service.enqueueWorkerEvent(l.workerContext, snapshotResyncServiceEvent{sender: job.sender, err: fmt.Errorf("%w: nil snapshot response", ErrSnapshotProtocol)})
		return
	}
	defer pending.close()
	if pending.senderID != job.sender.NodeID {
		l.service.enqueueWorkerEvent(l.workerContext, snapshotResyncServiceEvent{sender: job.sender, err: fmt.Errorf("%w: snapshot responder %d, expected %d", ErrSnapshotProtocol, pending.senderID, job.sender.NodeID)})
		return
	}
	applied := make(chan error, 1)
	if !l.service.enqueueWorkerEvent(l.workerContext, snapshotResyncServiceEvent{sender: job.sender, members: pending.members, floors: pending.floors, applied: applied}) {
		return
	}
	select {
	case applyError := <-applied:
		if applyError == nil {
			_ = pending.acknowledge(l.workerContext)
		}
	case <-l.workerContext.Done():
	}
}

func (l *serviceLoop) handleSnapshotResync(event snapshotResyncServiceEvent) error {
	delete(l.resyncing, event.sender.NodeID)
	if event.err != nil {
		return nil
	}
	current, exists := l.engine.table.Get(event.sender.NodeID)
	if !exists || current != event.sender || (current.Status != Alive && current.Status != Suspect) {
		if event.applied != nil {
			event.applied <- ErrSnapshotProtocol
		}
		return nil
	}
	for _, floor := range event.floors {
		if err := l.executeEffects(l.runContext, l.engine.ApplyIncarnationFloor(floor, event.sender.NodeID, l.service.options.Clock.Now())); err != nil {
			if event.applied != nil {
				event.applied <- err
			}
			return err
		}
	}
	for _, member := range event.members {
		if err := l.executeEffects(l.runContext, l.engine.ApplyUpdate(Update{Member: member, ReporterID: event.sender.NodeID}, l.service.options.Clock.Now())); err != nil {
			if event.applied != nil {
				event.applied <- err
			}
			return err
		}
	}
	if event.applied != nil {
		event.applied <- nil
	}
	return nil
}

func (l *serviceLoop) handleSnapshotServed(event snapshotServedServiceEvent) {
	current, exists := l.engine.table.Get(event.requester.NodeID)
	if !exists || current != event.requester || (current.Status != Alive && current.Status != Suspect) {
		return
	}
	l.dissemination.markDigestRepaired(event.digestGeneration)
}

func (l *serviceLoop) handleTimer(event timerServiceEvent) error {
	now := l.service.options.Clock.Now()
	if event.probe {
		if l.admitted {
			if err := l.executeEffects(l.runContext, l.engine.BeginProbe(now)); err != nil {
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
		effects = l.engine.HandleDirectTimeoutRequest(ProbeID{Sequence: request.Sequence, RequestID: request.RequestID}, now)
	case TimerIndirectProbe:
		effects = l.engine.HandleIndirectTimeoutRequest(ProbeID{Sequence: request.Sequence, RequestID: request.RequestID}, now)
	case TimerRelayProbe:
		effects = l.engine.HandleRelayTimeoutRequest(request.OriginID, request.Sequence, request.RequestID, now)
	case TimerSuspicion:
		effects = l.engine.HandleSuspicionTimeout(request.NodeID, request.Incarnation, now)
	case TimerTombstone:
		effects = l.engine.ExpireTombstone(request.NodeID, request.Incarnation, request.Status, now)
	case TimerLeaveDeadline:
		return nil
	}
	return l.executeEffects(l.runContext, effects)
}

func (l *serviceLoop) executeEffects(ctx context.Context, effects Effects) error {
	if effects.PersistIncarnation != nil {
		if err := l.service.options.Store.Store(*effects.PersistIncarnation); err != nil {
			return fmt.Errorf("persist SWIM incarnation %d: %w", *effects.PersistIncarnation, err)
		}
	}
	for _, event := range effects.Events {
		if event.Previous.NodeID != 0 && (event.Current.Incarnation > event.Previous.Incarnation || event.Current.Host != event.Previous.Host || event.Current.BasePort != event.Previous.BasePort) {
			l.service.addresses.invalidate(event.Previous.Host)
			l.service.addresses.invalidate(event.Current.Host)
		}
		nodeID := event.Current.NodeID
		switch event.Current.Status {
		case Alive:
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerSuspicion, NodeID: nodeID}))
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerTombstone, NodeID: nodeID}))
		case Suspect:
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerTombstone, NodeID: nodeID}))
		case Dead, Left:
			l.cancelClockEvent(serviceTimerKeyFor(TimerRequest{Kind: TimerSuspicion, NodeID: nodeID}))
		}
	}
	for _, timer := range effects.Timers {
		if timer.Kind != TimerLeaveDeadline {
			l.scheduleTimer(timer)
		}
	}
	if len(effects.Events) > 0 {
		for range effects.Events {
			l.membershipRevision++
			if l.membershipRevision == 0 {
				l.membershipRevision++
			}
		}
		if eventsChangeActiveMembership(effects.Events) {
			l.refreshActiveMembership()
		}
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
	for _, member := range l.activeMembers {
		if member.NodeID == l.service.options.Config.NodeID {
			continue
		}
		if _, err := l.sendMessage(ctx, member, GossipMessage{}); err != nil {
			return err
		}
	}
	return nil
}

func (l *serviceLoop) sendDigestRound(ctx context.Context) error {
	for _, member := range l.activeMembers {
		if member.NodeID == l.service.options.Config.NodeID {
			continue
		}
		_, err := l.sendMessage(ctx, member, DigestMessage{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *serviceLoop) sendMessage(ctx context.Context, member Member, message any) (bool, error) {
	requestID := l.nextRequestID()
	if probeRequestID, isProbe := outboundProbeRequestID(message); isProbe {
		if probeRequestID == (wire.RequestID{}) {
			message = withOutboundProbeRequestID(message, requestID)
		} else {
			requestID = probeRequestID
		}
	}
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
		RequestID:       requestID,
		TimestampMillis: l.service.options.Clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}
	encodeWithLimits := func(updates []Update, limits wire.Limits) ([]byte, error) {
		payload, err := wire.EncodeGob(payloadForUpdates(append([]Update(nil), updates...)))
		if err != nil {
			return nil, err
		}
		return wire.Encode(header, payload, l.service.options.Authenticator, limits)
	}
	sizingLimits := l.service.limits
	sizingLimits.MaxSWIMDatagramSize = sizingLimits.MaxFrameSize
	updates, err := l.dissemination.Take(l.service.limits.MaxSWIMDatagramSize, l.engine.aliveMembers(), func(updates []Update) ([]byte, error) {
		return encodeWithLimits(updates, sizingLimits)
	})
	if err != nil {
		return false, err
	}
	encoded, err := encodeWithLimits(updates, l.service.limits)
	if err != nil {
		return false, err
	}
	if err := l.sendDatagram(ctx, endpointService, destination, encoded); err != nil {
		return false, nil
	}
	return true, nil
}

func (l *serviceLoop) sendDatagram(ctx context.Context, sourceService config.Service, destination config.Endpoint, encoded []byte) error {
	source, err := l.service.options.Config.BindEndpoint(sourceService)
	if err != nil {
		return err
	}
	if l.sendJobs == nil {
		return l.datagram.SendFrom(ctx, source, destination, encoded)
	}
	job := datagramSendJob{source: source, destination: destination, payload: encoded}
	select {
	case l.sendJobs <- job:
		return nil
	case <-l.workerContext.Done():
		return l.workerContext.Err()
	default:
		return errors.New("swim: outbound datagram queue full")
	}
}

func (l *serviceLoop) sendDatagramDirect(ctx context.Context, sourceService config.Service, destination config.Endpoint, encoded []byte) error {
	source, err := l.service.options.Config.BindEndpoint(sourceService)
	if err != nil {
		return err
	}
	return l.datagram.SendFrom(ctx, source, destination, encoded)
}

func (l *serviceLoop) startDatagramSendWorkers() {
	for range serviceSendWorkers {
		l.workers.Add(1)
		go func() {
			defer l.workers.Done()
			for {
				select {
				case <-l.workerContext.Done():
					return
				default:
				}
				select {
				case job := <-l.sendJobs:
					_ = l.datagram.SendFrom(l.workerContext, job.source, job.destination, job.payload)
				case <-l.workerContext.Done():
					return
				}
			}
		}()
	}
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
	l.scheduleClockEvent(probeCycleTimerKey(), deadline, timerServiceEvent{probe: true})
}

func (l *serviceLoop) scheduleTimer(request TimerRequest) {
	l.scheduleClockEvent(serviceTimerKeyFor(request), request.Deadline, timerServiceEvent{request: request})
}

func (l *serviceLoop) refreshActiveMembership() {
	active := make(map[uint16]Member)
	activeMembers := make([]Member, 0)
	for _, member := range l.engine.Snapshot() {
		if member.Status == Alive || member.Status == Suspect {
			active[member.NodeID] = member
			activeMembers = append(activeMembers, member)
		}
	}
	l.activeMembers = activeMembers
	l.service.active.Store(active)
}

func eventsChangeActiveMembership(events []MembershipEvent) bool {
	for _, event := range events {
		previousActive := event.Previous.NodeID != 0 && (event.Previous.Status == Alive || event.Previous.Status == Suspect)
		currentActive := event.Current.Status == Alive || event.Current.Status == Suspect
		if previousActive != currentActive || (currentActive && event.Previous != event.Current) {
			return true
		}
	}
	return false
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

func (s *Service) matchesDatagramSource(ctx context.Context, source config.Endpoint, sender Member, message wire.MessageType) bool {
	var sourceService config.Service
	switch message {
	case wire.MessageSWIMPing, wire.MessageSWIMPingReq, wire.MessageSWIMGossip, wire.MessageSWIMDigest:
		sourceService = config.ServiceSWIMPing
	case wire.MessageSWIMAck, wire.MessageSWIMIndirectAck:
		sourceService = config.ServiceSWIMACK
	default:
		return false
	}
	expected, err := (config.NodeConfig{AdvertiseHost: sender.Host, BasePort: sender.BasePort}).AdvertiseEndpoint(sourceService)
	return err == nil && s.addresses.matchesSource(ctx, source, expected)
}

func validDatagramMessage(message any, sender Member, active map[uint16]Member, selfID uint16) bool {
	activeIdentity := func(member Member, requireAlive bool) bool {
		current, exists := active[member.NodeID]
		if !exists || current != member {
			return false
		}
		return !requireAlive || current.Status == Alive
	}
	validOrigin := func(originID uint16) bool {
		_, exists := active[originID]
		return originID != 0 && exists
	}

	switch message := message.(type) {
	case PingMessage:
		return message.Ping.Sequence != 0 && message.Ping.RequestID != (wire.RequestID{}) && validOrigin(message.Ping.OriginID)
	case AckMessage:
		return message.Ack.Sequence != 0 && message.Ack.RequestID != (wire.RequestID{}) && validOrigin(message.Ack.OriginID)
	case PingReqMessage:
		request := message.PingReq
		return request.Sequence != 0 && request.RequestID != (wire.RequestID{}) && request.OriginID == sender.NodeID && request.Target.NodeID != sender.NodeID && request.Target.NodeID != selfID && activeIdentity(request.Target, true)
	case IndirectAckMessage:
		ack := message.IndirectAck
		return ack.Sequence != 0 && ack.RequestID != (wire.RequestID{}) && ack.OriginID == selfID && activeIdentity(ack.Target, false)
	case GossipMessage, DigestMessage:
		return true
	default:
		return false
	}
}

func bindDatagramProbeRequestID(message any, headerID wire.RequestID) (any, bool) {
	if headerID == (wire.RequestID{}) {
		return nil, false
	}
	bind := func(payloadID wire.RequestID) (wire.RequestID, bool) {
		return payloadID, payloadID != (wire.RequestID{}) && payloadID == headerID
	}
	switch message := message.(type) {
	case PingMessage:
		var ok bool
		message.Ping.RequestID, ok = bind(message.Ping.RequestID)
		return message, ok
	case AckMessage:
		var ok bool
		message.Ack.RequestID, ok = bind(message.Ack.RequestID)
		return message, ok
	case PingReqMessage:
		var ok bool
		message.PingReq.RequestID, ok = bind(message.PingReq.RequestID)
		return message, ok
	case IndirectAckMessage:
		var ok bool
		message.IndirectAck.RequestID, ok = bind(message.IndirectAck.RequestID)
		return message, ok
	default:
		return message, true
	}
}

func outboundProbeRequestID(message any) (wire.RequestID, bool) {
	switch message := message.(type) {
	case Ping:
		return message.RequestID, true
	case Ack:
		return message.RequestID, true
	case PingReq:
		return message.RequestID, true
	case IndirectAck:
		return message.RequestID, true
	default:
		return wire.RequestID{}, false
	}
}

func withOutboundProbeRequestID(message any, requestID wire.RequestID) any {
	switch message := message.(type) {
	case Ping:
		message.RequestID = requestID
		return message
	case Ack:
		message.RequestID = requestID
		return message
	case PingReq:
		message.RequestID = requestID
		return message
	case IndirectAck:
		message.RequestID = requestID
		return message
	default:
		return message
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

func encodeProtocolError(err error) ProtocolErrorMessage {
	response := ProtocolErrorMessage{Code: protocolErrorInternal, Message: "internal service failure"}
	switch {
	case errors.Is(err, ErrDuplicateNodeID):
		response.Code, response.Message = protocolErrorDuplicateNodeID, err.Error()
	case errors.Is(err, ErrStaleJoinIncarnation):
		response.Code, response.Message = protocolErrorStaleIncarnation, err.Error()
	case errors.Is(err, ErrInvalidJoinAnnouncement):
		response.Code, response.Message = protocolErrorInvalidPayload, err.Error()
	case errors.Is(err, ErrInvalidSnapshotPayload):
		response.Code, response.Message = protocolErrorInvalidPayload, err.Error()
	case errors.Is(err, ErrServiceNotAdmitted):
		response.Code, response.Message = protocolErrorNotAdmitted, ErrServiceNotAdmitted.Error()
	case errors.Is(err, wire.ErrReplay), errors.Is(err, wire.ErrReplayCacheFull), errors.Is(err, wire.ErrTimestamp):
		response.Code, response.Message = protocolErrorReplay, "request rejected by replay defense"
	case errors.Is(err, ErrSnapshotProtocol):
		response.Code, response.Message = protocolErrorUnexpectedMessage, err.Error()
	}
	return response
}
