package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const WorkerStoreDirectory = "crane-worker"

var ErrServiceNotReady = errors.New("Crane worker service is not ready")

// ServiceOptions fixes every caller-owned dependency of one worker service.
// NewService retains these exact values but does not open any resource.
type ServiceOptions struct {
	Config        config.NodeConfig
	Authenticator wire.Authenticator
	Clock         clock.Clock
	Membership    *membership.Authorizer
	Gate          *admission.Gate
	OpenStore     func(string, store.Identity, store.Options) (*store.Store, error)
	Datagram      transport.SourceDatagram
	// ArtifactDirectory optionally locates the durable sealed result artifact
	// store created during Run. Empty keeps artifact receive and leader fetch
	// fail-closed unavailable, exactly as before this seam existed.
	ArtifactDirectory string
}

// Service composes one durable worker owner, one +5 listener, and exactly one
// shared +7 sender/receiver endpoint.
type Service struct {
	configuration     config.NodeConfig
	authenticator     wire.Authenticator
	clock             clock.Clock
	membership        *membership.Authorizer
	gate              *admission.Gate
	openStore         func(string, store.Identity, store.Options) (*store.Store, error)
	datagram          transport.SourceDatagram
	artifactDirectory string
	clusterID         [16]byte
	controlBind       config.Endpoint
	listen            func(string, string) (net.Listener, error)

	store      *store.Store
	repository *serviceRepository
	engine     *Engine
	endpoint   *TupleEndpoint
	tuple      *TupleService
	transfer   *TransferOwner
	control    *ControlOwner

	ready      chan struct{}
	started    atomic.Bool
	readyState atomic.Bool
}

// NewService validates and retains the complete worker composition without
// opening a store, binding a listener, activating a datagram, or starting work.
func NewService(options ServiceOptions) (*Service, error) {
	if options.Authenticator == nil || options.Clock == nil || options.Membership == nil || options.Gate == nil || options.OpenStore == nil || options.Datagram == nil {
		return nil, errors.New("Crane worker service requires authenticator, clock, membership, gate, store opener, and datagram")
	}
	configuration := cloneWorkerNodeConfig(options.Config)
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate Crane worker configuration: %w", err)
	}
	clusterID, err := decodeTupleClusterID(configuration.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("decode Crane cluster ID: %w", err)
	}
	controlBind, err := configuration.BindEndpoint(config.ServiceCraneWorker)
	if err != nil {
		return nil, fmt.Errorf("derive Crane worker control endpoint: %w", err)
	}
	return &Service{
		configuration: configuration, authenticator: options.Authenticator, clock: options.Clock,
		membership: options.Membership, gate: options.Gate, openStore: options.OpenStore,
		datagram: options.Datagram, artifactDirectory: options.ArtifactDirectory,
		clusterID: clusterID, controlBind: controlBind,
		listen: net.Listen, ready: make(chan struct{}),
	}, nil
}

func (*Service) Name() string { return "crane-worker" }

func (service *Service) Ready() <-chan struct{} { return service.ready }

// Run recovers durable identity before binding either network service, then
// joins every owned worker and closes the Store last.
func (service *Service) Run(ctx context.Context) (runErr error) {
	if ctx == nil {
		return errors.New("run Crane worker service: nil context")
	}
	if !service.started.CompareAndSwap(false, true) {
		return errors.New("Crane worker service Run called more than once")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := service.gate.CloseAndWait(ctx); err != nil {
		return err
	}

	workerStore, err := service.openStore(
		filepath.Join(service.configuration.StorageDir, WorkerStoreDirectory),
		store.Identity{ClusterID: service.clusterID, NodeID: service.configuration.NodeID},
		store.Options{MaxBytes: service.configuration.Crane.MaxWorkerStoreBytes},
	)
	if err != nil {
		return fmt.Errorf("open Crane worker store: %w", err)
	}
	if workerStore == nil {
		return errors.New("open Crane worker store: nil store")
	}
	service.store = workerStore
	defer func() {
		service.readyState.Store(false)
		runErr = errors.Join(runErr, workerStore.Close())
	}()

	repository := &serviceRepository{Store: workerStore, node: service.configuration.NodeID, fatal: make(chan error, 1)}
	service.repository = repository
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: service.configuration, Authenticator: service.authenticator, Clock: service.clock, Membership: service.membership, Datagram: service.datagram})
	if err != nil {
		return err
	}
	service.endpoint = endpoint
	replicator := &controlResultReplicator{configuration: service.configuration, authenticator: service.authenticator, clock: service.clock, membership: service.membership, repository: repository, clusterID: service.clusterID, timeout: time.Duration(service.configuration.Crane.WorkerControlTimeout), dial: (&net.Dialer{}).DialContext}
	engine, err := NewEngine(EngineOptions{Repository: repository, Sender: endpoint, Replicator: replicator, Gate: service.gate, Clock: service.clock, MaxExecutors: service.configuration.Crane.WorkerSlots, AcceptedRetryInterval: time.Duration(service.configuration.Crane.TupleRetryInterval), CompletedRetryInterval: time.Duration(service.configuration.Crane.TupleCompletionRetryInterval)})
	if err != nil {
		return err
	}
	service.engine = engine
	tuple, err := NewTupleService(TupleServiceOptions{Endpoint: endpoint, Engine: engine})
	if err != nil {
		return err
	}
	service.tuple = tuple
	transferOptions := TransferOptions{Repository: repository}
	if service.artifactDirectory != "" {
		if err := swim.EnsureStorageDirectory(service.artifactDirectory); err != nil {
			return fmt.Errorf("prepare Crane artifact directory: %w", err)
		}
		artifacts, err := NewArtifactStore(service.artifactDirectory)
		if err != nil {
			return fmt.Errorf("open Crane artifact store: %w", err)
		}
		transferOptions.Artifacts = artifacts
	}
	transfer, err := NewTransferOwner(transferOptions)
	if err != nil {
		return err
	}
	service.transfer = transfer
	control, err := NewControlOwner(ControlOptions{Config: service.configuration, ClusterID: service.clusterID, Repository: repository, Engine: engine, Transfer: transfer, Gate: service.gate, Membership: service.membership, Clock: service.clock})
	if err != nil {
		return err
	}
	service.control = control

	listener, err := service.listen("tcp", service.controlBind.String())
	if err != nil {
		return fmt.Errorf("listen Crane worker control: %w", err)
	}
	if listener == nil {
		return errors.New("listen Crane worker control: nil listener")
	}

	runContext, cancel := context.WithCancel(ctx)
	results := make(chan serviceRunResult, 3)
	controlReady := make(chan struct{})
	controlDispatch := make(chan struct{})
	go func() { results <- serviceRunResult{name: "engine", err: engine.Run(runContext)} }()
	go func() { results <- serviceRunResult{name: "tuple", err: tuple.Run(runContext)} }()
	go func() {
		results <- serviceRunResult{name: "control", err: service.runControl(runContext, listener, control, controlReady, controlDispatch)}
	}()

	engineReady, tupleReady := engine.Ready(), tuple.Ready()
	readyOwners, completedOwners := 0, 0
	engineIsReady, tupleIsReady, dispatchEnabled := false, false, false
	for readyOwners < 3 && runErr == nil {
		select {
		case <-engineReady:
			engineReady = nil
			engineIsReady = true
			readyOwners++
		case <-tupleReady:
			tupleReady = nil
			tupleIsReady = true
			readyOwners++
		case <-controlReady:
			controlReady = nil
			readyOwners++
		case result := <-results:
			completedOwners++
			runErr = fmt.Errorf("run Crane worker %s: %w", result.name, result.err)
		case <-ctx.Done():
			runErr = ctx.Err()
		case fatalErr := <-repository.fatal:
			runErr = fmt.Errorf("Crane worker durable authority: %w", fatalErr)
		}
		if engineIsReady && tupleIsReady && !dispatchEnabled {
			close(controlDispatch)
			dispatchEnabled = true
		}
	}
	if runErr == nil {
		service.readyState.Store(true)
		close(service.ready)
		select {
		case result := <-results:
			completedOwners++
			runErr = fmt.Errorf("run Crane worker %s: %w", result.name, result.err)
		case <-ctx.Done():
			runErr = ctx.Err()
		case fatalErr := <-repository.fatal:
			runErr = fmt.Errorf("Crane worker durable authority: %w", fatalErr)
		}
	}

	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), time.Duration(service.configuration.Crane.WorkerControlTimeout))
	gateErr := service.gate.CloseAndWait(cleanupContext)
	cleanupCancel()
	cancel()
	listenerErr := listener.Close()
	controlErr := control.Close()
	for completedOwners < 3 {
		result := <-results
		completedOwners++
		if runErr == nil && result.err != nil && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, net.ErrClosed) {
			runErr = fmt.Errorf("run Crane worker %s: %w", result.name, result.err)
		}
	}
	finalGateErr := service.gate.CloseAndWait(context.Background())
	if finalGateErr == nil && (errors.Is(gateErr, context.DeadlineExceeded) || errors.Is(gateErr, context.Canceled)) {
		gateErr = nil
	}
	if ctx.Err() != nil {
		runErr = ctx.Err()
	}
	return errors.Join(runErr, gateErr, finalGateErr, ignoreClosedNetworkError(listenerErr), ignoreClosedNetworkError(controlErr))
}

type serviceRunResult struct {
	name string
	err  error
}

// LocalStatus returns one deeply owned bounded local snapshot only while every
// runtime owner is ready.
func (service *Service) LocalStatus(ctx context.Context) (protocol.WorkerStatus, error) {
	if ctx == nil {
		return protocol.WorkerStatus{}, errors.New("nil Crane worker status context")
	}
	if !service.readyState.Load() || service.control == nil {
		return protocol.WorkerStatus{}, ErrServiceNotReady
	}
	return service.control.localStatus(ctx)
}

func (service *Service) runControl(ctx context.Context, listener net.Listener, owner *ControlOwner, ready chan<- struct{}, dispatch <-chan struct{}) error {
	close(ready)
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopAccept()
	handlerErrors := make(chan error, DefaultMaxControlSessions)
	var handlers sync.WaitGroup
	defer func() {
		_ = listener.Close()
		_ = owner.Close()
		handlers.Wait()
	}()
	select {
	case <-dispatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		connection, err := listener.Accept()
		if err != nil {
			select {
			case handlerErr := <-handlerErrors:
				return handlerErr
			default:
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept Crane worker control: %w", err)
		}
		session, err := owner.NewSession(connection.RemoteAddr(), connection.Close)
		if err != nil {
			_ = connection.Close()
			continue
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			if handlerErr := service.handleControlConnection(ctx, connection, session); fatalControlServiceError(ctx, handlerErr) {
				select {
				case handlerErrors <- handlerErr:
				default:
				}
				_ = listener.Close()
			}
		}()
		select {
		case err := <-handlerErrors:
			return err
		default:
		}
	}
}

func (service *Service) handleControlConnection(ctx context.Context, connection net.Conn, session *ControlSession) error {
	defer session.Close()
	sessionContext, cancel := context.WithTimeout(ctx, time.Duration(service.configuration.Crane.WorkerControlTimeout))
	defer cancel()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &service.clusterID
	stream := wire.NewTCPFrameStream(connection, service.authenticator, limits, time.Duration(service.configuration.Crane.WorkerControlTimeout))
	for {
		frame, err := stream.ReadFrame(sessionContext)
		if err != nil {
			return err
		}
		response, handleErr := session.Handle(sessionContext, frame)
		if handleErr != nil {
			response = service.controlError(frame.Header.Message, handleErr)
			if response == nil {
				return handleErr
			}
		}
		payload, err := protocol.MarshalWorkerMessage(response)
		if err != nil {
			return err
		}
		outbound := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: response.MessageType(), ClusterID: service.clusterID, SenderID: service.configuration.NodeID, RequestID: frame.Header.RequestID, TimestampMillis: service.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
		if err := stream.WriteFrame(sessionContext, outbound); err != nil {
			return err
		}
	}
}

func (service *Service) controlError(message wire.MessageType, err error) protocol.WorkerMessage {
	work, recoverErr := service.repository.RecoverWork()
	if recoverErr != nil || work.Fence.Validate() != nil {
		return nil
	}
	code, retryable, detail := protocol.WorkerErrorMalformed, false, "request rejected"
	switch {
	case errors.Is(err, ErrControlUnauthorized), errors.Is(err, ErrControlHandshakeRequired), errors.Is(err, ErrTransferUnauthorized):
		code, detail = protocol.WorkerErrorUnauthorized, "unauthorized"
	case errors.Is(err, ErrControlStaleEpoch), errors.Is(err, ErrTransferStaleAuthority):
		code, detail = protocol.WorkerErrorStaleEpoch, "stale coordinator epoch"
	case errors.Is(err, ErrControlStaleAssignment), errors.Is(err, ErrTransferIdentityReuse), errors.Is(err, model.ErrIdentityReuse):
		code, detail = protocol.WorkerErrorStaleAssignment, "stale assignment"
	case errors.Is(err, ErrControlCapacity), errors.Is(err, ErrTransferCapacity), errors.Is(err, store.ErrCapacity):
		code, retryable, detail = protocol.WorkerErrorCapacity, true, "worker capacity exhausted"
	case errors.Is(err, store.ErrUnavailable), errors.Is(err, store.ErrClosed), errors.Is(err, admission.ErrClosed):
		code, retryable, detail = protocol.WorkerErrorUnavailable, true, "worker store unavailable"
	case errors.Is(err, ErrResultArtifactUnavailable), errors.Is(err, ErrResultFetchUnavailable):
		code, retryable, detail = protocol.WorkerErrorUnavailable, true, "result storage unavailable"
	case errors.Is(err, store.ErrCorrupt):
		code, detail = protocol.WorkerErrorCorrupt, "worker store corrupt"
	}
	return protocol.WorkerError{NodeID: service.configuration.NodeID, WorkerEpoch: service.store.WorkerEpoch(), CoordinatorEpoch: work.Fence, RelatedMessage: message, Code: code, Retryable: retryable, Detail: []byte(detail)}
}

func fatalControlServiceError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return false
	}
	return errors.Is(err, store.ErrUnavailable) || errors.Is(err, store.ErrClosed) || errors.Is(err, store.ErrCorrupt)
}

func ignoreClosedNetworkError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

type serviceRepository struct {
	*store.Store
	node      uint16
	fatal     chan error
	fatalOnce sync.Once
}

func (repository *serviceRepository) RecoverWork() (store.RecoveredWork, error) {
	work, err := repository.Store.RecoverWork()
	if err != nil {
		repository.signalFatal(err)
	}
	return work, err
}

func (repository *serviceRepository) signalFatal(err error) {
	if err == nil {
		return
	}
	repository.fatalOnce.Do(func() { repository.fatal <- err })
}

func (repository *serviceRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return repository.node, repository.WorkerEpoch()
}

func (repository *serviceRepository) CurrentFence() model.CoordinatorEpoch {
	work, err := repository.RecoverWork()
	if err != nil {
		return model.CoordinatorEpoch{}
	}
	return work.Fence
}

func (repository *serviceRepository) InstalledAssignment(job model.JobID) (store.InstalledAssignment, bool) {
	work, err := repository.RecoverWork()
	if err != nil {
		return store.InstalledAssignment{}, false
	}
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == job {
			return assignment, true
		}
	}
	return store.InstalledAssignment{}, false
}

func (repository *serviceRepository) DurableTransactionID() (uint64, error) {
	if _, err := repository.RecoverWork(); err != nil {
		return 0, err
	}
	return repository.Recovered().LastSequence, nil
}

type controlResultReplicator struct {
	configuration config.NodeConfig
	authenticator wire.Authenticator
	clock         clock.Clock
	membership    *membership.Authorizer
	repository    *serviceRepository
	clusterID     [16]byte
	timeout       time.Duration
	dial          func(context.Context, string, string) (net.Conn, error)
}

func (replicator *controlResultReplicator) ReplicateRecord(ctx context.Context, record model.ResultRecord, provenance model.ResultCopyProvenance) (ResultReplicationReceipt, error) {
	if ctx == nil {
		return ResultReplicationReceipt{}, errors.New("nil result replication context")
	}
	if err := provenance.Validate(record); err != nil {
		return ResultReplicationReceipt{}, err
	}
	destination, destinationEpoch, err := resultReplicationDestination(provenance)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	chunk, err := buildResultRecordChunk(TransferNormalReplication, record, provenance, destination, destinationEpoch, [16]byte{}, [32]byte{})
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	for {
		current, authorityErr := replicator.currentAuthority(chunk)
		if authorityErr != nil {
			return ResultReplicationReceipt{}, authorityErr
		}
		if !current {
			return ResultReplicationReceipt{}, context.Canceled
		}
		receipt, replicateErr := replicator.replicateChunk(ctx, chunk, destination, destinationEpoch)
		if replicateErr == nil {
			return receipt, nil
		}
		if err := ctx.Err(); err != nil {
			return ResultReplicationReceipt{}, err
		}
		timer := replicator.clock.NewTimer(time.Duration(replicator.configuration.Crane.TupleRetryInterval))
		select {
		case <-timer.C():
		case <-ctx.Done():
			timer.Stop()
			return ResultReplicationReceipt{}, ctx.Err()
		}
	}
}

func (replicator *controlResultReplicator) replicateChunk(ctx context.Context, chunk protocol.ResultRecordChunk, destination uint16, destinationEpoch model.WorkerEpoch) (ResultReplicationReceipt, error) {
	member, ok := activeControlMember(replicator.membership.View(), destination)
	if !ok {
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}
	endpoint, err := memberServiceEndpoint(member.Host, member.BasePort, config.ServiceCraneWorker)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	operationContext, cancel := context.WithTimeout(ctx, replicator.timeout)
	defer cancel()
	connection, err := replicator.dial(operationContext, "tcp", endpoint.String())
	if err != nil {
		return ResultReplicationReceipt{}, fmt.Errorf("dial result replica: %w", err)
	}
	defer connection.Close()
	if err := replicator.membership.AuthorizeTCP(destination, connection.RemoteAddr()); err != nil {
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.WorkerControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &replicator.clusterID
	stream := wire.NewTCPFrameStream(connection, replicator.authenticator, limits, replicator.timeout)

	node, epoch := replicator.repository.LocalIdentity()
	handshake := protocol.WorkerHandshake{NodeID: node, WorkerEpoch: epoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
	handshakeResponse, err := replicator.exchange(operationContext, stream, handshake, destination)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	handshakeAck, ok := handshakeResponse.(protocol.WorkerHandshakeAck)
	if !ok || handshakeAck.NodeID != destination || handshakeAck.WorkerEpoch != destinationEpoch || handshakeAck.ConsensusFingerprint != model.ConsensusFingerprint() || handshakeAck.RegistryFingerprint != model.RegistryFingerprint() {
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}
	if !sameActiveControlMember(member, replicator.membership.View()) {
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}

	transferResponse, err := replicator.exchange(operationContext, stream, chunk, destination)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	ack, ok := transferResponse.(protocol.ResultRecordAck)
	if !ok || protocol.ValidateResultRecordAckCorrelation(chunk, ack) != nil || !sameActiveControlMember(member, replicator.membership.View()) {
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}
	return ResultReplicationReceipt{DestinationNodeID: ack.NodeID, DestinationWorkerEpoch: ack.WorkerEpoch, StreamChecksum: ack.Checksum, StreamLength: ack.TotalLength, CoordinatorEpoch: ack.CoordinatorEpoch}, nil
}

func (replicator *controlResultReplicator) currentAuthority(chunk protocol.ResultRecordChunk) (bool, error) {
	work, err := replicator.repository.RecoverWork()
	if err != nil {
		return false, err
	}
	assignment, ok := controlAssignment(work, chunk.Transfer.JobID)
	if !ok || assignment.SchedulingState != model.Running || assignment.CoordinatorEpoch != work.Fence || chunk.Provenance.CoordinatorEpoch != work.Fence || assignment.Assignment.Revision != chunk.Provenance.AssignmentRevision || assignment.Assignment.Digest != chunk.Provenance.AssignmentDigest || assignment.Topology.Digest() != chunk.Record.SpecificationHash {
		return false, nil
	}
	replica, ok := controlReplica(assignment.Assignment, chunk.Record.SinkTask)
	if !ok || replica != chunk.Provenance.ReplicaSet {
		return false, nil
	}
	destinationNode, destinationEpoch, sourceNode, sourceEpoch, ok := endpointsForRole(replica, chunk.Provenance.DestinationRole)
	localNode, localEpoch := replicator.repository.LocalIdentity()
	return ok && destinationNode == chunk.DestinationNodeID && destinationEpoch == chunk.DestinationWorkerEpoch && sourceNode == localNode && sourceEpoch == localEpoch, nil
}

func (replicator *controlResultReplicator) exchange(ctx context.Context, stream *wire.TCPFrameStream, request protocol.WorkerMessage, destination uint16) (protocol.WorkerMessage, error) {
	payload, err := protocol.MarshalWorkerMessage(request)
	if err != nil {
		return nil, err
	}
	var requestID wire.RequestID
	if _, err := rand.Read(requestID[:]); err != nil {
		return nil, fmt.Errorf("generate Crane control request ID: %w", err)
	}
	node, _ := replicator.repository.LocalIdentity()
	frame := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: request.MessageType(), ClusterID: replicator.clusterID, SenderID: node, RequestID: requestID, TimestampMillis: replicator.clock.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return nil, err
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if response.Header.SenderID != destination || response.Header.RequestID != requestID {
		return nil, ErrTransferUnauthorized
	}
	message, err := protocol.UnmarshalWorkerMessage(response.Header.Message, response.Payload)
	if err != nil {
		return nil, err
	}
	if workerError, ok := message.(protocol.WorkerError); ok {
		return nil, fmt.Errorf("remote worker rejected %d with code %d", workerError.RelatedMessage, workerError.Code)
	}
	return message, nil
}

func resultReplicationDestination(provenance model.ResultCopyProvenance) (uint16, model.WorkerEpoch, error) {
	switch provenance.DestinationRole {
	case model.PrimaryReplica:
		return provenance.ReplicaSet.PrimaryNodeID, provenance.ReplicaSet.PrimaryEpoch, nil
	case model.SecondaryReplica:
		return provenance.ReplicaSet.SecondaryNodeID, provenance.ReplicaSet.SecondaryEpoch, nil
	default:
		return 0, model.WorkerEpoch{}, ErrTransferUnauthorized
	}
}

func memberServiceEndpoint(host string, basePort uint16, service config.Service) (config.Endpoint, error) {
	specification, ok := config.LookupService(service)
	if !ok || uint32(basePort)+uint32(specification.Offset) > 65535 {
		return config.Endpoint{}, errors.New("invalid member service endpoint")
	}
	return config.CanonicalEndpoint(config.Endpoint{Host: host, Port: basePort + specification.Offset})
}

func sameActiveControlMember(want swim.Member, view membership.View) bool {
	current, ok := activeControlMember(view, want.NodeID)
	return ok && current == want
}

func (owner *ControlOwner) localStatus(ctx context.Context) (protocol.WorkerStatus, error) {
	if err := ctx.Err(); err != nil {
		return protocol.WorkerStatus{}, err
	}
	owner.mutations.Lock()
	defer owner.mutations.Unlock()
	work, err := owner.repository.RecoverWork()
	if err != nil {
		return protocol.WorkerStatus{}, err
	}
	transaction, err := owner.repository.DurableTransactionID()
	if err != nil {
		return protocol.WorkerStatus{}, err
	}
	events, last, more, err := owner.repository.PendingEvents(0, protocol.MaxWorkerStatusEvents)
	if err != nil {
		return protocol.WorkerStatus{}, err
	}
	if transaction == 0 || transaction < last {
		return protocol.WorkerStatus{}, ErrControlStaleAssignment
	}
	status := protocol.WorkerStatus{NodeID: owner.localNode, WorkerEpoch: owner.localEpoch, CoordinatorEpoch: work.Fence, StoreTransactionID: transaction, Events: cloneControlEvents(events), LastTransactionID: last, HasMore: more}
	status.Assignments = make([]protocol.InstalledAssignmentStatus, len(work.Assignments))
	for index, assignment := range work.Assignments {
		status.Assignments[index] = protocol.InstalledAssignmentStatus{JobID: assignment.Assignment.JobID, JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SpecificationDigest: assignment.Topology.Digest(), SchedulingState: assignment.SchedulingState}
	}
	sort.Slice(status.Assignments, func(i, j int) bool {
		return string(status.Assignments[i].JobID[:]) < string(status.Assignments[j].JobID[:])
	})
	return status, nil
}

var _ Repository = (*serviceRepository)(nil)
var _ TransferRepository = (*serviceRepository)(nil)
var _ controlRepository = (*serviceRepository)(nil)
