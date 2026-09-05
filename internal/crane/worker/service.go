package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/transport"
	"github.com/aadityakv/crane/internal/wire"
)

// WorkerStoreDirectory is the data-directory subdirectory that holds the worker's durable store.
const WorkerStoreDirectory = "crane-worker"

// ErrServiceNotReady reports a request made before the worker finished recovery and bound its listeners.
var ErrServiceNotReady = errors.New("crane worker service is not ready")

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
	// Hook optionally observes durable store boundaries and the real +5
	// send/receive paths; nil selects the production no-op hook.
	Hook integrationhook.Hook
}

// Service composes one durable worker owner, one +3 listener, and exactly one
// shared +5 sender/receiver endpoint.
type Service struct {
	configuration     config.NodeConfig
	authenticator     wire.Authenticator
	clock             clock.Clock
	membership        *membership.Authorizer
	gate              *admission.Gate
	openStore         func(string, store.Identity, store.Options) (*store.Store, error)
	datagram          transport.SourceDatagram
	artifactDirectory string
	hook              integrationhook.Hook
	clusterID         [16]byte
	controlBind       config.Endpoint
	listen            func(string, string) (net.Listener, error)

	store        *store.Store
	repository   *serviceRepository
	engine       *Engine
	endpoint     *TupleEndpoint
	tuple        *TupleService
	transfer     *TransferOwner
	control      *ControlOwner
	repairDriver *RepairDriver

	ready      chan struct{}
	started    atomic.Bool
	readyState atomic.Bool
}

// NewService validates and retains the complete worker composition without
// opening a store, binding a listener, activating a datagram, or starting work.
func NewService(options ServiceOptions) (*Service, error) {
	if options.Authenticator == nil || options.Clock == nil || options.Membership == nil || options.Gate == nil || options.OpenStore == nil || options.Datagram == nil {
		return nil, errors.New("crane worker service requires authenticator, clock, membership, gate, store opener, and datagram")
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
	hook := options.Hook
	if hook == nil {
		hook = integrationhook.Noop{}
	}
	return &Service{
		configuration: configuration, authenticator: options.Authenticator, clock: options.Clock,
		membership: options.Membership, gate: options.Gate, openStore: options.OpenStore,
		datagram: options.Datagram, artifactDirectory: options.ArtifactDirectory, hook: hook,
		clusterID: clusterID, controlBind: controlBind,
		listen: net.Listen, ready: make(chan struct{}),
	}, nil
}

// Name reports the service name used in logs and supervision.
func (*Service) Name() string { return "crane-worker" }

// Ready returns a channel that closes once recovery is complete and both network services are bound.
func (service *Service) Ready() <-chan struct{} { return service.ready }

// Run recovers durable identity before binding either network service, then
// joins every owned worker and closes the Store last.
func (service *Service) Run(ctx context.Context) (runErr error) {
	if ctx == nil {
		return errors.New("run Crane worker service: nil context")
	}
	if !service.started.CompareAndSwap(false, true) {
		return errors.New("crane worker service Run called more than once")
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
		store.Options{MaxBytes: service.configuration.Crane.MaxWorkerStoreBytes, Hook: service.hook},
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

	repository := &serviceRepository{Store: workerStore, node: service.configuration.NodeID, fatal: make(chan error, 1), controlWait: time.Duration(service.configuration.Crane.WorkerControlTimeout)}
	service.repository = repository
	endpoint, err := NewTupleEndpoint(TupleEndpointOptions{Config: service.configuration, Authenticator: service.authenticator, Clock: service.clock, Membership: service.membership, Datagram: service.datagram, Hook: service.hook})
	if err != nil {
		return err
	}
	service.endpoint = endpoint
	replicator := &controlResultReplicator{configuration: service.configuration, authenticator: service.authenticator, clock: service.clock, membership: service.membership, repository: repository, clusterID: service.clusterID, timeout: time.Duration(service.configuration.Crane.WorkerControlTimeout), dial: (&net.Dialer{}).DialContext}
	defer replicator.Close()
	engine, err := NewEngine(EngineOptions{Repository: repository, Sender: endpoint, Replicator: replicator, Gate: service.gate, Clock: service.clock, MaxExecutors: service.configuration.Crane.WorkerSlots, AcceptedRetryInterval: time.Duration(service.configuration.Crane.TupleRetryInterval), CompletedRetryInterval: time.Duration(service.configuration.Crane.TupleCompletionRetryInterval)})
	if err != nil {
		return err
	}
	service.engine = engine
	// The repository publishes the engine's immutable installed view to the
	// transfer owner; wired here, before any owner goroutine starts, so no
	// transfer path can observe a partially constructed composition.
	repository.engine = engine
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
	repairClient := newDialRepairSourceClient(dialRepairSourceClientOptions{ClusterID: service.clusterID, Authenticator: service.authenticator, Clock: service.clock, Membership: service.membership, Repository: repository, Timeout: time.Duration(service.configuration.Crane.WorkerControlTimeout), Dial: (&net.Dialer{}).DialContext})
	defer repairClient.Close()
	repairDriver, err := NewRepairDriver(RepairDriverOptions{Repository: repository, Transfer: transfer, Client: repairClient, Clock: service.clock, RetryInterval: time.Duration(service.configuration.Crane.TupleRetryInterval), MaxRetryInterval: time.Duration(service.configuration.Crane.TupleCompletionRetryInterval)})
	if err != nil {
		return err
	}
	service.repairDriver = repairDriver
	control, err := NewControlOwner(ControlOptions{Config: service.configuration, ClusterID: service.clusterID, Repository: repository, Engine: engine, Transfer: transfer, RepairScheduler: repairDriver, Gate: service.gate, Membership: service.membership, Clock: service.clock})
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
	results := make(chan serviceRunResult, 4)
	controlReady := make(chan struct{})
	controlDispatch := make(chan struct{})
	go func() { results <- serviceRunResult{name: "engine", err: engine.Run(runContext)} }()
	go func() { results <- serviceRunResult{name: "tuple", err: tuple.Run(runContext)} }()
	go func() { results <- serviceRunResult{name: "repair", err: service.repairDriver.Run(runContext)} }()
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
			runErr = fmt.Errorf("crane worker durable authority: %w", fatalErr)
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
			runErr = fmt.Errorf("crane worker durable authority: %w", fatalErr)
		}
	}

	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), time.Duration(service.configuration.Crane.WorkerControlTimeout))
	gateErr := service.gate.CloseAndWait(cleanupContext)
	cleanupCancel()
	cancel()
	listenerErr := listener.Close()
	controlErr := control.Close()
	for completedOwners < 4 {
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
	case errors.Is(err, wire.ErrReplayCacheFull):
		// Replay-budget exhaustion is transient admission capacity, never
		// deterministic malformation: the sender retries rather than
		// terminating grants or wedging activation.
		code, retryable, detail = protocol.WorkerErrorCapacity, true, "replay admission budget exhausted"
	case errors.Is(err, wire.ErrTimestamp):
		// A frame timestamp outside the replay window is likewise transient
		// (a fresh transmission carries a fresh timestamp) but names its own
		// cause so a skewed sender is not told the budget is exhausted.
		code, retryable, detail = protocol.WorkerErrorCapacity, true, "request timestamp outside the replay window"
	case errors.Is(err, ErrControlCapacity), errors.Is(err, ErrTransferCapacity), errors.Is(err, store.ErrCapacity):
		code, retryable, detail = protocol.WorkerErrorCapacity, true, "worker capacity exhausted"
	case errors.Is(err, store.ErrBusy):
		code, retryable, detail = protocol.WorkerErrorUnavailable, true, "worker store busy"
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
	// controlWait bounds control-path store reads (RecoverWorkBounded,
	// RecoverWorkViewWithin, and DurableTransactionID's sequence read).
	controlWait time.Duration
	// engine is wired by Service.Run immediately after engine construction,
	// before any owner goroutine starts; it publishes the immutable
	// installed view the transfer owner validates against.
	engine *Engine
}

// RecoverWorkBounded reads the recovered work with the control wait bound; a
// store.ErrBusy result is a retryable control rejection, never a poison.
func (repository *serviceRepository) RecoverWorkBounded() (store.RecoveredWork, error) {
	work, err := repository.Store.RecoverWorkWithin(repository.controlWait)
	if err != nil && !errors.Is(err, store.ErrBusy) {
		repository.signalFatal(err)
	}
	return work, err
}

// borrowedCallbackError marks errors raised by the borrowed-read callback —
// control-domain refusals (stale epoch, stale assignment) — so the service
// repository signals fatal only for store failures, exactly like the cloned
// bounded read did.
type borrowedCallbackError struct{ err error }

func (failure borrowedCallbackError) Error() string { return failure.err.Error() }
func (failure borrowedCallbackError) Unwrap() error { return failure.err }

// RecoverWorkViewWithin lends the callback the borrowed recovered-work view
// with the control wait bound, without cloning it; a store.ErrBusy result is
// a retryable control rejection, never a poison. Only store failures signal
// fatal: errors the callback itself returns belong to the caller.
func (repository *serviceRepository) RecoverWorkViewWithin(borrow func(*store.RecoveredWork) error) error {
	err := repository.Store.RecoverWorkViewWithin(repository.controlWait, func(work *store.RecoveredWork) error {
		if err := borrow(work); err != nil {
			return borrowedCallbackError{err}
		}
		return nil
	})
	var callback borrowedCallbackError
	if errors.As(err, &callback) {
		return callback.err
	}
	if err != nil && !errors.Is(err, store.ErrBusy) {
		repository.signalFatal(err)
	}
	return err
}

// RecoverWork reads the worker's durable identity and assignments, signalling a fatal error on failure.
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

// LocalIdentity reports this worker's node ID and current epoch.
func (repository *serviceRepository) LocalIdentity() (uint16, model.WorkerEpoch) {
	return repository.node, repository.WorkerEpoch()
}

// CurrentFence reports the coordinator epoch the store is fenced to, or zero when the store cannot be read.
func (repository *serviceRepository) CurrentFence() model.CoordinatorEpoch {
	work, err := repository.RecoverWork()
	if err != nil {
		return model.CoordinatorEpoch{}
	}
	return work.Fence
}

// InstalledAssignment looks up the assignment currently installed for a job.
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

// InstalledView serves the engine's immutable installed-assignment/fence
// snapshot for transfer-path authority validation. A wired engine that has
// not yet published a view fails closed with a nil map, exactly like a
// repository read failure. Without a wired engine (the composition tests'
// store-backed repositories) it falls back to one durable read and serves the
// same authority the store would.
func (repository *serviceRepository) InstalledView() (map[model.JobID]store.InstalledAssignment, model.CoordinatorEpoch) {
	if repository.engine == nil {
		work, err := repository.RecoverWork()
		if err != nil {
			return nil, model.CoordinatorEpoch{}
		}
		view := make(map[model.JobID]store.InstalledAssignment, len(work.Assignments))
		for _, assignment := range work.Assignments {
			view[assignment.Assignment.JobID] = assignment
		}
		return view, work.Fence
	}
	return repository.engine.InstalledView()
}

// DurableTransactionID reports the last store transaction that is durable on disk.
func (repository *serviceRepository) DurableTransactionID() (uint64, error) {
	transaction, err := repository.Store.DurableSequenceWithin(repository.controlWait)
	if err != nil && !errors.Is(err, store.ErrBusy) {
		repository.signalFatal(err)
	}
	return transaction, err
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

	// sessions caches one authenticated +3 session per destination
	// incarnation (Task 24 defect #8, the +3 half): a session is dialed once,
	// reused for every record and retry to that destination, and dropped on
	// any failure. The repair driver's pull client shares the same cache
	// type so repair pulls honor the same one-identity-per-record budget.
	once     sync.Once
	sessions *controlSessionCache
}

// replicationRetryCap bounds the doubling retry backoff of one record at a
// multiple of the configured tuple retry interval.
const replicationRetryCap = 32

// ReplicateRecord streams one result record to the replica named by its provenance, retrying transient rejections with backoff.
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
	interval := time.Duration(replicator.configuration.Crane.TupleRetryInterval)
	delay := interval
	for {
		current, authorityErr := replicator.currentAuthority(chunk)
		if authorityErr != nil {
			return ResultReplicationReceipt{}, authorityErr
		}
		if !current {
			tracef("replicator node=%d: record %x for sink %d.%d no longer current (rev=%d role=%d)", replicator.localNodeID(), chunk.Record.TupleID.PathDigest[:4], chunk.Record.SinkTask.StageID, chunk.Record.SinkTask.Partition, chunk.Provenance.AssignmentRevision, chunk.Provenance.DestinationRole)
			return ResultReplicationReceipt{}, context.Canceled
		}
		receipt, replicateErr := replicator.replicateChunk(ctx, chunk, destination, destinationEpoch)
		if replicateErr == nil {
			return receipt, nil
		}
		tracef("replicator node=%d: record seq=%d to node %d failed: %v", replicator.localNodeID(), chunk.Record.TupleID.SourceSequence, destination, replicateErr)
		if err := ctx.Err(); err != nil {
			return ResultReplicationReceipt{}, err
		}
		// Doubling backoff bounds the request rate against a destination that
		// keeps refusing (it may not hold the current install yet) so its
		// bounded per-peer replay cache is never exhausted by retries.
		timer := replicator.clock.NewTimer(delay)
		select {
		case <-timer.C():
		case <-ctx.Done():
			timer.Stop()
			return ResultReplicationReceipt{}, ctx.Err()
		}
		if delay < interval*replicationRetryCap {
			delay *= 2
			if delay > interval*replicationRetryCap {
				delay = interval * replicationRetryCap
			}
		}
	}
}

// cache returns the replicator's session cache, built once from its dial
// identity so literal construction stays valid.
func (replicator *controlResultReplicator) cache() *controlSessionCache {
	replicator.once.Do(func() {
		replicator.sessions = newControlSessionCache(controlSessionCacheOptions{ClusterID: replicator.clusterID, Authenticator: replicator.authenticator, Clock: replicator.clock, Membership: replicator.membership, Identity: replicator.repository, Timeout: replicator.timeout, Dial: replicator.dial})
	})
	return replicator.sessions
}

// Close closes every cached session; later replication attempts fail closed.
func (replicator *controlResultReplicator) Close() {
	replicator.cache().Close()
}

func (replicator *controlResultReplicator) replicateChunk(ctx context.Context, chunk protocol.ResultRecordChunk, destination uint16, destinationEpoch model.WorkerEpoch) (ResultReplicationReceipt, error) {
	sessions := replicator.cache()
	session, err := sessions.session(ctx, destination, destinationEpoch)
	if err != nil {
		return ResultReplicationReceipt{}, err
	}
	operationContext, cancel := context.WithTimeout(ctx, replicator.timeout)
	defer cancel()
	transferResponse, err := sessions.exchange(operationContext, session.stream, chunk, destination)
	if err != nil {
		sessions.dropSession(destination, destinationEpoch, session)
		return ResultReplicationReceipt{}, err
	}
	ack, ok := transferResponse.(protocol.ResultRecordAck)
	if !ok || protocol.ValidateResultRecordAckCorrelation(chunk, ack) != nil || !sameActiveControlMember(session.member, replicator.membership.View()) {
		sessions.dropSession(destination, destinationEpoch, session)
		return ResultReplicationReceipt{}, ErrTransferUnauthorized
	}
	return ResultReplicationReceipt{DestinationNodeID: ack.NodeID, DestinationWorkerEpoch: ack.WorkerEpoch, StreamChecksum: ack.Checksum, StreamLength: ack.TotalLength, CoordinatorEpoch: ack.CoordinatorEpoch}, nil
}

func (replicator *controlResultReplicator) localNodeID() uint16 {
	node, _ := replicator.repository.LocalIdentity()
	return node
}

func (replicator *controlResultReplicator) currentAuthority(chunk protocol.ResultRecordChunk) (bool, error) {
	work, err := replicator.repository.RecoverWork()
	if err != nil {
		return false, err
	}
	assignment, ok := controlAssignment(work, chunk.Transfer.JobID)
	if !ok || !replicationAdmitted(assignment.SchedulingState) || assignment.CoordinatorEpoch != work.Fence || chunk.Provenance.CoordinatorEpoch != work.Fence || assignment.Assignment.Revision != chunk.Provenance.AssignmentRevision || assignment.Assignment.Digest != chunk.Provenance.AssignmentDigest || assignment.Topology.Digest() != chunk.Record.SpecificationHash {
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
	// borrowed reads: the durable-sequence read is now bounded, so a held
	// store surfaces ErrBusy here transiently; the wire maps it to a
	// retryable WorkerErrorUnavailable and the peer retries.
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
