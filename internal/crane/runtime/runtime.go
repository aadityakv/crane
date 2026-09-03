// Package runtime composes one complete Crane node process: SWIM membership,
// the owned membership authorizer, the shared admission gate and replicated
// state machine, the durable worker services, Raft and the coordinator on
// configured voters, and the public +6 control service. Construction is
// side-effect free; Run supervises every composed service and synchronously
// closes the shared admission gate before service cancellation.
package runtime

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/control"
	"github.com/aaditya/cs425mp3/internal/crane/coordinator"
	"github.com/aaditya/cs425mp3/internal/crane/integrationhook"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/crane/worker"
	"github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/raft"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

// ArtifactDirectoryName is the sealed result artifact directory created below
// the node storage root during worker startup.
const ArtifactDirectoryName = "crane-artifacts"

// Dependencies fixes every caller-owned seam of one composed node runtime.
// Only Secret, Clock, and Random are required; every optional seam defaults
// to its production implementation.
type Dependencies struct {
	// Secret is the raw shared cluster secret; New retains an owned copy.
	Secret []byte
	// Clock supplies every timer, timestamp, and replay window.
	Clock clock.Clock
	// Random seeds SWIM peer selection and Raft election jitter.
	Random random.Source
	// SWIMDatagram optionally injects a deterministic SWIM transport; nil
	// asks the SWIM service to bind the real +0/+1 UDP sockets during Run.
	SWIMDatagram transport.Datagram
	// WorkerDatagram optionally injects a deterministic +7 transport; nil
	// binds the real bounded +7 UDP socket lazily during worker startup.
	WorkerDatagram transport.SourceDatagram
	// Dial optionally overrides outbound TCP dialing for the coordinator's
	// +5 worker-control and result-transfer exchanges; nil uses net.Dialer.
	Dial func(context.Context, string, string) (net.Conn, error)
	// SessionEpoch optionally fixes the coordinator's +5 client session
	// epoch; the zero value draws one cryptographically random epoch.
	SessionEpoch model.WorkerEpoch
	// OpenStore optionally overrides the durable worker store opener; nil
	// uses the production store.Open.
	OpenStore func(string, store.Identity, store.Options) (*store.Store, error)
	// Hook optionally observes worker durable boundaries and the real +7
	// send/receive paths. Nil asks integrationhook.LoadFromInheritedFD for
	// the process hook: the production no-op in every ordinary build, and
	// only under the craneintegration tag the seam activated from one
	// inherited test-owned descriptor.
	Hook integrationhook.Hook
}

// Runtime is one completely composed Crane node process. Exactly one
// state.Machine and one admission.Gate are shared by Raft apply, the
// coordinator, the worker services, and the +6 control service.
type Runtime struct {
	// SWIM is the failure-detection membership service every node runs.
	SWIM *swim.Service
	// Membership is the owned authorized projection of SWIM membership.
	Membership *membership.Authorizer
	// Gate is the process-wide shared admission gate.
	Gate *admission.Gate
	// Machine is the process-wide shared replicated Crane state machine.
	Machine *state.Machine
	// Worker composes the durable +5/+7 worker services every node runs.
	Worker *worker.Service
	// Raft is the consensus service; it is nil on configured nonvoters.
	Raft *raft.Service
	// Coordinator is the fenced leader actor; it is nil on nonvoters.
	Coordinator *coordinator.Actor
	// Control is the public +6 control service every node runs.
	Control *control.Service
	// Services is the exact supervised startup set in composition order.
	Services []node.Service

	// Retained composition inputs prove exact shared pointers in tests.
	raftOptions    raft.ServiceOptions
	actorOptions   coordinator.ActorOptions
	controlOptions control.ServiceOptions
	workerOptions  worker.ServiceOptions

	closeTimeout time.Duration
	ready        chan struct{}
	readyOnce    sync.Once
	started      atomic.Bool
}

// New validates the strict configuration (including the compiled consensus
// fingerprint) and composes the complete node runtime without opening a file,
// binding a listener or socket, or starting a goroutine. It consumes exactly
// these NodeConfig fields: NodeID selects the voter/nonvoter role and the
// process identity; ClusterID names the cluster; BindHost, AdvertiseHost, and
// BasePort derive the registry's exact +0..+8 endpoints; Introducer seeds
// SWIM joining; StorageDir locates the SWIM incarnation state, the worker
// store, the sealed artifact directory, and, on voters, the Raft store;
// RaftVoters fixes consensus membership and this node's role in it; Timing,
// Raft, and Crane carry the validated SWIM, Raft, and Crane operational
// fields. The cluster secret is injected through Dependencies.Secret and is
// never read from ClusterSecretFile here.
func New(configuration config.NodeConfig, dependencies Dependencies) (*Runtime, error) {
	if dependencies.Clock == nil || dependencies.Random == nil {
		return nil, errors.New("Crane runtime requires clock and random dependencies")
	}
	if len(dependencies.Secret) < config.MinClusterSecretBytes {
		return nil, fmt.Errorf("Crane runtime requires a cluster secret of at least %d bytes", config.MinClusterSecretBytes)
	}
	owned := configuration
	owned.RaftVoters = append([]config.RaftVoter(nil), configuration.RaftVoters...)
	if err := owned.Validate(); err != nil {
		return nil, fmt.Errorf("validate Crane runtime configuration: %w", err)
	}
	clusterID, err := decodeClusterID(owned.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("decode Crane runtime cluster ID: %w", err)
	}
	secret := append([]byte(nil), dependencies.Secret...)
	authenticator := wire.NewHMACAuthenticator(secret)

	swimService, err := swim.NewService(swim.ServiceOptions{
		Config:        owned,
		Authenticator: authenticator,
		Clock:         dependencies.Clock,
		Random:        dependencies.Random,
		Store:         swim.NewFileIncarnationStore(filepath.Join(owned.StorageDir, swim.IncarnationStateFilename)),
		Datagram:      dependencies.SWIMDatagram,
	})
	if err != nil {
		return nil, fmt.Errorf("construct SWIM service: %w", err)
	}
	authorizer, err := membership.NewAuthorizer(owned, swimService, nil, dependencies.Clock)
	if err != nil {
		return nil, fmt.Errorf("construct membership authorizer: %w", err)
	}
	gate := admission.NewGate()
	machine := state.NewMachine()
	runtime := &Runtime{
		SWIM:         swimService,
		Membership:   authorizer,
		Gate:         gate,
		Machine:      machine,
		closeTimeout: time.Duration(owned.Crane.WorkerControlTimeout),
		ready:        make(chan struct{}),
	}
	_, isVoter := owned.RaftVoterByID(owned.NodeID)
	if isVoter {
		runtime.raftOptions = raft.ServiceOptions{
			Config:                 owned,
			ApplicationFingerprint: model.ConsensusFingerprint(),
			Secret:                 append([]byte(nil), secret...),
			Clock:                  dependencies.Clock,
			Random:                 dependencies.Random,
			StateMachine:           machine,
		}
		raftService, err := raft.NewService(runtime.raftOptions)
		if err != nil {
			return nil, fmt.Errorf("construct Raft service: %w", err)
		}
		runtime.Raft = raftService
	}

	workerDatagram := dependencies.WorkerDatagram
	if workerDatagram == nil {
		tupleBind, err := owned.BindEndpoint(config.ServiceCraneTupleACK)
		if err != nil {
			return nil, fmt.Errorf("derive Crane +7 bind endpoint: %w", err)
		}
		workerDatagram = newLazyTupleDatagram(tupleBind)
	}
	openStore := dependencies.OpenStore
	if openStore == nil {
		openStore = store.Open
	}
	hook := dependencies.Hook
	if hook == nil {
		hook = integrationhook.LoadFromInheritedFD()
	}
	runtime.workerOptions = worker.ServiceOptions{
		Config:            owned,
		Authenticator:     authenticator,
		Clock:             dependencies.Clock,
		Membership:        authorizer,
		Gate:              gate,
		OpenStore:         openStore,
		Datagram:          workerDatagram,
		ArtifactDirectory: filepath.Join(owned.StorageDir, ArtifactDirectoryName),
		Hook:              hook,
	}
	workerService, err := worker.NewService(runtime.workerOptions)
	if err != nil {
		return nil, fmt.Errorf("construct Crane worker service: %w", err)
	}
	runtime.Worker = workerService

	sessionEpoch := dependencies.SessionEpoch
	if sessionEpoch == (model.WorkerEpoch{}) {
		if _, err := cryptorand.Read(sessionEpoch[:]); err != nil {
			return nil, fmt.Errorf("generate coordinator session epoch: %w", err)
		}
		if sessionEpoch == (model.WorkerEpoch{}) {
			sessionEpoch[0] = 1
		}
	}
	workers, err := coordinator.NewDialWorkerClient(coordinator.DialWorkerClientOptions{
		ClusterID:     clusterID,
		NodeID:        owned.NodeID,
		SessionEpoch:  sessionEpoch,
		Authenticator: authenticator,
		Clock:         dependencies.Clock,
		Membership:    authorizer,
		Timeout:       time.Duration(owned.Crane.WorkerControlTimeout),
		Dial:          dependencies.Dial,
	})
	if err != nil {
		return nil, fmt.Errorf("construct coordinator worker client: %w", err)
	}

	wakeCoordinator := func() {}
	if isVoter {
		runtime.actorOptions = coordinator.ActorOptions{
			NodeID:             owned.NodeID,
			Raft:               serviceRaft{service: runtime.Raft},
			Machine:            machine,
			WorkerReady:        workerService.Ready(),
			Membership:         coordinator.NewAuthorizerMembership(authorizer),
			Workers:            workers,
			Clock:              dependencies.Clock,
			Nonces:             coordinator.CryptoNonceSource{},
			Gate:               gate,
			Results:            workers,
			FailureGracePeriod: time.Duration(owned.Crane.FailureGracePeriod),
		}
		actor, err := coordinator.NewActor(runtime.actorOptions)
		if err != nil {
			return nil, fmt.Errorf("construct Crane coordinator: %w", err)
		}
		runtime.Coordinator = actor
		wakeCoordinator = actor.Wake
	}

	runtime.controlOptions = control.ServiceOptions{
		Config:          owned,
		Authenticator:   authenticator,
		Clock:           dependencies.Clock,
		Membership:      authorizer,
		Machine:         machine,
		Gate:            gate,
		Results:         &control.QueryEngine{Machine: machine, Fetcher: &artifactFetcher{client: workers}},
		WakeCoordinator: wakeCoordinator,
	}
	if isVoter {
		runtime.controlOptions.Raft = runtime.Raft
	}
	controlService, err := control.NewService(runtime.controlOptions)
	if err != nil {
		return nil, fmt.Errorf("construct Crane control service: %w", err)
	}
	runtime.Control = controlService

	runtime.Services = []node.Service{swimService, authorizer}
	if isVoter {
		runtime.Services = append(runtime.Services, runtime.Raft)
	}
	runtime.Services = append(runtime.Services, workerService)
	if isVoter {
		runtime.Services = append(runtime.Services, runtime.Coordinator)
	}
	runtime.Services = append(runtime.Services, controlService)
	return runtime, nil
}

// Name returns the stable supervisor registration name of the composite.
func (runtime *Runtime) Name() string { return "crane-runtime" }

// Ready closes once every composed service has causally reported readiness.
func (runtime *Runtime) Ready() <-chan struct{} { return runtime.ready }

// Run supervises every composed service until parent cancellation or the
// first service failure. Parent cancellation synchronously closes the shared
// admission gate before any composed service observes cancellation.
func (runtime *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run Crane runtime: nil context")
	}
	if !runtime.started.CompareAndSwap(false, true) {
		return errors.New("Crane runtime Run called more than once")
	}
	return superviseWithGate(ctx, runtime.Gate, runtime.Services, runtime.closeTimeout, func() {
		runtime.readyOnce.Do(func() { close(runtime.ready) })
	})
}

// superviseWithGate runs services under one supervisor whose cancellation is
// decoupled from the parent: when the parent context ends, the shared gate is
// closed (bounded by closeTimeout for the drain) strictly before the services
// are canceled, so no admitted mutation or source can race shutdown.
func superviseWithGate(parent context.Context, gate *admission.Gate, services []node.Service, closeTimeout time.Duration, onReady func()) error {
	if parent == nil || gate == nil || len(services) == 0 {
		return errors.New("supervise Crane runtime: incomplete inputs")
	}
	if closeTimeout <= 0 {
		closeTimeout = time.Second
	}
	serviceCtx, cancel := context.WithCancel(context.WithoutCancel(parent))
	defer cancel()
	stopWatch := context.AfterFunc(parent, func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = gate.CloseAndWait(closeCtx)
		closeCancel()
		cancel()
	})
	defer stopWatch()

	supervisor := node.NewSupervisor(services...)
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-supervisor.Ready():
			if onReady != nil {
				onReady()
			}
		case <-watchDone:
		}
	}()

	runErr := supervisor.Run(serviceCtx)
	finalCtx, finalCancel := context.WithTimeout(context.Background(), closeTimeout)
	finalErr := gate.CloseAndWait(finalCtx)
	finalCancel()
	return errors.Join(runErr, finalErr)
}

// serviceRaft adapts the concrete Raft service to the coordinator's seam.
type serviceRaft struct {
	service *raft.Service
}

// Ready reports Raft recovery and replay completion.
func (adapter serviceRaft) Ready() <-chan struct{} { return adapter.service.Ready() }

// Barrier commits and applies one current-term application fence.
func (adapter serviceRaft) Barrier(ctx context.Context) (uint64, error) {
	return adapter.service.Barrier(ctx)
}

// Propose submits one command and waits for its exact apply result.
func (adapter serviceRaft) Propose(ctx context.Context, command []byte) (raft.ProposalResult, error) {
	return adapter.service.Propose(ctx, command)
}

// SubscribeLeadership opens one bounded leadership subscription.
func (adapter serviceRaft) SubscribeLeadership(ctx context.Context, capacity int) (coordinator.LeadershipSubscription, error) {
	subscription, err := adapter.service.SubscribeLeadership(ctx, capacity)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

// resultFetchClient is the bounded +5 fetch surface the production result
// fetcher consumes; *coordinator.DialWorkerClient satisfies it directly.
type resultFetchClient interface {
	// Fetch performs one authenticated leader result-fetch exchange.
	Fetch(context.Context, uint16, protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error)
}

// artifactFetcher is the production control.ResultFetcher: it assembles one
// complete sealed artifact from bounded authenticated +5 fetch chunks with
// exact identity, contiguity, and checksum verification, then streams its
// canonical records one at a time.
type artifactFetcher struct {
	client resultFetchClient
}

// OpenPartition fetches and verifies the complete requested replica copy
// before returning a bounded canonical record stream over it.
func (fetcher *artifactFetcher) OpenPartition(ctx context.Context, request protocol.ResultFetchRequest) (control.RecordStream, error) {
	if ctx == nil {
		return nil, errors.New("open Crane result partition: nil context")
	}
	artifact := request.Artifact
	if artifact.TotalLength > model.LimitsV1().MaxResultRecordsBytesPerJob {
		return nil, fmt.Errorf("result artifact length %d exceeds the job bound", artifact.TotalLength)
	}
	stream := make([]byte, 0, artifact.TotalLength)
	offset := uint64(0)
	// Every accepted non-final chunk advances the offset, so TotalLength+2
	// iterations bound the assembly even at one byte of progress per chunk.
	maxChunks := artifact.TotalLength + 2
	for chunkIndex := uint64(0); chunkIndex < maxChunks; chunkIndex++ {
		chunkRequest := request
		chunkRequest.Offset = offset
		chunk, err := fetcher.client.Fetch(ctx, request.ReplicaNodeID, chunkRequest)
		if err != nil {
			return nil, err
		}
		if chunk.SourceNodeID != request.ReplicaNodeID || chunk.SourceWorkerEpoch != request.ReplicaWorkerEpoch ||
			chunk.CoordinatorEpoch != request.CoordinatorEpoch {
			return nil, errors.New("result fetch chunk source mismatch")
		}
		if chunk.Artifact != artifact {
			return nil, errors.New("result fetch chunk names a foreign artifact")
		}
		if chunk.Transfer.JobID != artifact.JobID || chunk.Transfer.TotalLength != artifact.TotalLength ||
			chunk.Transfer.Checksum != artifact.Checksum || chunk.Transfer.Offset != offset ||
			uint64(len(chunk.Transfer.Data)) > uint64(protocol.MaxTransferChunkBytes) {
			return nil, errors.New("result fetch chunk correlation mismatch")
		}
		end := chunk.Transfer.Offset + uint64(len(chunk.Transfer.Data))
		if end > artifact.TotalLength || chunk.Transfer.Final != (end == artifact.TotalLength) {
			return nil, errors.New("result fetch chunk breaks stream contiguity")
		}
		if !chunk.Transfer.Final && len(chunk.Transfer.Data) == 0 {
			return nil, errors.New("result fetch chunk makes no progress")
		}
		stream = append(stream, chunk.Transfer.Data...)
		offset = end
		if chunk.Transfer.Final {
			if uint64(len(stream)) != artifact.TotalLength || sha256.Sum256(stream) != artifact.Checksum {
				return nil, errors.New("assembled result stream fails checksum verification")
			}
			return &artifactRecordStream{artifact: artifact, data: stream}, nil
		}
	}
	return nil, errors.New("result fetch exceeded the bounded chunk budget")
}

// artifactRecordStream parses one verified sealed artifact stream into
// canonical records, enforcing per-record identity, strict TupleID order, and
// the exact declared record count.
type artifactRecordStream struct {
	artifact protocol.ResultArtifact
	data     []byte
	offset   int
	yielded  uint64
	previous model.TupleID
}

// Next returns the next canonical record or io.EOF after the exact count.
func (stream *artifactRecordStream) Next(ctx context.Context) (model.ResultRecord, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultRecord{}, err
	}
	if stream.offset == len(stream.data) {
		if stream.yielded != stream.artifact.RecordCount {
			return model.ResultRecord{}, fmt.Errorf("sealed artifact yielded %d records, declared %d", stream.yielded, stream.artifact.RecordCount)
		}
		return model.ResultRecord{}, io.EOF
	}
	if len(stream.data)-stream.offset < 4 {
		return model.ResultRecord{}, errors.New("sealed artifact entry prefix is truncated")
	}
	length := int(stream.data[stream.offset])<<24 | int(stream.data[stream.offset+1])<<16 |
		int(stream.data[stream.offset+2])<<8 | int(stream.data[stream.offset+3])
	if length <= 0 || length > len(stream.data)-stream.offset-4 || length+4 > int(protocol.MaxEncodedResultRecordBytes) {
		return model.ResultRecord{}, errors.New("sealed artifact entry length is out of bounds")
	}
	record, err := model.UnmarshalResultRecord(stream.data[stream.offset+4 : stream.offset+4+length])
	if err != nil {
		return model.ResultRecord{}, err
	}
	if record.TupleID.JobID != stream.artifact.JobID || record.SinkTask != stream.artifact.SinkTask ||
		record.SpecificationHash != stream.artifact.SpecificationHash {
		return model.ResultRecord{}, errors.New("sealed artifact record does not belong to the partition")
	}
	if stream.yielded > 0 && !tupleIDLess(stream.previous, record.TupleID) {
		return model.ResultRecord{}, errors.New("sealed artifact records violate canonical TupleID order")
	}
	if stream.yielded == stream.artifact.RecordCount {
		return model.ResultRecord{}, errors.New("sealed artifact holds more records than declared")
	}
	stream.offset += 4 + length
	stream.yielded++
	stream.previous = record.TupleID
	return record, nil
}

// Close releases the assembled stream buffer.
func (stream *artifactRecordStream) Close() error {
	stream.data = nil
	stream.offset = 0
	return nil
}

// tupleIDLess orders tuples by their complete canonical identity.
func tupleIDLess(left, right model.TupleID) bool {
	if left.JobID != right.JobID {
		return string(left.JobID[:]) < string(right.JobID[:])
	}
	if left.SourceTask != right.SourceTask {
		if left.SourceTask.StageID != right.SourceTask.StageID {
			return left.SourceTask.StageID < right.SourceTask.StageID
		}
		return left.SourceTask.Partition < right.SourceTask.Partition
	}
	if left.SourceSequence != right.SourceSequence {
		return left.SourceSequence < right.SourceSequence
	}
	return string(left.PathDigest[:]) < string(right.PathDigest[:])
}

// lazyTupleDatagram binds the production bounded +7 UDP socket on first use,
// keeping runtime construction side-effect free.
type lazyTupleDatagram struct {
	bind config.Endpoint

	mu       sync.Mutex
	datagram *transport.UDPDatagram
	closed   bool
}

// newLazyTupleDatagram retains the bind endpoint without opening a socket.
func newLazyTupleDatagram(bind config.Endpoint) *lazyTupleDatagram {
	return &lazyTupleDatagram{bind: bind}
}

// active returns the bound socket, binding it exactly once on first use.
func (lazy *lazyTupleDatagram) active() (*transport.UDPDatagram, error) {
	lazy.mu.Lock()
	defer lazy.mu.Unlock()
	if lazy.closed {
		return nil, transport.ErrDatagramClosed
	}
	if lazy.datagram == nil {
		datagram, err := transport.ListenUDPBounded(wire.MaxCraneDatagramBytesV1, lazy.bind)
		if err != nil {
			return nil, fmt.Errorf("bind Crane +7 datagram: %w", err)
		}
		lazy.datagram = datagram
	}
	return lazy.datagram, nil
}

// Send transmits one datagram from the lazily bound +7 socket.
func (lazy *lazyTupleDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	datagram, err := lazy.active()
	if err != nil {
		return err
	}
	return datagram.Send(ctx, destination, payload)
}

// SendFrom transmits one datagram from the selected bound local endpoint.
func (lazy *lazyTupleDatagram) SendFrom(ctx context.Context, from, destination config.Endpoint, payload []byte) error {
	datagram, err := lazy.active()
	if err != nil {
		return err
	}
	return datagram.SendFrom(ctx, from, destination, payload)
}

// Receive returns the next datagram from the lazily bound +7 socket.
func (lazy *lazyTupleDatagram) Receive(ctx context.Context) (transport.Packet, error) {
	datagram, err := lazy.active()
	if err != nil {
		return transport.Packet{}, err
	}
	return datagram.Receive(ctx)
}

// Close closes the socket when bound and refuses any later use.
func (lazy *lazyTupleDatagram) Close() error {
	lazy.mu.Lock()
	defer lazy.mu.Unlock()
	if lazy.closed {
		return nil
	}
	lazy.closed = true
	if lazy.datagram == nil {
		return nil
	}
	return lazy.datagram.Close()
}

// decodeClusterID parses the canonical UUID configuration form into the
// exact 16-byte wire cluster identity.
func decodeClusterID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("invalid cluster UUID")
	}
	var result [16]byte
	copy(result[:], decoded)
	return result, nil
}

var _ node.Service = (*Runtime)(nil)
var _ transport.SourceDatagram = (*lazyTupleDatagram)(nil)
var _ control.ResultFetcher = (*artifactFetcher)(nil)
var _ resultFetchClient = (*coordinator.DialWorkerClient)(nil)
