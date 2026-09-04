package control

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/admission"
	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/raft"
	"github.com/aadityakv/crane/internal/wire"
)

const (
	// DefaultMaxControlConnections bounds concurrently served +6 connections.
	DefaultMaxControlConnections = 128
	// DefaultMaxControlConnectionsPerPeer bounds concurrently served +6
	// connections from one peer source address, so a single peer cannot
	// consume the shared global connection budget.
	DefaultMaxControlConnectionsPerPeer = 4
	// DefaultMaxControlReplayEntries bounds retained request-replay identities.
	DefaultMaxControlReplayEntries = 65536
	// DefaultMaxControlReplayEntriesPerPeer bounds per-sender replay identities.
	DefaultMaxControlReplayEntriesPerPeer = 8192
)

// RaftAPI is the bounded consensus surface the public control service
// requires: readiness, a linearizing barrier, and canonical command proposal.
// *raft.Node satisfies it directly; leadership is proven per request by the
// barrier or proposal, whose typed *raft.NotLeaderError carries the checked
// redirect hint.
type RaftAPI interface {
	// Ready closes once local Raft recovery and replay have completed.
	Ready() <-chan struct{}
	// Barrier commits and applies one current-term no-op application fence.
	Barrier(context.Context) (uint64, error)
	// Propose submits one canonical replicated command and waits for its
	// exact apply result.
	Propose(context.Context, []byte) (raft.ProposalResult, error)
}

// membershipAuthorizer is the request-time admission surface the service
// consumes. *membership.Authorizer satisfies it directly.
type membershipAuthorizer interface {
	// Ready closes once an exact authorized membership snapshot is installed.
	Ready() <-chan struct{}
	// View returns the current owned monotonic membership view.
	View() membership.View
	// AuthorizeTCP accepts only the current active member's source IP.
	AuthorizeTCP(uint16, net.Addr) error
}

// ServiceOptions fixes every caller-owned dependency of one +6 public control
// service. NewService retains these exact values but opens no resource.
type ServiceOptions struct {
	// Config is the complete validated local node configuration.
	Config config.NodeConfig
	// Authenticator signs and verifies every +6 frame with the cluster secret.
	Authenticator wire.Authenticator
	// Clock supplies timestamps and replay-window time.
	Clock clock.Clock
	// Membership provides request-time sender admission.
	Membership *membership.Authorizer
	// Raft provides readiness, barriers, and proposals; it may be nil only on
	// a configured nonvoter, which redirects every request to the voters.
	Raft RaftAPI
	// Machine is the local replicated Crane state machine.
	Machine *state.Machine
	// Gate is the caller-owned shared process admission gate.
	Gate *admission.Gate
	// Results serves linearizable global result pages over the same Machine.
	Results *QueryEngine
	// WakeCoordinator is the best-effort coordinator latency hint sent only
	// after an applied mutation success; progress never depends on it.
	WakeCoordinator func()
}

// Service owns one bounded authenticated +6 TCP listener serving exactly one
// public control request and one correlated response per connection.
type Service struct {
	configuration  config.NodeConfig
	authenticator  wire.Authenticator
	clock          clock.Clock
	membership     membershipAuthorizer
	raft           RaftAPI
	machine        *state.Machine
	gate           *admission.Gate
	results        *QueryEngine
	wake           func()
	clusterID      [16]byte
	bind           config.Endpoint
	voter          bool
	voterEndpoints []string
	listen         func(string, string) (net.Listener, error)
	replay         *serviceReplay
	maxConnections int
	// peerMu guards peerConnections for the per-peer connection bound.
	peerMu                sync.Mutex
	peerConnections       map[string]int
	maxConnectionsPerPeer int
	maxCommand            atomic.Uint64
	timeout               time.Duration

	ready   chan struct{}
	started atomic.Bool
}

// NewService validates and retains the complete +6 composition without
// binding a listener, reading state, or starting any goroutine.
func NewService(options ServiceOptions) (*Service, error) {
	if options.Authenticator == nil || options.Clock == nil || options.Membership == nil || options.Machine == nil || options.Gate == nil || options.Results == nil || options.WakeCoordinator == nil {
		return nil, errors.New("crane control service requires authenticator, clock, membership, machine, gate, results, and coordinator wake")
	}
	if options.Results.Machine != options.Machine {
		return nil, errors.New("crane control results engine must share the exact service state machine")
	}
	configuration := cloneControlNodeConfig(options.Config)
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate Crane control configuration: %w", err)
	}
	clusterID, err := decodeControlClusterID(configuration.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("decode Crane cluster ID: %w", err)
	}
	bind, err := configuration.BindEndpoint(config.ServiceTopologyControl)
	if err != nil {
		return nil, fmt.Errorf("derive Crane control endpoint: %w", err)
	}
	_, voter := configuration.RaftVoterByID(configuration.NodeID)
	if voter && options.Raft == nil {
		return nil, errors.New("crane control service on a configured voter requires raft")
	}
	voterEndpoints, err := deriveVoterControlEndpoints(configuration.RaftVoters)
	if err != nil {
		return nil, err
	}
	window := time.Duration(configuration.Timing.ReplayWindow)
	if window <= 0 || window > config.MaxReplayWindow {
		return nil, errors.New("invalid Crane control replay window")
	}
	service := &Service{
		configuration: configuration, authenticator: options.Authenticator, clock: options.Clock,
		membership: options.Membership, raft: options.Raft, machine: options.Machine, gate: options.Gate,
		results: options.Results, wake: options.WakeCoordinator, clusterID: clusterID, bind: bind,
		voter: voter, voterEndpoints: voterEndpoints, listen: net.Listen,
		replay:                newServiceReplay(options.Clock, window, config.ReplayFutureSkewAllowance, DefaultMaxControlReplayEntries, DefaultMaxControlReplayEntriesPerPeer),
		maxConnections:        DefaultMaxControlConnections,
		peerConnections:       make(map[string]int),
		maxConnectionsPerPeer: DefaultMaxControlConnectionsPerPeer,
		timeout:               time.Duration(configuration.Crane.WorkerControlTimeout),
		ready:                 make(chan struct{}),
	}
	service.maxCommand.Store(config.MaxRaftCommandBytes)
	return service, nil
}

// Name returns the stable supervisor registration name.
func (*Service) Name() string { return "crane-control" }

// Ready closes once the exact +6 listener is bound.
func (service *Service) Ready() <-chan struct{} { return service.ready }

// Run binds the exact +6 endpoint, serves one bounded request/response per
// authenticated connection, and joins every handler before returning.
func (service *Service) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run Crane control service: nil context")
	}
	if !service.started.CompareAndSwap(false, true) {
		return errors.New("crane control service Run called more than once")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := service.listen("tcp", service.bind.String())
	if err != nil {
		return fmt.Errorf("listen Crane control: %w", err)
	}
	if listener == nil {
		return errors.New("listen Crane control: nil listener")
	}
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopAccept()
	close(service.ready)

	slots := make(chan struct{}, service.maxConnections)
	var handlers sync.WaitGroup
	defer func() {
		_ = listener.Close()
		handlers.Wait()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept Crane control: %w", err)
		}
		select {
		case slots <- struct{}{}:
		default:
			// The bounded connection budget is full: close immediately
			// without reading, so admitted requests keep their capacity.
			_ = connection.Close()
			continue
		}
		peer := controlPeerAddress(connection.RemoteAddr())
		if !service.reservePeerConnection(peer) {
			// The peer already holds its bounded share: close fail-closed
			// before reading or admitting any frame, with no mutation, no
			// replay eviction, and no response.
			_ = connection.Close()
			<-slots
			continue
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer service.releasePeerConnection(peer)
			defer func() { <-slots }()
			stopConnection := context.AfterFunc(ctx, func() { _ = connection.Close() })
			defer stopConnection()
			defer connection.Close()
			service.handleConnection(ctx, connection)
		}()
	}
}

// cloneControlNodeConfig deep-owns the mutable slices of one configuration.
func cloneControlNodeConfig(configuration config.NodeConfig) config.NodeConfig {
	configuration.RaftVoters = append([]config.RaftVoter(nil), configuration.RaftVoters...)
	return configuration
}

// decodeControlClusterID decodes the canonical UUID cluster identity.
func decodeControlClusterID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("invalid UUID")
	}
	var result [16]byte
	copy(result[:], decoded)
	return result, nil
}

// controlPeerAddress extracts the per-peer connection identity from one
// accepted remote: the source address that request admission binds to the
// authenticated sender. A remote without a usable source address is not
// attributable to any peer and is never counted per-peer, mirroring the
// worker control sessions that count only identified peers.
func controlPeerAddress(remote net.Addr) string {
	var address net.Addr = remote
	if address == nil {
		return ""
	}
	var ip net.IP
	if tcp, ok := address.(*net.TCPAddr); ok {
		ip = tcp.IP
	} else if host, _, err := net.SplitHostPort(address.String()); err == nil {
		ip = net.ParseIP(host)
	} else {
		ip = net.ParseIP(address.String())
	}
	if ip == nil {
		return ""
	}
	return string(ip.To16())
}

// reservePeerConnection takes one per-peer connection slot for the peer,
// reporting false when that peer already holds its bounded share of the
// global connection budget.
func (service *Service) reservePeerConnection(peer string) bool {
	if peer == "" {
		return true
	}
	service.peerMu.Lock()
	defer service.peerMu.Unlock()
	if service.peerConnections[peer] >= service.maxConnectionsPerPeer {
		return false
	}
	service.peerConnections[peer]++
	return true
}

// releasePeerConnection returns one per-peer connection slot taken by
// reservePeerConnection, on every connection termination path.
func (service *Service) releasePeerConnection(peer string) {
	if peer == "" {
		return
	}
	service.peerMu.Lock()
	defer service.peerMu.Unlock()
	if service.peerConnections[peer] <= 1 {
		delete(service.peerConnections, peer)
		return
	}
	service.peerConnections[peer]--
}

// serviceReplay pairs one global and bounded per-sender replay guards so no
// authenticated +6 request identity is ever served twice.
type serviceReplay struct {
	mu          sync.Mutex
	clock       clock.Clock
	window      time.Duration
	future      time.Duration
	perLimit    int
	senderLimit int
	global      *wire.ReplayGuard
	perSender   map[uint16]*wire.ReplayGuard
}

// newServiceReplay builds the paired replay guards without side effects.
func newServiceReplay(source clock.Clock, window, future time.Duration, globalLimit, perLimit int) *serviceReplay {
	return &serviceReplay{clock: source, window: window, future: future, perLimit: perLimit, senderLimit: globalLimit, global: wire.NewReplayGuard(source, window, future, globalLimit), perSender: make(map[uint16]*wire.ReplayGuard)}
}

// preflight rejects duplicate, stale, or capacity-exceeding request identities
// without consuming them.
func (replay *serviceReplay) preflight(sender uint16, request wire.RequestID, timestamp time.Time) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	if err := replay.global.Preflight(sender, request, timestamp); err != nil {
		return err
	}
	if guard := replay.perSender[sender]; guard != nil {
		return guard.Preflight(sender, request, timestamp)
	}
	if len(replay.perSender) >= replay.senderLimit {
		return wire.ErrReplayCacheFull
	}
	return nil
}

// commit durably consumes one request identity in both guards.
func (replay *serviceReplay) commit(sender uint16, request wire.RequestID, timestamp time.Time) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	guard := replay.perSender[sender]
	if guard == nil {
		if len(replay.perSender) >= replay.senderLimit {
			return wire.ErrReplayCacheFull
		}
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.future, replay.perLimit)
		if err := guard.Preflight(sender, request, timestamp); err != nil {
			return err
		}
	}
	if err := replay.global.Commit(sender, request, timestamp); err != nil {
		return err
	}
	if err := guard.Commit(sender, request, timestamp); err != nil {
		return err
	}
	replay.perSender[sender] = guard
	return nil
}

// recordInvalid retains a rejected identity so invalid retries cannot probe.
func (replay *serviceReplay) recordInvalid(sender uint16, request wire.RequestID, timestamp time.Time) {
	replay.mu.Lock()
	defer replay.mu.Unlock()
	replay.global.RecordInvalid(sender, request, timestamp)
	guard := replay.perSender[sender]
	if guard == nil {
		if len(replay.perSender) >= replay.senderLimit {
			return
		}
		guard = wire.NewReplayGuard(replay.clock, replay.window, replay.future, replay.perLimit)
		replay.perSender[sender] = guard
	}
	guard.RecordInvalid(sender, request, timestamp)
}
