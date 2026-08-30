package raft

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	internalnode "github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	raftServiceNew uint32 = iota
	raftServiceStarting
	raftServiceRunning
	raftServiceStopped
)

var _ internalnode.Service = (*Service)(nil)

// ServiceOptions supplies one configured voter and deterministic application dependencies.
type ServiceOptions struct {
	// Config is the validated fixed-voter runtime configuration owned by the service.
	Config config.NodeConfig
	// Secret is the raw cluster HMAC key; construction takes an owned copy.
	Secret []byte
	// Clock drives wire replay timestamps and the serialized Node timer.
	Clock clock.Clock
	// Random supplies deterministic election timeout samples.
	Random interface{ Uint64() uint64 }
	// StateMachine is restored during Run before the Raft listener is bound.
	StateMachine StateMachine
}

type serviceTransport interface {
	Transport
	Ready() <-chan struct{}
	Run(context.Context, net.Listener, RPCIngress) error
}

type serviceStoreOpener func(string, StorageIdentity, VoterSet, StoreOptions) (StableStore, error)
type serviceTransportFactory func(TCPTransportOptions) (serviceTransport, error)

// Service composes durable recovery, authenticated +8 TCP ownership, and one serialized Node.
type Service struct {
	options       ServiceOptions
	voters        VoterSet
	identity      StorageIdentity
	clusterID     [16]byte
	authenticator wire.Authenticator

	state atomic.Uint32
	node  atomic.Pointer[Node]
	ready chan struct{}
	done  chan struct{}

	listen         func(string, string) (net.Listener, error)
	openStore      serviceStoreOpener
	newTransport   serviceTransportFactory
	newTransferIDs func() TransferIDSource
}

// NewService validates and owns dependencies without touching storage, the application,
// listeners, connections, timers, or goroutines.
func NewService(options ServiceOptions) (*Service, error) {
	if err := options.Config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid configuration: %v", ErrInvalidCoreState, err)
	}
	if len(options.Secret) < config.MinClusterSecretBytes || options.Clock == nil || options.Random == nil || options.StateMachine == nil {
		return nil, fmt.Errorf("%w: service dependencies are incomplete", ErrInvalidCoreState)
	}
	voters, err := NewVoterSet(options.Config.RaftVoters)
	if err != nil {
		return nil, err
	}
	if err := voters.ValidateLocalID(options.Config.NodeID); err != nil {
		return nil, err
	}
	clusterID, err := parseRaftClusterID(options.Config.ClusterID)
	if err != nil {
		return nil, err
	}
	identity, err := NewStorageIdentity(StorageFormatVersion1, clusterID, options.Config.NodeID, voters)
	if err != nil {
		return nil, err
	}
	ownedConfig := options.Config
	ownedConfig.RaftVoters = append([]config.RaftVoter(nil), options.Config.RaftVoters...)
	ownedSecret := append([]byte(nil), options.Secret...)
	options.Config = ownedConfig
	options.Secret = ownedSecret
	service := &Service{
		options: options, voters: voters, identity: identity, clusterID: clusterID,
		authenticator: wire.NewHMACAuthenticator(ownedSecret),
		ready:         make(chan struct{}), done: make(chan struct{}),
		listen: net.Listen,
		openStore: func(directory string, identity StorageIdentity, voters VoterSet, options StoreOptions) (StableStore, error) {
			return OpenFileStoreWithOptions(directory, identity, voters, options)
		},
		newTransport: func(options TCPTransportOptions) (serviceTransport, error) {
			return NewTCPTransport(options)
		},
		newTransferIDs: func() TransferIDSource { return cryptoTransferIDSource{} },
	}
	return service, nil
}

// Name returns the supervisor registration name.
func (*Service) Name() string { return "raft" }

// Ready closes after recovery, exact +8 binding, transport ownership, and Node owner startup.
func (service *Service) Ready() <-chan struct{} {
	if service == nil {
		return nil
	}
	return service.ready
}

// Propose delegates one leader-only command to the prepared Node.
func (service *Service) Propose(ctx context.Context, command []byte) (ProposalResult, error) {
	if err := service.lifecycleError(); err != nil {
		return ProposalResult{}, err
	}
	return service.node.Load().Propose(ctx, command)
}

// Barrier delegates one current-term application fence to the prepared Node.
func (service *Service) Barrier(ctx context.Context) (uint64, error) {
	if err := service.lifecycleError(); err != nil {
		return 0, err
	}
	return service.node.Load().Barrier(ctx)
}

// Status delegates a race-safe diagnostic snapshot when a Node has been constructed.
func (service *Service) Status() Status {
	if service == nil {
		return Status{}
	}
	node := service.node.Load()
	if node == nil {
		return Status{}
	}
	return node.Status()
}

// SubscribeLeadership delegates owner-linearized leadership observation.
func (service *Service) SubscribeLeadership(ctx context.Context, capacity int) (*LeadershipSubscription, error) {
	if err := service.lifecycleError(); err != nil {
		return nil, err
	}
	return service.node.Load().SubscribeLeadership(ctx, capacity)
}

// Run recovers before binding +8, then starts transport ownership before the Node owner.
func (service *Service) Run(ctx context.Context) (runErr error) {
	if service == nil || ctx == nil {
		return fmt.Errorf("%w: nil service or context", ErrInvalidCoreState)
	}
	if !service.state.CompareAndSwap(raftServiceNew, raftServiceStarting) {
		return ErrStopped
	}
	defer func() {
		service.state.Store(raftServiceStopped)
		close(service.done)
	}()

	store, err := service.openStore(
		service.options.Config.StorageDir,
		service.identity,
		service.voters,
		StoreOptions{MaxSnapshotBytes: service.options.Config.Raft.MaxSnapshotBytes},
	)
	if err != nil {
		return fmt.Errorf("open Raft stable store: %w", err)
	}
	storeOwnedByNode := false
	defer func() {
		if !storeOwnedByNode {
			if closeErr := store.Close(); closeErr != nil && runErr == nil {
				runErr = fmt.Errorf("close Raft stable store: %w", closeErr)
			}
		}
	}()

	codecLimits := DefaultCodecLimits()
	codecLimits.MaxAppendEntries = service.options.Config.Raft.MaxAppendEntries
	codecLimits.MaxSnapshotBytes = service.options.Config.Raft.MaxSnapshotBytes
	transport, err := service.newTransport(TCPTransportOptions{
		LocalID: service.options.Config.NodeID, Voters: service.voters, ClusterID: service.clusterID,
		Authenticator: service.authenticator, Clock: service.options.Clock,
		ReplayWindow: time.Duration(service.options.Config.Timing.ReplayWindow),
		RPCTimeout:   time.Duration(service.options.Config.Raft.RPCTimeout), CodecLimits: codecLimits,
	})
	if err != nil {
		return fmt.Errorf("construct Raft TCP transport: %w", err)
	}
	node, err := NewNode(NodeOptions{
		LocalID: service.options.Config.NodeID, Voters: service.voters, Identity: service.identity,
		Store: store, StateMachine: service.options.StateMachine, Transport: transport,
		Clock: service.options.Clock, Random: service.options.Random, TransferIDs: service.newTransferIDs(),
		ElectionTimeoutMin:     time.Duration(service.options.Config.Raft.ElectionTimeoutMin),
		ElectionTimeoutMax:     time.Duration(service.options.Config.Raft.ElectionTimeoutMax),
		HeartbeatInterval:      time.Duration(service.options.Config.Raft.HeartbeatInterval),
		MaxAppendEntries:       service.options.Config.Raft.MaxAppendEntries,
		SnapshotEntryThreshold: service.options.Config.Raft.SnapshotEntryThreshold,
		SnapshotByteThreshold:  service.options.Config.Raft.SnapshotByteThreshold,
		MaxSnapshotBytes:       service.options.Config.Raft.MaxSnapshotBytes,
		MaxSnapshotChunkBytes:  config.MaxRaftSnapshotChunkBytes,
	})
	if err != nil {
		return fmt.Errorf("construct Raft Node: %w", err)
	}
	service.node.Store(node)
	if err := node.prepare(); err != nil {
		return err
	}
	endpoint, err := service.options.Config.BindEndpoint(config.ServiceRaftRPC)
	if err != nil {
		return err
	}
	listener, err := service.listen("tcp", endpoint.String())
	if err != nil {
		return fmt.Errorf("listen Raft TCP on %s: %w", endpoint, err)
	}

	childContext, cancelChildren := context.WithCancel(ctx)
	transportResult := make(chan error, 1)
	go func() { transportResult <- transport.Run(childContext, listener, node) }()
	transportDone := false
	var transportErr error
	select {
	case <-transport.Ready():
	case transportErr = <-transportResult:
		transportDone = true
		cancelChildren()
		_ = listener.Close()
		return childError("Raft transport", transportErr)
	case <-ctx.Done():
		cancelChildren()
		_ = listener.Close()
		transportErr = <-transportResult
		transportDone = true
		return childError("Raft transport", transportErr)
	}

	nodeResult := make(chan error, 1)
	storeOwnedByNode = true
	go func() { nodeResult <- node.Run(childContext) }()
	nodeDone := false
	var nodeErr error
	select {
	case <-node.Ready():
		service.state.Store(raftServiceRunning)
		close(service.ready)
	case nodeErr = <-nodeResult:
		nodeDone = true
		cancelChildren()
		_ = listener.Close()
		if !transportDone {
			transportErr = <-transportResult
			transportDone = true
		}
		return firstChildError(nodeErr, transportErr)
	case transportErr = <-transportResult:
		transportDone = true
		cancelChildren()
		_ = listener.Close()
		nodeErr = <-nodeResult
		nodeDone = true
		return firstChildError(transportErr, nodeErr)
	case <-ctx.Done():
		cancelChildren()
		_ = listener.Close()
		nodeErr = <-nodeResult
		nodeDone = true
		if !transportDone {
			transportErr = <-transportResult
			transportDone = true
		}
		return firstChildError(nodeErr, transportErr)
	}

	var firstErr error
	select {
	case nodeErr = <-nodeResult:
		nodeDone = true
		firstErr = childError("Raft Node", nodeErr)
	case transportErr = <-transportResult:
		transportDone = true
		firstErr = childError("Raft transport", transportErr)
	case <-ctx.Done():
	}
	cancelChildren()
	_ = listener.Close()
	if !nodeDone {
		nodeErr = <-nodeResult
	}
	if !transportDone {
		transportErr = <-transportResult
	}
	if firstErr != nil {
		return firstErr
	}
	return firstChildError(nodeErr, transportErr)
}

func (service *Service) lifecycleError() error {
	if service == nil {
		return ErrNotRunning
	}
	switch service.state.Load() {
	case raftServiceRunning:
		return nil
	case raftServiceStopped:
		return ErrStopped
	default:
		return ErrNotRunning
	}
}

func childError(name string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("%s failed: %w", name, err)
}

func firstChildError(first, second error) error {
	if err := childError("Raft child", first); err != nil {
		return err
	}
	return childError("Raft child", second)
}

func parseRaftClusterID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, fmt.Errorf("%w: cluster ID must contain 16 UUID bytes", ErrInvalidStorageIdentity)
	}
	var clusterID [16]byte
	copy(clusterID[:], decoded)
	return clusterID, nil
}

type cryptoTransferIDSource struct{}

func (cryptoTransferIDSource) NextTransferID(uint16) (TransferID, error) {
	var id TransferID
	if _, err := cryptorand.Read(id[:]); err != nil || id.IsZero() {
		return TransferID{}, fmt.Errorf("%w: %v", ErrTransferIDExhausted, err)
	}
	return id, nil
}
