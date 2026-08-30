package raft

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
)

func TestServiceConstructorIsSideEffectFreeAndRequiresLocalVoter(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33000)
	machine := &task8StateMachine{}
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if service.Name() != "raft" {
		t.Fatalf("Name = %q, want raft", service.Name())
	}
	if machine.restoreCalls != 0 {
		t.Fatalf("constructor Restore calls = %d, want zero", machine.restoreCalls)
	}
	if _, err := os.Stat(filepath.Join(configuration.StorageDir, RaftStorageDirectoryName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("constructor created raft storage: %v", err)
	}
	select {
	case <-service.Ready():
		t.Fatal("constructor closed Ready")
	default:
	}

	nonvoter := configuration
	nonvoter.NodeID = 4
	nonvoter.BasePort = 33030
	if _, err := NewService(ServiceOptions{
		Config: nonvoter, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	}); !errors.Is(err, ErrNotVoter) {
		t.Fatalf("nonvoter constructor error = %v, want ErrNotVoter", err)
	}
}

func TestServiceRunOrdersRecoveryBindWorkersOwnerAndIsolatedReady(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33100)
	events := &task8EventLog{}
	manualClock := clock.NewManual(time.Unix(1000, 0))
	serviceClock := &task10RecordingClock{Clock: manualClock, events: events}
	machine := &task8StateMachine{events: events}
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: serviceClock,
		Random: task8ZeroOffsetRandom{}, StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener := newBlockingListener()
	service.listen = func(network, address string) (net.Listener, error) {
		if network != "tcp" {
			t.Fatalf("listen network = %q, want tcp", network)
		}
		want, _ := configuration.BindEndpoint(config.ServiceRaftRPC)
		if address != want.String() {
			t.Fatalf("listen address = %q, want exact +8 %q", address, want)
		}
		events.add("bind")
		return listener, nil
	}
	fakeTransport := newTask10ServiceTransport(events)
	service.newTransport = func(TCPTransportOptions) (serviceTransport, error) { return fakeTransport, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	awaitClosed(t, service.Ready())
	if got, want := events.snapshot(), []string{"restore", "bind", "workers", "owner"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("startup events = %v, want %v", got, want)
	}
	if status := service.Status(); status.Role != RoleFollower || status.LeaderID != 0 {
		t.Fatalf("isolated ready status = %#v", status)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Service did not join children")
	}
	if _, err := service.Propose(context.Background(), []byte("after")); !errors.Is(err, ErrStopped) {
		t.Fatalf("post-stop Propose error = %v, want ErrStopped", err)
	}
	if err := service.Run(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("second Run error = %v, want ErrStopped", err)
	}
}

func TestServiceBindFailureOccursBeforeReadyAndReleasesStore(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33200)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	bindErr := errors.New("injected bind failure")
	service.listen = func(string, string) (net.Listener, error) { return nil, bindErr }
	if err := service.Run(context.Background()); !errors.Is(err, bindErr) {
		t.Fatalf("Run error = %v, want bind failure", err)
	}
	select {
	case <-service.Ready():
		t.Fatal("bind failure closed Ready")
	default:
	}
	voters, err := NewVoterSet(configuration.RaftVoters)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewStorageIdentity(StorageFormatVersion1, service.clusterID, configuration.NodeID, voters)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenFileStoreWithOptions(configuration.StorageDir, identity, voters, StoreOptions{MaxSnapshotBytes: configuration.Raft.MaxSnapshotBytes})
	if err != nil {
		t.Fatalf("bind failure retained store lock: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceStorageAndRestoreFailuresOccurBeforeBindAndReady(t *testing.T) {
	for _, test := range []struct {
		name string
		want error
	}{
		{name: "storage open", want: errors.New("injected storage open failure")},
		{name: "application restore", want: errors.New("injected application restore failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration, secret := task10ServiceConfig(t, 1, 33400)
			machine := &task8StateMachine{}
			service, err := NewService(ServiceOptions{
				Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
				Random: task8ZeroOffsetRandom{}, StateMachine: machine,
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.name == "storage open" {
				service.openStore = func(string, StorageIdentity, VoterSet, StoreOptions) (StableStore, error) {
					return nil, test.want
				}
			} else {
				machine.restoreErr = test.want
			}
			bound := false
			service.listen = func(string, string) (net.Listener, error) {
				bound = true
				return newBlockingListener(), nil
			}
			if err := service.Run(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("Run error = %v, want %v", err, test.want)
			}
			if bound {
				t.Fatal("early failure bound the Raft listener")
			}
			select {
			case <-service.Ready():
				t.Fatal("early failure closed Ready")
			default:
			}
		})
	}
}

func TestServiceRecoveredInvariantFailurePrecedesBindAndClosesStore(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33450)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &task8Store{
		state:         RecoveredState{Identity: service.identity, HardState: HardState{Term: 1, CommitIndex: 1}},
		snapshotLimit: configuration.Raft.MaxSnapshotBytes,
	}
	service.openStore = func(string, StorageIdentity, VoterSet, StoreOptions) (StableStore, error) { return store, nil }
	bound := false
	service.listen = func(string, string) (net.Listener, error) {
		bound = true
		return newBlockingListener(), nil
	}
	if err := service.Run(context.Background()); !errors.Is(err, ErrInvalidStorageState) {
		t.Fatalf("Run error = %v, want recovered invariant failure", err)
	}
	if bound {
		t.Fatal("recovered invariant failure bound listener")
	}
	if store.closes != 1 {
		t.Fatalf("store closes = %d, want exactly one", store.closes)
	}
	select {
	case <-service.Ready():
		t.Fatal("recovered invariant failure closed Ready")
	default:
	}
}

func TestServiceRuntimeListenerFailureIsFatalAndStopsNode(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33500)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener := newBlockingListener()
	service.listen = func(string, string) (net.Listener, error) { return listener, nil }
	done := make(chan error, 1)
	go func() { done <- service.Run(context.Background()) }()
	awaitClosed(t, service.Ready())
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("runtime listener failure = %v, want fatal net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime listener failure did not stop Service")
	}
	if _, err := service.Barrier(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("post-fatal Barrier error = %v, want ErrStopped", err)
	}
}

func TestServiceReadyDoesNotBeatAlreadyReportedNodeStartupFailure(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	configuration, secret := task10ServiceConfig(t, 1, 33550)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: newTask10StartupFailureClock(),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	service.listen = func(string, string) (net.Listener, error) { return newBlockingListener(), nil }
	service.newTransport = func(TCPTransportOptions) (serviceTransport, error) {
		return newTask10ServiceTransport(nil), nil
	}
	done := make(chan error, 1)
	go func() { done <- service.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrTickRegression) {
			t.Fatalf("Run error = %v, want startup ErrTickRegression", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup owner failure did not terminate Service")
	}
	select {
	case <-service.Ready():
		t.Fatal("Service Ready beat an already-reported Node startup failure")
	default:
	}
}

func TestServiceReportedFatalWinsConcurrentCancellation(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33570)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	service.listen = func(string, string) (net.Listener, error) { return newBlockingListener(), nil }
	failure := errors.New("reported transport failure")
	transport := newTask10FailingServiceTransport(failure)
	service.newTransport = func(TCPTransportOptions) (serviceTransport, error) { return transport, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	awaitClosed(t, service.Ready())
	close(transport.fail)
	awaitClosed(t, transport.returning)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, failure) {
			t.Fatalf("Run error = %v, want already-reported fatal over cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal/cancellation race did not join children")
	}
}

func TestServiceDelegatesPublicAPIWhileReady(t *testing.T) {
	configuration, secret := task10ServiceConfig(t, 1, 33300)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewManual(time.Unix(1000, 0)),
		Random: task8ZeroOffsetRandom{}, StateMachine: &task8StateMachine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener := newBlockingListener()
	service.listen = func(string, string) (net.Listener, error) { return listener, nil }
	service.newTransport = func(TCPTransportOptions) (serviceTransport, error) {
		return newTask10ServiceTransport(nil), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	awaitClosed(t, service.Ready())
	if _, err := service.Propose(context.Background(), []byte("not-leader")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose error = %v, want ErrNotLeader", err)
	}
	subscriptionContext, cancelSubscription := context.WithCancel(context.Background())
	subscription, err := service.SubscribeLeadership(subscriptionContext, 1)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := subscription.Snapshot(); snapshot.LocalID != 1 || snapshot.Role != RoleFollower {
		t.Fatalf("leadership snapshot = %#v", snapshot)
	}
	cancelSubscription()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type task10RecordingClock struct {
	clock.Clock
	events *task8EventLog
	once   sync.Once
}

type task10StartupFailureClock struct {
	mu    sync.Mutex
	epoch time.Time
	calls int
}

func newTask10StartupFailureClock() *task10StartupFailureClock {
	return &task10StartupFailureClock{epoch: time.Unix(5000, 0)}
}

func (source *task10StartupFailureClock) Now() time.Time {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	if source.calls == 1 {
		return source.epoch
	}
	return source.epoch.Add(-time.Second)
}

func (source *task10StartupFailureClock) NewTimer(time.Duration) clock.Timer {
	channel := make(chan time.Time, 1)
	channel <- source.epoch
	return task10ImmediateTimer{channel: channel}
}

type task10ImmediateTimer struct{ channel chan time.Time }

func (timer task10ImmediateTimer) C() <-chan time.Time { return timer.channel }
func (task10ImmediateTimer) Stop() bool                { return true }
func (task10ImmediateTimer) Reset(time.Duration) bool  { return false }

func (source *task10RecordingClock) NewTimer(duration time.Duration) clock.Timer {
	source.once.Do(func() { source.events.add("owner") })
	return source.Clock.NewTimer(duration)
}

type task10ServiceTransport struct {
	ready     chan struct{}
	done      chan struct{}
	events    *task8EventLog
	readyOnce sync.Once
}

type task10FailingServiceTransport struct {
	ready     chan struct{}
	fail      chan struct{}
	returning chan struct{}
	err       error
}

func newTask10FailingServiceTransport(err error) *task10FailingServiceTransport {
	return &task10FailingServiceTransport{ready: make(chan struct{}), fail: make(chan struct{}), returning: make(chan struct{}), err: err}
}

func (*task10FailingServiceTransport) Handoff(PeerMessage) (TransportHandoff, error) {
	return TransportUnavailable, nil
}

func (transport *task10FailingServiceTransport) Ready() <-chan struct{} { return transport.ready }

func (transport *task10FailingServiceTransport) Run(context.Context, net.Listener, RPCIngress) error {
	close(transport.ready)
	<-transport.fail
	close(transport.returning)
	return transport.err
}

func newTask10ServiceTransport(events *task8EventLog) *task10ServiceTransport {
	return &task10ServiceTransport{ready: make(chan struct{}), done: make(chan struct{}), events: events}
}

func (*task10ServiceTransport) Handoff(PeerMessage) (TransportHandoff, error) {
	return TransportUnavailable, nil
}

func (transport *task10ServiceTransport) Ready() <-chan struct{} { return transport.ready }

func (transport *task10ServiceTransport) Run(ctx context.Context, _ net.Listener, _ RPCIngress) error {
	if transport.events != nil {
		transport.events.add("workers")
	}
	transport.readyOnce.Do(func() { close(transport.ready) })
	<-ctx.Done()
	close(transport.done)
	return nil
}

func task10ServiceConfig(t *testing.T, localID, basePort uint16) (config.NodeConfig, []byte) {
	t.Helper()
	secret := []byte("01234567890123456789012345678901")
	secretPath := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	storage := t.TempDir()
	configuration := config.NodeConfig{
		NodeID: localID, ClusterID: "00112233-4455-6677-8899-aabbccddeeff",
		BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1", BasePort: basePort,
		Introducer: net.JoinHostPort("127.0.0.1", "1"), StorageDir: storage, ClusterSecretFile: secretPath,
		Timing: config.DefaultTimingConfig(), Raft: config.DefaultRaftConfig(),
		RaftVoters: []config.RaftVoter{
			{NodeID: 1, Endpoint: net.JoinHostPort("127.0.0.1", task10Port(basePort, 1))},
			{NodeID: 2, Endpoint: net.JoinHostPort("127.0.0.1", task10Port(basePort, 2))},
			{NodeID: 3, Endpoint: net.JoinHostPort("127.0.0.1", task10Port(basePort, 3))},
		},
	}
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	return configuration, secret
}

func task10Port(base uint16, voter uint16) string {
	port := uint32(base) + uint32(voter-1)*10 + 8
	return strconv.FormatUint(uint64(port), 10)
}
