package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	mathrand "math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/control"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/crane/worker"
	"github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/raft"
	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/wire"
)

var runtimeTestSecret = []byte("0123456789abcdef0123456789abcdef")

const runtimeTestClusterID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"

const runtimeReadyTimeout = 30 * time.Second

var runtimePortRandom = mathrand.New(mathrand.NewSource(time.Now().UnixNano() ^ int64(os.Getpid())<<20))
var runtimePortRandomMu sync.Mutex

// reserveRuntimeBasePort finds one base port whose complete +0..+8 service
// block is currently bindable on the loopback host. Bases stay below the OS
// ephemeral port range so concurrent test binaries' outbound connections
// cannot steal a probed port before the runtime binds it.
func reserveRuntimeBasePort(t *testing.T) uint16 {
	t.Helper()
	for attempt := 0; attempt < 64; attempt++ {
		runtimePortRandomMu.Lock()
		base := uint16(20000 + runtimePortRandom.Intn(25000))
		runtimePortRandomMu.Unlock()
		if probeRuntimePortBlock(t, base) {
			return base
		}
	}
	t.Fatal("could not reserve a runtime service port block")
	return 0
}

// isBindConflict reports a startup failure caused only by a lost port race
// with an unrelated process, which tests resolve by retrying a fresh block.
func isBindConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "address already in use")
}

func probeRuntimePortBlock(t *testing.T, base uint16) bool {
	t.Helper()
	for _, offset := range []uint16{2, 5, 6, 8} {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+offset))
		if err != nil {
			return false
		}
		_ = listener.Close()
	}
	for _, offset := range []uint16{0, 1, 7} {
		connection, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", base+offset))
		if err != nil {
			return false
		}
		_ = connection.Close()
	}
	return true
}

// runtimeTestConfig builds one strict validated node configuration whose SWIM
// introducer is the node itself, so a lone process can become ready.
func runtimeTestConfig(t *testing.T, nodeID uint16, base uint16) config.NodeConfig {
	t.Helper()
	secretPath := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretPath, runtimeTestSecret, 0o600); err != nil {
		t.Fatal(err)
	}
	voterBase := base
	if nodeID != 1 {
		// A nonvoter names foreign voter endpoints it never binds.
		voterBase = base + 300
	}
	configuration := config.NodeConfig{
		NodeID:            nodeID,
		ClusterID:         runtimeTestClusterID,
		BindHost:          "127.0.0.1",
		AdvertiseHost:     "127.0.0.1",
		BasePort:          base,
		Introducer:        fmt.Sprintf("127.0.0.1:%d", base+2),
		StorageDir:        filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", nodeID)),
		ClusterSecretFile: secretPath,
		RaftVoters: []config.RaftVoter{
			{NodeID: 1, Endpoint: fmt.Sprintf("127.0.0.1:%d", voterBase+8)},
			{NodeID: 2, Endpoint: fmt.Sprintf("127.0.0.1:%d", voterBase+108)},
			{NodeID: 3, Endpoint: fmt.Sprintf("127.0.0.1:%d", voterBase+208)},
		},
		Timing: config.DefaultTimingConfig(),
		Raft:   config.DefaultRaftConfig(),
		Crane:  config.DefaultCraneConfig(),
	}
	if err := configuration.Validate(); err != nil {
		t.Fatalf("validate runtime test configuration: %v", err)
	}
	return configuration
}

// prepareRuntimeStorage creates the owner-only storage directory and the
// trusted initial SWIM incarnation, exactly as cluster provisioning does.
func prepareRuntimeStorage(t *testing.T, configuration config.NodeConfig) {
	t.Helper()
	if err := os.MkdirAll(configuration.StorageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	incarnation := swim.NewFileIncarnationStore(filepath.Join(configuration.StorageDir, swim.IncarnationStateFilename))
	if err := incarnation.Store(1); err != nil {
		t.Fatal(err)
	}
}

func runtimeTestDependencies() Dependencies {
	return Dependencies{
		Secret: append([]byte(nil), runtimeTestSecret...),
		Clock:  clock.NewReal(),
		Random: random.NewLockedSource(1),
	}
}

func storageEntries(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestServiceEndpointRegistryTable(t *testing.T) {
	want := []config.ServiceSpec{
		{Service: config.ServiceSWIMPing, Name: "swim-ping", Offset: 0, Transport: config.TransportUDP},
		{Service: config.ServiceSWIMACK, Name: "swim-ack", Offset: 1, Transport: config.TransportUDP},
		{Service: config.ServiceSWIMSnapshot, Name: "swim-snapshot", Offset: 2, Transport: config.TransportTCP},
		{Service: config.ServiceFileRPC, Name: "file-rpc", Offset: 3, Transport: config.TransportTCP},
		{Service: config.ServiceGrepRPC, Name: "grep-rpc", Offset: 4, Transport: config.TransportTCP},
		{Service: config.ServiceCraneWorker, Name: "crane-worker", Offset: 5, Transport: config.TransportTCP},
		{Service: config.ServiceTopologyControl, Name: "topology-control", Offset: 6, Transport: config.TransportTCP},
		{Service: config.ServiceCraneTupleACK, Name: "crane-tuple-ack", Offset: 7, Transport: config.TransportUDP},
		{Service: config.ServiceRaftRPC, Name: "raft-rpc", Offset: 8, Transport: config.TransportTCP},
	}
	if got := config.Services(); !reflect.DeepEqual(got, want) {
		t.Fatalf("service registry = %#v, want %#v", got, want)
	}
	configuration := runtimeTestConfig(t, 1, 8000)
	for _, spec := range want {
		endpoint, err := configuration.BindEndpoint(spec.Service)
		if err != nil {
			t.Fatalf("derive %s bind endpoint: %v", spec.Name, err)
		}
		if endpoint.Port != configuration.BasePort+spec.Offset || endpoint.Host != configuration.BindHost {
			t.Fatalf("%s endpoint = %v, want %s:%d", spec.Name, endpoint, configuration.BindHost, configuration.BasePort+spec.Offset)
		}
	}
}

func TestNewComposesSharedMachineAndGate(t *testing.T) {
	configuration := runtimeTestConfig(t, 1, reserveRuntimeBasePort(t))
	prepareRuntimeStorage(t, configuration)

	runtime, err := New(configuration, runtimeTestDependencies())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if runtime.Machine == nil || runtime.Gate == nil || runtime.SWIM == nil || runtime.Membership == nil ||
		runtime.Worker == nil || runtime.Raft == nil || runtime.Coordinator == nil || runtime.Control == nil {
		t.Fatalf("incomplete voter composition: %#v", runtime)
	}
	if runtime.raftOptions.StateMachine != raft.StateMachine(runtime.Machine) {
		t.Fatal("Raft state machine is not the exact shared Crane machine")
	}
	if runtime.actorOptions.Machine != runtime.Machine {
		t.Fatal("coordinator machine is not the exact shared Crane machine")
	}
	if runtime.controlOptions.Machine != runtime.Machine {
		t.Fatal("control machine is not the exact shared Crane machine")
	}
	if runtime.controlOptions.Results == nil || runtime.controlOptions.Results.Machine != runtime.Machine {
		t.Fatal("result query engine does not share the exact Crane machine")
	}
	if runtime.workerOptions.Gate != runtime.Gate || runtime.actorOptions.Gate != runtime.Gate || runtime.controlOptions.Gate != runtime.Gate {
		t.Fatal("worker, coordinator, and control do not share the exact admission gate")
	}
	if runtime.actorOptions.WorkerReady != runtime.Worker.Ready() {
		t.Fatal("coordinator does not wait on the exact worker readiness channel")
	}
	if runtime.actorOptions.Results == nil {
		t.Fatal("coordinator terminal result transfer client is not wired")
	}
	if runtime.controlOptions.Results.Fetcher == nil {
		t.Fatal("control result fetcher is not wired")
	}
	wantArtifacts := filepath.Join(configuration.StorageDir, ArtifactDirectoryName)
	if runtime.workerOptions.ArtifactDirectory != wantArtifacts {
		t.Fatalf("artifact directory = %q, want %q", runtime.workerOptions.ArtifactDirectory, wantArtifacts)
	}
	if runtime.actorOptions.FailureGracePeriod != time.Duration(configuration.Crane.FailureGracePeriod) {
		t.Fatalf("failure grace period = %v, want configured %v", runtime.actorOptions.FailureGracePeriod, time.Duration(configuration.Crane.FailureGracePeriod))
	}
}

func TestNewIsSideEffectFree(t *testing.T) {
	base := reserveRuntimeBasePort(t)
	configuration := runtimeTestConfig(t, 1, base)
	prepareRuntimeStorage(t, configuration)
	before := storageEntries(t, configuration.StorageDir)
	goroutinesBefore := goruntime.NumGoroutine()

	if _, err := New(configuration, runtimeTestDependencies()); err != nil {
		t.Fatalf("New: %v", err)
	}
	if after := storageEntries(t, configuration.StorageDir); !reflect.DeepEqual(after, before) {
		t.Fatalf("construction touched storage: before=%v after=%v", before, after)
	}
	if !probeRuntimePortBlock(t, base) {
		t.Fatal("construction bound a service port")
	}
	if goroutinesAfter := goruntime.NumGoroutine(); goroutinesAfter > goroutinesBefore {
		t.Fatalf("construction leaked goroutines: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
}

func TestNewOwnsConfigurationAndSecret(t *testing.T) {
	configuration := runtimeTestConfig(t, 1, reserveRuntimeBasePort(t))
	prepareRuntimeStorage(t, configuration)
	dependencies := runtimeTestDependencies()

	runtime, err := New(configuration, dependencies)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	originalEndpoint := configuration.RaftVoters[0].Endpoint
	configuration.RaftVoters[0].Endpoint = "10.9.9.9:1"
	for index := range dependencies.Secret {
		dependencies.Secret[index] = 0
	}
	if got := runtime.workerOptions.Config.RaftVoters[0].Endpoint; got != originalEndpoint {
		t.Fatalf("caller mutation reached the retained worker configuration: %q", got)
	}
	if got := runtime.controlOptions.Config.RaftVoters[0].Endpoint; got != originalEndpoint {
		t.Fatalf("caller mutation reached the retained control configuration: %q", got)
	}
	if got := runtime.raftOptions.Config.RaftVoters[0].Endpoint; got != originalEndpoint {
		t.Fatalf("caller mutation reached the retained Raft configuration: %q", got)
	}
	if string(runtime.raftOptions.Secret) != string(runtimeTestSecret) {
		t.Fatal("caller mutation reached the retained cluster secret")
	}
}

func TestNewRejectsInvalidCompositionInputs(t *testing.T) {
	base := reserveRuntimeBasePort(t)
	valid := runtimeTestConfig(t, 1, base)
	prepareRuntimeStorage(t, valid)
	tests := []struct {
		name         string
		mutateConfig func(*config.NodeConfig)
		mutateDeps   func(*Dependencies)
	}{
		{name: "nil clock", mutateDeps: func(d *Dependencies) { d.Clock = nil }},
		{name: "nil random", mutateDeps: func(d *Dependencies) { d.Random = nil }},
		{name: "short secret", mutateDeps: func(d *Dependencies) { d.Secret = []byte("short") }},
		{name: "zero node", mutateConfig: func(c *config.NodeConfig) { c.NodeID = 0 }},
		{name: "foreign fingerprint", mutateConfig: func(c *config.NodeConfig) {
			c.Crane.ConsensusFingerprint = strings.Repeat("0", 64)
		}},
		{name: "invalid cluster id", mutateConfig: func(c *config.NodeConfig) { c.ClusterID = "not-a-uuid" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := valid
			configuration.RaftVoters = append([]config.RaftVoter(nil), valid.RaftVoters...)
			dependencies := runtimeTestDependencies()
			if test.mutateConfig != nil {
				test.mutateConfig(&configuration)
			}
			if test.mutateDeps != nil {
				test.mutateDeps(&dependencies)
			}
			before := storageEntries(t, valid.StorageDir)
			runtime, err := New(configuration, dependencies)
			if err == nil || runtime != nil {
				t.Fatalf("New accepted invalid composition: runtime=%v err=%v", runtime, err)
			}
			if after := storageEntries(t, valid.StorageDir); !reflect.DeepEqual(after, before) {
				t.Fatalf("failed construction touched storage: before=%v after=%v", before, after)
			}
		})
	}
}

// gateObservingService records whether the shared gate was already closed at
// the moment its run context was canceled.
type gateObservingService struct {
	name         string
	gate         *admission.Gate
	ready        chan struct{}
	closedAtStop chan bool
	enterGate    bool
}

func newGateObservingService(name string, gate *admission.Gate, enterGate bool) *gateObservingService {
	return &gateObservingService{name: name, gate: gate, ready: make(chan struct{}), closedAtStop: make(chan bool, 1), enterGate: enterGate}
}

func (s *gateObservingService) Name() string { return s.name }

func (s *gateObservingService) Ready() <-chan struct{} { return s.ready }

func (s *gateObservingService) Run(ctx context.Context) error {
	var release func()
	if s.enterGate {
		var err error
		release, err = s.gate.Enter()
		if err != nil {
			return err
		}
	}
	close(s.ready)
	<-ctx.Done()
	_, open := s.gate.AdmissionEpoch()
	s.closedAtStop <- !open
	if release != nil {
		release()
	}
	return nil
}

func TestParentCancellationClosesGateBeforeServiceCancellation(t *testing.T) {
	tests := []struct {
		name      string
		enterGate bool
	}{
		{name: "idle gate closes synchronously"},
		{name: "held permit bounds the close wait", enterGate: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate := admission.NewGate()
			epoch := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
			if err := gate.Open(epoch); err != nil {
				t.Fatalf("open gate: %v", err)
			}
			first := newGateObservingService("first", gate, test.enterGate)
			second := newGateObservingService("second", gate, false)
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			readySignal := make(chan struct{})
			go func() {
				result <- superviseWithGate(ctx, gate, []node.Service{first, second}, 100*time.Millisecond, func() { close(readySignal) })
			}()
			select {
			case <-readySignal:
			case err := <-result:
				t.Fatalf("supervision ended before readiness: %v", err)
			case <-time.After(runtimeReadyTimeout):
				t.Fatal("services never became ready")
			}
			cancel()
			for _, service := range []*gateObservingService{first, second} {
				select {
				case closed := <-service.closedAtStop:
					if !closed {
						t.Fatalf("service %q was canceled before the shared gate closed", service.name)
					}
				case <-time.After(runtimeReadyTimeout):
					t.Fatalf("service %q never observed cancellation", service.name)
				}
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("supervision after cancellation: %v", err)
				}
			case <-time.After(runtimeReadyTimeout):
				t.Fatal("supervision did not join after cancellation")
			}
			if _, open := gate.AdmissionEpoch(); open {
				t.Fatal("gate reopened after shutdown")
			}
		})
	}
}

func TestNewComposesVoterAndNonvoterTopology(t *testing.T) {
	tests := []struct {
		name      string
		nodeID    uint16
		wantNames []string
		voter     bool
	}{
		{
			name:   "voter",
			nodeID: 1,
			wantNames: []string{
				"swim", "crane-membership-authorizer", "raft",
				"crane-worker", "crane-coordinator", "crane-control",
			},
			voter: true,
		},
		{
			name:   "nonvoter",
			nodeID: 4,
			wantNames: []string{
				"swim", "crane-membership-authorizer",
				"crane-worker", "crane-control",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := runtimeTestConfig(t, test.nodeID, reserveRuntimeBasePort(t))
			prepareRuntimeStorage(t, configuration)
			runtime, err := New(configuration, runtimeTestDependencies())
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			gotNames := make([]string, len(runtime.Services))
			for index, service := range runtime.Services {
				gotNames[index] = service.Name()
			}
			if !reflect.DeepEqual(gotNames, test.wantNames) {
				t.Fatalf("service names = %v, want %v", gotNames, test.wantNames)
			}
			if (runtime.Raft != nil) != test.voter || (runtime.Coordinator != nil) != test.voter {
				t.Fatalf("voter composition mismatch: raft=%v coordinator=%v want voter=%t", runtime.Raft, runtime.Coordinator, test.voter)
			}
			if test.voter {
				if runtime.controlOptions.Raft == nil {
					t.Fatal("voter control service has no Raft surface")
				}
			} else {
				if runtime.controlOptions.Raft != nil {
					t.Fatal("nonvoter control service must redirect without Raft")
				}
				if runtime.raftOptions.StateMachine != nil {
					t.Fatal("nonvoter retained Raft construction inputs")
				}
			}
		})
	}
}

type runningRuntime struct {
	runtime *Runtime
	cancel  context.CancelFunc
	done    chan error
}

func startRuntime(t *testing.T, configuration config.NodeConfig, dependencies Dependencies) *runningRuntime {
	t.Helper()
	runtime, err := New(configuration, dependencies)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(runtimeReadyTimeout):
			t.Error("runtime did not join during cleanup")
		}
	})
	return &runningRuntime{runtime: runtime, cancel: cancel, done: done}
}

// startReadyRuntime starts one runtime on a fresh port block and waits for
// readiness, retrying with a new block when an unrelated process wins a port
// race between the probe and the bind.
func startReadyRuntime(t *testing.T, nodeID uint16, seed func(*testing.T, config.NodeConfig)) (*runningRuntime, config.NodeConfig, uint16) {
	t.Helper()
	for attempt := 0; attempt < 5; attempt++ {
		base := reserveRuntimeBasePort(t)
		configuration := runtimeTestConfig(t, nodeID, base)
		prepareRuntimeStorage(t, configuration)
		if seed != nil {
			seed(t, configuration)
		}
		running := startRuntime(t, configuration, runtimeTestDependencies())
		select {
		case <-running.runtime.Ready():
			return running, configuration, base
		case err := <-running.done:
			running.done <- err
			if isBindConflict(err) {
				continue
			}
			t.Fatalf("runtime ended before readiness: %v", err)
		case <-time.After(runtimeReadyTimeout):
			t.Fatal("runtime never became ready")
		}
	}
	t.Fatal("could not start a runtime without losing port races")
	return nil, config.NodeConfig{}, 0
}

func (r *runningRuntime) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		r.done <- err
		return err
	case <-time.After(runtimeReadyTimeout):
		t.Fatal("runtime did not join after cancellation")
		return nil
	}
}

func dialRuntimePort(t *testing.T, base uint16, offset uint16) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", base+offset), 5*time.Second)
	if err != nil {
		t.Fatalf("dial +%d endpoint: %v", offset, err)
	}
	_ = connection.Close()
}

func TestRunBecomesReadyWithoutLeaderAndReleasesEverythingOnCancel(t *testing.T) {
	running, configuration, base := startReadyRuntime(t, 1, nil)

	for index, service := range running.runtime.Services {
		select {
		case <-service.Ready():
		default:
			t.Fatalf("process ready before service %d (%s) reported ready", index, service.Name())
		}
	}
	dialRuntimePort(t, base, 5)
	dialRuntimePort(t, base, 6)
	dialRuntimePort(t, base, 8)
	if _, open := running.runtime.Gate.AdmissionEpoch(); open {
		t.Fatal("admission gate open without a committed coordinator epoch")
	}
	artifactDirectory := filepath.Join(configuration.StorageDir, ArtifactDirectoryName)
	if _, err := os.Stat(artifactDirectory); err != nil {
		t.Fatalf("worker artifact store directory missing: %v", err)
	}

	// One authenticated +6 status request is answered with a bound typed
	// rejection (Starting or a checked static redirect), never served stale.
	client, err := control.NewClient(control.ClientOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(runtimeTestSecret),
		Clock:         clock.NewReal(),
		MaxAttempts:   2,
	})
	if err != nil {
		t.Fatalf("construct control client: %v", err)
	}
	statusContext, statusCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer statusCancel()
	_, err = client.Status(statusContext, model.JobID{1})
	if err == nil {
		t.Fatal("status without a leader was served")
	}

	if err := running.stop(t); err != nil {
		t.Fatalf("runtime shutdown: %v", err)
	}
	if !probeRuntimePortBlock(t, base) {
		t.Fatal("a service listener survived shutdown")
	}
	clusterID, err := decodeClusterID(configuration.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(
		filepath.Join(configuration.StorageDir, worker.WorkerStoreDirectory),
		store.Identity{ClusterID: clusterID, NodeID: configuration.NodeID},
		store.Options{MaxBytes: configuration.Crane.MaxWorkerStoreBytes},
	)
	if err != nil {
		t.Fatalf("worker store lock survived shutdown: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened worker store: %v", err)
	}
}

func TestRunCancellationAtEveryPartialStartJoinsCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("partial-start matrix runs full runtimes")
	}
	configuration := runtimeTestConfig(t, 1, reserveRuntimeBasePort(t))
	prepareRuntimeStorage(t, configuration)
	probe, err := New(configuration, runtimeTestDependencies())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stages := len(probe.Services)

	for stage := -1; stage < stages; stage++ {
		name := "canceled-before-start"
		if stage >= 0 {
			name = fmt.Sprintf("canceled-after-%s-ready", probe.Services[stage].Name())
		}
		t.Run(name, func(t *testing.T) {
			for attempt := 0; attempt < 5; attempt++ {
				base := reserveRuntimeBasePort(t)
				stageConfiguration := runtimeTestConfig(t, 1, base)
				prepareRuntimeStorage(t, stageConfiguration)
				running := startRuntime(t, stageConfiguration, runtimeTestDependencies())
				lostPortRace := false
				if stage < 0 {
					running.cancel()
				} else {
					select {
					case <-running.runtime.Services[stage].Ready():
					case err := <-running.done:
						running.done <- err
						if isBindConflict(err) {
							lostPortRace = true
						} else {
							t.Fatalf("runtime ended before stage %d readiness: %v", stage, err)
						}
					case <-time.After(runtimeReadyTimeout):
						t.Fatalf("stage %d never became ready", stage)
					}
				}
				stopErr := running.stop(t)
				if lostPortRace || isBindConflict(stopErr) {
					continue
				}
				if !probeRuntimePortBlock(t, base) {
					t.Fatal("a listener or socket survived partial-start cancellation")
				}
				workerDirectory := filepath.Join(stageConfiguration.StorageDir, worker.WorkerStoreDirectory)
				if _, statErr := os.Stat(workerDirectory); statErr == nil {
					clusterID, err := decodeClusterID(stageConfiguration.ClusterID)
					if err != nil {
						t.Fatal(err)
					}
					reopened, err := store.Open(workerDirectory,
						store.Identity{ClusterID: clusterID, NodeID: stageConfiguration.NodeID},
						store.Options{MaxBytes: stageConfiguration.Crane.MaxWorkerStoreBytes})
					if err != nil {
						t.Fatalf("worker store lock survived cancellation: %v", err)
					}
					_ = reopened.Close()
				}
				return
			}
			t.Fatal("could not run the stage without losing port races")
		})
	}
}

func TestNonvoterRunServesWithoutRaftState(t *testing.T) {
	running, configuration, base := startReadyRuntime(t, 4, nil)

	dialRuntimePort(t, base, 5)
	dialRuntimePort(t, base, 6)
	raftListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+8))
	if err != nil {
		t.Fatalf("nonvoter bound the +8 Raft endpoint: %v", err)
	}
	_ = raftListener.Close()
	raftDirectory := filepath.Join(configuration.StorageDir, raft.RaftStorageDirectoryName)
	if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nonvoter created Raft durable state: %v", err)
	}
	if err := running.stop(t); err != nil {
		t.Fatalf("nonvoter shutdown: %v", err)
	}
	if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nonvoter created Raft durable state during shutdown: %v", err)
	}
}

// seedLegacyRaftSnapshot persists one legacy application snapshot into the
// node's durable Raft store exactly as an old binary would have left it.
func seedLegacyRaftSnapshot(t *testing.T, configuration config.NodeConfig, schema uint32, payload []byte) {
	t.Helper()
	voters, err := raft.NewVoterSet(configuration.RaftVoters)
	if err != nil {
		t.Fatal(err)
	}
	clusterID, err := decodeClusterID(configuration.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raft.NewStorageIdentity(raft.StorageFormatVersion1, clusterID, configuration.NodeID, voters)
	if err != nil {
		t.Fatal(err)
	}
	fileStore, err := raft.OpenFileStoreWithOptions(configuration.StorageDir, identity, voters,
		raft.StoreOptions{MaxSnapshotBytes: configuration.Raft.MaxSnapshotBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fileStore.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	entry, err := raft.NewEntry(1, 1, raft.EntryNoOp, nil)
	if err != nil {
		t.Fatal(err)
	}
	applied := uint64(1)
	if err := fileStore.Persist(raft.PersistenceBatch{
		HardState:    &raft.HardState{Term: 1, CommitIndex: 1},
		ReplaceFrom:  1,
		Entries:      []raft.Entry{entry},
		AppliedIndex: &applied,
	}); err != nil {
		t.Fatalf("seed committed legacy log: %v", err)
	}
	snapshot, err := raft.NewSnapshot(identity,
		raft.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1, StateMachineSchemaVersion: schema},
		payload, configuration.Raft.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileStore.PersistSnapshot(snapshot); err != nil {
		t.Fatalf("seed legacy snapshot: %v", err)
	}
}

func TestBootstrapMigrationThroughRealRuntime(t *testing.T) {
	t.Run("empty schema-1 snapshot migrates to the Crane schema", func(t *testing.T) {
		running, _, _ := startReadyRuntime(t, 1, func(t *testing.T, configuration config.NodeConfig) {
			seedLegacyRaftSnapshot(t, configuration, 1, []byte{})
		})
		machine := running.runtime.Machine
		if machine != running.runtime.actorOptions.Machine || machine != running.runtime.controlOptions.Machine ||
			running.runtime.raftOptions.StateMachine != raft.StateMachine(machine) {
			t.Fatal("migration runtime does not share one exact state machine")
		}
		if err := running.stop(t); err != nil {
			t.Fatalf("migration runtime shutdown: %v", err)
		}

		capture, err := machine.Capture(2, 1)
		if err != nil {
			t.Fatalf("capture after migration: %v", err)
		}
		if capture.SchemaVersion() != state.SnapshotSchemaVersion {
			t.Fatalf("post-migration snapshot schema = %d, want Crane schema %d", capture.SchemaVersion(), state.SnapshotSchemaVersion)
		}
		encoded, err := capture.MarshalBinary()
		if err != nil || len(encoded) == 0 {
			t.Fatalf("post-migration snapshot bytes = %d, err = %v", len(encoded), err)
		}
	})

	failing := []struct {
		name    string
		schema  uint32
		payload []byte
	}{
		{name: "nonempty legacy state fails closed", schema: 1, payload: []byte{1}},
		{name: "unknown legacy schema fails closed", schema: 7, payload: []byte{}},
	}
	for _, test := range failing {
		t.Run(test.name, func(t *testing.T) {
			for attempt := 0; attempt < 5; attempt++ {
				base := reserveRuntimeBasePort(t)
				configuration := runtimeTestConfig(t, 1, base)
				prepareRuntimeStorage(t, configuration)
				seedLegacyRaftSnapshot(t, configuration, test.schema, test.payload)

				runtime, err := New(configuration, runtimeTestDependencies())
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- runtime.Run(ctx) }()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("runtime accepted an unsupported legacy snapshot")
					}
					if isBindConflict(err) {
						continue
					}
					if !strings.Contains(err.Error(), "snapshot") && !strings.Contains(err.Error(), "schema") {
						t.Fatalf("runtime failed outside the snapshot migration: %v", err)
					}
				case <-runtime.Ready():
					t.Fatal("runtime became ready on an unsupported legacy snapshot")
				case <-time.After(runtimeReadyTimeout):
					t.Fatal("runtime neither failed nor stopped on an unsupported legacy snapshot")
				}
				return
			}
			t.Fatal("could not run the migration case without losing port races")
		})
	}
}

// scriptedFetchClient serves one sealed artifact stream in bounded chunks.
type scriptedFetchClient struct {
	mu        sync.Mutex
	artifact  protocol.ResultArtifact
	stream    []byte
	node      uint16
	epoch     model.WorkerEpoch
	chunkSize uint64
	corrupt   func(*protocol.ResultFetchChunk)
	requests  []protocol.ResultFetchRequest
}

func (client *scriptedFetchClient) Fetch(_ context.Context, node uint16, request protocol.ResultFetchRequest) (protocol.ResultFetchChunk, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.requests = append(client.requests, request)
	if node != client.node {
		return protocol.ResultFetchChunk{}, fmt.Errorf("unexpected fetch target %d", node)
	}
	end := request.Offset + client.chunkSize
	if end > client.artifact.TotalLength {
		end = client.artifact.TotalLength
	}
	chunk := protocol.ResultFetchChunk{
		Transfer: protocol.TransferChunk{
			JobID: client.artifact.JobID, TotalLength: client.artifact.TotalLength,
			Checksum: client.artifact.Checksum, Offset: request.Offset,
			Data:  append([]byte(nil), client.stream[request.Offset:end]...),
			Final: end == client.artifact.TotalLength,
		},
		Artifact:          client.artifact,
		SourceNodeID:      client.node,
		SourceWorkerEpoch: client.epoch,
		CoordinatorEpoch:  request.CoordinatorEpoch,
	}
	if client.corrupt != nil {
		client.corrupt(&chunk)
	}
	return chunk, nil
}

func sealedFetchFixture(t *testing.T) (protocol.ResultArtifact, []byte, []model.ResultRecord) {
	t.Helper()
	job := model.JobID{0xAA, 0x01}
	source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	sink := model.TaskID{JobID: job, StageID: 2, Partition: 0}
	specification := sha256.Sum256([]byte("runtime-fetch-spec"))
	value, err := model.MarshalTuple(model.Tuple{})
	if err != nil {
		t.Fatal(err)
	}
	records := make([]model.ResultRecord, 0, 3)
	for sequence := uint64(1); sequence <= 3; sequence++ {
		record, err := model.NewResultRecord(
			model.DeriveSourceTupleID(job, source, sequence), sink, specification, value)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	artifact, stream, err := worker.SealResultPartition(job, sink, specification, records)
	if err != nil {
		t.Fatal(err)
	}
	return artifact, stream, records
}

func fetchRequestFor(artifact protocol.ResultArtifact, node uint16, epoch model.WorkerEpoch) protocol.ResultFetchRequest {
	return protocol.ResultFetchRequest{
		Artifact: artifact, ReplicaNodeID: node, ReplicaWorkerEpoch: epoch,
		Offset:           0,
		CoordinatorEpoch: model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}},
	}
}

func TestArtifactFetcherStreamsVerifiedRecords(t *testing.T) {
	artifact, stream, records := sealedFetchFixture(t)
	epoch := model.WorkerEpoch{7}
	client := &scriptedFetchClient{artifact: artifact, stream: stream, node: 3, epoch: epoch, chunkSize: 7}
	fetcher := &artifactFetcher{client: client}

	opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(artifact, 3, epoch))
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer opened.Close()
	for index, want := range records {
		got, err := opened.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(%d): %v", index, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("record %d = %#v, want %#v", index, got, want)
		}
	}
	if _, err := opened.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("stream end = %v, want EOF", err)
	}
	if len(client.requests) < 2 {
		t.Fatalf("fetch used %d chunks, want a bounded multi-chunk assembly", len(client.requests))
	}
	for index, request := range client.requests {
		if request.Artifact != artifact || request.ReplicaNodeID != 3 || request.ReplicaWorkerEpoch != epoch {
			t.Fatalf("fetch request %d lost its binding: %#v", index, request)
		}
	}
}

func TestArtifactFetcherServesEmptyPartitions(t *testing.T) {
	job := model.JobID{0xBB, 0x02}
	sink := model.TaskID{JobID: job, StageID: 2, Partition: 1}
	specification := sha256.Sum256([]byte("runtime-empty-spec"))
	artifact, stream, err := worker.SealResultPartition(job, sink, specification, nil)
	if err != nil {
		t.Fatal(err)
	}
	epoch := model.WorkerEpoch{9}
	client := &scriptedFetchClient{artifact: artifact, stream: stream, node: 2, epoch: epoch, chunkSize: 8}
	fetcher := &artifactFetcher{client: client}
	opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(artifact, 2, epoch))
	if err != nil {
		t.Fatalf("OpenPartition(empty): %v", err)
	}
	defer opened.Close()
	if _, err := opened.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("empty stream end = %v, want EOF", err)
	}
}

func TestArtifactFetcherRejectsCorruptStreams(t *testing.T) {
	epoch := model.WorkerEpoch{7}
	tests := []struct {
		name    string
		corrupt func(*protocol.ResultFetchChunk)
	}{
		{name: "tampered payload", corrupt: func(chunk *protocol.ResultFetchChunk) {
			if chunk.Transfer.Final && len(chunk.Transfer.Data) > 0 {
				chunk.Transfer.Data[0] ^= 0xFF
			}
		}},
		{name: "foreign artifact identity", corrupt: func(chunk *protocol.ResultFetchChunk) {
			chunk.Artifact.RecordCount++
		}},
		{name: "foreign source", corrupt: func(chunk *protocol.ResultFetchChunk) {
			chunk.SourceNodeID = 9
		}},
		{name: "broken contiguity", corrupt: func(chunk *protocol.ResultFetchChunk) {
			chunk.Transfer.Offset++
		}},
		{name: "wrong transfer checksum", corrupt: func(chunk *protocol.ResultFetchChunk) {
			chunk.Transfer.Checksum = [32]byte{1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact, stream, _ := sealedFetchFixture(t)
			client := &scriptedFetchClient{artifact: artifact, stream: stream, node: 3, epoch: epoch, chunkSize: 7, corrupt: test.corrupt}
			fetcher := &artifactFetcher{client: client}
			if opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(artifact, 3, epoch)); err == nil {
				_ = opened.Close()
				t.Fatal("fetcher accepted a corrupt stream")
			}
		})
	}
}

func TestArtifactFetcherRejectsUnorderedOrMiscountedRecords(t *testing.T) {
	artifact, stream, records := sealedFetchFixture(t)
	epoch := model.WorkerEpoch{7}

	t.Run("record order violation", func(t *testing.T) {
		first, err := protocol.EncodedResultPageRecordBytes(records[0])
		if err != nil {
			t.Fatal(err)
		}
		second, err := protocol.EncodedResultPageRecordBytes(records[1])
		if err != nil {
			t.Fatal(err)
		}
		swapped := append(append(append([]byte(nil), second...), first...), stream[len(first)+len(second):]...)
		tampered := artifact
		tampered.Checksum = sha256.Sum256(swapped)
		client := &scriptedFetchClient{artifact: tampered, stream: swapped, node: 3, epoch: epoch, chunkSize: 64}
		fetcher := &artifactFetcher{client: client}
		opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(tampered, 3, epoch))
		if err != nil {
			return
		}
		defer opened.Close()
		for {
			if _, err := opened.Next(context.Background()); err != nil {
				if strings.Contains(err.Error(), "EOF") {
					t.Fatal("unordered records streamed to EOF")
				}
				return
			}
		}
	})

	t.Run("record count mismatch", func(t *testing.T) {
		tampered := artifact
		tampered.RecordCount--
		client := &scriptedFetchClient{artifact: tampered, stream: stream, node: 3, epoch: epoch, chunkSize: 64}
		fetcher := &artifactFetcher{client: client}
		opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(tampered, 3, epoch))
		if err != nil {
			return
		}
		defer opened.Close()
		for {
			if _, err := opened.Next(context.Background()); err != nil {
				if strings.Contains(err.Error(), "EOF") {
					t.Fatal("miscounted records streamed to EOF")
				}
				return
			}
		}
	})

	t.Run("oversize artifact refused before any fetch", func(t *testing.T) {
		tampered := artifact
		tampered.TotalLength = model.LimitsV1().MaxResultRecordsBytesPerJob + 1
		client := &scriptedFetchClient{artifact: tampered, stream: stream, node: 3, epoch: epoch, chunkSize: 64}
		fetcher := &artifactFetcher{client: client}
		if opened, err := fetcher.OpenPartition(context.Background(), fetchRequestFor(tampered, 3, epoch)); err == nil {
			_ = opened.Close()
			t.Fatal("fetcher accepted an over-limit artifact")
		}
		if len(client.requests) != 0 {
			t.Fatalf("over-limit artifact reached the network: %d requests", len(client.requests))
		}
	})
}

func TestLazyTupleDatagramBindsOnFirstUseAndRefusesAfterClose(t *testing.T) {
	base := reserveRuntimeBasePort(t)
	endpoint := config.Endpoint{Host: "127.0.0.1", Port: base + 7}

	t.Run("binds lazily and loops a datagram", func(t *testing.T) {
		datagram := newLazyTupleDatagram(endpoint)
		if !probeRuntimePortBlock(t, base) {
			t.Fatal("construction bound the +7 socket")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := datagram.SendFrom(ctx, endpoint, endpoint, []byte("ping")); err != nil {
			t.Fatalf("SendFrom: %v", err)
		}
		packet, err := datagram.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if string(packet.Data) != "ping" {
			t.Fatalf("packet = %q, want ping", packet.Data)
		}
		if err := datagram.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := datagram.SendFrom(ctx, endpoint, endpoint, []byte("x")); err == nil {
			t.Fatal("SendFrom succeeded after Close")
		}
		if !probeRuntimePortBlock(t, base) {
			t.Fatal("+7 socket survived Close")
		}
	})

	t.Run("close before first use stays closed", func(t *testing.T) {
		datagram := newLazyTupleDatagram(endpoint)
		if err := datagram.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := datagram.SendFrom(ctx, endpoint, endpoint, []byte("x")); err == nil {
			t.Fatal("closed lazy datagram accepted a send")
		}
		if _, err := datagram.Receive(ctx); err == nil {
			t.Fatal("closed lazy datagram accepted a receive")
		}
	})
}
