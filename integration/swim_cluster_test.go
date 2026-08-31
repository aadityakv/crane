//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	internalrandom "github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/testutil"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	integrationClusterID      = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	integrationNodePortStride = 100
)

func TestTypedPortAllocatorUsesAuthoritativeRegistryBounds(t *testing.T) {
	snapshotOffset, maxOffset, err := typedServicePortBounds()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := config.LookupService(config.ServiceSWIMSnapshot)
	if !ok {
		t.Fatal("SWIM snapshot service is not registered")
	}
	if snapshotOffset != int(snapshot.Offset) {
		t.Fatalf("snapshot offset = %d, want registry offset %d", snapshotOffset, snapshot.Offset)
	}
	wantMax := 0
	for _, service := range config.Services() {
		if int(service.Offset) > wantMax {
			wantMax = int(service.Offset)
		}
	}
	if maxOffset != wantMax {
		t.Fatalf("maximum offset = %d, want registry maximum %d", maxOffset, wantMax)
	}
}

func TestLocalClusterLauncher(t *testing.T) {
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	nodeBinary := buildNodeBinary(t, repositoryRoot)
	clusterBinary := buildClusterBinary(t, repositoryRoot)
	secret := []byte("launcher-integration-secret-32bytes")
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(secretFile, 0o600); err != nil {
		t.Fatal(err)
	}
	basePort, releasePorts := reserveTypedClusterPorts(t, 3)
	t.Cleanup(releasePorts)
	dataRoot := filepath.Join(t.TempDir(), "launcher-data")
	releasePorts()

	harness := newProcessHarness(t)
	launcher := harness.startCluster(clusterBinary, nodeBinary, dataRoot, secretFile, basePort)
	testContext, cancelTest := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTest()
	configurations := waitForLauncherConfigs(t, testContext, harness, launcher, dataRoot, 3)
	clients := newSnapshotClients(t, configurations, secret)
	waitForCluster(t, testContext, harness, "launcher nodes to share one Alive view", func() (bool, error) {
		views, err := clients.views(testContext, 1, 2, 3)
		if err != nil {
			return false, err
		}
		for observer, members := range views {
			if len(members) != 3 || !hasMember(members, 1, swim.Alive, 0) || !hasMember(members, 2, swim.Alive, 0) || !hasMember(members, 3, swim.Alive, 0) {
				return false, fmt.Errorf("observer %d view = %#v", observer, members)
			}
		}
		return true, nil
	})
	readyLine := "[node-1] CS425_NODE_READY node_id=1"
	if logs := launcher.log.String(); !strings.Contains(logs, readyLine) {
		t.Fatalf("launcher logs do not contain seed readiness %q:\n%s", readyLine, harness.logs())
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	if err := launcher.terminate(shutdownContext); err != nil {
		t.Fatalf("terminate cluster launcher: %v\n%s", err, harness.logs())
	}
}

func TestLocalSWIMCluster(t *testing.T) {
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := buildNodeBinary(t, repositoryRoot)
	secret := []byte("integration-cluster-secret-32bytes")
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(secretFile, 0o600); err != nil {
		t.Fatal(err)
	}

	basePort, releasePorts := reserveTypedClusterPorts(t, 3)
	t.Cleanup(releasePorts)
	configurations := integrationConfigs(t, basePort, secretFile)
	configPaths := writeIntegrationConfigs(t, configurations)
	releasePorts()

	harness := newProcessHarness(t)
	clients := newSnapshotClients(t, configurations, secret)
	testContext, cancelTest := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTest()

	seed := harness.start(binary, configPaths[0], "node-1")
	waitForCluster(t, testContext, harness, "seed to admit itself", func() (bool, error) {
		members, err := clients.snapshot(testContext, 1)
		return hasMember(members, 1, swim.Alive, 0), err
	})

	harness.start(binary, configPaths[1], "node-2")
	node3 := harness.start(binary, configPaths[2], "node-3")
	var originalNode3Incarnation uint64
	waitForCluster(t, testContext, harness, "all nodes to share the initial Alive view", func() (bool, error) {
		views, err := clients.views(testContext, 1, 2, 3)
		if err != nil {
			return false, err
		}
		for observer, members := range views {
			if len(members) != 3 || !hasMember(members, 1, swim.Alive, 0) || !hasMember(members, 2, swim.Alive, 0) || !hasMember(members, 3, swim.Alive, 0) {
				return false, fmt.Errorf("observer %d view = %#v", observer, members)
			}
		}
		originalNode3Incarnation = memberByID(views[1], 3).Incarnation
		return true, nil
	})

	if err := node3.kill(testContext); err != nil {
		t.Fatalf("kill node 3: %v\n%s", err, harness.logs())
	}
	waitForCluster(t, testContext, harness, "nodes 1 and 2 to observe node 3 Suspect", func() (bool, error) {
		return clients.allHave(testContext, []uint16{1, 2}, 3, swim.Suspect, originalNode3Incarnation)
	})
	waitForCluster(t, testContext, harness, "nodes 1 and 2 to observe node 3 Dead", func() (bool, error) {
		return clients.allHave(testContext, []uint16{1, 2}, 3, swim.Dead, originalNode3Incarnation)
	})

	restartedNode3 := harness.start(binary, configPaths[2], "node-3-restart")
	waitForCluster(t, testContext, harness, "restarted node 3 to rejoin Alive at a higher incarnation", func() (bool, error) {
		views, err := clients.views(testContext, 1, 2, 3)
		if err != nil {
			return false, err
		}
		for observer, members := range views {
			restarted := memberByID(members, 3)
			if restarted.Status != swim.Alive || restarted.Incarnation <= originalNode3Incarnation {
				return false, fmt.Errorf("observer %d restarted node 3 = %#v, prior incarnation %d", observer, restarted, originalNode3Incarnation)
			}
		}
		return true, nil
	})

	if err := seed.kill(testContext); err != nil {
		t.Fatalf("kill introducer node 1: %v\n%s", err, harness.logs())
	}
	waitForCluster(t, testContext, harness, "nodes 2 and 3 to keep probing and agree the introducer is Dead", func() (bool, error) {
		views, err := clients.views(testContext, 2, 3)
		if err != nil {
			return false, err
		}
		for observer, members := range views {
			if !hasMember(members, 1, swim.Dead, 0) || !hasMember(members, 2, swim.Alive, 0) || !hasMember(members, 3, swim.Alive, originalNode3Incarnation+1) {
				return false, fmt.Errorf("observer %d post-introducer view = %#v", observer, members)
			}
		}
		return true, nil
	})

	if err := restartedNode3.terminate(testContext); err != nil {
		t.Fatalf("gracefully terminate restarted node 3: %v\n%s", err, harness.logs())
	}
	waitForCluster(t, testContext, harness, "node 2 to observe node 3 graceful Left", func() (bool, error) {
		return clients.allHave(testContext, []uint16{2}, 3, swim.Left, originalNode3Incarnation+2)
	})
}

func buildNodeBinary(t *testing.T, repositoryRoot string) string {
	return buildGoBinary(t, repositoryRoot, "cs425-node", "./cmd/node")
}

func buildClusterBinary(t *testing.T, repositoryRoot string) string {
	return buildGoBinary(t, repositoryRoot, "cs425-cluster", "./cmd/cluster")
}

func buildGoBinary(t *testing.T, repositoryRoot, name, packagePath string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, packagePath)
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return binary
}

func waitForLauncherConfigs(t *testing.T, ctx context.Context, harness *processHarness, launcher *nodeProcess, dataRoot string, nodes int) []config.NodeConfig {
	t.Helper()
	configurations := make([]config.NodeConfig, nodes)
	err := testutil.WaitFor(ctx, 25*time.Millisecond, func() (bool, error) {
		select {
		case <-launcher.done:
			return false, fmt.Errorf("launcher exited while generating configs: %v", launcher.result())
		default:
		}
		for index := range configurations {
			path := filepath.Join(dataRoot, "configs", fmt.Sprintf("node-%d.json", index+1))
			loaded, err := config.Load(path)
			if err != nil {
				return false, err
			}
			configurations[index] = loaded
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for launcher configs: %v\n%s", err, harness.logs())
	}
	return configurations
}

func integrationConfigs(t *testing.T, startingBasePort uint16, secretFile string) []config.NodeConfig {
	return integrationConfigsForNodes(t, startingBasePort, secretFile, 3)
}

func integrationConfigsForNodes(t *testing.T, startingBasePort uint16, secretFile string, nodes int) []config.NodeConfig {
	t.Helper()
	voters := make([]config.RaftVoter, 3)
	for index := range voters {
		base := startingBasePort + uint16(index*integrationNodePortStride)
		endpoint, err := (config.NodeConfig{AdvertiseHost: "127.0.0.1", BasePort: base}).AdvertiseEndpoint(config.ServiceRaftRPC)
		if err != nil {
			t.Fatal(err)
		}
		voters[index] = config.RaftVoter{NodeID: uint16(index + 1), Endpoint: endpoint.String()}
	}
	introducer, err := (config.NodeConfig{AdvertiseHost: "127.0.0.1", BasePort: startingBasePort}).AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	configurations := make([]config.NodeConfig, nodes)
	for index := range configurations {
		storageDir := filepath.Join(root, fmt.Sprintf("node-%d", index+1))
		if err := os.MkdirAll(storageDir, 0o700); err != nil {
			t.Fatal(err)
		}
		store := swim.NewFileIncarnationStore(filepath.Join(storageDir, swim.IncarnationStateFilename))
		if err := store.Store(1); err != nil {
			t.Fatalf("initialize node %d incarnation: %v", index+1, err)
		}
		configuration := config.NodeConfig{
			NodeID:            uint16(index + 1),
			ClusterID:         integrationClusterID,
			BindHost:          "127.0.0.1",
			AdvertiseHost:     "127.0.0.1",
			BasePort:          startingBasePort + uint16(index*integrationNodePortStride),
			Introducer:        introducer.String(),
			StorageDir:        storageDir,
			ClusterSecretFile: secretFile,
			RaftVoters:        append([]config.RaftVoter(nil), voters...),
			Timing: config.TimingConfig{
				ProbeInterval:        config.Duration(200 * time.Millisecond),
				DirectProbeTimeout:   config.Duration(50 * time.Millisecond),
				IndirectProbeTimeout: config.Duration(50 * time.Millisecond),
				SuspicionMultiplier:  2,
				IndirectChecks:       2,
				ReplayWindow:         config.Duration(time.Minute),
			},
			Raft: config.DefaultRaftConfig(),
			Crane: config.DefaultCraneConfig(),
		}
		if err := configuration.Validate(); err != nil {
			t.Fatalf("validate node %d config: %v", index+1, err)
		}
		configurations[index] = configuration
	}
	return configurations
}

func writeIntegrationConfigs(t *testing.T, configurations []config.NodeConfig) []string {
	t.Helper()
	directory := t.TempDir()
	paths := make([]string, len(configurations))
	for index, configuration := range configurations {
		content, err := json.MarshalIndent(configuration, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(path, append(content, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[index] = path
	}
	return paths
}

type portReservation struct {
	closers []io.Closer
	once    sync.Once
}

func (r *portReservation) release() {
	r.once.Do(func() {
		for _, closer := range r.closers {
			_ = closer.Close()
		}
	})
}

func reserveTypedClusterPorts(t *testing.T, nodes int) (uint16, func()) {
	t.Helper()
	snapshotOffset, maxOffset, err := typedServicePortBounds()
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 100; attempt++ {
		seed, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := seed.Addr().(*net.TCPAddr).Port
		_ = seed.Close()
		candidate := port - snapshotOffset
		if candidate < 1024 || candidate+integrationNodePortStride*(nodes-1)+maxOffset > 65535 {
			continue
		}
		reservation := &portReservation{}
		valid := true
		for nodeIndex := 0; nodeIndex < nodes && valid; nodeIndex++ {
			base := candidate + nodeIndex*integrationNodePortStride
			for _, service := range config.Services() {
				address := fmt.Sprintf("127.0.0.1:%d", base+int(service.Offset))
				var closer io.Closer
				if service.Transport == config.TransportUDP {
					udpAddress, resolveErr := net.ResolveUDPAddr("udp", address)
					if resolveErr == nil {
						closer, err = net.ListenUDP("udp", udpAddress)
					} else {
						err = resolveErr
					}
				} else {
					closer, err = net.Listen("tcp", address)
				}
				if err != nil {
					valid = false
					break
				}
				reservation.closers = append(reservation.closers, closer)
			}
		}
		if valid {
			return uint16(candidate), reservation.release
		}
		reservation.release()
	}
	t.Fatalf("could not reserve a free typed %d-node port range", nodes)
	return 0, func() {}
}

func typedServicePortBounds() (int, int, error) {
	snapshot, ok := config.LookupService(config.ServiceSWIMSnapshot)
	if !ok {
		return 0, 0, fmt.Errorf("SWIM snapshot service is not registered")
	}
	services := config.Services()
	if len(services) == 0 {
		return 0, 0, fmt.Errorf("service registry is empty")
	}
	maxOffset := 0
	for _, service := range services {
		if int(service.Offset) > maxOffset {
			maxOffset = int(service.Offset)
		}
	}
	return int(snapshot.Offset), maxOffset, nil
}

type synchronizedLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (l *synchronizedLog) Write(content []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.Write(content)
}

func (l *synchronizedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.String()
}

type nodeProcess struct {
	name              string
	command           *exec.Cmd
	log               *synchronizedLog
	done              chan struct{}
	gracefulOnCleanup bool
	mu                sync.Mutex
	waitErr           error
}

func startNodeProcess(binary, configPath, name string) (*nodeProcess, error) {
	return startManagedProcess(binary, []string{"-config", configPath}, name, false)
}

func startClusterProcess(clusterBinary, nodeBinary, dataRoot, secretFile string, basePort uint16) (*nodeProcess, error) {
	return startManagedProcess(clusterBinary, []string{
		"-nodes", "3",
		"-base-port", fmt.Sprint(basePort),
		"-data-root", dataRoot,
		"-secret-file", secretFile,
		"-node-binary", nodeBinary,
	}, "cluster-launcher", true)
}

func startManagedProcess(binary string, args []string, name string, gracefulOnCleanup bool) (*nodeProcess, error) {
	process := &nodeProcess{name: name, log: &synchronizedLog{}, done: make(chan struct{}), gracefulOnCleanup: gracefulOnCleanup}
	process.command = exec.Command(binary, args...)
	process.command.Stdout = process.log
	process.command.Stderr = process.log
	process.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := process.command.Start(); err != nil {
		return nil, err
	}
	go func() {
		err := process.command.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

func (p *nodeProcess) kill(ctx context.Context) error {
	return p.signalAndWait(ctx, syscall.SIGKILL, false)
}

func (p *nodeProcess) terminate(ctx context.Context) error {
	return p.signalAndWait(ctx, syscall.SIGTERM, true)
}

func (p *nodeProcess) signalAndWait(ctx context.Context, signal syscall.Signal, requireClean bool) error {
	select {
	case <-p.done:
		return fmt.Errorf("process already exited: %v", p.result())
	default:
	}
	if err := syscall.Kill(-p.command.Process.Pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	select {
	case <-p.done:
		if requireClean && p.result() != nil {
			return fmt.Errorf("process exit: %w", p.result())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *nodeProcess) result() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *nodeProcess) cleanup() {
	select {
	case <-p.done:
		return
	default:
	}
	signal := syscall.SIGKILL
	timeout := 5 * time.Second
	if p.gracefulOnCleanup {
		signal = syscall.SIGTERM
		timeout = 20 * time.Second
	}
	_ = syscall.Kill(-p.command.Process.Pid, signal)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-timer.C:
		_ = p.command.Process.Kill()
		timer.Reset(5 * time.Second)
		select {
		case <-p.done:
		case <-timer.C:
		}
	}
}

type processHarness struct {
	t         *testing.T
	mu        sync.Mutex
	processes []*nodeProcess
}

func newProcessHarness(t *testing.T) *processHarness {
	harness := &processHarness{t: t}
	t.Cleanup(func() {
		harness.mu.Lock()
		processes := append([]*nodeProcess(nil), harness.processes...)
		harness.mu.Unlock()
		for _, process := range processes {
			process.cleanup()
		}
	})
	return harness
}

func (h *processHarness) start(binary, configPath, name string) *nodeProcess {
	h.t.Helper()
	process, err := startNodeProcess(binary, configPath, name)
	if err != nil {
		h.t.Fatalf("start %s: %v\n%s", name, err, h.logs())
	}
	h.mu.Lock()
	h.processes = append(h.processes, process)
	h.mu.Unlock()
	return process
}

func (h *processHarness) startCluster(clusterBinary, nodeBinary, dataRoot, secretFile string, basePort uint16) *nodeProcess {
	h.t.Helper()
	process, err := startClusterProcess(clusterBinary, nodeBinary, dataRoot, secretFile, basePort)
	if err != nil {
		h.t.Fatalf("start cluster launcher: %v\n%s", err, h.logs())
	}
	h.mu.Lock()
	h.processes = append(h.processes, process)
	h.mu.Unlock()
	return process
}

func (h *processHarness) logs() string {
	h.mu.Lock()
	processes := append([]*nodeProcess(nil), h.processes...)
	h.mu.Unlock()
	var output bytes.Buffer
	for _, process := range processes {
		fmt.Fprintf(&output, "--- %s (wait=%v) ---\n%s", process.name, process.result(), process.log.String())
	}
	return output.String()
}

type snapshotClients struct {
	clients   map[uint16]*swim.SnapshotClient
	endpoints map[uint16]config.Endpoint
}

func newSnapshotClients(t *testing.T, configurations []config.NodeConfig, secret []byte) *snapshotClients {
	t.Helper()
	result := &snapshotClients{clients: make(map[uint16]*swim.SnapshotClient), endpoints: make(map[uint16]config.Endpoint)}
	for _, configuration := range configurations {
		client, err := swim.NewSnapshotClient(swim.SnapshotClientOptions{
			Config:        configuration,
			Authenticator: wire.NewHMACAuthenticator(secret),
			Clock:         clock.NewReal(),
			Random:        internalrandom.NewLockedSource(int64(1000 + configuration.NodeID)),
			IOTimeout:     500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("create snapshot client %d: %v", configuration.NodeID, err)
		}
		endpoint, err := configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		result.clients[configuration.NodeID] = client
		result.endpoints[configuration.NodeID] = endpoint
	}
	return result
}

func (c *snapshotClients) snapshot(ctx context.Context, nodeID uint16) ([]swim.Member, error) {
	members, err := c.clients[nodeID].Snapshot(ctx, c.endpoints[nodeID])
	if err != nil {
		return nil, fmt.Errorf("snapshot node %d: %w", nodeID, err)
	}
	return members, nil
}

func (c *snapshotClients) views(ctx context.Context, observers ...uint16) (map[uint16][]swim.Member, error) {
	views := make(map[uint16][]swim.Member, len(observers))
	for _, observer := range observers {
		members, err := c.snapshot(ctx, observer)
		if err != nil {
			return nil, err
		}
		views[observer] = members
	}
	return views, nil
}

func (c *snapshotClients) allHave(ctx context.Context, observers []uint16, memberID uint16, status swim.Status, incarnation uint64) (bool, error) {
	views, err := c.views(ctx, observers...)
	if err != nil {
		return false, err
	}
	for observer, members := range views {
		if !hasMember(members, memberID, status, incarnation) {
			return false, fmt.Errorf("observer %d view = %#v, want node %d status %d incarnation >= %d", observer, members, memberID, status, incarnation)
		}
	}
	return true, nil
}

func memberByID(members []swim.Member, nodeID uint16) swim.Member {
	for _, member := range members {
		if member.NodeID == nodeID {
			return member
		}
	}
	return swim.Member{}
}

func hasMember(members []swim.Member, nodeID uint16, status swim.Status, minimumIncarnation uint64) bool {
	member := memberByID(members, nodeID)
	return member.NodeID == nodeID && member.Status == status && member.Incarnation >= minimumIncarnation
}

func waitForCluster(t *testing.T, ctx context.Context, harness *processHarness, description string, condition func() (bool, error)) {
	t.Helper()
	if err := testutil.WaitFor(ctx, 25*time.Millisecond, condition); err != nil {
		t.Fatalf("wait for %s: %v\n%s", description, err, harness.logs())
	}
}
