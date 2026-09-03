//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/testutil"
)

// buildGoBinaryWithTags builds one package into a test-owned path with the
// given build tags and extra build flags.
func buildGoBinaryWithTags(t *testing.T, repositoryRoot, name, packagePath string, tags []string, extra ...string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	args := []string{"build"}
	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}
	args = append(args, extra...)
	args = append(args, "-o", binary, packagePath)
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, output)
	}
	return binary
}

// startManagedProcessWithFiles launches one process in its own process group
// with additional inherited descriptors (ExtraFiles[i] becomes descriptor
// 3+i in the child) and captures its combined output.
func startManagedProcessWithFiles(binary string, args []string, name string, files []*os.File) (*nodeProcess, error) {
	process := &nodeProcess{name: name, log: &synchronizedLog{}, done: make(chan struct{})}
	process.command = exec.Command(binary, args...)
	process.command.Stdout = process.log
	process.command.Stderr = process.log
	process.command.ExtraFiles = files
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

// startWithFiles registers a process launched with inherited descriptors.
func (h *processHarness) startWithFiles(binary string, args []string, name string, files []*os.File) *nodeProcess {
	h.t.Helper()
	process, err := startManagedProcessWithFiles(binary, args, name, files)
	if err != nil {
		h.t.Fatalf("start %s: %v\n%s", name, err, h.logs())
	}
	h.mu.Lock()
	h.processes = append(h.processes, process)
	h.mu.Unlock()
	return process
}

// exited reports whether the process has terminated.
func (p *nodeProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// signal delivers one signal to the whole process group without waiting.
func (p *nodeProcess) signal(signal syscall.Signal) error {
	if err := syscall.Kill(-p.command.Process.Pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// waitExit waits for the process to terminate for any reason.
func (p *nodeProcess) waitExit(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForNormalNodeReadiness waits for the documented single readiness line
// of a node process and fails if the process exits first.
func waitForNormalNodeReadiness(t *testing.T, ctx context.Context, harness *processHarness, process *nodeProcess, nodeID uint16) {
	t.Helper()
	want := node.ReadySignal(nodeID)
	waitForCluster(t, ctx, harness, fmt.Sprintf("node %d normal readiness line", nodeID), func() (bool, error) {
		select {
		case <-process.done:
			return false, fmt.Errorf("process exited before readiness: %v", process.result())
		default:
		}
		logs := process.log.String()
		if strings.Count(logs, want) > 1 {
			return false, fmt.Errorf("readiness line %q appeared more than once in %q", want, logs)
		}
		return strings.Contains(logs, want), nil
	})
}

// craneIntegrationConfigs builds a 4-node (voters 1–3, nonvoter 4) real
// loopback cluster configuration with fast but legal SWIM/Raft/Crane timing.
// mutate, when non-nil, adjusts each configuration before validation.
func craneIntegrationConfigs(t *testing.T, startingBasePort uint16, secretFile string, nodes int, mutate func(*config.NodeConfig)) []config.NodeConfig {
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
		crane := config.DefaultCraneConfig()
		crane.WorkerSlots = 16
		crane.WorkerControlTimeout = config.Duration(time.Second)
		crane.FailureGracePeriod = config.Duration(2 * time.Second)
		// Retries are fsyncs: a retained outbox retried into a Closed
		// destination is re-dispatched durably each interval, so keep the
		// interval production-like to bound the fsync load.
		crane.TupleRetryInterval = config.Duration(time.Second)
		crane.TupleCompletionRetryInterval = config.Duration(2 * time.Second)
		// Real fsync-bound retry bursts stall heartbeats; a wider election
		// window keeps leadership stable so failover is caused by the
		// scenario, not by load.
		raft := config.DefaultRaftConfig()
		raft.ElectionTimeoutMin = config.Duration(2 * time.Second)
		raft.ElectionTimeoutMax = config.Duration(4 * time.Second)
		raft.HeartbeatInterval = config.Duration(300 * time.Millisecond)
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
				SuspicionMultiplier:  4,
				IndirectChecks:       2,
				ReplayWindow:         config.Duration(time.Minute),
			},
			Raft:  raft,
			Crane: crane,
		}
		if mutate != nil {
			mutate(&configuration)
		}
		if err := configuration.Validate(); err != nil {
			t.Fatalf("validate node %d config: %v", index+1, err)
		}
		configurations[index] = configuration
	}
	return configurations
}

// endpointBindable reports whether the typed endpoint can currently be bound,
// i.e. no process holds it.
func endpointBindable(endpoint config.Endpoint, transport config.Transport) bool {
	address := endpoint.String()
	if transport == config.TransportUDP {
		udpAddress, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			return false
		}
		socket, err := net.ListenUDP("udp", udpAddress)
		if err != nil {
			return false
		}
		_ = socket.Close()
		return true
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return false
	}
	_ = listener.Close()
	return true
}

// waitCondition polls a condition until it holds or the context ends.
func waitCondition(ctx context.Context, interval time.Duration, condition func() (bool, error)) error {
	return testutil.WaitFor(ctx, interval, condition)
}
