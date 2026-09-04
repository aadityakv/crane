package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/config"
	craneworker "github.com/aadityakv/crane/internal/crane/worker"
	internalnode "github.com/aadityakv/crane/internal/node"
	"github.com/aadityakv/crane/internal/raft"
)

func TestRunSupervisedNodeEmitsSignalOnlyAfterServiceReady(t *testing.T) {
	service := newReadyControlledService("controlled")
	output := &channelWriter{writes: make(chan string, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runSupervisedNode(ctx, 7, []internalnode.Service{service}, output) }()

	<-service.started
	select {
	case line := <-output.writes:
		t.Fatalf("readiness emitted before service ready: %q", line)
	default:
	}
	close(service.ready)
	select {
	case line := <-output.writes:
		if want := internalnode.ReadySignal(7) + "\n"; line != want {
			t.Fatalf("readiness line = %q, want %q", line, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for readiness line")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runSupervisedNode: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for supervised node shutdown")
	}
}

func TestRunSupervisedNodeEmitsOneSignalAfterEveryServiceReady(t *testing.T) {
	first := newReadyControlledService("swim")
	second := newReadyControlledService("raft")
	output := &channelWriter{writes: make(chan string, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- runSupervisedNode(ctx, 9, []internalnode.Service{first, second}, output)
	}()

	<-first.started
	<-second.started
	close(first.ready)
	select {
	case line := <-output.writes:
		t.Fatalf("readiness emitted before Raft ready: %q", line)
	default:
	}
	close(second.ready)
	select {
	case line := <-output.writes:
		if want := internalnode.ReadySignal(9) + "\n"; line != want {
			t.Fatalf("readiness line = %q, want %q", line, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for aggregate readiness line")
	}
	select {
	case duplicate := <-output.writes:
		t.Fatalf("duplicate readiness line %q", duplicate)
	default:
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("runSupervisedNode: %v", err)
	}
}

func TestRunSupervisedNodeRejectsEmptyOrNilServicesBeforeWriting(t *testing.T) {
	tests := []struct {
		name     string
		services []internalnode.Service
	}{
		{name: "nil slice", services: nil},
		{name: "empty slice", services: []internalnode.Service{}},
		{name: "nil service", services: []internalnode.Service{nil}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := &channelWriter{writes: make(chan string, 1)}
			if err := runSupervisedNode(context.Background(), 1, test.services, output); err == nil {
				t.Fatal("runSupervisedNode succeeded")
			}
			select {
			case line := <-output.writes:
				t.Fatalf("wrote readiness for invalid services: %q", line)
			default:
			}
		})
	}
}

func TestRunSupervisedNodeOutputFailureCancelsAndJoinsEveryService(t *testing.T) {
	first := newReadyControlledService("swim")
	second := newReadyControlledService("raft")
	close(first.ready)
	close(second.ready)
	writeFailure := errors.New("stdout unavailable")
	result := make(chan error, 1)
	go func() {
		result <- runSupervisedNode(context.Background(), 2, []internalnode.Service{first, second}, failingWriter{err: writeFailure})
	}()

	<-first.started
	<-second.started
	if err := <-result; !errors.Is(err, writeFailure) {
		t.Fatalf("runSupervisedNode error = %v, want output failure %v", err, writeFailure)
	}
	select {
	case <-first.returned:
	default:
		t.Fatal("first service was not joined before return")
	}
	select {
	case <-second.returned:
	default:
		t.Fatal("second service was not joined before return")
	}
}

func TestNewLocalRuntimeComposesOnlyConfiguredVotersWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name      string
		nodeID    uint16
		wantNames []string
		wantRaft  bool
	}{
		{
			name:   "voter",
			nodeID: 1,
			wantNames: []string{
				"swim", "crane-membership-authorizer", "raft",
				"crane-worker", "crane-coordinator", "crane-control",
			},
			wantRaft: true,
		},
		{
			name:   "nonvoter",
			nodeID: 4,
			wantNames: []string{
				"swim", "crane-membership-authorizer",
				"crane-worker", "crane-control",
			},
			wantRaft: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := writeNodeTestConfig(t)
			configuration.NodeID = test.nodeID
			reservedRaftListener := reserveConfiguredRaftEndpoint(t, &configuration)
			defer reservedRaftListener.Close()
			if err := configuration.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			raftDirectory := filepath.Join(configuration.StorageDir, raft.RaftStorageDirectoryName)
			workerDirectory := filepath.Join(configuration.StorageDir, craneworker.WorkerStoreDirectory)
			if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Raft directory exists before construction: %v", err)
			}

			runtime, err := newLocalRuntime(configuration)
			if err != nil {
				t.Fatalf("newLocalRuntime: %v", err)
			}
			if runtime == nil || runtime.SWIM == nil || runtime.Machine == nil || runtime.Gate == nil ||
				runtime.Worker == nil || runtime.Control == nil ||
				(runtime.Raft != nil) != test.wantRaft || (runtime.Coordinator != nil) != test.wantRaft {
				t.Fatalf("runtime composition = %#v, wantRaft=%t", runtime, test.wantRaft)
			}
			gotNames := make([]string, len(runtime.Services))
			for index, service := range runtime.Services {
				gotNames[index] = service.Name()
			}
			if !reflect.DeepEqual(gotNames, test.wantNames) {
				t.Fatalf("service names = %v, want %v", gotNames, test.wantNames)
			}
			if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("construction created Raft durable state: %v", err)
			}
			if _, err := os.Stat(workerDirectory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("construction created Crane worker durable state: %v", err)
			}
		})
	}
}

func TestNewLocalRuntimeRejectsConsensusFingerprintMismatchBeforeServiceConstruction(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	configuration.Crane.ConsensusFingerprint = strings.Repeat("0", 64)
	raftDirectory := filepath.Join(configuration.StorageDir, raft.RaftStorageDirectoryName)

	runtime, err := newLocalRuntime(configuration)
	if err == nil || runtime != nil {
		t.Fatalf("newLocalRuntime accepted consensus mismatch: runtime=%#v err=%v", runtime, err)
	}
	if _, statErr := os.Stat(raftDirectory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("consensus mismatch reached Raft service construction: %v", statErr)
	}
}

func reserveConfiguredRaftEndpoint(t *testing.T, configuration *config.NodeConfig) net.Listener {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if port <= 1032 {
			_ = listener.Close()
			continue
		}
		configuration.BasePort = uint16(port - 8)
		if _, voter := configuration.RaftVoterByID(configuration.NodeID); voter {
			for index := range configuration.RaftVoters {
				if configuration.RaftVoters[index].NodeID == configuration.NodeID {
					configuration.RaftVoters[index].Endpoint = net.JoinHostPort(configuration.AdvertiseHost, fmt.Sprint(port))
				}
			}
		}
		return listener
	}
	t.Fatal("could not reserve a Raft endpoint")
	return nil
}

func TestNodeRuntimeMachinePreservesBootstrapMigration(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	reservedRaftListener := reserveConfiguredRaftEndpoint(t, &configuration)
	defer reservedRaftListener.Close()
	runtime, err := newLocalRuntime(configuration)
	if err != nil {
		t.Fatalf("newLocalRuntime: %v", err)
	}
	machine := runtime.Machine
	for _, schema := range []uint32{0, 1} {
		if err := machine.Restore(schema, nil); err != nil {
			t.Fatalf("Restore(empty schema %d): %v", schema, err)
		}
	}
	for _, test := range []struct {
		name    string
		schema  uint32
		payload []byte
	}{
		{name: "unknown schema", schema: 7},
		{name: "nonempty legacy", schema: 1, payload: []byte{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := machine.Restore(test.schema, test.payload); err == nil {
				t.Fatal("Restore accepted unsupported legacy snapshot")
			}
		})
	}
	capture, err := machine.Capture(1, 1)
	if err != nil {
		t.Fatalf("Capture after bootstrap restore: %v", err)
	}
	if capture.SchemaVersion() <= 1 {
		t.Fatalf("post-bootstrap snapshot schema = %d, want the Crane schema", capture.SchemaVersion())
	}
}

func TestLoadNodeConfigurationAppliesOnlyLocalOverrides(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	configPath := writeNodeTestConfigFile(t, configuration)

	got, err := loadNodeConfiguration([]string{
		"-config", configPath,
		"-node-id", "4",
		"-bind-host", "localhost",
		"-advertise-host", "localhost",
		"-base-port", "8300",
		"-storage-dir", filepath.Join(t.TempDir(), "overridden-node-4"),
	})
	if err != nil {
		t.Fatalf("loadNodeConfiguration: %v", err)
	}
	if got.NodeID != 4 || got.BindHost != "localhost" || got.AdvertiseHost != "localhost" || got.BasePort != 8300 {
		t.Fatalf("local overrides not applied: %#v", got)
	}
	if got.ClusterID != configuration.ClusterID || got.Introducer != configuration.Introducer || got.ClusterSecretFile != configuration.ClusterSecretFile {
		t.Fatalf("file-controlled security or cluster fields changed: %#v", got)
	}
	if !reflect.DeepEqual(got.RaftVoters, configuration.RaftVoters) || got.Timing != configuration.Timing || got.Raft != configuration.Raft {
		t.Fatalf("file-controlled voters, timing, or raft settings changed: %#v", got)
	}
}

func TestLoadNodeConfigurationRejectsUnknownOrMissingConfigFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing_config", args: nil},
		{name: "unknown_flag", args: []string{"-not-a-node-option", "value"}},
		{name: "positional_argument", args: []string{"configuration.json"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := loadNodeConfiguration(test.args); err == nil {
				t.Fatal("loadNodeConfiguration succeeded")
			}
		})
	}
}

func TestNewSWIMServiceDoesNotBootstrapMissingIdentityState(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	incarnationPath := filepath.Join(configuration.StorageDir, incarnationStateFilename)
	if _, err := os.Stat(incarnationPath); !os.IsNotExist(err) {
		t.Fatalf("incarnation state exists before construction: %v", err)
	}

	service, err := newSWIMService(configuration)
	if err != nil {
		t.Fatalf("newSWIMService: %v", err)
	}
	if service == nil {
		t.Fatal("newSWIMService returned nil")
	}
	if _, err := os.Stat(incarnationPath); !os.IsNotExist(err) {
		t.Fatalf("service construction initialized untrusted incarnation state: %v", err)
	}
}

func TestNewSWIMServiceReloadsAndValidatesSecretAfterConfigValidation(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := os.WriteFile(configuration.ClusterSecretFile, []byte("too-short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if service, err := newSWIMService(configuration); err == nil || service != nil {
		t.Fatalf("newSWIMService accepted a replaced short secret: service=%v err=%v", service, err)
	}
}

func TestNewSWIMServiceRejectsUnsafeExistingStorageDirectory(t *testing.T) {
	configuration := writeNodeTestConfig(t)
	if err := os.Chmod(configuration.StorageDir, 0o755); err != nil {
		t.Fatal(err)
	}

	service, err := newSWIMService(configuration)
	if err == nil || service != nil {
		t.Fatalf("newSWIMService accepted permissive storage directory: service=%v err=%v", service, err)
	}
	info, statErr := os.Stat(configuration.StorageDir)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("newSWIMService silently changed storage mode to %o", got)
	}
}

func writeNodeTestConfig(t *testing.T) config.NodeConfig {
	t.Helper()
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	storageDir := filepath.Join(t.TempDir(), "node-1")
	if err := os.MkdirAll(storageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	return config.NodeConfig{
		NodeID:            1,
		ClusterID:         "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		BindHost:          "127.0.0.1",
		AdvertiseHost:     "127.0.0.1",
		BasePort:          8000,
		Introducer:        "127.0.0.1:8002",
		StorageDir:        storageDir,
		ClusterSecretFile: secretFile,
		RaftVoters: []config.RaftVoter{
			{NodeID: 1, Endpoint: "127.0.0.1:8008"},
			{NodeID: 2, Endpoint: "127.0.0.1:8108"},
			{NodeID: 3, Endpoint: "127.0.0.1:8208"},
		},
		Timing: config.DefaultTimingConfig(),
		Raft:   config.DefaultRaftConfig(),
		Crane:  config.DefaultCraneConfig(),
	}
}

func writeNodeTestConfigFile(t *testing.T, configuration config.NodeConfig) string {
	t.Helper()
	content, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type readyControlledService struct {
	name     string
	ready    chan struct{}
	started  chan struct{}
	returned chan struct{}
}

func newReadyControlledService(name string) *readyControlledService {
	return &readyControlledService{name: name, ready: make(chan struct{}), started: make(chan struct{}), returned: make(chan struct{})}
}

func (s *readyControlledService) Name() string { return s.name }

func (s *readyControlledService) Ready() <-chan struct{} { return s.ready }

func (s *readyControlledService) Run(ctx context.Context) error {
	close(s.started)
	defer close(s.returned)
	<-ctx.Done()
	return nil
}

type channelWriter struct {
	writes chan string
}

func (w *channelWriter) Write(content []byte) (int, error) {
	w.writes <- string(content)
	return len(content), nil
}

type failingWriter struct{ err error }

func (writer failingWriter) Write([]byte) (int, error) { return 0, writer.err }
