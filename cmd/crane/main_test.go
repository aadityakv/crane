package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/crane/clientstate"
	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/wire"
)

const cliTestClusterUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

var cliTestClusterID = [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

var cliTestSecret = bytes.Repeat([]byte{0x2a}, 32)

// cliControlServer is one scripted +6 responder behind real authenticated framing.
type cliControlServer struct {
	t        *testing.T
	listener net.Listener

	mu      sync.Mutex
	handle  func(protocol.ControlMessage) protocol.ControlMessage
	senders []uint16
}

func startCLIControlServer(t *testing.T) *cliControlServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &cliControlServer{t: t, listener: listener}
	t.Cleanup(func() { _ = listener.Close() })
	authenticator := wire.NewHMACAuthenticator(cliTestSecret)
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.PublicControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &cliTestClusterID
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				frame, err := wire.ReadTCPFrame(ctx, connection, authenticator, limits, 2*time.Second)
				if err != nil {
					return
				}
				message, err := protocol.UnmarshalControlMessage(frame.Header.Message, frame.Payload)
				if err != nil {
					return
				}
				server.mu.Lock()
				server.senders = append(server.senders, frame.Header.SenderID)
				handle := server.handle
				server.mu.Unlock()
				if handle == nil {
					return
				}
				response := handle(message)
				if response == nil {
					return
				}
				payload, err := protocol.MarshalControlMessage(response)
				if err != nil {
					server.t.Errorf("marshal scripted response: %v", err)
					return
				}
				outbound := wire.Frame{Header: wire.Header{
					Version: wire.Version1, Message: response.MessageType(), ClusterID: cliTestClusterID, SenderID: 1,
					RequestID: frame.Header.RequestID, TimestampMillis: time.Now().UnixMilli(), Codec: wire.CodecBinary,
				}, Payload: payload}
				_ = wire.WriteTCPFrame(ctx, connection, outbound, authenticator, limits, 2*time.Second)
			}(connection)
		}
	}()
	return server
}

func (server *cliControlServer) setHandler(handle func(protocol.ControlMessage) protocol.ControlMessage) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.handle = handle
}

func (server *cliControlServer) observedSenders() []uint16 {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]uint16(nil), server.senders...)
}

func (server *cliControlServer) port() int {
	return server.listener.Addr().(*net.TCPAddr).Port
}

// closedLoopbackPort reserves a loopback port and closes it so dials are
// refused deterministically.
func closedLoopbackPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// writeCLIConfig writes a valid strict node configuration whose first voter's
// derived +6 endpoint is the scripted server.
func writeCLIConfig(t *testing.T, controlPort int) string {
	t.Helper()
	directory := t.TempDir()
	secretPath := filepath.Join(directory, "cluster.secret")
	if err := os.WriteFile(secretPath, cliTestSecret, 0o600); err != nil {
		t.Fatal(err)
	}
	deadOne, deadTwo := closedLoopbackPort(t), closedLoopbackPort(t)
	configuration := fmt.Sprintf(`{
  "node_id": 4,
  "cluster_id": %q,
  "bind_host": "127.0.0.1",
  "advertise_host": "127.0.0.1",
  "base_port": 19100,
  "introducer": "127.0.0.1:19102",
  "storage_dir": %q,
  "cluster_secret_file": %q,
  "raft_voters": [
    {"node_id": 1, "endpoint": "127.0.0.1:%d"},
    {"node_id": 2, "endpoint": "127.0.0.1:%d"},
    {"node_id": 3, "endpoint": "127.0.0.1:%d"}
  ],
  "crane": {"consensus_fingerprint": %q}
}`, cliTestClusterUUID, filepath.Join(directory, "storage"), secretPath, controlPort+2, deadOne+2, deadTwo+2, model.ConsensusFingerprintHex())
	configPath := filepath.Join(directory, "node.json")
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func cliStatePath(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "crane-client")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "client-state.crane")
}

func writeExampleTopologyFile(t *testing.T) (string, model.TopologySpec) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := executeCrane(context.Background(), []string{"example-topology"}, &stdout, &stderr); err != nil {
		t.Fatalf("example-topology: %v", err)
	}
	topology, err := parseTopologyDocument(stdout.Bytes())
	if err != nil {
		t.Fatalf("parse example topology: %v", err)
	}
	path := filepath.Join(t.TempDir(), "dag.json")
	if err := os.WriteFile(path, stdout.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, topology
}

func runCLI(t *testing.T, ctx context.Context, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := executeCrane(ctx, args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func decodeJSONLines(t *testing.T, output string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("output line %q is not JSON: %v", line, err)
		}
		lines = append(lines, decoded)
	}
	return lines
}

func TestCLIStrictFlagAndConfigValidation(t *testing.T) {
	statePath := cliStatePath(t)
	server := startCLIControlServer(t)
	configPath := writeCLIConfig(t, server.port())
	topologyPath, _ := writeExampleTopologyFile(t)
	cases := []struct {
		name string
		args []string
	}{
		{name: "no subcommand", args: nil},
		{name: "unknown subcommand", args: []string{"destroy"}},
		{name: "submit without config", args: []string{"submit", "-state", statePath, "-topology", topologyPath}},
		{name: "submit without state", args: []string{"submit", "-config", configPath, "-topology", topologyPath}},
		{name: "submit without topology", args: []string{"submit", "-config", configPath, "-state", statePath}},
		{name: "submit with unknown flag", args: []string{"submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "-bogus", "1"}},
		{name: "submit with positional junk", args: []string{"submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "extra"}},
		{name: "submit with missing config file", args: []string{"submit", "-config", filepath.Join(t.TempDir(), "absent.json"), "-state", statePath, "-topology", topologyPath}},
		{name: "cancel without job", args: []string{"cancel", "-config", configPath, "-state", statePath, "-expected-revision", "1"}},
		{name: "cancel with malformed job", args: []string{"cancel", "-config", configPath, "-state", statePath, "-job", "zz", "-expected-revision", "1"}},
		{name: "cancel without expected revision", args: []string{"cancel", "-config", configPath, "-state", statePath, "-job", strings.Repeat("ab", 16)}},
		{name: "status without job", args: []string{"status", "-config", configPath}},
		{name: "results without job", args: []string{"results", "-config", configPath}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stdout, _, err := runCLI(t, context.Background(), testCase.args...)
			if err == nil {
				t.Fatalf("args %v must fail strictly", testCase.args)
			}
			if stdout != "" {
				t.Fatalf("failed invocation wrote stdout %q", stdout)
			}
		})
	}
}

func TestCLIExampleTopologyIsFiniteAndStrictlyParsed(t *testing.T) {
	_, topology := writeExampleTopologyFile(t)
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatalf("example topology must validate: %v", err)
	}
	spec := validated.Spec()
	if spec.Stages[0].Operator.Name != "range" {
		t.Fatalf("example source operator = %q, want the finite range source", spec.Stages[0].Operator.Name)
	}
	finite := false
	for _, setting := range spec.Stages[0].Operator.Settings {
		if setting.Key == "end_exclusive" {
			finite = true
		}
	}
	if !finite {
		t.Fatal("example source must be finite via end_exclusive")
	}

	t.Run("unknown fields rejected", func(t *testing.T) {
		if _, err := parseTopologyDocument([]byte(`{"schema_version":1,"name":"x","surprise":true}`)); err == nil {
			t.Fatal("unknown topology fields must be rejected")
		}
	})
	t.Run("trailing data rejected", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		if err := executeCrane(context.Background(), []string{"example-topology"}, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		if _, err := parseTopologyDocument(append(stdout.Bytes(), []byte("{}")...)); err == nil {
			t.Fatal("trailing topology JSON must be rejected")
		}
	})
	t.Run("invalid graph rejected", func(t *testing.T) {
		if _, err := parseTopologyDocument([]byte(`{"schema_version":1,"name":"x","stages":[],"edges":[]}`)); err == nil {
			t.Fatal("an invalid topology graph must be rejected")
		}
	})
}

func TestCLISubmitEmitsMachineReadableResultWithoutSecretLogs(t *testing.T) {
	server := startCLIControlServer(t)
	server.setHandler(func(message protocol.ControlMessage) protocol.ControlMessage {
		request, ok := message.(protocol.SubmitRequest)
		if !ok {
			return nil
		}
		validated, err := model.ValidateTopology(request.Topology)
		if err != nil {
			return nil
		}
		return protocol.SubmitResponse{
			Request: request.Request, Digest: request.Digest,
			JobID: model.DeriveJobID(request.Request, validated.Digest()), JobControlRevision: 1, State: protocol.JobPending,
		}
	})
	configPath := writeCLIConfig(t, server.port())
	statePath := cliStatePath(t)
	topologyPath, topology := writeExampleTopologyFile(t)
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, err := runCLI(t, context.Background(), "submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "-backoff", "1ms")
	if err != nil {
		t.Fatalf("submit: %v (stderr %q)", err, stderr)
	}
	lines := decodeJSONLines(t, stdout)
	if len(lines) != 1 {
		t.Fatalf("submit stdout = %q, want one machine-readable line", stdout)
	}
	store, err := clientstate.OpenClientState(statePath, cliTestClusterID)
	if err != nil {
		t.Fatalf("open CLI state store: %v", err)
	}
	wantJob := model.DeriveJobID(model.ClientRequestID{ClientID: store.State().ClientID, Sequence: 1}, validated.Digest())
	if lines[0]["job_id"] != hex.EncodeToString(wantJob[:]) {
		t.Fatalf("submit job_id = %v, want %s", lines[0]["job_id"], hex.EncodeToString(wantJob[:]))
	}
	if lines[0]["job_control_revision"] != float64(1) || lines[0]["state"] != "pending" {
		t.Fatalf("submit output = %v", lines[0])
	}
	if store.State().NextSequence != 2 {
		t.Fatalf("CLI state sequence = %d, want 2", store.State().NextSequence)
	}

	if strings.Contains(stderr, hex.EncodeToString(cliTestSecret)) || strings.Contains(stderr, string(cliTestSecret)) {
		t.Fatalf("stderr leaks the cluster secret: %q", stderr)
	}
	for _, sender := range server.observedSenders() {
		if sender != 4 {
			t.Fatalf("server observed sender %d, want the configured member identity 4", sender)
		}
	}

	// A second identical invocation is a new sequenced command, not a replay.
	secondOut, _, err := runCLI(t, context.Background(), "submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "-backoff", "1ms")
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	secondLines := decodeJSONLines(t, secondOut)
	if secondLines[0]["job_id"] == lines[0]["job_id"] {
		t.Fatal("a resolved submit must not be replayed as the same job")
	}
}

func TestCLICancelEmitsCanceledRevision(t *testing.T) {
	server := startCLIControlServer(t)
	server.setHandler(func(message protocol.ControlMessage) protocol.ControlMessage {
		request, ok := message.(protocol.CancelRequest)
		if !ok {
			return nil
		}
		return protocol.CancelResponse{
			Request: request.Request, Digest: request.Digest, JobID: request.JobID,
			JobControlRevision: request.ExpectedJobControlRevision + 1, State: protocol.JobCanceled,
		}
	})
	configPath := writeCLIConfig(t, server.port())
	statePath := cliStatePath(t)
	job := strings.Repeat("ab", 16)

	stdout, _, err := runCLI(t, context.Background(), "cancel", "-config", configPath, "-state", statePath, "-job", job, "-expected-revision", "3", "-backoff", "1ms")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	lines := decodeJSONLines(t, stdout)
	if len(lines) != 1 || lines[0]["job_id"] != job || lines[0]["job_control_revision"] != float64(4) || lines[0]["state"] != "canceled" {
		t.Fatalf("cancel output = %v", lines)
	}
}

func cliSucceededStatus(job model.JobID, manifestDigest [32]byte) protocol.StatusResponse {
	return protocol.StatusResponse{
		JobID: job, AppliedIndex: 12, TopologyDigest: [32]byte{0x0a, 0x0b}, JobControlRevision: 3,
		State: protocol.JobSucceeded, HasAssignment: true, AssignmentRevision: 2, AssignmentDigest: [32]byte{0x0c},
		SourceTaskCount: 1, ResultPartitionCount: 1, CompletedSourceTasks: 1,
		ManifestCount: 1, HasManifestSet: true, ManifestSetDigest: manifestDigest,
	}
}

func TestCLIStatusEmitsBoundSummary(t *testing.T) {
	var job model.JobID
	job[0], job[15] = 0x51, 0x52
	manifestDigest := [32]byte{0x71, 0x72}
	server := startCLIControlServer(t)
	server.setHandler(func(message protocol.ControlMessage) protocol.ControlMessage {
		request, ok := message.(protocol.StatusRequest)
		if !ok || request.JobID != job {
			return nil
		}
		return cliSucceededStatus(job, manifestDigest)
	})
	configPath := writeCLIConfig(t, server.port())

	stdout, _, err := runCLI(t, context.Background(), "status", "-config", configPath, "-job", hex.EncodeToString(job[:]), "-backoff", "1ms")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	lines := decodeJSONLines(t, stdout)
	if len(lines) != 1 {
		t.Fatalf("status stdout = %q", stdout)
	}
	line := lines[0]
	if line["job_id"] != hex.EncodeToString(job[:]) || line["state"] != "succeeded" || line["job_control_revision"] != float64(3) {
		t.Fatalf("status output = %v", line)
	}
	if line["manifest_set_digest"] != hex.EncodeToString(manifestDigest[:]) {
		t.Fatalf("status manifest digest = %v", line["manifest_set_digest"])
	}
	if line["completed_source_tasks"] != float64(1) || line["source_task_count"] != float64(1) {
		t.Fatalf("status progress = %v", line)
	}
}

func TestCLIResultsBindsManifestDigestAndPagesToEnd(t *testing.T) {
	_, topology := writeExampleTopologyFile(t)
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	var job model.JobID
	job[3] = 0x9d
	manifestDigest := [32]byte{0x61, 0x62}
	source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	sink := model.TaskID{JobID: job, StageID: 3, Partition: 0}
	var records []model.ResultRecord
	for sequence := uint64(1); ; sequence++ {
		tuple, exists, err := model.SourceTuple(validated, source, sequence)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			break
		}
		encoded, err := model.MarshalTuple(tuple)
		if err != nil {
			t.Fatal(err)
		}
		record, err := model.NewResultRecord(model.DeriveSourceTupleID(job, source, sequence), sink, validated.Digest(), encoded)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) < 3 {
		t.Fatalf("example topology produced %d records, want at least 3 for paging", len(records))
	}

	var digestsMu sync.Mutex
	var boundDigests [][32]byte
	server := startCLIControlServer(t)
	server.setHandler(func(message protocol.ControlMessage) protocol.ControlMessage {
		switch request := message.(type) {
		case protocol.StatusRequest:
			if request.JobID != job {
				return nil
			}
			return cliSucceededStatus(job, manifestDigest)
		case protocol.ResultPageRequest:
			digestsMu.Lock()
			boundDigests = append(boundDigests, request.ManifestDigest)
			digestsMu.Unlock()
			response := protocol.ResultPageResponse{
				JobID: request.JobID, ManifestDigest: request.ManifestDigest, RequestHasLastTuple: request.HasLastTuple,
				RequestLast: request.Last, PageBytes: request.PageBytes,
			}
			if !request.HasLastTuple {
				response.Records = records[:2]
				response.NextHasLastTuple = true
				response.NextLast = records[1].TupleID
				return response
			}
			if request.Last == records[1].TupleID {
				response.Records = records[2:]
				response.NextHasLastTuple = true
				response.NextLast = records[len(records)-1].TupleID
				response.End = true
				return response
			}
			return nil
		default:
			return nil
		}
	})
	configPath := writeCLIConfig(t, server.port())

	stdout, stderr, err := runCLI(t, context.Background(), "results", "-config", configPath, "-job", hex.EncodeToString(job[:]), "-page-bytes", "8192", "-backoff", "1ms")
	if err != nil {
		t.Fatalf("results: %v (stderr %q)", err, stderr)
	}
	lines := decodeJSONLines(t, stdout)
	if len(lines) != len(records)+1 {
		t.Fatalf("results emitted %d lines, want %d records plus one summary", len(lines), len(records)+1)
	}
	for index, line := range lines[:len(records)] {
		if line["source_sequence"] != float64(index+1) {
			t.Fatalf("record %d = %v", index, line)
		}
		if _, ok := line["fields"].(map[string]any); !ok {
			t.Fatalf("record %d has no decoded fields: %v", index, line)
		}
	}
	summary := lines[len(records)]
	if summary["records"] != float64(len(records)) || summary["complete"] != true {
		t.Fatalf("results summary = %v", summary)
	}

	digestsMu.Lock()
	defer digestsMu.Unlock()
	if len(boundDigests) < 2 {
		t.Fatalf("server observed %d page requests, want paged iteration", len(boundDigests))
	}
	for _, digest := range boundDigests {
		if digest != manifestDigest {
			t.Fatalf("page request bound digest %x, want the status manifest digest %x", digest, manifestDigest)
		}
	}
}

func TestCLIContextCancellationStopsPromptly(t *testing.T) {
	server := startCLIControlServer(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	server.setHandler(func(protocol.ControlMessage) protocol.ControlMessage {
		<-release
		return nil
	})
	configPath := writeCLIConfig(t, server.port())
	statePath := cliStatePath(t)
	topologyPath, _ := writeExampleTopologyFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, _, err := runCLI(t, ctx, "submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "-timeout", "30s")
	if err == nil {
		t.Fatal("canceled submit must fail")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancellation took %s, want prompt shutdown", elapsed)
	}
}

// TestCLISubmitResolvesResultTooLargeAsTerminalRejection pins that a consumed
// ResultTooLarge rejection ends the submit with a typed error and durably
// resolves the reservation: the sequence advances and nothing stays pending,
// so a later invocation is a fresh command rather than an endless resume.
func TestCLISubmitResolvesResultTooLargeAsTerminalRejection(t *testing.T) {
	server := startCLIControlServer(t)
	server.setHandler(func(message protocol.ControlMessage) protocol.ControlMessage {
		request, ok := message.(protocol.SubmitRequest)
		if !ok {
			return nil
		}
		return protocol.ControlError{
			RelatedMessage: wire.MessageCraneSubmitRequest, Code: protocol.ControlErrorResultTooLarge,
			HasClientRequest: true, ClientRequest: request.Request, ClientDigest: request.Digest,
			Detail: []byte("durable command result exceeds the replicated cache bound"),
		}
	})
	configPath := writeCLIConfig(t, server.port())
	statePath := cliStatePath(t)
	topologyPath, _ := writeExampleTopologyFile(t)

	stdout, _, err := runCLI(t, context.Background(), "submit", "-config", configPath, "-state", statePath, "-topology", topologyPath, "-backoff", "1ms")
	var rejection *control.RequestRejectedError
	if !errors.As(err, &rejection) || rejection.Code != protocol.ControlErrorResultTooLarge || rejection.Retryable {
		t.Fatalf("submit error = %v, want a terminal ResultTooLarge RequestRejectedError", err)
	}
	if stdout != "" {
		t.Fatalf("rejected submit wrote stdout %q", stdout)
	}
	store, err := clientstate.OpenClientState(statePath, cliTestClusterID)
	if err != nil {
		t.Fatalf("open CLI state store: %v", err)
	}
	if state := store.State(); state.NextSequence != 2 || len(state.Pending) != 0 {
		t.Fatalf("state after consumed rejection = sequence %d pending %d bytes, want sequence 2 and nothing pending", state.NextSequence, len(state.Pending))
	}
}

func TestJobsOutputRendersLeaderAndSummaries(t *testing.T) {
	status := protocol.StatusResponse{JobID: model.JobID{0x01}, AppliedIndex: 5, TopologyDigest: [32]byte{0x02}, JobControlRevision: 2, State: protocol.JobRunning, SourceTaskCount: 2, CompletedSourceTasks: 1, AssignmentRevision: 1}
	var buffer bytes.Buffer
	if err := writeJSONLine(&buffer, jobsOutput(protocol.JobListResponse{LeaderNodeID: 3, AppliedIndex: 5, Jobs: []protocol.StatusResponse{status}})); err != nil {
		t.Fatal(err)
	}
	line := buffer.String()
	for _, want := range []string{`"command":"jobs"`, `"leader_node_id":3`, `"state":"running"`, `"completed_source_tasks":1`} {
		if !strings.Contains(line, want) {
			t.Fatalf("jobs output %q lacks %q", line, want)
		}
	}
}
