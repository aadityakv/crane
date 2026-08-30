package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	internalnode "github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/testutil"
)

func TestParseClusterFlagsUsesSpecifiedLocalLayout(t *testing.T) {
	options, nodeBinary, err := parseClusterFlags([]string{
		"-nodes", "5",
		"-base-port", "12000",
		"-data-root", "/tmp/local-data",
		"-secret-file", "/tmp/local.secret",
		"-node-binary", "/tmp/cs425-node",
	})
	if err != nil {
		t.Fatalf("parseClusterFlags: %v", err)
	}
	if options.Nodes != 5 || options.Voters != 5 || options.Host != "127.0.0.1" || options.StartingBasePort != 12000 {
		t.Fatalf("cluster options = %#v", options)
	}
	if options.DataRoot != "/tmp/local-data" || options.SecretFile != "/tmp/local.secret" || nodeBinary != "/tmp/cs425-node" {
		t.Fatalf("paths = %#v, binary %q", options, nodeBinary)
	}
}

func TestRunClusterProcessesCancelsPeersAfterChildFailure(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := runClusterProcesses(ctx, nodeBinary, configPaths, nil, os.Stderr, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "node 3") {
		t.Fatalf("runClusterProcesses error = %v, want node 3 failure", err)
	}
	for _, index := range []int{1, 2} {
		if _, err := os.Stat(configPaths[index-1] + ".stopped"); err != nil {
			t.Fatalf("node %d did not record forwarded termination: %v", index, err)
		}
	}
}

func TestRunClusterProcessesWaitsForSeedReadinessBeforeStartingPeers(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(configPaths[0]+".delay-ready", []byte("delay\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPaths[2]+".stay-alive", []byte("stay\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() { result <- runClusterProcesses(ctx, nodeBinary, configPaths, signals, os.Stderr, os.Stderr) }()
	stop := func() error {
		select {
		case err := <-result:
			return err
		default:
		}
		signals <- syscall.SIGTERM
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	waitForFile(t, ctx, configPaths[0]+".started")
	if err := os.WriteFile(configPaths[0]+".check-peers", []byte("check\n"), 0o600); err != nil {
		_ = stop()
		t.Fatal(err)
	}
	waitForFile(t, ctx, configPaths[0]+".check-ack")
	if fileExists(configPaths[1]+".started") || fileExists(configPaths[2]+".started") {
		err := stop()
		t.Fatalf("peers started before seed readiness (shutdown error %v)", err)
	}
	if err := os.WriteFile(configPaths[0]+".release-ready", []byte("release\n"), 0o600); err != nil {
		_ = stop()
		t.Fatal(err)
	}
	waitForFile(t, ctx, configPaths[1]+".started")
	waitForFile(t, ctx, configPaths[2]+".started")
	if err := stop(); err != nil {
		t.Fatalf("runClusterProcesses shutdown error = %v", err)
	}
}

func TestWaitForSeedReadinessTimeoutTerminatesAndJoinsSeed(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configPath := filepath.Join(t.TempDir(), "node-1.json")
	if err := os.WriteFile(configPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath+".delay-ready", []byte("delay\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	results := make(chan clusterChildResult, 1)
	seed, err := startClusterChild(1, nodeBinary, configPath, os.Stderr, os.Stderr, results)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready, err := waitForSeedReadiness(ctx, seed, []*clusterChild{seed}, results, nil, 50*time.Millisecond)
	if ready || err == nil || !strings.Contains(err.Error(), "readiness exceeded") {
		t.Fatalf("waitForSeedReadiness = %v, %v", ready, err)
	}
	if seed.cmd.ProcessState == nil {
		t.Fatal("seed was not Wait-joined before timeout returned")
	}
	if signalErr := seed.cmd.Process.Signal(syscall.Signal(0)); signalErr == nil {
		t.Fatal("seed process still exists after timeout cleanup")
	}
}

func TestRunClusterProcessesSeedFailureDoesNotStartPeers(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(configPaths[0]+".exit-before-ready", []byte("exit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runClusterProcesses(ctx, nodeBinary, configPaths, nil, os.Stderr, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "seed node 1 failed before readiness") {
		t.Fatalf("runClusterProcesses error = %v", err)
	}
	if fileExists(configPaths[1]+".started") || fileExists(configPaths[2]+".started") {
		t.Fatal("peer process started after seed failed before readiness")
	}
}

func TestRunClusterProcessesClosedSignalBeforeSeedReadyTerminatesAndReapsSeed(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(configPaths[0]+".delay-ready", []byte("delay\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signals := make(chan os.Signal)
	result := make(chan error, 1)
	go func() {
		result <- runClusterProcesses(ctx, nodeBinary, configPaths, signals, os.Stderr, os.Stderr)
	}()
	waitForFile(t, ctx, configPaths[0]+".started")
	seedPID := readHelperPID(t, configPaths[0]+".pid")
	defer func() {
		_ = syscall.Kill(-seedPID, syscall.SIGKILL)
	}()
	close(signals)

	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "signal channel closed") {
			t.Fatalf("runClusterProcesses error = %v, want closed signal-channel error", err)
		}
	case <-ctx.Done():
		t.Fatal("launcher did not return after signal channel closed")
	}
	if !fileExists(configPaths[0] + ".stopped") {
		t.Fatal("seed did not record forwarded termination before launcher returned")
	}
	if signalErr := syscall.Kill(seedPID, syscall.Signal(0)); signalErr == nil {
		t.Fatal("seed process still exists after launcher returned")
	}
	if fileExists(configPaths[1]+".started") || fileExists(configPaths[2]+".started") {
		t.Fatal("peer process started after readiness signal channel closed")
	}
}

func TestRunClusterProcessesContextCancellationGrantsGracePeriod(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPaths[index]+".delay-stop", []byte("delay\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(configPaths[2]+".stay-alive", []byte("stay\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	deadlineContext, cancelDeadline := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDeadline()
	runContext, cancelRun := context.WithCancel(deadlineContext)
	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() {
		result <- runClusterProcesses(runContext, nodeBinary, configPaths, signals, os.Stderr, os.Stderr)
	}()
	defer func() {
		for _, configPath := range configPaths {
			_ = os.WriteFile(configPath+".release-stop", []byte("release\n"), 0o600)
		}
		select {
		case <-result:
		default:
			select {
			case signals <- syscall.SIGTERM:
			default:
			}
		}
	}()

	for _, configPath := range configPaths {
		waitForFile(t, deadlineContext, configPath+".started")
	}
	cancelRun()
	for _, configPath := range configPaths {
		waitForFile(t, deadlineContext, configPath+".term-received")
	}
	select {
	case err := <-result:
		t.Fatalf("launcher returned before graceful barriers released: %v", err)
	default:
	}
	for _, configPath := range configPaths {
		if err := os.WriteFile(configPath+".release-stop", []byte("release\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runClusterProcesses error = %v, want context cancellation", err)
		}
	case <-deadlineContext.Done():
		t.Fatal("timed out waiting for graceful context shutdown")
	}
}

func TestRunClusterProcessesSecondSignalEscalatesGracefulShutdown(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := []string{
		filepath.Join(configDirectory, "node-1.json"),
		filepath.Join(configDirectory, "node-2.json"),
	}
	for _, configPath := range configPaths {
		if err := os.WriteFile(configPath, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath+".delay-stop", []byte("delay\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signals := make(chan os.Signal, 2)
	result := make(chan error, 1)
	go func() {
		result <- runClusterProcesses(ctx, nodeBinary, configPaths, signals, os.Stderr, os.Stderr)
	}()
	for _, configPath := range configPaths {
		waitForFile(t, ctx, configPath+".started")
	}
	signals <- syscall.SIGTERM
	for _, configPath := range configPaths {
		waitForFile(t, ctx, configPath+".term-received")
	}
	select {
	case err := <-result:
		t.Fatalf("launcher returned during first-signal grace period: %v", err)
	default:
	}

	signals <- syscall.SIGTERM
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runClusterProcesses error = %v, want clean signal shutdown", err)
		}
	case <-ctx.Done():
		t.Fatal("second signal did not escalate shutdown")
	}
	for _, configPath := range configPaths {
		if fileExists(configPath + ".stopped") {
			t.Fatalf("child %q completed graceful barrier after second-signal escalation", configPath)
		}
	}
}

func TestRunClusterProcessesPreservesChildFailureDuringRequestedShutdown(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configDirectory := t.TempDir()
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", index+1))
		if err := os.WriteFile(configPaths[index], []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(configPaths[2]+".stay-alive", []byte("stay\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPaths[1]+".fail-on-stop", []byte("fail\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() {
		result <- runClusterProcesses(ctx, nodeBinary, configPaths, signals, os.Stderr, os.Stderr)
	}()
	for _, configPath := range configPaths {
		waitForFile(t, ctx, configPath+".started")
	}
	signals <- syscall.SIGTERM
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "node 2 shutdown") {
			t.Fatalf("runClusterProcesses error = %v, want node 2 shutdown failure", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for requested cluster shutdown")
	}
}

func TestDrainChildrenReapsChildAfterGraceExpires(t *testing.T) {
	nodeBinary := writeClusterHelperWrapper(t)
	configPath := filepath.Join(t.TempDir(), "node-1.json")
	if err := os.WriteFile(configPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath+".delay-stop", []byte("delay\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	results := make(chan clusterChildResult, 1)
	child, err := startClusterChild(1, nodeBinary, configPath, os.Stderr, os.Stderr, results)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitForFile(t, ctx, configPath+".started")
	terminateChildren([]*clusterChild{child}, syscall.SIGTERM)
	waitForFile(t, ctx, configPath+".term-received")

	err = drainChildrenWithin([]*clusterChild{child}, results, 25*time.Millisecond, time.Second)
	if err == nil || !strings.Contains(err.Error(), "shutdown exceeded") {
		t.Fatalf("drainChildrenWithin error = %v, want grace timeout", err)
	}
	if child.cmd.ProcessState == nil {
		t.Fatal("child was not Wait-joined after SIGKILL")
	}
	if signalErr := child.cmd.Process.Signal(syscall.Signal(0)); signalErr == nil {
		t.Fatal("child process still exists after drain returned")
	}
}

func TestClusterChildHelper(t *testing.T) {
	if os.Getenv("CS425_CLUSTER_HELPER") != "1" {
		return
	}
	args := flag.Args()
	if len(args) != 2 || args[0] != "-config" {
		os.Exit(91)
	}
	configPath := args[1]
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := os.WriteFile(configPath+".pid", []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		os.Exit(90)
	}
	if err := os.WriteFile(configPath+".started", []byte("started\n"), 0o600); err != nil {
		os.Exit(92)
	}
	if filepath.Base(configPath) == "node-1.json" {
		if fileExists(configPath + ".exit-before-ready") {
			os.Exit(99)
		}
		if fileExists(configPath + ".delay-ready") {
			ready, stopped := waitForHelperFile(configPath+".check-peers", 5*time.Second, signals)
			if stopped {
				recordHelperStopped(configPath)
				return
			}
			if !ready {
				os.Exit(96)
			}
			if err := os.WriteFile(configPath+".check-ack", []byte("checked\n"), 0o600); err != nil {
				os.Exit(97)
			}
			ready, stopped = waitForHelperFile(configPath+".release-ready", 5*time.Second, signals)
			if stopped {
				recordHelperStopped(configPath)
				return
			}
			if !ready {
				os.Exit(98)
			}
		}
		fmt.Println(internalnode.ReadySignal(1))
	}
	if filepath.Base(configPath) == "node-3.json" && !fileExists(configPath+".stay-alive") {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			firstReady := fileExists(filepath.Join(filepath.Dir(configPath), "node-1.json.started"))
			secondReady := fileExists(filepath.Join(filepath.Dir(configPath), "node-2.json.started"))
			if firstReady && secondReady {
				os.Exit(23)
			}
			select {
			case <-ticker.C:
			case <-deadline.C:
				os.Exit(93)
			}
		}
	}
	select {
	case <-signals:
		if err := os.WriteFile(configPath+".term-received", []byte("term\n"), 0o600); err != nil {
			os.Exit(94)
		}
		if fileExists(configPath + ".fail-on-stop") {
			os.Exit(24)
		}
		if fileExists(configPath + ".delay-stop") {
			released, _ := waitForHelperFile(configPath+".release-stop", 5*time.Second, signals)
			if !released {
				return
			}
		}
		recordHelperStopped(configPath)
	case <-time.After(8 * time.Second):
		os.Exit(95)
	}
}

func writeClusterHelperWrapper(t *testing.T) string {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cluster-child")
	content := fmt.Sprintf("#!/bin/sh\nCS425_CLUSTER_HELPER=1 exec %q -test.run=TestClusterChildHelper -- \"$@\"\n", testBinary)
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func waitForFile(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	if err := testutil.WaitFor(ctx, 5*time.Millisecond, func() (bool, error) {
		return fileExists(path), nil
	}); err != nil {
		t.Fatalf("wait for file %q: %v", path, err)
	}
}

func readHelperPID(t *testing.T, path string) int {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(content))
	if err != nil || pid <= 0 {
		t.Fatalf("helper PID %q: %v", content, err)
	}
	return pid
}

func waitForHelperFile(path string, timeout time.Duration, signals <-chan os.Signal) (bool, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fileExists(path) {
			return true, false
		}
		select {
		case <-ticker.C:
		case <-signals:
			return false, true
		case <-deadline.C:
			return false, false
		}
	}
}

func recordHelperStopped(configPath string) {
	if err := os.WriteFile(configPath+".stopped", []byte("stopped\n"), 0o600); err != nil {
		os.Exit(94)
	}
}
