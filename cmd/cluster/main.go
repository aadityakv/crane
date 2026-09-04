package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	internalnode "crane/internal/node"
)

const (
	clusterStartupDeadline  = 10 * time.Second
	clusterShutdownGrace    = 10 * time.Second
	clusterKillDrainTimeout = 5 * time.Second
)

func main() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := executeCluster(context.Background(), os.Args[1:], signals, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "cluster: %v\n", err)
		os.Exit(1)
	}
}

func executeCluster(ctx context.Context, args []string, signals <-chan os.Signal, stdout, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("cluster context is nil")
	}
	options, nodeBinary, err := parseClusterFlags(args)
	if err != nil {
		return err
	}
	if err := ensureClusterSecret(options.SecretFile); err != nil {
		return err
	}
	clusterID, err := resolveClusterID(options.DataRoot)
	if err != nil {
		return err
	}
	options.ClusterID = clusterID
	configurations, err := GenerateConfigs(options)
	if err != nil {
		return err
	}
	configPaths, err := prepareClusterFiles(options.DataRoot, configurations)
	if err != nil {
		return err
	}
	if options.Dashboard != "" {
		fetcher, err := newDashboardFetcher(configurations[0], options.SecretFile)
		if err != nil {
			return err
		}
		url, stopDashboard, err := startDashboard(ctx, options.ClusterID, options.Dashboard, fetcher, stderr)
		if err != nil {
			return err
		}
		defer stopDashboard()
		fmt.Fprintf(stdout, "dashboard: %s\n", url)
	}
	return runClusterProcesses(ctx, nodeBinary, configPaths, signals, stdout, stderr)
}

func parseClusterFlags(args []string) (ClusterOptions, string, error) {
	flags := flag.NewFlagSet("cluster", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	nodes := flags.Int("nodes", 3, "number of local nodes")
	basePort := flags.Uint64("base-port", 8000, "first node base port")
	dataRoot := flags.String("data-root", "./data/local", "local cluster data root")
	secretFile := flags.String("secret-file", "./local.secret", "permission-restricted cluster secret file")
	nodeBinary := flags.String("node-binary", "./bin/crane-node", "node executable")
	dashboard := flags.String("dashboard", "", "read-only loopback job dashboard address")
	if err := flags.Parse(args); err != nil {
		return ClusterOptions{}, "", fmt.Errorf("parse cluster flags: %w", err)
	}
	if flags.NArg() != 0 {
		return ClusterOptions{}, "", fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *basePort > 65535 {
		return ClusterOptions{}, "", fmt.Errorf("-base-port %d exceeds 65535", *basePort)
	}
	voters := 3
	if *nodes >= 5 {
		voters = 5
	}
	if *dashboard != "" {
		if err := validateDashboardAddress(*dashboard); err != nil {
			return ClusterOptions{}, "", fmt.Errorf("parse cluster flags: %w", err)
		}
	}
	return ClusterOptions{
		Dashboard:        *dashboard,
		Nodes:            *nodes,
		Voters:           voters,
		Host:             "127.0.0.1",
		StartingBasePort: uint16(*basePort),
		DataRoot:         *dataRoot,
		SecretFile:       *secretFile,
	}, *nodeBinary, nil
}

type clusterChild struct {
	nodeID uint16
	cmd    *exec.Cmd
	stdout *prefixWriter
	stderr *prefixWriter
	ready  chan struct{}
	once   sync.Once
}

type clusterChildResult struct {
	nodeID uint16
	err    error
}

func runClusterProcesses(ctx context.Context, nodeBinary string, configPaths []string, signals <-chan os.Signal, stdout, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("cluster process context is nil")
	}
	if nodeBinary == "" {
		return errors.New("node binary is empty")
	}
	if len(configPaths) == 0 {
		return errors.New("no node configs to launch")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	sharedStdout := &synchronizedWriter{output: stdout}
	sharedStderr := &synchronizedWriter{output: stderr}
	children := make([]*clusterChild, 0, len(configPaths))
	results := make(chan clusterChildResult, len(configPaths))
	seed, err := startClusterChild(1, nodeBinary, configPaths[0], sharedStdout, sharedStderr, results)
	if err != nil {
		return err
	}
	children = append(children, seed)
	ready, err := waitForSeedReadiness(ctx, seed, children, results, signals, clusterStartupDeadline)
	if !ready {
		return err
	}
	for index, configPath := range configPaths[1:] {
		nodeID := uint16(index + 2)
		child, err := startClusterChild(nodeID, nodeBinary, configPath, sharedStdout, sharedStderr, results)
		if err != nil {
			terminateChildren(children, syscall.SIGTERM)
			return errors.Join(err, drainChildren(children, results))
		}
		children = append(children, child)
	}

	remaining := len(children)
	shuttingDown := false
	var cause error
	var graceTimer *time.Timer
	var graceDeadline <-chan time.Time
	forceKilling := false
	contextDone := ctx.Done()
	signalStream := signals
	beginShutdown := func(sig os.Signal, err error) {
		shuttingDown = true
		cause = err
		terminateChildren(children, sig)
		graceTimer = time.NewTimer(clusterShutdownGrace)
		graceDeadline = graceTimer.C
	}
	forceKill := func() {
		terminateChildren(children, syscall.SIGKILL)
		if forceKilling {
			return
		}
		forceKilling = true
		if graceTimer == nil {
			graceTimer = time.NewTimer(clusterKillDrainTimeout)
		} else {
			if !graceTimer.Stop() {
				select {
				case <-graceTimer.C:
				default:
				}
			}
			graceTimer.Reset(clusterKillDrainTimeout)
		}
		graceDeadline = graceTimer.C
	}
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()

	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if !shuttingDown {
				if result.err == nil {
					beginShutdown(syscall.SIGTERM, fmt.Errorf("node %d exited unexpectedly", result.nodeID))
				} else {
					beginShutdown(syscall.SIGTERM, fmt.Errorf("node %d exited: %w", result.nodeID, result.err))
				}
			} else if result.err != nil {
				cause = errors.Join(cause, fmt.Errorf("node %d shutdown: %w", result.nodeID, result.err))
			}
		case sig, ok := <-signalStream:
			if !ok {
				signalStream = nil
				continue
			}
			if shuttingDown {
				forceKill()
			} else {
				beginShutdown(sig, nil)
			}
		case <-contextDone:
			contextDone = nil
			if !shuttingDown {
				beginShutdown(syscall.SIGTERM, ctx.Err())
			}
		case <-graceDeadline:
			if forceKilling {
				return errors.Join(cause, fmt.Errorf("cluster child wait after SIGKILL exceeded %s", clusterKillDrainTimeout))
			}
			if cause == nil {
				cause = fmt.Errorf("cluster shutdown exceeded %s", clusterShutdownGrace)
			}
			forceKill()
		}
	}
	return cause
}

func startClusterChild(nodeID uint16, nodeBinary, configPath string, stdout, stderr io.Writer, results chan<- clusterChildResult) (*clusterChild, error) {
	child := &clusterChild{
		nodeID: nodeID,
		cmd:    exec.Command(nodeBinary, "-config", configPath),
		ready:  make(chan struct{}),
	}
	observe := func(line string) {
		readyNodeID, ok := internalnode.ParseReadySignal(strings.TrimSuffix(line, "\n"))
		if ok && readyNodeID == child.nodeID {
			child.once.Do(func() { close(child.ready) })
		}
	}
	prefix := fmt.Sprintf("[node-%d] ", nodeID)
	child.stdout = newObservedPrefixWriter(stdout, prefix, observe)
	child.stderr = newObservedPrefixWriter(stderr, prefix, observe)
	child.cmd.Stdout = child.stdout
	child.cmd.Stderr = child.stderr
	child.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.cmd.Start(); err != nil {
		return nil, fmt.Errorf("start node %d: %w", nodeID, err)
	}
	go waitClusterChild(child, results)
	return child, nil
}

func waitForSeedReadiness(ctx context.Context, seed *clusterChild, children []*clusterChild, results <-chan clusterChildResult, signals <-chan os.Signal, startupDeadline time.Duration) (bool, error) {
	timer := time.NewTimer(startupDeadline)
	defer timer.Stop()
	select {
	case <-seed.ready:
		return true, nil
	case result := <-results:
		if result.err == nil {
			return false, fmt.Errorf("seed node %d exited before readiness", result.nodeID)
		}
		return false, fmt.Errorf("seed node %d failed before readiness: %w", result.nodeID, result.err)
	case signal, ok := <-signals:
		if !ok {
			terminateChildren(children, syscall.SIGTERM)
			return false, errors.Join(errors.New("seed readiness signal channel closed"), drainChildren(children, results))
		}
		terminateChildren(children, signal)
		return false, drainChildren(children, results)
	case <-ctx.Done():
		terminateChildren(children, syscall.SIGTERM)
		return false, errors.Join(ctx.Err(), drainChildren(children, results))
	case <-timer.C:
		terminateChildren(children, syscall.SIGTERM)
		return false, errors.Join(fmt.Errorf("seed node %d readiness exceeded %s", seed.nodeID, startupDeadline), drainChildren(children, results))
	}
}

func waitClusterChild(child *clusterChild, results chan<- clusterChildResult) {
	waitError := child.cmd.Wait()
	if childExitedBySignal(waitError) {
		waitError = nil
	}
	err := errors.Join(waitError, child.stdout.Flush(), child.stderr.Flush())
	results <- clusterChildResult{nodeID: child.nodeID, err: err}
}

func childExitedBySignal(err error) bool {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return false
	}
	status, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled()
}

func drainChildren(children []*clusterChild, results <-chan clusterChildResult) error {
	return drainChildrenWithin(children, results, clusterShutdownGrace, clusterKillDrainTimeout)
}

func drainChildrenWithin(children []*clusterChild, results <-chan clusterChildResult, shutdownGrace, killDrainTimeout time.Duration) error {
	if len(children) == 0 {
		return nil
	}
	timer := time.NewTimer(shutdownGrace)
	defer timer.Stop()
	remaining := len(children)
	var failures []error
	forceKilling := false
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.err != nil {
				failures = append(failures, fmt.Errorf("node %d shutdown: %w", result.nodeID, result.err))
			}
		case <-timer.C:
			if forceKilling {
				return errors.Join(append(failures, fmt.Errorf("cluster child wait after SIGKILL exceeded %s", killDrainTimeout))...)
			}
			terminateChildren(children, syscall.SIGKILL)
			failures = append(failures, fmt.Errorf("cluster shutdown exceeded %s", shutdownGrace))
			forceKilling = true
			timer.Reset(killDrainTimeout)
		}
	}
	return errors.Join(failures...)
}

func terminateChildren(children []*clusterChild, signal os.Signal) {
	for _, child := range children {
		if child == nil || child.cmd == nil || child.cmd.Process == nil {
			continue
		}
		if systemSignal, ok := signal.(syscall.Signal); ok {
			if err := syscall.Kill(-child.cmd.Process.Pid, systemSignal); err == nil || errors.Is(err, syscall.ESRCH) {
				continue
			}
		}
		_ = child.cmd.Process.Signal(signal)
	}
}

type synchronizedWriter struct {
	mu     sync.Mutex
	output io.Writer
}

func (w *synchronizedWriter) Write(content []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.Write(content)
}

type prefixWriter struct {
	mu      sync.Mutex
	output  io.Writer
	prefix  []byte
	pending bytes.Buffer
	observe func(string)
}

func newPrefixWriter(output io.Writer, prefix string) *prefixWriter {
	return newObservedPrefixWriter(output, prefix, nil)
}

func newObservedPrefixWriter(output io.Writer, prefix string, observe func(string)) *prefixWriter {
	return &prefixWriter{output: output, prefix: []byte(prefix), observe: observe}
}

func (w *prefixWriter) Write(content []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.pending.Write(content); err != nil {
		return 0, err
	}
	for {
		line, err := w.pending.ReadString('\n')
		if err != nil {
			_, _ = w.pending.WriteString(line)
			break
		}
		if w.observe != nil {
			w.observe(line)
		}
		if _, err := w.output.Write(w.prefix); err != nil {
			return 0, err
		}
		if _, err := io.WriteString(w.output, line); err != nil {
			return 0, err
		}
	}
	return len(content), nil
}

func (w *prefixWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending.Len() == 0 {
		return nil
	}
	if _, err := w.output.Write(w.prefix); err != nil {
		return err
	}
	if _, err := w.pending.WriteTo(w.output); err != nil {
		return err
	}
	_, err := io.WriteString(w.output, "\n")
	return err
}
