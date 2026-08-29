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
	"sync"
	"syscall"
	"time"
)

const clusterShutdownGrace = 10 * time.Second

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
	configurations, err := GenerateConfigs(options)
	if err != nil {
		return err
	}
	configPaths, err := prepareClusterFiles(options.DataRoot, configurations)
	if err != nil {
		return err
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
	nodeBinary := flags.String("node-binary", "./bin/cs425-node", "node executable")
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
	return ClusterOptions{
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
	for index, configPath := range configPaths {
		nodeID := uint16(index + 1)
		prefix := fmt.Sprintf("[node-%d] ", nodeID)
		child := &clusterChild{
			nodeID: nodeID,
			cmd:    exec.Command(nodeBinary, "-config", configPath),
			stdout: newPrefixWriter(sharedStdout, prefix),
			stderr: newPrefixWriter(sharedStderr, prefix),
		}
		child.cmd.Stdout = child.stdout
		child.cmd.Stderr = child.stderr
		child.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := child.cmd.Start(); err != nil {
			cause := fmt.Errorf("start node %d: %w", nodeID, err)
			terminateChildren(children, syscall.SIGTERM)
			return errors.Join(cause, drainChildren(children, results))
		}
		children = append(children, child)
		go waitClusterChild(child, results)
	}

	remaining := len(children)
	shuttingDown := false
	var cause error
	var graceTimer *time.Timer
	var graceDeadline <-chan time.Time
	beginShutdown := func(sig os.Signal, err error) {
		if shuttingDown {
			terminateChildren(children, syscall.SIGKILL)
			return
		}
		shuttingDown = true
		cause = err
		terminateChildren(children, sig)
		graceTimer = time.NewTimer(clusterShutdownGrace)
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
			}
		case sig := <-signals:
			if sig != nil {
				beginShutdown(sig, nil)
			}
		case <-ctx.Done():
			beginShutdown(syscall.SIGTERM, ctx.Err())
		case <-graceDeadline:
			terminateChildren(children, syscall.SIGKILL)
			graceDeadline = nil
			if cause == nil {
				cause = fmt.Errorf("cluster shutdown exceeded %s", clusterShutdownGrace)
			}
		}
	}
	return cause
}

func waitClusterChild(child *clusterChild, results chan<- clusterChildResult) {
	err := child.cmd.Wait()
	err = errors.Join(err, child.stdout.Flush(), child.stderr.Flush())
	results <- clusterChildResult{nodeID: child.nodeID, err: err}
}

func drainChildren(children []*clusterChild, results <-chan clusterChildResult) error {
	if len(children) == 0 {
		return nil
	}
	timer := time.NewTimer(clusterShutdownGrace)
	defer timer.Stop()
	remaining := len(children)
	var failures []error
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.err != nil {
				failures = append(failures, fmt.Errorf("node %d shutdown: %w", result.nodeID, result.err))
			}
		case <-timer.C:
			terminateChildren(children, syscall.SIGKILL)
			return errors.Join(append(failures, fmt.Errorf("cluster shutdown exceeded %s", clusterShutdownGrace))...)
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
}

func newPrefixWriter(output io.Writer, prefix string) *prefixWriter {
	return &prefixWriter{output: output, prefix: []byte(prefix)}
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
