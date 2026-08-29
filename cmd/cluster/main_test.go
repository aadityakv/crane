package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
	if err := os.WriteFile(configPath+".started", []byte("started\n"), 0o600); err != nil {
		os.Exit(92)
	}
	if filepath.Base(configPath) == "node-3.json" {
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
		if err := os.WriteFile(configPath+".stopped", []byte("stopped\n"), 0o600); err != nil {
			os.Exit(94)
		}
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
