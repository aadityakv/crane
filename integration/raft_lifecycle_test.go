//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crane/internal/config"
	"crane/internal/raft"
	"crane/internal/swim"
)

func TestFourProcessFixedVoterRaftLifecycle(t *testing.T) {
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binary := buildNodeBinary(t, repositoryRoot)
	secret := []byte("four-process-raft-secret-32bytes")
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	basePort, releasePorts := reserveTypedClusterPorts(t, 4)
	t.Cleanup(releasePorts)
	configurations := integrationConfigsForNodes(t, basePort, secretFile, 4)
	configPaths := writeIntegrationConfigs(t, configurations)
	releasePorts()

	harness := newProcessHarness(t)
	processes := make([]*nodeProcess, 4)
	processes[0] = harness.start(binary, configPaths[0], "raft-node-1")
	startupContext, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStartup()
	waitForNormalNodeReadiness(t, startupContext, harness, processes[0], 1)
	for index := 1; index < len(processes); index++ {
		processes[index] = harness.start(binary, configPaths[index], fmt.Sprintf("raft-node-%d", index+1))
	}
	for index, process := range processes {
		waitForNormalNodeReadiness(t, startupContext, harness, process, uint16(index+1))
	}

	clients := newSnapshotClients(t, configurations, secret)
	waitForCluster(t, startupContext, harness, "all four SWIM members to become Alive without changing fixed voters", func() (bool, error) {
		views, err := clients.views(startupContext, 1, 2, 3, 4)
		if err != nil {
			return false, err
		}
		for observer, members := range views {
			for memberID := uint16(1); memberID <= 4; memberID++ {
				if !hasMember(members, memberID, swim.Alive, 0) {
					return false, fmt.Errorf("observer %d view = %#v", observer, members)
				}
			}
		}
		return true, nil
	})

	for index, configuration := range configurations {
		endpoint, err := configuration.BindEndpoint(config.ServiceRaftRPC)
		if err != nil {
			t.Fatal(err)
		}
		raftDirectory := filepath.Join(configuration.StorageDir, raft.RaftStorageDirectoryName)
		if index < 3 {
			connection, err := net.DialTimeout("tcp", endpoint.String(), time.Second)
			if err != nil {
				t.Fatalf("voter %d Raft endpoint %s does not accept TCP: %v\n%s", index+1, endpoint, err, harness.logs())
			}
			_ = connection.Close()
			for _, artifact := range []string{raft.RaftLockFilename, raft.RaftIdentityFilename, raft.RaftWALFilename} {
				if _, err := os.Stat(filepath.Join(raftDirectory, artifact)); err != nil {
					t.Fatalf("voter %d missing Raft artifact %s: %v\n%s", index+1, artifact, err, harness.logs())
				}
			}
			continue
		}
		listener, err := net.Listen("tcp", endpoint.String())
		if err != nil {
			t.Fatalf("nonvoter node 4 unexpectedly binds Raft endpoint %s: %v\n%s", endpoint, err, harness.logs())
		}
		_ = listener.Close()
		if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("nonvoter node 4 created Raft storage at %s: %v", raftDirectory, err)
		}
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	for index := len(processes) - 1; index >= 0; index-- {
		if err := processes[index].terminate(shutdownContext); err != nil {
			t.Fatalf("terminate node %d: %v\n%s", index+1, err, harness.logs())
		}
	}
	if _, err := os.Stat(filepath.Join(configurations[3].StorageDir, raft.RaftStorageDirectoryName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nonvoter created Raft storage during its full lifecycle: %v", err)
	}
}
