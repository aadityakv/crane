package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/config"
)

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
	if !reflect.DeepEqual(got.RaftVoters, configuration.RaftVoters) || got.Timing != configuration.Timing {
		t.Fatalf("file-controlled voters or timing changed: %#v", got)
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

func writeNodeTestConfig(t *testing.T) config.NodeConfig {
	t.Helper()
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, []byte("node-command-test-secret"), 0o600); err != nil {
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
