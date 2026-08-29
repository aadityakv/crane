package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/swim"
)

func TestGenerateConfigsBuildsStrictSharedLocalLayout(t *testing.T) {
	secretContents := "launcher-secret-that-must-not-enter-json"
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, []byte(secretContents), 0o600); err != nil {
		t.Fatal(err)
	}
	dataRoot := filepath.Join(t.TempDir(), "data")

	configs, err := GenerateConfigs(ClusterOptions{
		Nodes:            3,
		Voters:           3,
		Host:             "127.0.0.1",
		StartingBasePort: 8000,
		DataRoot:         dataRoot,
		SecretFile:       secretFile,
	})
	if err != nil {
		t.Fatalf("GenerateConfigs: %v", err)
	}
	if len(configs) != 3 {
		t.Fatalf("len(configs) = %d, want 3", len(configs))
	}

	wantVoters := []config.RaftVoter{
		{NodeID: 1, Endpoint: "127.0.0.1:8008"},
		{NodeID: 2, Endpoint: "127.0.0.1:8108"},
		{NodeID: 3, Endpoint: "127.0.0.1:8208"},
	}
	wantBases := []uint16{8000, 8100, 8200}
	seenStorage := make(map[string]bool)
	for index, generated := range configs {
		if generated.NodeID != uint16(index+1) {
			t.Errorf("config %d node ID = %d, want %d", index, generated.NodeID, index+1)
		}
		if generated.BasePort != wantBases[index] {
			t.Errorf("config %d base port = %d, want %d", index, generated.BasePort, wantBases[index])
		}
		if generated.Introducer != "127.0.0.1:8002" {
			t.Errorf("config %d introducer = %q, want 127.0.0.1:8002", index, generated.Introducer)
		}
		if generated.ClusterSecretFile != secretFile {
			t.Errorf("config %d secret path = %q, want %q", index, generated.ClusterSecretFile, secretFile)
		}
		if !reflect.DeepEqual(generated.RaftVoters, wantVoters) {
			t.Errorf("config %d voters = %#v, want %#v", index, generated.RaftVoters, wantVoters)
		}
		wantStorage := filepath.Join(dataRoot, "node-"+string(rune('1'+index)))
		if generated.StorageDir != wantStorage {
			t.Errorf("config %d storage = %q, want %q", index, generated.StorageDir, wantStorage)
		}
		if seenStorage[generated.StorageDir] {
			t.Errorf("duplicate storage directory %q", generated.StorageDir)
		}
		seenStorage[generated.StorageDir] = true
		if err := generated.Validate(); err != nil {
			t.Errorf("config %d is not strict-valid: %v", index, err)
		}
	}
	if configs[0].ClusterID == "" || configs[0].ClusterID != configs[1].ClusterID || configs[0].ClusterID != configs[2].ClusterID {
		t.Fatalf("cluster IDs are not one shared nonempty value: %#v", configs)
	}

	encoded, err := json.Marshal(configs)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), secretFile) {
		t.Fatalf("generated JSON does not contain secret path %q: %s", secretFile, encoded)
	}
	if strings.Contains(string(encoded), secretContents) {
		t.Fatalf("generated JSON leaked secret contents: %s", encoded)
	}
}

func TestGenerateConfigsRejectsInvalidClusterLayouts(t *testing.T) {
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	valid := ClusterOptions{
		Nodes:            3,
		Voters:           3,
		Host:             "127.0.0.1",
		StartingBasePort: 8000,
		DataRoot:         filepath.Join(t.TempDir(), "data"),
		SecretFile:       secretFile,
	}
	tests := []struct {
		name   string
		mutate func(*ClusterOptions)
	}{
		{name: "fewer_than_three_nodes", mutate: func(options *ClusterOptions) { options.Nodes = 2 }},
		{name: "even_voter_count", mutate: func(options *ClusterOptions) { options.Voters = 4; options.Nodes = 4 }},
		{name: "more_voters_than_nodes", mutate: func(options *ClusterOptions) { options.Voters = 5 }},
		{name: "base_service_range_overflow", mutate: func(options *ClusterOptions) { options.StartingBasePort = 65530 }},
		{name: "later_node_range_overflow", mutate: func(options *ClusterOptions) { options.StartingBasePort = 65400 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if configs, err := GenerateConfigs(options); err == nil {
				t.Fatalf("GenerateConfigs returned %d configs", len(configs))
			}
		})
	}
}

func TestValidateGeneratedPortRangesRejectsOverlap(t *testing.T) {
	services := []config.ServiceSpec{
		{Service: config.ServiceSWIMPing, Name: "first", Offset: 0, Transport: config.TransportUDP},
		{Service: config.ServiceRaftRPC, Name: "last", Offset: 8, Transport: config.TransportTCP},
	}
	err := validateGeneratedPortRanges([]uint64{8000, 8005}, services)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("validateGeneratedPortRanges error = %v, want overlap rejection", err)
	}
}

func TestPrepareClusterFilesWritesStrictConfigsAndTrustworthyInitialState(t *testing.T) {
	secretContents := []byte("private-local-cluster-secret")
	secretFile := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretFile, secretContents, 0o600); err != nil {
		t.Fatal(err)
	}
	dataRoot := filepath.Join(t.TempDir(), "cluster-data")
	configs, err := GenerateConfigs(ClusterOptions{
		Nodes:            3,
		Voters:           3,
		Host:             "127.0.0.1",
		StartingBasePort: 8000,
		DataRoot:         dataRoot,
		SecretFile:       secretFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(configs[1].StorageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	existingStore := swim.NewFileIncarnationStore(filepath.Join(configs[1].StorageDir, swim.IncarnationStateFilename))
	if err := existingStore.Store(7); err != nil {
		t.Fatal(err)
	}

	paths, err := prepareClusterFiles(dataRoot, configs)
	if err != nil {
		t.Fatalf("prepareClusterFiles: %v", err)
	}
	if len(paths) != len(configs) {
		t.Fatalf("len(paths) = %d, want %d", len(paths), len(configs))
	}
	for index, path := range paths {
		wantPath := filepath.Join(dataRoot, "configs", "node-"+string(rune('1'+index))+".json")
		if path != wantPath {
			t.Errorf("path %d = %q, want %q", index, path, wantPath)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("config %d mode = %o, want 600", index, info.Mode().Perm())
		}
		decoded, err := config.Load(path)
		if err != nil {
			t.Fatalf("strict load config %d: %v", index, err)
		}
		if !reflect.DeepEqual(decoded, configs[index]) {
			t.Errorf("decoded config %d = %#v, want %#v", index, decoded, configs[index])
		}
		store := swim.NewFileIncarnationStore(filepath.Join(configs[index].StorageDir, swim.IncarnationStateFilename))
		got, err := store.Load()
		if err != nil {
			t.Fatalf("load node %d incarnation: %v", index+1, err)
		}
		want := uint64(1)
		if index == 1 {
			want = 7
		}
		if got != want {
			t.Errorf("node %d incarnation = %d, want %d", index+1, got, want)
		}
	}
	info, err := os.Stat(secretFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret mode changed to %o", info.Mode().Perm())
	}
	gotSecret, err := os.ReadFile(secretFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotSecret, secretContents) {
		t.Fatalf("secret contents changed to %q", gotSecret)
	}
}

func TestPrefixWriterPrefixesEveryLineAcrossChunkBoundaries(t *testing.T) {
	var output bytes.Buffer
	writer := newPrefixWriter(&output, "[node-2] ")
	if _, err := writer.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(" line\nsecond\npartial")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "[node-2] first line\n[node-2] second\n[node-2] partial\n"; got != want {
		t.Fatalf("prefixed output = %q, want %q", got, want)
	}
}
