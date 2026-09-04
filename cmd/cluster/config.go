package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/swim"
)

const localNodePortStride = 100

// consensusStampFilename names the data-root file recording the consensus
// fingerprint of the binary that bootstrapped the persisted cluster state.
const consensusStampFilename = "consensus-fingerprint"

// ClusterOptions describes the local cluster to generate node configurations for.
type ClusterOptions struct {
	Nodes            int
	Voters           int
	Host             string
	StartingBasePort uint16
	DataRoot         string
	SecretFile       string
	// ClusterID, when set, is the persisted cluster UUID to reuse instead of
	// minting a fresh one (see resolveClusterID).
	ClusterID string
	// Dashboard is the optional loopback host:port of the read-only job
	// dashboard; empty disables it.
	Dashboard string
}

// GenerateConfigs derives one validated node configuration per node, with contiguous ports and a shared secret and cluster ID.
func GenerateConfigs(options ClusterOptions) ([]config.NodeConfig, error) {
	if options.Nodes < 3 {
		return nil, fmt.Errorf("local cluster requires at least three nodes")
	}
	if options.Nodes > math.MaxUint16 {
		return nil, fmt.Errorf("node count %d exceeds node ID range", options.Nodes)
	}
	if options.Voters != 3 && options.Voters != 5 {
		return nil, fmt.Errorf("voter count must be three or five")
	}
	if options.Voters > options.Nodes {
		return nil, fmt.Errorf("voter count %d exceeds node count %d", options.Voters, options.Nodes)
	}
	if options.DataRoot == "" {
		return nil, fmt.Errorf("data root is empty")
	}
	if options.SecretFile == "" {
		return nil, fmt.Errorf("secret file is empty")
	}

	bases, err := generatedBasePorts(options.Nodes, options.StartingBasePort)
	if err != nil {
		return nil, err
	}
	voters := make([]config.RaftVoter, options.Voters)
	for index := range voters {
		candidate := config.NodeConfig{AdvertiseHost: options.Host, BasePort: bases[index]}
		endpoint, err := candidate.AdvertiseEndpoint(config.ServiceRaftRPC)
		if err != nil {
			return nil, fmt.Errorf("derive voter %d endpoint: %w", index+1, err)
		}
		voters[index] = config.RaftVoter{NodeID: uint16(index + 1), Endpoint: endpoint.String()}
	}
	introducerConfig := config.NodeConfig{AdvertiseHost: options.Host, BasePort: bases[0]}
	introducer, err := introducerConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	if err != nil {
		return nil, fmt.Errorf("derive introducer endpoint: %w", err)
	}
	clusterID := options.ClusterID
	if clusterID == "" {
		fresh, err := newClusterID()
		if err != nil {
			return nil, err
		}
		clusterID = fresh
	}

	result := make([]config.NodeConfig, options.Nodes)
	for index := range result {
		result[index] = config.NodeConfig{
			NodeID:            uint16(index + 1),
			ClusterID:         clusterID,
			BindHost:          options.Host,
			AdvertiseHost:     options.Host,
			BasePort:          bases[index],
			Introducer:        introducer.String(),
			StorageDir:        filepath.Join(options.DataRoot, fmt.Sprintf("node-%d", index+1)),
			ClusterSecretFile: options.SecretFile,
			RaftVoters:        append([]config.RaftVoter(nil), voters...),
			Timing:            config.DefaultTimingConfig(),
			Raft:              config.DefaultRaftConfig(),
			Crane:             config.DefaultCraneConfig(),
		}
		if err := result[index].Validate(); err != nil {
			return nil, fmt.Errorf("validate generated config for node %d: %w", index+1, err)
		}
	}
	return result, nil
}

func generatedBasePorts(nodes int, startingBasePort uint16) ([]uint16, error) {
	services := config.Services()
	rawBases := make([]uint64, nodes)
	for index := range rawBases {
		base := uint64(startingBasePort) + uint64(index)*localNodePortStride
		rawBases[index] = base
	}
	if err := validateGeneratedPortRanges(rawBases, services); err != nil {
		return nil, err
	}
	bases := make([]uint16, len(rawBases))
	for index, base := range rawBases {
		bases[index] = uint16(base)
	}
	return bases, nil
}

func validateGeneratedPortRanges(bases []uint64, services []config.ServiceSpec) error {
	if len(services) == 0 {
		return fmt.Errorf("service registry is empty")
	}
	var maxOffset uint16
	for _, service := range services {
		if service.Offset > maxOffset {
			maxOffset = service.Offset
		}
	}
	previousEnd := uint64(0)
	for index, base := range bases {
		end := base + uint64(maxOffset)
		if base == 0 || end > math.MaxUint16 {
			return fmt.Errorf("node %d port range %d-%d exceeds valid port range", index+1, base, end)
		}
		if index > 0 && base <= previousEnd {
			return fmt.Errorf("node %d port range %d-%d overlaps previous range ending at %d", index+1, base, end, previousEnd)
		}
		previousEnd = end
	}
	return nil
}

func newClusterID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate cluster ID: %w", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func prepareClusterFiles(dataRoot string, configurations []config.NodeConfig) ([]string, error) {
	configDirectory := filepath.Join(dataRoot, "configs")
	if err := swim.EnsureStorageDirectory(configDirectory); err != nil {
		return nil, fmt.Errorf("prepare cluster config directory: %w", err)
	}
	for _, configuration := range configurations {
		if err := configuration.Validate(); err != nil {
			return nil, fmt.Errorf("validate node %d config: %w", configuration.NodeID, err)
		}
		if err := swim.EnsureStorageDirectory(configuration.StorageDir); err != nil {
			return nil, fmt.Errorf("prepare node %d storage directory: %w", configuration.NodeID, err)
		}
	}
	paths := make([]string, len(configurations))
	for index, configuration := range configurations {
		store := swim.NewFileIncarnationStore(filepath.Join(configuration.StorageDir, swim.IncarnationStateFilename))
		incarnation, err := store.Load()
		if err != nil {
			return nil, fmt.Errorf("load node %d incarnation: %w", configuration.NodeID, err)
		}
		if incarnation == 0 {
			if err := store.Store(1); err != nil {
				return nil, fmt.Errorf("initialize node %d incarnation: %w", configuration.NodeID, err)
			}
		}

		content, err := json.MarshalIndent(configuration, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("encode node %d config: %w", configuration.NodeID, err)
		}
		content = append(content, '\n')
		path := filepath.Join(configDirectory, fmt.Sprintf("node-%d.json", configuration.NodeID))
		if err := os.WriteFile(path, content, 0o600); err != nil {
			return nil, fmt.Errorf("write node %d config: %w", configuration.NodeID, err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, fmt.Errorf("protect node %d config: %w", configuration.NodeID, err)
		}
		paths[index] = path
	}
	return paths, nil
}

// ensureClusterSecret creates a missing cluster secret with owner-only
// permissions and leaves an existing secret untouched. A lost creation race
// surfaces as an error rather than a silent overwrite.
func ensureClusterSecret(secretFile string) error {
	if _, err := os.Stat(secretFile); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect cluster secret: %w", err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate cluster secret: %w", err)
	}
	file, err := os.OpenFile(secretFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create cluster secret: %w", err)
	}
	if _, err := file.Write(secret); err != nil {
		_ = file.Close()
		return fmt.Errorf("write cluster secret: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync cluster secret: %w", err)
	}
	return file.Close()
}

// resolveClusterID returns the persisted data-root cluster UUID, creating one
// for a fresh data root and refusing existing node storage without one, so a
// re-run resumes the same cluster instead of invalidating its state.
func resolveClusterID(dataRoot string) (string, error) {
	path := filepath.Join(dataRoot, "cluster-id")
	if persisted, err := os.ReadFile(path); err == nil {
		value := strings.TrimSpace(string(persisted))
		if value == "" {
			return "", fmt.Errorf("cluster ID file %s is empty", path)
		}
		return value, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read cluster ID: %w", err)
	}
	if err := os.MkdirAll(dataRoot, 0o700); err != nil {
		return "", fmt.Errorf("prepare data root: %w", err)
	}
	if holdsNodeStorage(dataRoot) {
		return "", fmt.Errorf("data root %s holds existing node storage without %s; choose a fresh data root or restore the cluster-id file", dataRoot, path)
	}
	clusterID, err := newClusterID()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(clusterID+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("persist cluster ID: %w", err)
	}
	return clusterID, nil
}

// holdsNodeStorage reports whether the data root already contains node-*
// storage directories.
func holdsNodeStorage(dataRoot string) bool {
	entries, err := os.ReadDir(dataRoot)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "node-") {
			return true
		}
	}
	return false
}

// reconcileDataRoot keeps local development resume-and-reset semantics
// honest: it compares the persisted consensus fingerprint stamp with the
// compiled one and either resumes a compatible data root, wipes and
// re-stamps an incompatible one under resetIncompatible, or refuses the
// mismatch up front with an actionable error. The stamp is written only
// when a fresh bootstrap begins, never onto an existing unstamped legacy
// root whose compatibility is unknowable.
func reconcileDataRoot(dataRoot string, resetIncompatible bool, stdout io.Writer) error {
	path := filepath.Join(dataRoot, consensusStampFilename)
	current := model.ConsensusFingerprintHex()
	persisted, err := os.ReadFile(path)
	switch {
	case err == nil:
		stored := strings.TrimSpace(string(persisted))
		if stored == current {
			return nil
		}
		if !resetIncompatible {
			return fmt.Errorf("data root %s holds state written under consensus fingerprint %s but this binary requires %s; pass -reset-incompatible to reset it or choose a fresh -data-root", dataRoot, stored, current)
		}
		if err := wipeIncompatibleDataRoot(dataRoot); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "cluster: data root %s was written under consensus fingerprint %s; reset for %s\n", dataRoot, stored, current)
		return nil
	case errors.Is(err, os.ErrNotExist):
		if !holdsNodeStorage(dataRoot) {
			return writeConsensusStamp(dataRoot)
		}
		if !resetIncompatible {
			return nil
		}
		if err := wipeIncompatibleDataRoot(dataRoot); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "cluster: data root %s predates consensus fingerprint stamping; reset for %s\n", dataRoot, current)
		return nil
	default:
		return fmt.Errorf("read consensus fingerprint stamp: %w", err)
	}
}

// wipeIncompatibleDataRoot removes the data root contents and writes a
// fresh stamp so the launcher bootstraps a clean cluster under the current
// binary.
func wipeIncompatibleDataRoot(dataRoot string) error {
	if err := resetDataRoot(dataRoot); err != nil {
		return err
	}
	return writeConsensusStamp(dataRoot)
}

// resetDataRoot removes every child of the data root, refusing paths that
// would erase the working directory or a filesystem root.
func resetDataRoot(dataRoot string) error {
	cleaned := filepath.Clean(dataRoot)
	if cleaned == "." || cleaned == string(filepath.Separator) || filepath.Dir(cleaned) == cleaned {
		return fmt.Errorf("refusing to reset unsafe data root %q", dataRoot)
	}
	if err := os.RemoveAll(dataRoot); err != nil {
		return fmt.Errorf("remove incompatible data root: %w", err)
	}
	if err := os.MkdirAll(dataRoot, 0o700); err != nil {
		return fmt.Errorf("recreate data root: %w", err)
	}
	return nil
}

// writeConsensusStamp records the compiled consensus fingerprint in the
// data root so later runs can tell compatible persisted state from
// incompatible.
func writeConsensusStamp(dataRoot string) error {
	if err := os.MkdirAll(dataRoot, 0o700); err != nil {
		return fmt.Errorf("prepare data root: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dataRoot, consensusStampFilename), []byte(model.ConsensusFingerprintHex()+"\n"), 0o600); err != nil {
		return fmt.Errorf("persist consensus fingerprint stamp: %w", err)
	}
	return nil
}
