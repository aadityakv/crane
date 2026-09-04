package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"crane/internal/config"
	"crane/internal/swim"
)

const localNodePortStride = 100

type ClusterOptions struct {
	Nodes            int
	Voters           int
	Host             string
	StartingBasePort uint16
	DataRoot         string
	SecretFile       string
}

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
	clusterID, err := newClusterID()
	if err != nil {
		return nil, err
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
