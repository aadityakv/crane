package config

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NodeConfig is the complete file-controlled identity, endpoint, security, and timing configuration for one node.
type NodeConfig struct {
	// NodeID is the stable logical identity advertised to cluster peers.
	NodeID uint16 `json:"node_id"`
	// ClusterID separates messages and membership belonging to different clusters.
	ClusterID string `json:"cluster_id"`
	// BindHost is the local interface on which this process opens listeners.
	BindHost string `json:"bind_host"`
	// AdvertiseHost is the routable IP or DNS name other nodes use to contact this node.
	AdvertiseHost string `json:"advertise_host"`
	// BasePort is combined with the modeled service offsets to derive listener ports.
	BasePort uint16 `json:"base_port"`
	// Introducer is the SWIM join/snapshot endpoint used only for initial admission.
	Introducer string `json:"introducer"`
	// StorageDir contains this node's persistent SWIM, Raft, and SDFS state.
	StorageDir string `json:"storage_dir"`
	// ClusterSecretFile names the permission-restricted HMAC key file.
	ClusterSecretFile string `json:"cluster_secret_file"`
	// RaftVoters is the identical fixed voter ID/endpoint map configured on every node.
	RaftVoters []RaftVoter `json:"raft_voters"`
	// Timing controls validated SWIM and replay-protection intervals.
	Timing TimingConfig `json:"timing"`
}

// RaftVoter identifies one member of the fixed Raft voting set.
type RaftVoter struct {
	// NodeID is the stable identity of the voter.
	NodeID uint16 `json:"node_id"`
	// Endpoint is the voter's statically configured Raft TCP endpoint.
	Endpoint string `json:"endpoint"`
}

// TimingConfig contains the deterministic timing values used by SWIM and replay protection.
type TimingConfig struct {
	// ProbeInterval is the target duration between the start of probe rounds.
	ProbeInterval Duration `json:"probe_interval"`
	// DirectProbeTimeout bounds the initial direct ping attempt.
	DirectProbeTimeout Duration `json:"direct_probe_timeout"`
	// IndirectProbeTimeout bounds the PING-REQ phase after direct failure.
	IndirectProbeTimeout Duration `json:"indirect_probe_timeout"`
	// SuspicionMultiplier scales suspicion duration with cluster size.
	SuspicionMultiplier uint16 `json:"suspicion_multiplier"`
	// IndirectChecks is the maximum number of relay peers per failed direct probe.
	IndirectChecks uint16 `json:"indirect_checks"`
	// ReplayWindow bounds accepted message age and request-ID retention.
	ReplayWindow Duration `json:"replay_window"`
}

// DefaultTimingConfig returns the deterministic timing configuration used when a JSON config omits timing fields.
func DefaultTimingConfig() TimingConfig {
	return TimingConfig{
		ProbeInterval:        Duration(time.Second),
		DirectProbeTimeout:   Duration(300 * time.Millisecond),
		IndirectProbeTimeout: Duration(700 * time.Millisecond),
		SuspicionMultiplier:  5,
		IndirectChecks:       3,
		ReplayWindow:         Duration(2 * time.Minute),
	}
}

// Decode strictly decodes, defaults, and validates one JSON node configuration.
func Decode(reader io.Reader) (NodeConfig, error) {
	config := NodeConfig{Timing: DefaultTimingConfig()}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return NodeConfig{}, fmt.Errorf("decode node config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return NodeConfig{}, fmt.Errorf("decode node config: trailing JSON value")
		}
		return NodeConfig{}, fmt.Errorf("decode node config trailing data: %w", err)
	}
	if err := config.Validate(); err != nil {
		return NodeConfig{}, err
	}
	return config, nil
}

// Load opens, decodes, defaults, and validates the configuration stored at path.
func Load(path string) (NodeConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("open node config %q: %w", path, err)
	}
	defer file.Close()
	return Decode(file)
}

// Validate rejects configurations that could create an ambiguous identity, unsafe endpoint, or invalid timing domain.
func (c NodeConfig) Validate() error {
	if c.NodeID == 0 {
		return fmt.Errorf("node ID must be nonzero")
	}
	if err := validateUUID(c.ClusterID); err != nil {
		return err
	}
	if err := validateBindHost(c.BindHost); err != nil {
		return err
	}
	if err := validateAdvertiseHost(c.AdvertiseHost); err != nil {
		return err
	}
	for _, service := range Services() {
		if _, err := c.BindEndpoint(service.Service); err != nil {
			return fmt.Errorf("derive bind endpoint for %s: %w", service.Name, err)
		}
		if _, err := c.AdvertiseEndpoint(service.Service); err != nil {
			return fmt.Errorf("derive advertise endpoint for %s: %w", service.Name, err)
		}
	}
	if _, err := ParseEndpoint(c.Introducer); err != nil {
		return fmt.Errorf("invalid introducer: %w", err)
	}
	if err := validateStorageDir(c.StorageDir); err != nil {
		return err
	}
	if err := validateSecretFile(c.ClusterSecretFile); err != nil {
		return err
	}
	if err := validateTiming(c.Timing); err != nil {
		return err
	}
	if len(c.RaftVoters) != 3 && len(c.RaftVoters) != 5 {
		return fmt.Errorf("raft voters must contain exactly three or five voters")
	}
	voterIDs := make(map[uint16]struct{}, len(c.RaftVoters))
	voterEndpoints := make(map[string]struct{}, len(c.RaftVoters))
	localEndpoint, err := c.AdvertiseEndpoint(ServiceRaftRPC)
	if err != nil {
		return err
	}
	localFound := false
	for _, voter := range c.RaftVoters {
		if voter.NodeID == 0 {
			return fmt.Errorf("raft voter ID must be nonzero")
		}
		if _, exists := voterIDs[voter.NodeID]; exists {
			return fmt.Errorf("duplicate raft voter ID %d", voter.NodeID)
		}
		voterIDs[voter.NodeID] = struct{}{}
		endpoint, err := ParseEndpoint(voter.Endpoint)
		if err != nil {
			return fmt.Errorf("invalid raft voter endpoint for node %d: %w", voter.NodeID, err)
		}
		endpointKey := endpoint.String()
		if _, exists := voterEndpoints[endpointKey]; exists {
			return fmt.Errorf("duplicate raft voter endpoint %q", endpointKey)
		}
		voterEndpoints[endpointKey] = struct{}{}
		if voter.NodeID == c.NodeID {
			localFound = true
			if endpoint != localEndpoint {
				return fmt.Errorf("local raft voter endpoint %q does not match advertised endpoint %q", endpoint, localEndpoint)
			}
		}
	}
	if !localFound {
		return fmt.Errorf("node ID %d is not a raft voter", c.NodeID)
	}
	return nil
}

func validateUUID(value string) error {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return fmt.Errorf("cluster ID must be a UUID")
	}
	encoded := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 16 {
		return fmt.Errorf("cluster ID must be a UUID")
	}
	return nil
}

func validateAdvertiseHost(host string) error {
	if err := validateHost(host); err != nil {
		return fmt.Errorf("invalid advertise host: %w", err)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("advertise host must not be a wildcard address")
	}
	return nil
}

func validateBindHost(host string) error {
	if err := validateHost(host); err != nil {
		return fmt.Errorf("invalid bind host: %w", err)
	}
	return nil
}

func validateHost(host string) error {
	if host == "" {
		return fmt.Errorf("host is empty")
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	if len(host) > 253 {
		return fmt.Errorf("DNS host exceeds 253 characters")
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return fmt.Errorf("DNS host contains an invalid label length")
		}
		if !isASCIIAlphaNumeric(label[0]) || !isASCIIAlphaNumeric(label[len(label)-1]) {
			return fmt.Errorf("DNS labels must start and end with an alphanumeric character")
		}
		for i := 1; i < len(label)-1; i++ {
			if label[i] != '-' && !isASCIIAlphaNumeric(label[i]) {
				return fmt.Errorf("DNS labels may contain only ASCII letters, digits, and hyphens")
			}
		}
	}
	return nil
}

func isASCIIAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func validateStorageDir(path string) error {
	if path == "" {
		return fmt.Errorf("storage directory is empty")
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("storage directory must not be a filesystem root")
	}
	return nil
}

func validateSecretFile(path string) error {
	if path == "" {
		return fmt.Errorf("cluster secret file is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat cluster secret file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("cluster secret file must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("cluster secret file permissions must not grant group or other access")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open cluster secret file: %w", err)
	}
	defer file.Close()
	return nil
}

func validateTiming(timing TimingConfig) error {
	if timing.ProbeInterval <= 0 || timing.DirectProbeTimeout <= 0 || timing.IndirectProbeTimeout <= 0 || timing.ReplayWindow <= 0 {
		return fmt.Errorf("all timing durations must be greater than zero")
	}
	if timing.SuspicionMultiplier == 0 {
		return fmt.Errorf("suspicion multiplier must be nonzero")
	}
	if timing.IndirectChecks == 0 {
		return fmt.Errorf("indirect checks must be nonzero")
	}
	if time.Duration(timing.DirectProbeTimeout)+time.Duration(timing.IndirectProbeTimeout) > time.Duration(timing.ProbeInterval) {
		return fmt.Errorf("direct and indirect probe timeouts exceed probe interval")
	}
	return nil
}
