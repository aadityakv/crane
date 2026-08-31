package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/node"
	"github.com/aaditya/cs425mp3/internal/raft"
	internalrandom "github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/swim"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const incarnationStateFilename = swim.IncarnationStateFilename

const bootstrapSnapshotSchemaVersion uint32 = 1

type nodeFlags struct {
	configPath    string
	nodeID        uint64
	bindHost      string
	advertiseHost string
	basePort      uint64
	storageDir    string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := executeNode(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "node: %v\n", err)
		os.Exit(1)
	}
}

func executeNode(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("node context is nil")
	}
	configuration, err := loadNodeConfiguration(args)
	if err != nil {
		return err
	}
	runtime, err := newLocalRuntime(configuration)
	if err != nil {
		return err
	}
	return runSupervisedNode(ctx, configuration.NodeID, runtime.services, os.Stdout)
}

func runSupervisedNode(ctx context.Context, nodeID uint16, services []node.Service, readyWriter io.Writer) error {
	if ctx == nil {
		return errors.New("supervised node context is nil")
	}
	if nodeID == 0 {
		return errors.New("supervised node ID is zero")
	}
	if len(services) == 0 {
		return errors.New("supervised node services are empty")
	}
	for index, service := range services {
		if service == nil {
			return fmt.Errorf("supervised node service at index %d is nil", index)
		}
	}
	if readyWriter == nil {
		return errors.New("supervised node readiness writer is nil")
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	supervisor := node.NewSupervisor(services...)
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(runContext) }()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return <-result
	case <-supervisor.Ready():
		select {
		case err := <-result:
			return err
		default:
		}
		if err := ctx.Err(); err != nil {
			return <-result
		}
		if _, err := fmt.Fprintln(readyWriter, node.ReadySignal(nodeID)); err != nil {
			cancel()
			return errors.Join(fmt.Errorf("write node readiness signal: %w", err), <-result)
		}
		return <-result
	}
}

type localRuntime struct {
	swim     *swim.Service
	raft     *raft.Service
	services []node.Service
}

func newLocalRuntime(configuration config.NodeConfig) (*localRuntime, error) {
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate node configuration: %w", err)
	}
	if err := swim.EnsureStorageDirectory(configuration.StorageDir); err != nil {
		return nil, fmt.Errorf("prepare node storage: %w", err)
	}
	secret, err := config.LoadClusterSecret(configuration.ClusterSecretFile)
	if err != nil {
		return nil, err
	}
	seed, err := randomSeed()
	if err != nil {
		return nil, err
	}
	realClock := clock.NewReal()
	sharedRandom := internalrandom.NewLockedSource(seed)

	swimService, err := constructSWIMService(configuration, secret, realClock, sharedRandom)
	if err != nil {
		return nil, err
	}
	runtime := &localRuntime{
		swim:     swimService,
		services: []node.Service{swimService},
	}
	if _, voter := configuration.RaftVoterByID(configuration.NodeID); !voter {
		return runtime, nil
	}
	raftService, err := raft.NewService(raft.ServiceOptions{
		Config:                 ownedNodeConfig(configuration),
		ApplicationFingerprint: model.ConsensusFingerprint(),
		Secret:                 append([]byte(nil), secret...),
		Clock:                  realClock,
		Random:                 sharedRandom,
		StateMachine:           &bootstrapStateMachine{},
	})
	if err != nil {
		return nil, fmt.Errorf("construct Raft service: %w", err)
	}
	runtime.raft = raftService
	runtime.services = append(runtime.services, raftService)
	return runtime, nil
}

type bootstrapStateMachine struct{}

func (*bootstrapStateMachine) Apply(index, term uint64, command []byte) ([]byte, error) {
	return nil, fmt.Errorf("bootstrap Raft application rejects command at index %d term %d (%d bytes)", index, term, len(command))
}

func (*bootstrapStateMachine) Capture(uint64, uint64) (raft.SnapshotCapture, error) {
	return bootstrapSnapshot{}, nil
}

func (*bootstrapStateMachine) Restore(schemaVersion uint32, snapshot []byte) error {
	if len(snapshot) != 0 {
		return fmt.Errorf("bootstrap Raft application rejects non-empty snapshot")
	}
	if schemaVersion != 0 && schemaVersion != bootstrapSnapshotSchemaVersion {
		return fmt.Errorf("bootstrap Raft application rejects snapshot schema %d", schemaVersion)
	}
	return nil
}

type bootstrapSnapshot struct{}

func (bootstrapSnapshot) SchemaVersion() uint32 { return bootstrapSnapshotSchemaVersion }

func (bootstrapSnapshot) MarshalBinary() ([]byte, error) { return []byte{}, nil }

func loadNodeConfiguration(args []string) (config.NodeConfig, error) {
	options, err := parseNodeFlags(args)
	if err != nil {
		return config.NodeConfig{}, err
	}
	configuration, err := config.Load(options.configPath)
	if err != nil {
		return config.NodeConfig{}, err
	}
	if options.nodeID != 0 {
		configuration.NodeID = uint16(options.nodeID)
	}
	if options.bindHost != "" {
		configuration.BindHost = options.bindHost
	}
	if options.advertiseHost != "" {
		configuration.AdvertiseHost = options.advertiseHost
	}
	if options.basePort != 0 {
		configuration.BasePort = uint16(options.basePort)
	}
	if options.storageDir != "" {
		configuration.StorageDir = options.storageDir
	}
	if err := configuration.Validate(); err != nil {
		return config.NodeConfig{}, fmt.Errorf("validate node configuration after local overrides: %w", err)
	}
	return configuration, nil
}

func parseNodeFlags(args []string) (nodeFlags, error) {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options nodeFlags
	flags.StringVar(&options.configPath, "config", "", "strict node configuration file")
	flags.Uint64Var(&options.nodeID, "node-id", 0, "local node ID override")
	flags.StringVar(&options.bindHost, "bind-host", "", "local bind host override")
	flags.StringVar(&options.advertiseHost, "advertise-host", "", "local advertise host override")
	flags.Uint64Var(&options.basePort, "base-port", 0, "local base port override")
	flags.StringVar(&options.storageDir, "storage-dir", "", "local storage directory override")
	if err := flags.Parse(args); err != nil {
		return nodeFlags{}, fmt.Errorf("parse node flags: %w", err)
	}
	if flags.NArg() != 0 {
		return nodeFlags{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if options.configPath == "" {
		return nodeFlags{}, fmt.Errorf("-config is required")
	}
	if options.nodeID > 65535 {
		return nodeFlags{}, fmt.Errorf("-node-id %d exceeds 65535", options.nodeID)
	}
	if options.basePort > 65535 {
		return nodeFlags{}, fmt.Errorf("-base-port %d exceeds 65535", options.basePort)
	}
	return options, nil
}

func newSWIMService(configuration config.NodeConfig) (*swim.Service, error) {
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate node configuration: %w", err)
	}
	if err := swim.EnsureStorageDirectory(configuration.StorageDir); err != nil {
		return nil, fmt.Errorf("prepare node storage: %w", err)
	}
	secret, err := config.LoadClusterSecret(configuration.ClusterSecretFile)
	if err != nil {
		return nil, err
	}
	seed, err := randomSeed()
	if err != nil {
		return nil, err
	}
	return constructSWIMService(configuration, secret, clock.NewReal(), internalrandom.NewLockedSource(seed))
}

func constructSWIMService(configuration config.NodeConfig, secret []byte, realClock clock.Clock, randomSource *internalrandom.LockedSource) (*swim.Service, error) {
	return swim.NewService(swim.ServiceOptions{
		Config:        ownedNodeConfig(configuration),
		Authenticator: wire.NewHMACAuthenticator(secret),
		Clock:         realClock,
		Random:        randomSource,
		Store:         swim.NewFileIncarnationStore(filepath.Join(configuration.StorageDir, incarnationStateFilename)),
	})
}

func ownedNodeConfig(configuration config.NodeConfig) config.NodeConfig {
	owned := configuration
	owned.RaftVoters = append([]config.RaftVoter(nil), configuration.RaftVoters...)
	return owned
}

func randomSeed() (int64, error) {
	var encoded [8]byte
	if _, err := cryptorand.Read(encoded[:]); err != nil {
		return 0, fmt.Errorf("seed node randomness: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(encoded[:])), nil
}
