// Command node runs one Crane cluster member. It loads and validates the
// strict node configuration, composes the complete runtime (SWIM membership,
// the admission gate and replicated state machine, the worker services, Raft
// and the coordinator on configured voters, and the control service), and
// runs it under a supervisor that reports readiness on stdout. The process
// exits non-zero whenever any owned service fails, so a node is either fully
// serving or not running at all.
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

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	craneruntime "github.com/aadityakv/crane/internal/crane/runtime"
	"github.com/aadityakv/crane/internal/node"
	internalrandom "github.com/aadityakv/crane/internal/random"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

const incarnationStateFilename = swim.IncarnationStateFilename

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
	return runSupervisedNode(ctx, configuration.NodeID, []node.Service{runtime}, os.Stdout)
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

// newLocalRuntime validates the strict configuration and composes the
// complete Crane node runtime: SWIM, the membership authorizer, the shared
// admission gate and state machine, the worker services, Raft plus the
// coordinator on configured voters, and the +4 control service.
func newLocalRuntime(configuration config.NodeConfig) (*craneruntime.Runtime, error) {
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
	return craneruntime.New(configuration, craneruntime.Dependencies{
		Secret: secret,
		Clock:  clock.NewReal(),
		Random: internalrandom.NewLockedSource(seed),
	})
}

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
