// Command crane is the crash-safe Crane control CLI. It authenticates as one
// configured cluster member, keeps a durable client identity store so
// interrupted submissions and cancellations resume with their exact reserved
// bytes, and emits stable machine-readable JSON lines on stdout.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/model"
)

// defaultResultPageBytes is the default complete-record page budget.
const defaultResultPageBytes = 64 << 10

const craneUsage = "usage: crane <submit|cancel|status|results|jobs> [flags]\n" +
	"  submit  -config FILE -state FILE -topology FILE\n" +
	"  cancel  -config FILE -state FILE -job HEX32 -expected-revision N\n" +
	"  status  -config FILE -job HEX32\n" +
	"  results -config FILE -job HEX32 [-page-bytes N] [-count-by FIELD [-top N]]\n" +
	"  jobs    -config FILE\n" +
	"network subcommands also accept -attempts N (1..1024), -backoff DURATION, and -timeout DURATION"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := executeCrane(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "crane: %v\n", err)
		os.Exit(1)
	}
}

// craneFlags carries the validated flags shared by every network subcommand.
type craneFlags struct {
	configPath string
	statePath  string
	jobHex     string
	topology   string
	revision   uint64
	pageBytes  uint
	countBy    string
	top        uint
	attempts   uint
	backoff    time.Duration
	timeout    time.Duration
}

// parseCraneFlags strictly parses one subcommand flag set with no positional
// arguments permitted.
func parseCraneFlags(name string, args []string, register func(*flag.FlagSet, *craneFlags)) (craneFlags, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options craneFlags
	flags.StringVar(&options.configPath, "config", "", "strict node configuration file")
	flags.UintVar(&options.attempts, "attempts", control.DefaultClientMaxAttempts, "bounded request attempts")
	flags.DurationVar(&options.backoff, "backoff", control.DefaultClientRetryBackoff, "retry backoff between attempts")
	flags.DurationVar(&options.timeout, "timeout", 0, "per-exchange timeout override")
	if register != nil {
		if register != nil {
			register(flags, &options)
		}
	}
	if err := flags.Parse(args); err != nil {
		return craneFlags{}, fmt.Errorf("parse %s flags: %w", name, err)
	}
	if flags.NArg() != 0 {
		return craneFlags{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if options.configPath == "" {
		return craneFlags{}, errors.New("-config is required")
	}
	if options.attempts == 0 || options.attempts > 1024 {
		return craneFlags{}, fmt.Errorf("-attempts %d is outside 1 through 1024", options.attempts)
	}
	if options.backoff < 0 || options.timeout < 0 {
		return craneFlags{}, errors.New("-backoff and -timeout must not be negative")
	}
	return options, nil
}

func registerStateFlag(flags *flag.FlagSet, options *craneFlags) {
	flags.StringVar(&options.statePath, "state", "", "durable client identity state file")
}

func registerJobFlag(flags *flag.FlagSet, options *craneFlags) {
	flags.StringVar(&options.jobHex, "job", "", "job ID as 32 hexadecimal characters")
}

func parseStageRole(role string) (model.StageRole, error) {
	switch role {
	case "source":
		return model.StageSource, nil
	case "transform":
		return model.StageTransform, nil
	case "sink":
		return model.StageSink, nil
	default:
		return 0, fmt.Errorf("unknown stage role %q", role)
	}
}

func parseRoutingMode(routing string) (model.RoutingMode, error) {
	switch routing {
	case "shuffle":
		return model.RoutingShuffle, nil
	case "field_hash":
		return model.RoutingFieldHash, nil
	case "broadcast":
		return model.RoutingBroadcast, nil
	default:
		return 0, fmt.Errorf("unknown edge routing %q", routing)
	}
}

// parseJobID parses one 32-character lowercase hexadecimal job identity.
func parseJobID(encoded string) (model.JobID, error) {
	if encoded == "" {
		return model.JobID{}, errors.New("-job is required")
	}
	if len(encoded) != 32 || encoded != strings.ToLower(encoded) {
		return model.JobID{}, fmt.Errorf("-job must be 32 lowercase hexadecimal characters, got %q", encoded)
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return model.JobID{}, fmt.Errorf("-job is not hexadecimal: %w", err)
	}
	var job model.JobID
	copy(job[:], decoded)
	if err := job.Validate(); err != nil {
		return model.JobID{}, fmt.Errorf("invalid job ID: %w", err)
	}
	return job, nil
}

// decodeClusterUUID decodes the canonical configuration UUID into frame bytes.
func decodeClusterUUID(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("cluster ID is not a UUID")
	}
	var clusterID [16]byte
	copy(clusterID[:], decoded)
	return clusterID, nil
}

// readBoundedFile reads one regular file, refusing oversize content before
// use.
func readBoundedFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%q is not a regular file", path)
	}
	if info.Size() > maximum {
		return nil, fmt.Errorf("%q is %d bytes, maximum is %d", path, info.Size(), maximum)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	return encoded, nil
}
