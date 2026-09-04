package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/clientstate"
	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/wire"
)

// executeCrane dispatches one strict subcommand invocation.
func executeCrane(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("crane context is nil")
	}
	if stdout == nil || stderr == nil {
		return errors.New("crane output writers are nil")
	}
	if len(args) == 0 {
		return errors.New(craneUsage)
	}
	switch args[0] {
	case "submit":
		return executeSubmit(ctx, args[1:], stdout)
	case "cancel":
		return executeCancel(ctx, args[1:], stdout)
	case "status":
		return executeStatus(ctx, args[1:], stdout)
	case "results":
		return executeResults(ctx, args[1:], stdout)
	case "jobs":
		return executeJobs(ctx, args[1:], stdout)
	default:
		return fmt.Errorf("unknown subcommand %q: %s", args[0], craneUsage)
	}
}

// craneClient composes the durable client for one subcommand invocation.
func craneClient(options craneFlags, needStore bool) (*control.Client, *clientstate.ClientStore, error) {
	configuration, err := config.Load(options.configPath)
	if err != nil {
		return nil, nil, err
	}
	secret, err := config.LoadClusterSecret(configuration.ClusterSecretFile)
	if err != nil {
		return nil, nil, err
	}
	clusterID, err := decodeClusterUUID(configuration.ClusterID)
	if err != nil {
		return nil, nil, err
	}
	var store *clientstate.ClientStore
	if needStore {
		if options.statePath == "" {
			return nil, nil, errors.New("-state is required for durable commands")
		}
		store, err = clientstate.OpenClientState(options.statePath, clusterID)
		if err != nil {
			return nil, nil, err
		}
	}
	client, err := control.NewClient(control.ClientOptions{
		Config:         configuration,
		Authenticator:  wire.NewHMACAuthenticator(secret),
		Clock:          clock.NewReal(),
		Store:          store,
		MaxAttempts:    int(options.attempts),
		RetryBackoff:   options.backoff,
		RequestTimeout: options.timeout,
	})
	if err != nil {
		return nil, nil, err
	}
	return client, store, nil
}

// executeSubmit submits one strict topology document as a durable command.
func executeSubmit(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("submit", args, func(flags *flag.FlagSet, options *craneFlags) {
		registerStateFlag(flags, options)
		flags.StringVar(&options.topology, "topology", "", "topology DAG JSON file")
	})
	if err != nil {
		return err
	}
	if options.topology == "" {
		return errors.New("-topology is required")
	}
	document, err := readBoundedFile(options.topology, maxTopologyDocumentBytes)
	if err != nil {
		return err
	}
	topology, err := parseTopologyDocument(document)
	if err != nil {
		return err
	}
	client, _, err := craneClient(options, true)
	if err != nil {
		return err
	}
	job, revision, err := client.Submit(ctx, topology)
	if err != nil {
		return err
	}
	return writeJSONLine(stdout, map[string]any{
		"command":              "submit",
		"job_id":               hex.EncodeToString(job[:]),
		"job_control_revision": revision,
		"state":                "pending",
	})
}

// executeCancel cancels one exact retained job revision as a durable command.
func executeCancel(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("cancel", args, func(flags *flag.FlagSet, options *craneFlags) {
		registerStateFlag(flags, options)
		registerJobFlag(flags, options)
		flags.Uint64Var(&options.revision, "expected-revision", 0, "expected job-control revision")
	})
	if err != nil {
		return err
	}
	job, err := parseJobID(options.jobHex)
	if err != nil {
		return err
	}
	if options.revision == 0 {
		return errors.New("-expected-revision is required and must be nonzero")
	}
	client, _, err := craneClient(options, true)
	if err != nil {
		return err
	}
	revision, err := client.Cancel(ctx, job, options.revision)
	if err != nil {
		return err
	}
	return writeJSONLine(stdout, map[string]any{
		"command":              "cancel",
		"job_id":               options.jobHex,
		"job_control_revision": revision,
		"state":                "canceled",
	})
}

// executeStatus reads one linearizable retained-job summary.
func executeStatus(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("status", args, registerJobFlag)
	if err != nil {
		return err
	}
	job, err := parseJobID(options.jobHex)
	if err != nil {
		return err
	}
	client, _, err := craneClient(options, false)
	if err != nil {
		return err
	}
	status, err := client.Status(ctx, job)
	if err != nil {
		return err
	}
	return writeJSONLine(stdout, statusOutput(status))
}

// executeJobs reads one linearizable summary of every retained job.
func executeJobs(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("jobs", args, nil)
	if err != nil {
		return err
	}
	client, _, err := craneClient(options, false)
	if err != nil {
		return err
	}
	listing, err := client.ListJobs(ctx)
	if err != nil {
		return err
	}
	return writeJSONLine(stdout, jobsOutput(listing))
}

// executeResults streams every committed result record for one job, binding
// the status-provided manifest-set digest into every page request.
func executeResults(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("results", args, func(flags *flag.FlagSet, options *craneFlags) {
		registerJobFlag(flags, options)
		flags.UintVar(&options.pageBytes, "page-bytes", defaultResultPageBytes, "complete-record page budget in bytes")
		flags.StringVar(&options.countBy, "count-by", "", "aggregate at read time: print how often each value of this field occurs")
		flags.UintVar(&options.top, "top", 0, "with -count-by, print only the N most frequent values (0 = all)")
	})
	if err != nil {
		return err
	}
	job, err := parseJobID(options.jobHex)
	if err != nil {
		return err
	}
	if options.pageBytes == 0 || options.pageBytes > uint(protocol.MaxResultPageBytes) {
		return fmt.Errorf("-page-bytes %d is outside 1 through %d", options.pageBytes, protocol.MaxResultPageBytes)
	}
	client, _, err := craneClient(options, false)
	if err != nil {
		return err
	}
	status, err := client.Status(ctx, job)
	if err != nil {
		return err
	}
	if !status.HasManifestSet {
		return fmt.Errorf("job %s has no committed manifest set yet; results are not complete", options.jobHex)
	}

	request := protocol.ResultPageRequest{JobID: job, ManifestDigest: status.ManifestSetDigest, PageBytes: uint32(options.pageBytes)}
	total := 0
	counts := map[string]int{}
	for {
		page, err := client.ResultPage(ctx, request)
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			line, err := resultRecordOutput(record)
			if err != nil {
				return err
			}
			if options.countBy != "" {
				value, ok := line["fields"].(map[string]any)[options.countBy]
				if !ok {
					return fmt.Errorf("result record has no field %q", options.countBy)
				}
				counts[fmt.Sprint(value)]++
			} else if err := writeJSONLine(stdout, line); err != nil {
				return err
			}
			total++
		}
		if page.End {
			break
		}
		if !page.NextHasLastTuple {
			return errors.New("result page neither ended nor advanced its cursor")
		}
		request.HasLastTuple = true
		request.Last = page.NextLast
	}
	if options.countBy != "" {
		writeCountTable(stdout, options.countBy, counts, total, int(options.top))
	}
	return writeJSONLine(stdout, map[string]any{
		"command":  "results",
		"job_id":   options.jobHex,
		"records":  total,
		"complete": true,
	})
}
