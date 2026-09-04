// Command crane is the crash-safe Crane control CLI. It authenticates as one
// configured cluster member, keeps a durable client identity store so
// interrupted submissions and cancellations resume with their exact reserved
// bytes, and emits stable machine-readable JSON lines on stdout.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/clientstate"
	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/wire"
)

// maxTopologyDocumentBytes bounds one topology JSON document before parsing.
const maxTopologyDocumentBytes = 1 << 20

// defaultResultPageBytes is the default complete-record page budget.
const defaultResultPageBytes = 64 << 10

const craneUsage = "usage: crane <submit|cancel|status|results|example-topology> [flags]\n" +
	"  submit  -config FILE -state FILE -topology FILE\n" +
	"  cancel  -config FILE -state FILE -job HEX32 -expected-revision N\n" +
	"  status  -config FILE -job HEX32\n" +
	"  results -config FILE -job HEX32 [-page-bytes N]\n" +
	"  example-topology\n" +
	"network subcommands also accept -attempts N (1..1024), -backoff DURATION, and -timeout DURATION"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := executeCrane(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "crane: %v\n", err)
		os.Exit(1)
	}
}

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
	case "example-topology":
		return executeExampleTopology(args[1:], stdout)
	default:
		return fmt.Errorf("unknown subcommand %q: %s", args[0], craneUsage)
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
		register(flags, &options)
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

// statusOutput renders one stable machine-readable status line.
func statusOutput(status protocol.StatusResponse) map[string]any {
	output := map[string]any{
		"command":                "status",
		"job_id":                 hex.EncodeToString(status.JobID[:]),
		"state":                  jobStateName(status.State),
		"job_control_revision":   status.JobControlRevision,
		"applied_index":          status.AppliedIndex,
		"topology_digest":        hex.EncodeToString(status.TopologyDigest[:]),
		"source_task_count":      status.SourceTaskCount,
		"completed_source_tasks": status.CompletedSourceTasks,
		"result_partition_count": status.ResultPartitionCount,
		"manifest_count":         status.ManifestCount,
		"manifest_set_digest":    "",
		"assignment_revision":    status.AssignmentRevision,
		"has_failure":            status.HasFailure,
	}
	if status.HasManifestSet {
		output["manifest_set_digest"] = hex.EncodeToString(status.ManifestSetDigest[:])
	}
	if status.HasFailure {
		output["failure_code"] = uint16(status.FailureCode)
	}
	return output
}

// jobStateName renders the stable lowercase public lifecycle name.
func jobStateName(state protocol.JobState) string {
	switch state {
	case protocol.JobPending:
		return "pending"
	case protocol.JobDeploying:
		return "deploying"
	case protocol.JobRunning:
		return "running"
	case protocol.JobDraining:
		return "draining"
	case protocol.JobSucceeded:
		return "succeeded"
	case protocol.JobFailed:
		return "failed"
	case protocol.JobCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// executeResults streams every committed result record for one job, binding
// the status-provided manifest-set digest into every page request.
func executeResults(ctx context.Context, args []string, stdout io.Writer) error {
	options, err := parseCraneFlags("results", args, func(flags *flag.FlagSet, options *craneFlags) {
		registerJobFlag(flags, options)
		flags.UintVar(&options.pageBytes, "page-bytes", defaultResultPageBytes, "complete-record page budget in bytes")
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
			if err := writeJSONLine(stdout, line); err != nil {
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
	return writeJSONLine(stdout, map[string]any{
		"command":  "results",
		"job_id":   options.jobHex,
		"records":  total,
		"complete": true,
	})
}

// resultRecordOutput renders one canonical result record as decoded fields.
func resultRecordOutput(record model.ResultRecord) (map[string]any, error) {
	tuple, err := model.UnmarshalTuple(record.Value)
	if err != nil {
		return nil, fmt.Errorf("decode result tuple: %w", err)
	}
	fields := make(map[string]any, len(tuple.Fields))
	for _, field := range tuple.Fields {
		switch field.Value.Type {
		case model.ValueInt64:
			fields[field.Name] = field.Value.Int64
		case model.ValueString:
			fields[field.Name] = field.Value.String
		case model.ValueBytes:
			fields[field.Name] = base64.StdEncoding.EncodeToString(field.Value.Bytes)
		default:
			return nil, fmt.Errorf("result field %q has unknown value type %d", field.Name, field.Value.Type)
		}
	}
	return map[string]any{
		"job_id":           hex.EncodeToString(record.TupleID.JobID[:]),
		"source_stage_id":  record.TupleID.SourceTask.StageID,
		"source_partition": record.TupleID.SourceTask.Partition,
		"source_sequence":  record.TupleID.SourceSequence,
		"sink_stage_id":    record.SinkTask.StageID,
		"sink_partition":   record.SinkTask.Partition,
		"fields":           fields,
	}, nil
}

// executeExampleTopology emits the finite example DAG document.
func executeExampleTopology(args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("example-topology accepts no arguments: %v", args)
	}
	if _, err := stdout.Write(exampleTopologyJSON()); err != nil {
		return fmt.Errorf("write example topology: %w", err)
	}
	return nil
}

// topologyDocument is the strict JSON schema of one submitted DAG. The
// registry fingerprint is always the compiled contract and never user input.
type topologyDocument struct {
	SchemaVersion uint16          `json:"schema_version"`
	Name          string          `json:"name"`
	Stages        []stageDocument `json:"stages"`
	Edges         []edgeDocument  `json:"edges"`
}

type stageDocument struct {
	StageID     uint16           `json:"stage_id"`
	Name        string           `json:"name"`
	Role        string           `json:"role"`
	Parallelism uint16           `json:"parallelism"`
	Operator    operatorDocument `json:"operator"`
}

type operatorDocument struct {
	Name     string            `json:"name"`
	Version  uint16            `json:"version"`
	Settings []settingDocument `json:"settings,omitempty"`
}

type settingDocument struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type edgeDocument struct {
	EdgeID             uint16 `json:"edge_id"`
	SourceStageID      uint16 `json:"source_stage_id"`
	DestinationStageID uint16 `json:"destination_stage_id"`
	Routing            string `json:"routing"`
	Field              string `json:"field,omitempty"`
}

// parseTopologyDocument strictly decodes and fully validates one DAG document.
func parseTopologyDocument(encoded []byte) (model.TopologySpec, error) {
	if len(encoded) == 0 || len(encoded) > maxTopologyDocumentBytes {
		return model.TopologySpec{}, fmt.Errorf("topology document is %d bytes, maximum is %d", len(encoded), maxTopologyDocumentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var document topologyDocument
	if err := decoder.Decode(&document); err != nil {
		return model.TopologySpec{}, fmt.Errorf("decode topology document: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return model.TopologySpec{}, errors.New("topology document has trailing JSON data")
	}
	topology := model.TopologySpec{
		SchemaVersion:       document.SchemaVersion,
		Name:                document.Name,
		RegistryFingerprint: model.RegistryFingerprint(),
	}
	for _, stage := range document.Stages {
		role, err := parseStageRole(stage.Role)
		if err != nil {
			return model.TopologySpec{}, fmt.Errorf("stage %d: %w", stage.StageID, err)
		}
		settings := make([]model.Setting, 0, len(stage.Operator.Settings))
		for _, setting := range stage.Operator.Settings {
			settings = append(settings, model.Setting{Key: setting.Key, Value: setting.Value})
		}
		topology.Stages = append(topology.Stages, model.StageSpec{
			StageID: stage.StageID, Name: stage.Name, Role: role, Parallelism: stage.Parallelism,
			Operator: model.OperatorSpec{Name: stage.Operator.Name, Version: stage.Operator.Version, Settings: settings},
		})
	}
	for _, edge := range document.Edges {
		routing, err := parseRoutingMode(edge.Routing)
		if err != nil {
			return model.TopologySpec{}, fmt.Errorf("edge %d: %w", edge.EdgeID, err)
		}
		topology.Edges = append(topology.Edges, model.EdgeSpec{
			EdgeID: edge.EdgeID, SourceStageID: edge.SourceStageID, DestinationStageID: edge.DestinationStageID,
			Routing: routing, Field: edge.Field,
		})
	}
	if _, err := model.ValidateTopology(topology); err != nil {
		return model.TopologySpec{}, fmt.Errorf("validate topology document: %w", err)
	}
	return topology, nil
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

// exampleTopologyJSON returns the finite range → multiply → collect example.
func exampleTopologyJSON() []byte {
	document := topologyDocument{
		SchemaVersion: 1,
		Name:          "example-scaled-range",
		Stages: []stageDocument{
			{StageID: 1, Name: "numbers", Role: "source", Parallelism: 1, Operator: operatorDocument{Name: "range", Version: 1, Settings: []settingDocument{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "1"}}}},
			{StageID: 2, Name: "scaled", Role: "transform", Parallelism: 1, Operator: operatorDocument{Name: "multiply", Version: 1, Settings: []settingDocument{{Key: "factor", Value: "3"}}}},
			{StageID: 3, Name: "collected", Role: "sink", Parallelism: 1, Operator: operatorDocument{Name: "collect", Version: 1}},
		},
		Edges: []edgeDocument{
			{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: "shuffle"},
			{EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: "shuffle"},
		},
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		panic("example topology cannot marshal: " + err.Error())
	}
	return append(encoded, '\n')
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

// writeJSONLine emits one stable machine-readable line on stdout.
func writeJSONLine(stdout io.Writer, value any) error {
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write output line: %w", err)
	}
	return nil
}
