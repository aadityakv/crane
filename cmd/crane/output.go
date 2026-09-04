package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

// statusOutput renders one stable machine-readable status line.
// jobSummaryOutput renders one stable machine-readable job summary shared by
// the status and jobs subcommands.
func jobSummaryOutput(status protocol.StatusResponse) map[string]any {
	output := map[string]any{
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

// statusOutput renders one stable machine-readable status line.
func statusOutput(status protocol.StatusResponse) map[string]any {
	output := jobSummaryOutput(status)
	output["command"] = "status"
	return output
}

// jobsOutput renders one stable machine-readable job-listing line.
func jobsOutput(listing protocol.JobListResponse) map[string]any {
	jobs := make([]map[string]any, 0, len(listing.Jobs))
	for _, status := range listing.Jobs {
		jobs = append(jobs, jobSummaryOutput(status))
	}
	return map[string]any{
		"command":        "jobs",
		"leader_node_id": listing.LeaderNodeID,
		"applied_index":  listing.AppliedIndex,
		"jobs":           jobs,
	}
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

// writeCountTable prints a read-side aggregation of the sealed result set:
// each distinct value of one field with its occurrence count, most frequent
// first, ties broken by value. Aggregation happens at read time over the
// exactly-once result set, never inside the pipeline.
func writeCountTable(stdout io.Writer, field string, counts map[string]int, total, top int) {
	type entry struct {
		value string
		count int
	}
	entries := make([]entry, 0, len(counts))
	for value, count := range counts {
		entries = append(entries, entry{value, count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].value < entries[j].value
	})
	if top > 0 && len(entries) > top {
		entries = entries[:top]
	}
	fmt.Fprintf(stdout, "%8s  %s\n", "count", field)
	for _, entry := range entries {
		fmt.Fprintf(stdout, "%8d  %s\n", entry.count, entry.value)
	}
	fmt.Fprintf(stdout, "%8d  records, %d distinct %s\n", total, len(counts), field)
}

// writeJSONLine emits one stable machine-readable line on stdout.
func writeJSONLine(stdout io.Writer, value any) error {
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write output line: %w", err)
	}
	return nil
}
