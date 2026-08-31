package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/raft"
)

const (
	// SnapshotSchemaVersion is the sole schema emitted for Crane snapshots.
	SnapshotSchemaVersion uint32 = model.StateSnapshotSchemaVersionV2
	snapshotMagic                = "CRSN"
)

type immutableSnapshot struct {
	encoded []byte
}

func (snapshot immutableSnapshot) SchemaVersion() uint32 { return SnapshotSchemaVersion }

func (snapshot immutableSnapshot) MarshalBinary() ([]byte, error) {
	return append([]byte(nil), snapshot.encoded...), nil
}

// Capture freezes one canonical owned Crane snapshot. The Raft capture index
// fences when the capture was taken; the payload deliberately retains the last
// Crane command index, which can be below a no-op barrier index.
func (machine *Machine) Capture(index, term uint64) (raft.SnapshotCapture, error) {
	if index == 0 || term == 0 {
		return nil, fmt.Errorf("%w: zero Raft capture position", ErrInvalidSnapshot)
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if index < machine.lastAppliedIndex {
		return nil, fmt.Errorf("%w: capture index %d precedes Crane index %d", ErrInvalidSnapshot, index, machine.lastAppliedIndex)
	}
	if err := validateSnapshotState(machine); err != nil {
		return nil, fmt.Errorf("%w: live state: %w", ErrInvalidSnapshot, err)
	}
	estimated, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok {
		return nil, fmt.Errorf("%w: canonical state exceeds snapshot bound", ErrInvalidSnapshot)
	}
	if machine.estimatedSnapshotBytes != estimated {
		return nil, fmt.Errorf("%w: incremental estimator=%d canonical=%d", ErrInvalidSnapshot, machine.estimatedSnapshotBytes, estimated)
	}
	encoded := machine.appendCanonicalSnapshotLocked(make([]byte, 0, estimated), nil)
	if uint64(len(encoded)) != estimated || uint64(len(encoded)) > model.StateCommandMaxSnapshotBytesV1 {
		return nil, fmt.Errorf("%w: estimator=%d encoded=%d", ErrInvalidSnapshot, estimated, len(encoded))
	}
	return immutableSnapshot{encoded: encoded}, nil
}

// Restore validates a complete snapshot in temporary owned state and swaps it
// into the machine atomically. Schemas zero and one are bootstrap-only and are
// accepted solely with an empty payload.
func (machine *Machine) Restore(schemaVersion uint32, encoded []byte) error {
	policy, supported := snapshotMigrationFor(schemaVersion)
	if !supported {
		return fmt.Errorf("%w: %w: snapshot schema %d", ErrInvalidSnapshot, ErrUnsupportedCommandSchema, schemaVersion)
	}
	if policy.emptyOnly {
		if len(encoded) != 0 {
			return fmt.Errorf("%w: legacy schema %d has a payload", ErrInvalidSnapshot, schemaVersion)
		}
		empty := NewMachine()
		machine.replaceWith(empty)
		return nil
	}
	if len(encoded) == 0 || uint64(len(encoded)) > model.StateCommandMaxSnapshotBytesV1 {
		if uint64(len(encoded)) > model.StateCommandMaxSnapshotBytesV1 {
			return fmt.Errorf("%w: %w: payload length %d", ErrInvalidSnapshot, ErrSnapshotTooLarge, len(encoded))
		}
		return fmt.Errorf("%w: payload length %d", ErrInvalidSnapshot, len(encoded))
	}
	temporary, err := decodeSnapshot(encoded)
	if err != nil {
		return err
	}
	canonical := temporary.appendCanonicalSnapshotLocked(make([]byte, 0, len(encoded)), nil)
	if snapshotRequireCanonicalReencode && !bytes.Equal(canonical, encoded) {
		return fmt.Errorf("%w: payload is not canonical", ErrInvalidSnapshot)
	}
	estimated, ok := temporary.estimateCanonicalSnapshotBytesLocked()
	if !ok || estimated != uint64(len(encoded)) {
		return fmt.Errorf("%w: estimator=%d encoded=%d", ErrInvalidSnapshot, estimated, len(encoded))
	}
	temporary.estimatedSnapshotBytes = estimated
	machine.replaceWith(temporary)
	return nil
}

type snapshotMigrationPolicy struct {
	schema     uint32
	emptyOnly  bool
	descriptor string
}

var snapshotMigrationPolicies = []snapshotMigrationPolicy{
	{schema: 0, emptyOnly: true, descriptor: "schema-0:empty-payload-only:restore-empty"},
	{schema: 1, emptyOnly: true, descriptor: "schema-1:empty-payload-only:restore-empty"},
	{schema: SnapshotSchemaVersion, descriptor: "schema-2:nonempty-bounded-canonical:decode-current"},
}

const snapshotRequireCanonicalReencode = true

func snapshotMigrationFor(schema uint32) (snapshotMigrationPolicy, bool) {
	for _, policy := range snapshotMigrationPolicies {
		if policy.schema == schema {
			return policy, true
		}
	}
	return snapshotMigrationPolicy{}, false
}

// SnapshotMigrationRules returns the production-used accepted-schema table.
func SnapshotMigrationRules() []string {
	rules := make([]string, 0, len(snapshotMigrationPolicies)+1)
	for _, policy := range snapshotMigrationPolicies {
		rules = append(rules, policy.descriptor)
	}
	return append(rules, "other-schema:reject-unsupported")
}

// SnapshotValidationRules returns consensus-visible validation and error policy.
func SnapshotValidationRules() []string {
	return []string{
		"capture:incremental-estimate-equals-canonical-estimate-equals-encoded-length",
		"restore:temporary-owned-validate-canonical-reencode-byte-equality-then-atomic-swap",
		"ordering:all-declared-sorted-collections-are-strictly-increasing-and-duplicate-free",
		"errors:every-failure-wraps-ErrInvalidSnapshot-and-preserves-nested-sentinel",
		"cached-results:canonical-command-result-and-subject-identity-revision-epoch-correlated",
		"retained-targets:canonical-complete-semantic-correlation-with-authoritative-state",
		"job-control-lag:exact-active-provenance-count-plus-optional-client-cancellation",
		"worker-invalidations:bounded-causal-provenance-exact-retained-affected-target-and-repair-predecessor-fences",
		"job-definitions:DefiningRequest-unique-across-all-retained-jobs",
		"assigned-jobs:complete-immutable-source-eofs-including-terminal",
		"reverse-references:coordinator-worker-job-control-eof-checkpoint-manifest",
		"artifacts:checked-per-job-aggregate-at-most-MaxResultBytes",
	}
}

// SnapshotSortRules returns every production collection's canonical key order.
func SnapshotSortRules() []string {
	return []string{
		"Clients:ClientID:unsigned-lexicographic-bytes16",
		"Subjects:SubjectKey:unsigned-lexicographic-canonical-bytes39",
		"Workers:NodeID:unsigned-u16",
		"Jobs:JobID:unsigned-lexicographic-bytes16",
		"WorkerEvents:WorkerID-unsigned-u16,WorkerEpoch-unsigned-lexicographic-bytes16",
		"NeedsReassignment:Kind,active-TaskID,ReplicaRole,OldWorkerID,OldWorkerEpoch",
		"InvalidationHistory:JobControlRevision-strictly-increasing",
		"SourceEOFs:TaskID:unsigned-lexicographic-canonical-bytes20",
		"Checkpoints:TaskID:unsigned-lexicographic-canonical-bytes20",
		"Manifests:TaskID:unsigned-lexicographic-canonical-bytes20",
	}
}

func (machine *Machine) replaceWith(source *Machine) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.clients = source.clients
	machine.subjects = source.subjects
	machine.coordinatorRevision = source.coordinatorRevision
	machine.coordinatorEpoch = source.coordinatorEpoch
	machine.workers = source.workers
	machine.jobs = source.jobs
	machine.workerEvents = source.workerEvents
	machine.lastAppliedIndex = source.lastAppliedIndex
	machine.estimatedSnapshotBytes = source.estimatedSnapshotBytes
}

// SnapshotEncodingLayouts traces the production snapshot appenders and returns
// the exact nested layout incorporated into the consensus fingerprint.
func SnapshotEncodingLayouts() []model.StateCommandLayoutDescriptor {
	return []model.StateCommandLayoutDescriptor{
		traceSnapshotLayout("SnapshotRoot", func(trace *[]string) { (&Machine{}).appendCanonicalSnapshotLocked(nil, trace) }),
		traceSnapshotLayout("ClientHistory", func(trace *[]string) { appendClientHistory(nil, model.ClientID{}, clientHistory{}, trace) }),
		traceSnapshotLayout("SubjectHistory", func(trace *[]string) { appendSubjectHistory(nil, SubjectKey{}, subjectHistory{}, trace) }),
		traceSnapshotLayout("WorkerEntry", func(trace *[]string) { appendWorkerEntry(nil, 0, WorkerRecord{}, trace) }),
		traceSnapshotLayout("JobRecord", func(trace *[]string) { appendJobEntry(nil, model.JobID{}, JobRecord{}, trace) }),
		traceSnapshotLayout("ClientRequestID", func(trace *[]string) { appendClientRequest(nil, model.ClientRequestID{}, trace) }),
		traceSnapshotLayout("SourceEOFEntry", func(trace *[]string) { appendSourceEOFEntry(nil, model.TaskID{}, SourceEOFRecord{}, trace) }),
		traceSnapshotLayout("CheckpointEntry", func(trace *[]string) { appendCheckpointEntry(nil, model.TaskID{}, CheckpointRecord{}, trace) }),
		traceSnapshotLayout("ManifestEntry", func(trace *[]string) { appendManifestEntry(nil, model.TaskID{}, ResultManifest{}, trace) }),
		traceSnapshotLayout("WorkerEventEntry", func(trace *[]string) { appendWorkerEventEntry(nil, workerEventKey{}, workerEventCursor{}, trace) }),
		traceSnapshotLayout("WorkerRecord", func(trace *[]string) { appendWorkerRecordTraced(nil, WorkerRecord{}, trace) }),
		traceSnapshotLayout("SubjectKey", func(trace *[]string) { appendSubjectTraced(nil, SubjectKey{}, trace) }),
		traceSnapshotLayout("TaskID", func(trace *[]string) { appendTaskTraced(nil, model.TaskID{}, trace) }),
		traceSnapshotLayout("CoordinatorEpoch", func(trace *[]string) { appendEpochTraced(nil, model.CoordinatorEpoch{}, trace) }),
		traceSnapshotLayout("AssignmentSet", func(trace *[]string) { appendAssignmentTraced(nil, model.AssignmentSet{}, trace) }),
		traceSnapshotLayout("AssignmentToken", func(trace *[]string) { appendTokenTraced(nil, model.AssignmentToken{}, trace) }),
		traceSnapshotLayout("ResultReplicaSet", func(trace *[]string) { appendReplicaTraced(nil, model.ResultReplicaSet{}, trace) }),
		traceSnapshotLayout("NeedsReassignment", func(trace *[]string) { appendMarkerTraced(nil, NeedsReassignment{}, trace) }),
		traceSnapshotLayout("InvalidationProvenance", func(trace *[]string) { appendInvalidationProvenance(nil, invalidationProvenance{}, trace) }),
		traceSnapshotLayout("ResultManifest", func(trace *[]string) { appendManifestTraced(nil, ResultManifest{}, trace) }),
		traceSnapshotLayout("JobFailureReport", func(trace *[]string) { appendFailureReportTraced(nil, model.JobFailureReport{}, trace) }),
	}
}

func traceSnapshotLayout(name string, traceFunc func(*[]string)) model.StateCommandLayoutDescriptor {
	var fields []string
	traceFunc(&fields)
	return model.StateCommandLayoutDescriptor{Name: name, Fields: fields}
}

func (machine *Machine) appendCanonicalSnapshotLocked(encoded []byte, trace *[]string) []byte {
	snapshotField(&encoded, trace, "Magic:bytes4(CRSN)", func(out []byte) []byte { return append(out, snapshotMagic...) })
	snapshotField(&encoded, trace, "SchemaVersion:u16(2)", func(out []byte) []byte { return appendU16(out, uint16(SnapshotSchemaVersion)) })
	fingerprint := model.ConsensusFingerprint()
	snapshotField(&encoded, trace, "ConsensusFingerprint:sha256", func(out []byte) []byte { return append(out, fingerprint[:]...) })
	snapshotField(&encoded, trace, "LastAppliedCraneIndex:u64", func(out []byte) []byte { return appendU64(out, machine.lastAppliedIndex) })
	snapshotField(&encoded, trace, "CoordinatorRevision:u64", func(out []byte) []byte { return appendU64(out, machine.coordinatorRevision) })
	snapshotField(&encoded, trace, "CoordinatorEpoch:CoordinatorEpoch", func(out []byte) []byte { return appendEpoch(out, machine.coordinatorEpoch) })
	snapshotField(&encoded, trace, "Clients:u64-count+sorted(ClientHistory)", func(out []byte) []byte { return appendU64(out, uint64(len(machine.clients))) })
	snapshotField(&encoded, trace, "Subjects:u64-count+sorted(SubjectHistory)", func(out []byte) []byte { return appendU64(out, uint64(len(machine.subjects))) })
	snapshotField(&encoded, trace, "Workers:u64-count+sorted(WorkerEntry)", func(out []byte) []byte { return appendU64(out, uint64(len(machine.workers))) })
	snapshotField(&encoded, trace, "Jobs:u64-count+sorted(JobRecord)", func(out []byte) []byte { return appendU64(out, uint64(len(machine.jobs))) })
	snapshotField(&encoded, trace, "WorkerEvents:u64-count+sorted(WorkerEventEntry)", func(out []byte) []byte { return appendU64(out, uint64(len(machine.workerEvents))) })
	if trace != nil {
		return encoded
	}
	for _, id := range sortedClientIDs(machine.clients) {
		encoded = appendClientHistory(encoded, id, machine.clients[id], nil)
	}
	for _, subject := range sortedSubjectKeys(machine.subjects) {
		encoded = appendSubjectHistory(encoded, subject, machine.subjects[subject], nil)
	}
	workerIDs := make([]uint16, 0, len(machine.workers))
	for id := range machine.workers {
		workerIDs = append(workerIDs, id)
	}
	sort.Slice(workerIDs, func(i, j int) bool { return compareSnapshotWorkerID(workerIDs[i], workerIDs[j]) < 0 })
	for _, id := range workerIDs {
		encoded = appendWorkerEntry(encoded, id, machine.workers[id], nil)
	}
	for _, id := range sortedJobIDs(machine.jobs) {
		encoded = appendJobEntry(encoded, id, machine.jobs[id], nil)
	}
	for _, key := range sortedWorkerEventKeys(machine.workerEvents) {
		encoded = appendWorkerEventEntry(encoded, key, machine.workerEvents[key], nil)
	}
	return encoded
}

func snapshotField(encoded *[]byte, trace *[]string, descriptor string, appendValue func([]byte) []byte) {
	if trace != nil {
		*trace = append(*trace, descriptor)
	}
	*encoded = appendValue(*encoded)
}

func appendClientHistory(encoded []byte, id model.ClientID, history clientHistory, trace *[]string) []byte {
	snapshotField(&encoded, trace, "ClientID:bytes16(nonzero)", func(out []byte) []byte { return append(out, id[:]...) })
	snapshotField(&encoded, trace, "Sequence:u64(nonzero)", func(out []byte) []byte { return appendU64(out, history.sequence) })
	snapshotField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, history.digest[:]...) })
	snapshotField(&encoded, trace, "Result:u32-bytes(owned,bounded)", func(out []byte) []byte { return appendBytes32(out, history.result) })
	return encoded
}

func appendSubjectHistory(encoded []byte, subject SubjectKey, history subjectHistory, trace *[]string) []byte {
	snapshotField(&encoded, trace, "Subject:SubjectKey", func(out []byte) []byte { return appendSubject(out, subject) })
	snapshotField(&encoded, trace, "Revision:u64", func(out []byte) []byte { return appendU64(out, history.revision) })
	snapshotField(&encoded, trace, "ID:bytes32(nonzero)", func(out []byte) []byte { return append(out, history.id[:]...) })
	snapshotField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, history.digest[:]...) })
	snapshotField(&encoded, trace, "Target:u32-bytes(owned,bounded)", func(out []byte) []byte { return appendBytes32(out, history.target) })
	snapshotField(&encoded, trace, "Result:u32-bytes(owned,bounded)", func(out []byte) []byte { return appendBytes32(out, history.result) })
	snapshotField(&encoded, trace, "Applied:u8(bool)", func(out []byte) []byte {
		if history.applied {
			return append(out, 1)
		}
		return append(out, 0)
	})
	snapshotField(&encoded, trace, "AppliedRevision:u64", func(out []byte) []byte { return appendU64(out, history.appliedRevision) })
	snapshotField(&encoded, trace, "AppliedTarget:u32-bytes(owned,bounded)", func(out []byte) []byte { return appendBytes32(out, history.appliedTarget) })
	snapshotField(&encoded, trace, "AppliedResult:u32-bytes(owned,bounded)", func(out []byte) []byte { return appendBytes32(out, history.appliedResult) })
	return encoded
}

func appendWorkerEntry(encoded []byte, key uint16, worker WorkerRecord, trace *[]string) []byte {
	snapshotField(&encoded, trace, "NodeIDKey:u16(nonzero)", func(out []byte) []byte { return appendU16(out, key) })
	snapshotField(&encoded, trace, "Worker:WorkerRecord", func(out []byte) []byte { return appendWorkerRecord(out, worker) })
	return encoded
}

func appendJobEntry(encoded []byte, key model.JobID, job JobRecord, trace *[]string) []byte {
	snapshotField(&encoded, trace, "JobIDKey:bytes16(nonzero)", func(out []byte) []byte { return append(out, key[:]...) })
	snapshotField(&encoded, trace, "JobID:bytes16(nonzero)", func(out []byte) []byte { return append(out, job.JobID[:]...) })
	snapshotField(&encoded, trace, "DefiningRequest:ClientRequestID", func(out []byte) []byte { return appendClientRequest(out, job.DefiningRequest, nil) })
	snapshotField(&encoded, trace, "TopologyDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, job.TopologyDigest[:]...) })
	snapshotField(&encoded, trace, "TopologyBytes:u64-bytes(canonical,bounded)", func(out []byte) []byte {
		out = appendU64(out, uint64(len(job.TopologyBytes)))
		return append(out, job.TopologyBytes...)
	})
	snapshotField(&encoded, trace, "Lifecycle:JobLifecycle", func(out []byte) []byte { return append(out, byte(job.Lifecycle)) })
	snapshotField(&encoded, trace, "JobControlRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, job.JobControlRevision) })
	snapshotField(&encoded, trace, "Assignment:optional(AssignmentSet)", func(out []byte) []byte {
		if job.Assignment == nil {
			return append(out, 0)
		}
		return appendAssignment(append(out, 1), *job.Assignment)
	})
	snapshotField(&encoded, trace, "NeedsReassignment:u16-count+sorted(NeedsReassignment)", func(out []byte) []byte {
		out = appendU16(out, uint16(len(job.NeedsReassignment)))
		for _, marker := range job.NeedsReassignment {
			out = appendMarker(out, marker)
		}
		return out
	})
	snapshotField(&encoded, trace, "InvalidationHistory:u16-count+chronological(InvalidationProvenance)", func(out []byte) []byte {
		out = appendU16(out, uint16(len(job.invalidationHistory)))
		for _, provenance := range job.invalidationHistory {
			out = appendInvalidationProvenance(out, provenance, nil)
		}
		return out
	})
	snapshotField(&encoded, trace, "SourceEOFs:u16-count+sorted(SourceEOFEntry)", func(out []byte) []byte {
		keys := sortedTaskKeysEOF(job.SourceEOFs)
		out = appendU16(out, uint16(len(keys)))
		for _, key := range keys {
			out = appendSourceEOFEntry(out, key, job.SourceEOFs[key], nil)
		}
		return out
	})
	snapshotField(&encoded, trace, "Checkpoints:u16-count+sorted(CheckpointEntry)", func(out []byte) []byte {
		keys := sortedTaskKeysCheckpoint(job.Checkpoints)
		out = appendU16(out, uint16(len(keys)))
		for _, key := range keys {
			out = appendCheckpointEntry(out, key, job.Checkpoints[key], nil)
		}
		return out
	})
	snapshotField(&encoded, trace, "Manifests:u16-count+sorted(ManifestEntry)", func(out []byte) []byte {
		keys := sortedTaskKeysManifest(job.Manifests)
		out = appendU16(out, uint16(len(keys)))
		for _, key := range keys {
			out = appendManifestEntry(out, key, job.Manifests[key], nil)
		}
		return out
	})
	snapshotField(&encoded, trace, "Failure:optional(JobFailureReport)", func(out []byte) []byte {
		if job.Failure == nil {
			return append(out, 0)
		}
		return appendFailureReportTraced(append(out, 1), *job.Failure, nil)
	})
	return encoded
}

func appendInvalidationProvenance(encoded []byte, provenance invalidationProvenance, trace *[]string) []byte {
	snapshotField(&encoded, trace, "Kind:WorkerInvalidationKind", func(out []byte) []byte { return append(out, byte(provenance.Kind)) })
	snapshotField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, provenance.WorkerID) })
	snapshotField(&encoded, trace, "WorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, provenance.WorkerEpoch[:]...) })
	snapshotField(&encoded, trace, "WorkerRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, provenance.WorkerRevision) })
	snapshotField(&encoded, trace, "JobControlRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, provenance.JobControlRevision) })
	snapshotField(&encoded, trace, "AssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, provenance.AssignmentRevision) })
	snapshotField(&encoded, trace, "AssignmentDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, provenance.AssignmentDigest[:]...) })
	snapshotField(&encoded, trace, "Markers:u16-count+sorted(NeedsReassignment)", func(out []byte) []byte {
		out = appendU16(out, uint16(len(provenance.Markers)))
		for _, marker := range provenance.Markers {
			out = appendMarker(out, marker)
		}
		return out
	})
	snapshotField(&encoded, trace, "RepairJobControlRevision:u64(zero-or-nonzero)", func(out []byte) []byte { return appendU64(out, provenance.RepairJobControlRevision) })
	snapshotField(&encoded, trace, "RepairAssignmentRevision:u64(zero-or-successor)", func(out []byte) []byte { return appendU64(out, provenance.RepairAssignmentRevision) })
	snapshotField(&encoded, trace, "RepairAssignmentDigest:sha256(zero-iff-active)", func(out []byte) []byte { return append(out, provenance.RepairAssignmentDigest[:]...) })
	snapshotField(&encoded, trace, "RepairMarkersDigest:sha256(zero-iff-active)", func(out []byte) []byte { return append(out, provenance.RepairMarkersDigest[:]...) })
	return encoded
}

func appendClientRequest(encoded []byte, request model.ClientRequestID, trace *[]string) []byte {
	snapshotField(&encoded, trace, "ClientID:bytes16(nonzero)", func(out []byte) []byte { return append(out, request.ClientID[:]...) })
	snapshotField(&encoded, trace, "Sequence:u64(nonzero)", func(out []byte) []byte { return appendU64(out, request.Sequence) })
	return encoded
}

func appendSourceEOFEntry(encoded []byte, source model.TaskID, record SourceEOFRecord, trace *[]string) []byte {
	snapshotField(&encoded, trace, "Source:TaskID", func(out []byte) []byte { return appendTask(out, source) })
	snapshotField(&encoded, trace, "EOF:u64", func(out []byte) []byte { return appendU64(out, record.EOF) })
	snapshotField(&encoded, trace, "Revision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, record.Revision) })
	return encoded
}

func appendCheckpointEntry(encoded []byte, source model.TaskID, record CheckpointRecord, trace *[]string) []byte {
	snapshotField(&encoded, trace, "Source:TaskID", func(out []byte) []byte { return appendTask(out, source) })
	snapshotField(&encoded, trace, "Watermark:u64", func(out []byte) []byte { return appendU64(out, record.Watermark) })
	snapshotField(&encoded, trace, "Revision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, record.Revision) })
	return encoded
}

func appendManifestEntry(encoded []byte, sink model.TaskID, manifest ResultManifest, trace *[]string) []byte {
	snapshotField(&encoded, trace, "SinkTask:TaskID", func(out []byte) []byte { return appendTask(out, sink) })
	snapshotField(&encoded, trace, "Manifest:ResultManifest", func(out []byte) []byte { return appendManifest(out, manifest) })
	return encoded
}

func appendWorkerEventEntry(encoded []byte, key workerEventKey, cursor workerEventCursor, trace *[]string) []byte {
	snapshotField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, key.WorkerID) })
	snapshotField(&encoded, trace, "WorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, key.WorkerEpoch[:]...) })
	snapshotField(&encoded, trace, "TransactionID:u64(nonzero)", func(out []byte) []byte { return appendU64(out, cursor.TransactionID) })
	snapshotField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, cursor.Digest[:]...) })
	return encoded
}

func appendBytes32(encoded, value []byte) []byte {
	var fixed [model.StateCommandSnapshotBytes32PrefixV2]byte
	binary.BigEndian.PutUint32(fixed[:], uint32(len(value)))
	encoded = append(encoded, fixed[:]...)
	return append(encoded, value...)
}

func sortedClientIDs(values map[model.ClientID]clientHistory) []model.ClientID {
	keys := make([]model.ClientID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotClientID(keys[i], keys[j]) < 0 })
	return keys
}
func sortedSubjectKeys(values map[SubjectKey]subjectHistory) []SubjectKey {
	keys := make([]SubjectKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotSubject(keys[i], keys[j]) < 0 })
	return keys
}
func sortedJobIDs(values map[model.JobID]JobRecord) []model.JobID {
	keys := make([]model.JobID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotJobID(keys[i], keys[j]) < 0 })
	return keys
}
func sortedWorkerEventKeys(values map[workerEventKey]workerEventCursor) []workerEventKey {
	keys := make([]workerEventKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotWorkerEvent(keys[i], keys[j]) < 0 })
	return keys
}
func sortedTaskKeysEOF(values map[model.TaskID]SourceEOFRecord) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotTask(keys[i], keys[j]) < 0 })
	return keys
}
func sortedTaskKeysCheckpoint(values map[model.TaskID]CheckpointRecord) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotTask(keys[i], keys[j]) < 0 })
	return keys
}
func sortedTaskKeysManifest(values map[model.TaskID]ResultManifest) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return compareSnapshotTask(keys[i], keys[j]) < 0 })
	return keys
}

func compareSnapshotClientID(left, right model.ClientID) int { return bytes.Compare(left[:], right[:]) }
func compareSnapshotSubject(left, right SubjectKey) int {
	return bytes.Compare(appendSubject(nil, left), appendSubject(nil, right))
}
func compareSnapshotWorkerID(left, right uint16) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
func compareSnapshotJobID(left, right model.JobID) int { return bytes.Compare(left[:], right[:]) }
func compareSnapshotWorkerEvent(left, right workerEventKey) int {
	if comparison := compareSnapshotWorkerID(left.WorkerID, right.WorkerID); comparison != 0 {
		return comparison
	}
	return bytes.Compare(left.WorkerEpoch[:], right.WorkerEpoch[:])
}
func compareSnapshotTask(left, right model.TaskID) int {
	return bytes.Compare(taskBytes(left), taskBytes(right))
}
func compareSnapshotMarker(left, right NeedsReassignment) int {
	if markerLess(left, right) {
		return -1
	}
	if markerLess(right, left) {
		return 1
	}
	return 0
}
func compareSnapshotInvalidation(left, right invalidationProvenance) int {
	if left.JobControlRevision < right.JobControlRevision {
		return -1
	}
	if left.JobControlRevision > right.JobControlRevision {
		return 1
	}
	return 0
}

type snapshotDecoder struct{ commandDecoder }

func decodeSnapshot(encoded []byte) (*Machine, error) {
	decoder := snapshotDecoder{commandDecoder{input: encoded}}
	magic, err := decoder.take(4)
	if err != nil || string(magic) != snapshotMagic {
		return nil, snapshotDecodeError("magic", err)
	}
	version, err := decoder.u16()
	if err != nil {
		return nil, snapshotDecodeError("schema", err)
	}
	if version != uint16(SnapshotSchemaVersion) {
		return nil, fmt.Errorf("%w: %w: embedded snapshot schema %d", ErrInvalidSnapshot, ErrUnsupportedCommandSchema, version)
	}
	fingerprint, err := decoder.array32()
	if err != nil {
		return nil, snapshotDecodeError("consensus fingerprint", err)
	}
	if fingerprint != model.ConsensusFingerprint() {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSnapshot, ErrConsensusFingerprintMismatch)
	}
	lastApplied, err := decoder.u64()
	if err != nil {
		return nil, snapshotDecodeError("last applied index", err)
	}
	coordinatorRevision, err := decoder.u64()
	if err != nil {
		return nil, snapshotDecodeError("coordinator revision", err)
	}
	epoch, err := decoder.epoch()
	if err != nil {
		return nil, snapshotDecodeError("coordinator epoch", err)
	}
	clientCount, err := decoder.boundedCount(model.StateCommandMaxClientSessionsV1, model.StateCommandClientHistoryFixedV1, "clients")
	if err != nil {
		return nil, err
	}
	subjectCount, err := decoder.boundedCount(model.StateCommandMaxSubjectHistoriesV1, model.StateCommandSubjectHistoryFixedV1, "subjects")
	if err != nil {
		return nil, err
	}
	workerCount, err := decoder.boundedCount(model.LimitsV1().MaxRegisteredWorkers, model.StateCommandWorkerRecordBytesV1, "workers")
	if err != nil {
		return nil, err
	}
	jobCount, err := decoder.boundedCount(model.LimitsV1().MaxRetainedJobs, model.StateCommandJobRecordFixedBytesV1, "jobs")
	if err != nil {
		return nil, err
	}
	eventCount, err := decoder.boundedCount(model.LimitsV1().MaxRegisteredWorkers, model.StateCommandWorkerEventBytesV1, "worker events")
	if err != nil {
		return nil, err
	}
	temporary := NewMachine()
	temporary.lastAppliedIndex, temporary.coordinatorRevision, temporary.coordinatorEpoch = lastApplied, coordinatorRevision, epoch
	if err := decoder.clients(temporary, clientCount); err != nil {
		return nil, err
	}
	if err := decoder.subjects(temporary, subjectCount); err != nil {
		return nil, err
	}
	if err := decoder.workers(temporary, workerCount); err != nil {
		return nil, err
	}
	if err := decoder.jobs(temporary, jobCount); err != nil {
		return nil, err
	}
	if err := decoder.workerEvents(temporary, eventCount); err != nil {
		return nil, err
	}
	if !decoder.done() {
		return nil, fmt.Errorf("%w: trailing bytes", ErrInvalidSnapshot)
	}
	if err := validateSnapshotState(temporary); err != nil {
		return nil, fmt.Errorf("%w: state validation: %w", ErrInvalidSnapshot, err)
	}
	return temporary, nil
}

func snapshotDecodeError(field string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: invalid %s", ErrInvalidSnapshot, field)
	}
	return fmt.Errorf("%w: %s: %w", ErrInvalidSnapshot, field, err)
}

func (decoder *snapshotDecoder) boundedCount(limit, minimum uint64, field string) (uint64, error) {
	count, err := decoder.u64()
	if err != nil {
		return 0, snapshotDecodeError(field+" count", err)
	}
	if count > limit || minimum != 0 && count > uint64(decoder.remaining())/minimum {
		return 0, fmt.Errorf("%w: %s count %d", ErrInvalidSnapshot, field, count)
	}
	return count, nil
}

func (decoder *snapshotDecoder) bytes32(limit uint64, field string) ([]byte, error) {
	fixed, err := decoder.take(int(model.StateCommandSnapshotBytes32PrefixV2))
	if err != nil {
		return nil, snapshotDecodeError(field+" length", err)
	}
	length := uint64(binary.BigEndian.Uint32(fixed))
	if length > limit || length > uint64(decoder.remaining()) {
		return nil, fmt.Errorf("%w: %s length %d", ErrInvalidSnapshot, field, length)
	}
	value, err := decoder.take(int(length))
	if err != nil {
		return nil, snapshotDecodeError(field, err)
	}
	return append([]byte(nil), value...), nil
}

func (decoder *snapshotDecoder) bytes64(limit uint64, field string) ([]byte, error) {
	length, err := decoder.u64()
	if err != nil {
		return nil, snapshotDecodeError(field+" length", err)
	}
	if length > limit || length > uint64(decoder.remaining()) {
		return nil, fmt.Errorf("%w: %s length %d", ErrInvalidSnapshot, field, length)
	}
	value, err := decoder.take(int(length))
	if err != nil {
		return nil, snapshotDecodeError(field, err)
	}
	return append([]byte(nil), value...), nil
}

func (decoder *snapshotDecoder) clients(machine *Machine, count uint64) error {
	var previous model.ClientID
	for index := uint64(0); index < count; index++ {
		idBytes, err := decoder.array16()
		if err != nil {
			return snapshotDecodeError("client ID", err)
		}
		id := model.ClientID(idBytes)
		if id.Validate() != nil || index > 0 && compareSnapshotClientID(previous, id) >= 0 {
			return fmt.Errorf("%w: %w: client keys are invalid or unsorted", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		sequence, err := decoder.u64()
		if err != nil {
			return snapshotDecodeError("client sequence", err)
		}
		digest, err := decoder.array32()
		if err != nil {
			return snapshotDecodeError("client digest", err)
		}
		result, err := decoder.bytes32(model.StateCommandMaxCachedResultBytesV1, "client result")
		if err != nil {
			return err
		}
		if sequence == 0 || digest == ([32]byte{}) {
			return fmt.Errorf("%w: invalid client history", ErrInvalidSnapshot)
		}
		machine.clients[id] = clientHistory{sequence: sequence, digest: digest, result: result}
		previous = id
	}
	return nil
}

func (decoder *snapshotDecoder) subjects(machine *Machine, count uint64) error {
	var previous SubjectKey
	for index := uint64(0); index < count; index++ {
		subject, err := decoder.subject()
		if err != nil {
			return snapshotDecodeError("subject key", err)
		}
		if subject.Validate() != nil || index > 0 && compareSnapshotSubject(previous, subject) >= 0 {
			return fmt.Errorf("%w: %w: subject keys are invalid or unsorted", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		revision, err := decoder.u64()
		if err != nil {
			return snapshotDecodeError("subject revision", err)
		}
		id, err := decoder.array32()
		if err != nil {
			return snapshotDecodeError("subject ID", err)
		}
		digest, err := decoder.array32()
		if err != nil {
			return snapshotDecodeError("subject digest", err)
		}
		target, err := decoder.bytes32(model.LimitsV1().MaxSubmitJobBytes, "subject target")
		if err != nil {
			return err
		}
		result, err := decoder.bytes32(model.StateCommandMaxCachedResultBytesV1, "subject result")
		if err != nil {
			return err
		}
		appliedByte, err := decoder.byte()
		if err != nil || appliedByte > 1 {
			return snapshotDecodeError("subject applied selector", err)
		}
		appliedRevision, err := decoder.u64()
		if err != nil {
			return snapshotDecodeError("subject applied revision", err)
		}
		appliedTarget, err := decoder.bytes32(model.LimitsV1().MaxSubmitJobBytes, "subject applied target")
		if err != nil {
			return err
		}
		appliedResult, err := decoder.bytes32(model.StateCommandMaxCachedResultBytesV1, "subject applied result")
		if err != nil {
			return err
		}
		if id == ([32]byte{}) || digest == ([32]byte{}) {
			return fmt.Errorf("%w: zero subject history identity", ErrInvalidSnapshot)
		}
		machine.subjects[subject] = subjectHistory{revision: revision, id: InternalCommandID(id), digest: digest, target: target, result: result, applied: appliedByte == 1, appliedRevision: appliedRevision, appliedTarget: appliedTarget, appliedResult: appliedResult}
		previous = subject
	}
	return nil
}

func (decoder *snapshotDecoder) workers(machine *Machine, count uint64) error {
	var previous uint16
	for index := uint64(0); index < count; index++ {
		key, err := decoder.u16()
		if err != nil {
			return snapshotDecodeError("worker key", err)
		}
		worker, err := decoder.workerRecord()
		if err != nil {
			return snapshotDecodeError("worker record", err)
		}
		if key == 0 || index > 0 && compareSnapshotWorkerID(previous, key) >= 0 || worker.NodeID != key || worker.Validate() != nil {
			return fmt.Errorf("%w: %w: invalid or unsorted worker entry", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		machine.workers[key] = worker
		previous = key
	}
	return nil
}

func (decoder *snapshotDecoder) jobs(machine *Machine, count uint64) error {
	var previous model.JobID
	for index := uint64(0); index < count; index++ {
		key, err := decoder.jobID()
		if err != nil {
			return snapshotDecodeError("job key", err)
		}
		if key.Validate() != nil || index > 0 && compareSnapshotJobID(previous, key) >= 0 {
			return fmt.Errorf("%w: %w: invalid or unsorted job key", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		record, err := decoder.jobRecord()
		if err != nil {
			return err
		}
		if record.JobID != key {
			return fmt.Errorf("%w: job key mismatch", ErrInvalidSnapshot)
		}
		machine.jobs[key] = record
		previous = key
	}
	return nil
}

func (decoder *snapshotDecoder) jobRecord() (JobRecord, error) {
	jobID, err := decoder.jobID()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("job ID", err)
	}
	client, err := decoder.array16()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("defining client", err)
	}
	sequence, err := decoder.u64()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("defining sequence", err)
	}
	digest, err := decoder.array32()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("topology digest", err)
	}
	topology, err := decoder.bytes64(model.LimitsV1().MaxTopologyBytes, "topology")
	if err != nil {
		return JobRecord{}, err
	}
	lifecycle, err := decoder.byte()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("lifecycle", err)
	}
	revision, err := decoder.u64()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("job revision", err)
	}
	record := JobRecord{JobID: jobID, DefiningRequest: model.ClientRequestID{ClientID: model.ClientID(client), Sequence: sequence}, TopologyDigest: digest, TopologyBytes: topology, Lifecycle: JobLifecycle(lifecycle), JobControlRevision: revision}
	selector, err := decoder.byte()
	if err != nil || selector > 1 {
		return JobRecord{}, snapshotDecodeError("assignment selector", err)
	}
	if selector == 1 {
		assignment, err := decoder.assignment()
		if err != nil {
			return JobRecord{}, snapshotDecodeError("assignment", err)
		}
		record.Assignment = assignmentPointer(assignment)
	}
	markerCount, err := decoder.u16()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("marker count", err)
	}
	maxMarkers := model.LimitsV1().MaxTasksPerJob + 2*model.LimitsV1().MaxResultManifestsPerJob
	if uint64(markerCount) > maxMarkers || int(markerCount) > decoder.remaining()/int(model.StateCommandReassignmentBytesV1) {
		return JobRecord{}, fmt.Errorf("%w: marker count", ErrInvalidSnapshot)
	}
	if markerCount != 0 {
		record.NeedsReassignment = make([]NeedsReassignment, int(markerCount))
		for index := range record.NeedsReassignment {
			marker, err := decoder.marker()
			if err != nil {
				return JobRecord{}, err
			}
			record.NeedsReassignment[index] = marker
		}
	}
	provenanceCount, err := decoder.u16()
	if err != nil {
		return JobRecord{}, snapshotDecodeError("invalidation provenance count", err)
	}
	if uint64(provenanceCount) > model.StateCommandMaxInvalidationProvenanceV1 || int(provenanceCount) > decoder.remaining()/int(model.StateCommandInvalidationProvenanceFixedBytesV1) {
		return JobRecord{}, fmt.Errorf("%w: invalidation provenance count", ErrInvalidSnapshot)
	}
	if provenanceCount != 0 {
		record.invalidationHistory = make([]invalidationProvenance, int(provenanceCount))
		for index := range record.invalidationHistory {
			provenance, provenanceErr := decoder.invalidationProvenance()
			if provenanceErr != nil {
				return JobRecord{}, provenanceErr
			}
			record.invalidationHistory[index] = provenance
		}
	}
	record.SourceEOFs, err = decoder.sourceEOFs(jobID)
	if err != nil {
		return JobRecord{}, err
	}
	record.Checkpoints, err = decoder.checkpoints(jobID)
	if err != nil {
		return JobRecord{}, err
	}
	record.Manifests, err = decoder.manifests(jobID)
	if err != nil {
		return JobRecord{}, err
	}
	failureSelector, err := decoder.byte()
	if err != nil || failureSelector > 1 {
		return JobRecord{}, snapshotDecodeError("failure selector", err)
	}
	if failureSelector == 1 {
		failure, err := decoder.failureReport()
		if err != nil {
			return JobRecord{}, snapshotDecodeError("failure", err)
		}
		record.Failure = &failure
	}
	return record, nil
}

func (decoder *snapshotDecoder) invalidationProvenance() (invalidationProvenance, error) {
	kind, err := decoder.byte()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation kind", err)
	}
	workerID, err := decoder.u16()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation worker", err)
	}
	epoch, err := decoder.workerEpoch()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation epoch", err)
	}
	workerRevision, err := decoder.u64()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation worker revision", err)
	}
	jobRevision, err := decoder.u64()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation job revision", err)
	}
	assignmentRevision, err := decoder.u64()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation assignment revision", err)
	}
	assignmentDigest, err := decoder.array32()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation assignment digest", err)
	}
	markerCount, err := decoder.u16()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("invalidation marker count", err)
	}
	maxMarkers := model.LimitsV1().MaxTasksPerJob + 2*model.LimitsV1().MaxResultManifestsPerJob
	if markerCount == 0 || uint64(markerCount) > maxMarkers || int(markerCount) > decoder.remaining()/int(model.StateCommandReassignmentBytesV1) {
		return invalidationProvenance{}, fmt.Errorf("%w: invalidation marker count", ErrInvalidSnapshot)
	}
	markers := make([]NeedsReassignment, int(markerCount))
	for index := range markers {
		marker, markerErr := decoder.marker()
		if markerErr != nil {
			return invalidationProvenance{}, markerErr
		}
		markers[index] = marker
	}
	repairJobRevision, err := decoder.u64()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("repair job revision", err)
	}
	repairAssignmentRevision, err := decoder.u64()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("repair assignment revision", err)
	}
	repairAssignmentDigest, err := decoder.array32()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("repair assignment digest", err)
	}
	repairMarkersDigest, err := decoder.array32()
	if err != nil {
		return invalidationProvenance{}, snapshotDecodeError("repair markers digest", err)
	}
	return invalidationProvenance{
		Kind: workerInvalidationKind(kind), WorkerID: workerID, WorkerEpoch: epoch, WorkerRevision: workerRevision,
		JobControlRevision: jobRevision, AssignmentRevision: assignmentRevision, AssignmentDigest: assignmentDigest, Markers: markers,
		RepairJobControlRevision: repairJobRevision, RepairAssignmentRevision: repairAssignmentRevision,
		RepairAssignmentDigest: repairAssignmentDigest, RepairMarkersDigest: repairMarkersDigest,
	}, nil
}

func (decoder *snapshotDecoder) marker() (NeedsReassignment, error) {
	kind, err := decoder.byte()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker kind", err)
	}
	task, err := decoder.taskID()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker task", err)
	}
	sink, err := decoder.taskID()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker sink", err)
	}
	role, err := decoder.byte()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker role", err)
	}
	worker, err := decoder.u16()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker worker", err)
	}
	epoch, err := decoder.workerEpoch()
	if err != nil {
		return NeedsReassignment{}, snapshotDecodeError("marker epoch", err)
	}
	return NeedsReassignment{Kind: ReassignmentTargetKind(kind), Task: task, SinkTask: sink, ReplicaRole: model.ResultReplicaRole(role), OldWorkerID: worker, OldWorkerEpoch: epoch}, nil
}

func (decoder *snapshotDecoder) sourceEOFs(job model.JobID) (map[model.TaskID]SourceEOFRecord, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, snapshotDecodeError("source EOF count", err)
	}
	if uint64(count) > model.LimitsV1().MaxTasksPerJob || int(count) > decoder.remaining()/int(model.StateCommandSourceEOFEntryBytesV1) {
		return nil, fmt.Errorf("%w: source EOF count", ErrInvalidSnapshot)
	}
	result := make(map[model.TaskID]SourceEOFRecord, int(count))
	var previous model.TaskID
	for index := 0; index < int(count); index++ {
		task, err := decoder.taskID()
		if err != nil {
			return nil, err
		}
		eof, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		if task.JobID != job || index > 0 && compareSnapshotTask(previous, task) >= 0 {
			return nil, fmt.Errorf("%w: %w: source EOF keys", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		result[task] = SourceEOFRecord{EOF: eof, Revision: revision}
		previous = task
	}
	return result, nil
}

func (decoder *snapshotDecoder) checkpoints(job model.JobID) (map[model.TaskID]CheckpointRecord, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, snapshotDecodeError("checkpoint count", err)
	}
	if uint64(count) > model.LimitsV1().MaxTasksPerJob || int(count) > decoder.remaining()/int(model.StateCommandCheckpointEntryBytesV1) {
		return nil, fmt.Errorf("%w: checkpoint count", ErrInvalidSnapshot)
	}
	result := make(map[model.TaskID]CheckpointRecord, int(count))
	var previous model.TaskID
	for index := 0; index < int(count); index++ {
		task, err := decoder.taskID()
		if err != nil {
			return nil, err
		}
		watermark, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		if task.JobID != job || index > 0 && compareSnapshotTask(previous, task) >= 0 {
			return nil, fmt.Errorf("%w: %w: checkpoint keys", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		result[task] = CheckpointRecord{Watermark: watermark, Revision: revision}
		previous = task
	}
	return result, nil
}

func (decoder *snapshotDecoder) manifests(job model.JobID) (map[model.TaskID]ResultManifest, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, snapshotDecodeError("manifest count", err)
	}
	if uint64(count) > model.LimitsV1().MaxResultManifestsPerJob || int(count) > decoder.remaining()/int(model.StateCommandManifestEntryBytesV1) {
		return nil, fmt.Errorf("%w: manifest count", ErrInvalidSnapshot)
	}
	result := make(map[model.TaskID]ResultManifest, int(count))
	var previous model.TaskID
	for index := 0; index < int(count); index++ {
		task, err := decoder.taskID()
		if err != nil {
			return nil, err
		}
		manifest, err := decoder.manifest()
		if err != nil {
			return nil, err
		}
		if task.JobID != job || manifest.SinkTask != task || index > 0 && compareSnapshotTask(previous, task) >= 0 {
			return nil, fmt.Errorf("%w: %w: manifest keys", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		result[task] = manifest
		previous = task
	}
	return result, nil
}

func (decoder *snapshotDecoder) workerEvents(machine *Machine, count uint64) error {
	var previous workerEventKey
	for index := uint64(0); index < count; index++ {
		worker, err := decoder.u16()
		if err != nil {
			return err
		}
		epoch, err := decoder.workerEpoch()
		if err != nil {
			return err
		}
		transaction, err := decoder.u64()
		if err != nil {
			return err
		}
		digest, err := decoder.array32()
		if err != nil {
			return err
		}
		key := workerEventKey{WorkerID: worker, WorkerEpoch: epoch}
		if worker == 0 || index > 0 && compareSnapshotWorkerEvent(previous, key) >= 0 {
			return fmt.Errorf("%w: %w: worker event keys", ErrInvalidSnapshot, ErrSnapshotOrder)
		}
		machine.workerEvents[key] = workerEventCursor{TransactionID: transaction, Digest: digest}
		previous = key
	}
	return nil
}

func workerEventLess(left, right workerEventKey) bool {
	return compareSnapshotWorkerEvent(left, right) < 0
}

func validateSnapshotState(machine *Machine) error {
	if uint64(len(machine.clients)) > model.StateCommandMaxClientSessionsV1 || uint64(len(machine.subjects)) > model.StateCommandMaxSubjectHistoriesV1 || uint64(len(machine.workers)) > model.LimitsV1().MaxRegisteredWorkers || uint64(len(machine.jobs)) > model.LimitsV1().MaxRetainedJobs || uint64(len(machine.workerEvents)) > model.LimitsV1().MaxRegisteredWorkers {
		return errors.New("snapshot collection count exceeds bound")
	}
	if machine.coordinatorRevision == 0 {
		if machine.coordinatorEpoch != (model.CoordinatorEpoch{}) {
			return errors.New("zero coordinator revision has a nonzero epoch")
		}
	} else if machine.coordinatorEpoch.Validate() != nil || machine.lastAppliedIndex < machine.coordinatorEpoch.BeginIndex {
		return errors.New("coordinator epoch or last applied index is invalid")
	}
	if machine.activeJobCount() > model.LimitsV1().MaxActiveJobs {
		return errors.New("active job count exceeds bound")
	}
	for id, history := range machine.clients {
		if err := validateClientSnapshotHistory(machine, id, history); err != nil {
			return err
		}
	}
	for id, worker := range machine.workers {
		if id == 0 || worker.NodeID != id || worker.Validate() != nil {
			return errors.New("invalid worker entry")
		}
		if err := requireAppliedSubject(machine, SubjectKey{Kind: SubjectWorker, WorkerID: id}, worker.Revision); err != nil {
			return fmt.Errorf("worker %d history: %w", id, err)
		}
	}
	for _, subject := range sortedSubjectKeys(machine.subjects) {
		if err := validateSubjectHistory(machine, subject, machine.subjects[subject]); err != nil {
			return err
		}
	}
	usedSlots := make(map[uint16]uint64)
	definingRequests := make(map[model.ClientRequestID]model.JobID, len(machine.jobs))
	for id, job := range machine.jobs {
		if prior, exists := definingRequests[job.DefiningRequest]; exists && prior != id {
			return fmt.Errorf("%w: defining request identifies jobs %x and %x", ErrSnapshotCrossReference, prior, id)
		}
		definingRequests[job.DefiningRequest] = id
		if err := validateSnapshotJob(machine, id, job, usedSlots); err != nil {
			return err
		}
	}
	if machine.coordinatorRevision != 0 {
		if err := requireAppliedSubject(machine, SubjectKey{Kind: SubjectCoordinator}, machine.coordinatorRevision); err != nil {
			return fmt.Errorf("coordinator history: %w", err)
		}
	}
	for id, used := range usedSlots {
		if used > uint64(machine.workers[id].Slots) {
			return fmt.Errorf("worker %d task slots exceed capacity", id)
		}
	}
	for key, cursor := range machine.workerEvents {
		worker, ok := machine.workers[key.WorkerID]
		if !ok || worker.Epoch != key.WorkerEpoch || cursor.TransactionID == 0 || cursor.Digest == ([32]byte{}) {
			return errors.New("worker event does not reference the current worker incarnation")
		}
	}
	return nil
}

func validateSubjectHistory(machine *Machine, subject SubjectKey, history subjectHistory) error {
	if subject.Validate() != nil || history.id == (InternalCommandID{}) || history.digest == ([32]byte{}) || len(history.target) == 0 || len(history.result) == 0 || uint64(len(history.result)) > model.StateCommandMaxCachedResultBytesV1 || uint64(len(history.appliedResult)) > model.StateCommandMaxCachedResultBytesV1 || uint64(len(history.target)) > model.LimitsV1().MaxSubmitJobBytes || uint64(len(history.appliedTarget)) > model.LimitsV1().MaxSubmitJobBytes {
		return errors.New("invalid subject history")
	}
	if history.appliedRevision > history.revision || history.appliedRevision == 0 && (len(history.appliedTarget) != 0 || len(history.appliedResult) != 0 || history.applied) || history.appliedRevision != 0 && (len(history.appliedTarget) == 0 || len(history.appliedResult) == 0) || history.applied && history.appliedRevision != history.revision {
		return errors.New("invalid applied subject history")
	}
	current, err := decodeCanonicalSnapshotResult(history.result, "subject current result")
	if err != nil {
		return err
	}
	if err := validateSubjectResult(machine, subject, history.revision, current, false); err != nil {
		return err
	}
	if err := validateSnapshotSubjectTarget(machine, subject, history.target, history.revision, false); err != nil {
		return fmt.Errorf("current target: %w", err)
	}
	if history.appliedRevision != 0 {
		applied, err := decodeCanonicalSnapshotResult(history.appliedResult, "subject applied result")
		if err != nil {
			return err
		}
		if err := validateSubjectResult(machine, subject, history.appliedRevision, applied, true); err != nil {
			return err
		}
		if err := validateSnapshotSubjectTarget(machine, subject, history.appliedTarget, history.appliedRevision, true); err != nil {
			return fmt.Errorf("applied target: %w", err)
		}
	}
	if history.applied {
		if !bytes.Equal(history.result, history.appliedResult) {
			return errors.New("applied current result differs from retained applied result")
		}
		if subject.Kind != SubjectCoordinator && !bytes.Equal(history.target, history.appliedTarget) {
			return errors.New("applied current target differs from retained applied target")
		}
	} else if current.Code == ResultSuccess {
		return errors.New("non-applied current history retains success")
	}
	authoritative := machine.subjectRevision(subject)
	if subject.Kind == SubjectJobControl {
		if history.revision > authoritative {
			return errors.New("job-control history exceeds authoritative revision")
		}
	} else if history.revision != authoritative {
		return errors.New("subject history revision mismatch")
	}
	return nil
}

func validateClientSnapshotHistory(machine *Machine, id model.ClientID, history clientHistory) error {
	if id.Validate() != nil || history.sequence == 0 || history.digest == ([32]byte{}) || len(history.result) == 0 || uint64(len(history.result)) > model.StateCommandMaxCachedResultBytesV1 {
		return errors.New("invalid client history")
	}
	result, err := decodeCanonicalSnapshotResult(history.result, "client result")
	if err != nil {
		return err
	}
	if result.Subject != SubjectJobControl || result.JobID.Validate() != nil || result.WorkerID != 0 || result.Epoch != (model.CoordinatorEpoch{}) {
		return errors.New("client result is not bound to one job-control subject")
	}
	if result.Code != ResultSuccess && result.Code != ResultIdentityReuse && result.Code != ResultRevisionMismatch {
		return errors.New("client result code cannot be retained")
	}
	request := model.ClientRequestID{ClientID: id, Sequence: history.sequence}
	for _, job := range machine.jobs {
		if job.DefiningRequest != request {
			continue
		}
		if result.Code != ResultSuccess || result.JobID != job.JobID || history.digest != model.PublicSubmitCommandDigest(request, job.TopologyBytes) {
			return errors.New("defining submit client history mismatch")
		}
	}
	return nil
}

func decodeCanonicalSnapshotResult(encoded []byte, field string) (CommandResult, error) {
	result, err := UnmarshalCommandResult(encoded)
	if err != nil {
		return CommandResult{}, fmt.Errorf("%s: %w", field, err)
	}
	canonical, err := MarshalCommandResult(result)
	if err != nil {
		return CommandResult{}, fmt.Errorf("%s canonical encode: %w", field, err)
	}
	if !bytes.Equal(canonical, encoded) {
		return CommandResult{}, fmt.Errorf("%s is noncanonical", field)
	}
	return result, nil
}

func validateSubjectResult(machine *Machine, subject SubjectKey, revision uint64, result CommandResult, applied bool) error {
	if result.Subject != subject.Kind || result.Revision != revision {
		return errors.New("subject result kind or revision mismatch")
	}
	switch subject.Kind {
	case SubjectCoordinator:
		if result.JobID != (model.JobID{}) || result.WorkerID != 0 || revision != 0 && result.Epoch != machine.coordinatorEpoch {
			return errors.New("coordinator result identity or epoch mismatch")
		}
	case SubjectWorker:
		if result.JobID != (model.JobID{}) || result.WorkerID != subject.WorkerID || result.Epoch != (model.CoordinatorEpoch{}) {
			return errors.New("worker result identity mismatch")
		}
	default:
		if result.JobID != subject.JobID || result.WorkerID != 0 || result.Epoch != (model.CoordinatorEpoch{}) {
			return errors.New("job-scoped result identity mismatch")
		}
	}
	if applied {
		if result.Code != ResultSuccess {
			return errors.New("retained applied result is not success")
		}
		return nil
	}
	if result.Code != ResultSuccess && result.Code != ResultRevisionMismatch && result.Code != ResultResultTooLarge {
		return errors.New("subject result code cannot be retained")
	}
	return nil
}

func requireAppliedSubject(machine *Machine, subject SubjectKey, revision uint64) error {
	history, ok := machine.subjects[subject]
	if !ok {
		return fmt.Errorf("%w: missing required subject history", ErrSnapshotCrossReference)
	}
	if revision == 0 || history.appliedRevision != revision {
		return fmt.Errorf("%w: applied revision %d does not match authoritative %d", ErrSnapshotCrossReference, history.appliedRevision, revision)
	}
	return nil
}

func validateSnapshotSubjectTarget(machine *Machine, subject SubjectKey, target []byte, revision uint64, applied bool) error {
	switch subject.Kind {
	case SubjectCoordinator:
		return validateCoordinatorSnapshotTarget(machine, target, applied)
	case SubjectWorker:
		return validateWorkerSnapshotTarget(machine, subject, target, revision, applied)
	case SubjectJobControl:
		return validateJobControlSnapshotTarget(machine, subject, target, revision, applied)
	case SubjectSourceEOF:
		decoder := commandDecoder{input: target}
		task, err := decoder.taskID()
		if err != nil {
			return err
		}
		eof, err := decoder.u64()
		if err != nil || !decoder.done() || task != subject.TaskID {
			return fmt.Errorf("%w: malformed source EOF target", ErrMalformedCommand)
		}
		if applied {
			record, ok := machine.jobs[subject.JobID].SourceEOFs[subject.TaskID]
			if !ok || record.Revision != revision || record.EOF != eof {
				return errors.New("source EOF applied target mismatch")
			}
		}
		return nil
	case SubjectSourceCheckpoint:
		decoder := commandDecoder{input: target}
		report, err := decoder.completionReport()
		if err != nil {
			return err
		}
		if !decoder.done() || report.Validate() != nil || report.JobID != subject.JobID || report.Source != subject.TaskID {
			return fmt.Errorf("%w: malformed checkpoint target", ErrMalformedCommand)
		}
		if applied {
			record, ok := machine.jobs[subject.JobID].Checkpoints[subject.TaskID]
			if !ok || record.Revision != revision || report.ExpectedCheckpointRevision == ^uint64(0) || report.ExpectedCheckpointRevision+1 != revision || report.New != record.Watermark {
				return errors.New("checkpoint applied target mismatch")
			}
		}
		return nil
	case SubjectResultManifest:
		decoder := commandDecoder{input: target}
		manifest, err := decoder.manifest()
		if err != nil {
			return err
		}
		if !decoder.done() || manifest.Validate() != nil || manifest.JobID != subject.JobID || manifest.SinkTask != subject.TaskID {
			return fmt.Errorf("%w: malformed manifest target", ErrMalformedCommand)
		}
		if applied {
			record, ok := machine.jobs[subject.JobID].Manifests[subject.TaskID]
			if !ok || record != manifest || manifest.ManifestRevision != revision {
				return errors.New("manifest applied target mismatch")
			}
		}
		return nil
	default:
		return errors.New("unknown snapshot subject target")
	}
}

func validateCoordinatorSnapshotTarget(machine *Machine, target []byte, applied bool) error {
	decoder := commandDecoder{input: target}
	if applied {
		term, err := decoder.u64()
		if err != nil {
			return err
		}
		coordinator, err := decoder.u16()
		if err != nil {
			return err
		}
		nonce, err := decoder.array16()
		if err != nil || !decoder.done() {
			return fmt.Errorf("%w: malformed coordinator applied target", ErrMalformedCommand)
		}
		if term != machine.coordinatorEpoch.Term || coordinator != machine.coordinatorEpoch.Coordinator || nonce != machine.coordinatorEpoch.Nonce {
			return errors.New("coordinator applied target does not identify current epoch")
		}
		return nil
	}
	coordinator, err := decoder.u16()
	if err != nil {
		return err
	}
	nonce, err := decoder.array16()
	if err != nil || !decoder.done() || coordinator == 0 || nonce == ([16]byte{}) {
		return fmt.Errorf("%w: malformed coordinator target", ErrMalformedCommand)
	}
	return nil
}

func validateWorkerSnapshotTarget(machine *Machine, subject SubjectKey, target []byte, revision uint64, applied bool) error {
	const workerRecordBytes = int(model.StateCommandWorkerRecordBytesV1 - 2)
	if len(target) == workerRecordBytes {
		decoder := commandDecoder{input: target}
		record, err := decoder.workerRecord()
		if err != nil || !decoder.done() || record.Validate() != nil || record.NodeID != subject.WorkerID {
			return fmt.Errorf("%w: malformed worker registration target", ErrMalformedCommand)
		}
		if applied && (record.Revision != revision || machine.workers[subject.WorkerID] != record) {
			return errors.New("worker registration applied target mismatch")
		}
		return nil
	}
	if len(target) == 18 {
		decoder := commandDecoder{input: target}
		workerID, err := decoder.u16()
		if err != nil {
			return err
		}
		epoch, err := decoder.workerEpoch()
		if err != nil || !decoder.done() || workerID != subject.WorkerID || epoch.Validate() != nil {
			return fmt.Errorf("%w: malformed worker drain target", ErrMalformedCommand)
		}
		if applied {
			worker := machine.workers[subject.WorkerID]
			if worker.Revision != revision || worker.Epoch != epoch || worker.State != WorkerDraining {
				return errors.New("worker drain applied target mismatch")
			}
		}
		return nil
	}
	if len(target) >= 20 && (len(target)-20)%64 == 0 {
		decoder := commandDecoder{input: target}
		workerID, err := decoder.u16()
		if err != nil {
			return err
		}
		epoch, err := decoder.workerEpoch()
		if err != nil {
			return err
		}
		affected, err := decoder.affected()
		if err != nil || !decoder.done() || validateAffected(affected) != nil || workerID != subject.WorkerID || epoch.Validate() != nil {
			return fmt.Errorf("%w: malformed worker deactivation target", ErrMalformedCommand)
		}
		if applied {
			worker := machine.workers[subject.WorkerID]
			if worker.Revision != revision || worker.Epoch != epoch || worker.State != WorkerOffline {
				return errors.New("worker deactivation applied target mismatch")
			}
			if err := validateAppliedWorkerAffected(machine, workerInvalidationDeactivate, revision, workerID, epoch, affected); err != nil {
				return err
			}
		}
		return nil
	}
	if len(target) >= 113 && (len(target)-113)%64 == 0 {
		decoder := commandDecoder{input: target}
		workerID, err := decoder.u16()
		if err != nil {
			return err
		}
		oldEpoch, err := decoder.workerEpoch()
		if err != nil {
			return err
		}
		record, err := decoder.workerRecord()
		if err != nil {
			return err
		}
		affected, err := decoder.affected()
		if err != nil || !decoder.done() || validateAffected(affected) != nil || workerID != subject.WorkerID || oldEpoch.Validate() != nil || record.Validate() != nil || record.NodeID != workerID || record.Epoch == oldEpoch {
			return fmt.Errorf("%w: malformed worker replacement target", ErrMalformedCommand)
		}
		if applied {
			if record.Revision != revision || machine.workers[subject.WorkerID] != record {
				return errors.New("worker replacement applied target mismatch")
			}
			if err := validateAppliedWorkerAffected(machine, workerInvalidationReplaceEpoch, revision, workerID, oldEpoch, affected); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("%w: unknown worker target layout", ErrMalformedCommand)
}

func validateAppliedWorkerAffected(machine *Machine, kind workerInvalidationKind, workerRevision uint64, workerID uint16, epoch model.WorkerEpoch, affected []AffectedAssignment) error {
	presented := make(map[model.JobID]AffectedAssignment, len(affected))
	for _, item := range affected {
		job, exists := machine.jobs[item.JobID]
		if !exists || !jobHasInvalidationProvenance(job, kind, workerRevision, workerID, epoch, item) {
			return fmt.Errorf("%w: affected job fence lacks exact invalidation provenance", ErrSnapshotCrossReference)
		}
		presented[item.JobID] = item
	}
	for jobID, job := range machine.jobs {
		for _, provenance := range job.invalidationHistory {
			if provenance.Kind != kind || provenance.WorkerRevision != workerRevision || provenance.WorkerID != workerID || provenance.WorkerEpoch != epoch {
				continue
			}
			want := provenanceAffected(jobID, provenance)
			if got, exists := presented[jobID]; !exists || got != want {
				return fmt.Errorf("%w: affected list omits exact retained invalidation provenance", ErrSnapshotCrossReference)
			}
		}
	}
	return nil
}

func provenanceAffected(jobID model.JobID, provenance invalidationProvenance) AffectedAssignment {
	return AffectedAssignment{
		JobID:              jobID,
		JobControlRevision: provenance.JobControlRevision,
		AssignmentRevision: provenance.AssignmentRevision,
		AssignmentDigest:   provenance.AssignmentDigest,
	}
}

func jobHasInvalidationProvenance(job JobRecord, kind workerInvalidationKind, workerRevision uint64, workerID uint16, epoch model.WorkerEpoch, affected AffectedAssignment) bool {
	for _, provenance := range job.invalidationHistory {
		if provenance.Kind == kind && provenance.WorkerRevision == workerRevision && provenance.WorkerID == workerID && provenance.WorkerEpoch == epoch && provenanceAffected(job.JobID, provenance) == affected {
			return true
		}
	}
	return false
}

func validateJobControlSnapshotTarget(machine *Machine, subject SubjectKey, target []byte, revision uint64, applied bool) error {
	if len(target) == 18 {
		decoder := commandDecoder{input: target}
		job, err := decoder.jobID()
		if err != nil {
			return err
		}
		from, err := decoder.byte()
		if err != nil {
			return err
		}
		to, err := decoder.byte()
		if err != nil || !decoder.done() || job != subject.JobID || JobLifecycle(from) < JobPending || JobLifecycle(from) > JobCanceled || JobLifecycle(to) < JobPending || JobLifecycle(to) > JobCanceled || from == to {
			return fmt.Errorf("%w: malformed job transition target", ErrMalformedCommand)
		}
		if applied {
			legal := JobLifecycle(from) == JobDeploying && JobLifecycle(to) == JobRunning || JobLifecycle(from) == JobRunning && JobLifecycle(to) == JobDraining || JobLifecycle(from) == JobDraining && JobLifecycle(to) == JobSucceeded
			record, exists := machine.jobs[subject.JobID]
			canceledSuccess := record.Lifecycle == JobCanceled && JobLifecycle(to) != JobSucceeded
			if !legal || !exists || record.JobControlRevision < revision || record.Lifecycle != JobLifecycle(to) && !canceledSuccess {
				return errors.New("job transition applied target mismatch")
			}
		}
		return nil
	}
	if len(target) == int(model.StateCommandFailureBytesV1) {
		decoder := commandDecoder{input: target}
		report, err := decoder.failureReport()
		if err != nil || !decoder.done() || report.Validate() != nil || report.JobID != subject.JobID {
			return fmt.Errorf("%w: malformed failure target", ErrMalformedCommand)
		}
		if applied {
			record, exists := machine.jobs[subject.JobID]
			if report.JobControlRevision == ^uint64(0) || report.JobControlRevision+1 != revision || !exists || record.JobControlRevision != revision || record.Failure == nil || *record.Failure != report {
				return errors.New("job failure applied target mismatch")
			}
		}
		return nil
	}
	if assignment, ok := decodeSnapshotAssignmentTarget(target); ok {
		if assignment.JobID != subject.JobID {
			return errors.New("assignment target job mismatch")
		}
		if job, exists := machine.jobs[subject.JobID]; exists {
			topology, err := model.DecodeTopology(job.TopologyBytes)
			if err != nil {
				return fmt.Errorf("%w: assignment target topology: %w", ErrInvalidCommand, err)
			}
			if err := assignment.Validate(topology); err != nil {
				return fmt.Errorf("%w: assignment target: %w", ErrInvalidCommandSubject, err)
			}
			if applied && (assignment.Revision != 1 || job.Assignment == nil || !bytes.Equal(appendAssignment(nil, assignment), appendAssignment(nil, *job.Assignment))) {
				return errors.New("assignment applied target mismatch")
			}
		}
		return nil
	}
	if replacement, ok := decodeSnapshotReplacementTarget(target); ok {
		if replacement.JobID != subject.JobID || replacement.ExpectedRevision == 0 || replacement.ExpectedRevision == ^uint64(0) || replacement.ExpectedDigest == ([32]byte{}) || replacement.ExpectedMarkersDigest == ([32]byte{}) || replacement.Target.JobID != subject.JobID || replacement.Target.Revision != replacement.ExpectedRevision+1 {
			return errors.New("replacement assignment target mismatch")
		}
		if job, exists := machine.jobs[subject.JobID]; exists {
			topology, err := model.DecodeTopology(job.TopologyBytes)
			if err != nil {
				return fmt.Errorf("%w: replacement target topology: %w", ErrInvalidCommand, err)
			}
			if err := replacement.Target.Validate(topology); err != nil {
				return fmt.Errorf("%w: replacement target: %w", ErrInvalidCommandSubject, err)
			}
			if applied && (replacement.ExpectedDigest == replacement.Target.Digest || replacement.ExpectedMarkersDigest == NeedsReassignmentDigest(nil) || job.Assignment == nil || !bytes.Equal(appendAssignment(nil, replacement.Target), appendAssignment(nil, *job.Assignment))) {
				return errors.New("replacement applied target mismatch")
			}
		}
		return nil
	}
	return fmt.Errorf("%w: unknown job-control target layout", ErrMalformedCommand)
}

type snapshotReplacementTarget struct {
	JobID                 model.JobID
	ExpectedRevision      uint64
	ExpectedDigest        [32]byte
	ExpectedMarkersDigest [32]byte
	Target                model.AssignmentSet
}

func decodeSnapshotAssignmentTarget(target []byte) (model.AssignmentSet, bool) {
	decoder := commandDecoder{input: target}
	assignment, err := decoder.assignment()
	return assignment, err == nil && decoder.done()
}

func decodeSnapshotReplacementTarget(target []byte) (snapshotReplacementTarget, bool) {
	decoder := commandDecoder{input: target}
	job, err := decoder.jobID()
	if err != nil {
		return snapshotReplacementTarget{}, false
	}
	revision, err := decoder.u64()
	if err != nil {
		return snapshotReplacementTarget{}, false
	}
	digest, err := decoder.array32()
	if err != nil {
		return snapshotReplacementTarget{}, false
	}
	markers, err := decoder.array32()
	if err != nil {
		return snapshotReplacementTarget{}, false
	}
	assignment, err := decoder.assignment()
	if err != nil || !decoder.done() {
		return snapshotReplacementTarget{}, false
	}
	return snapshotReplacementTarget{JobID: job, ExpectedRevision: revision, ExpectedDigest: digest, ExpectedMarkersDigest: markers, Target: assignment}, true
}

func validateSnapshotJob(machine *Machine, key model.JobID, job JobRecord, usedSlots map[uint16]uint64) error {
	if job.JobID != key || key.Validate() != nil || job.DefiningRequest.Validate() != nil || job.JobControlRevision == 0 || job.Lifecycle < JobPending || job.Lifecycle > JobCanceled {
		return errors.New("invalid job identity, revision, or lifecycle")
	}
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return fmt.Errorf("%w: topology: %w", ErrInvalidCommand, err)
	}
	if topology.Digest() != job.TopologyDigest || model.DeriveJobID(job.DefiningRequest, job.TopologyDigest) != job.JobID {
		return errors.New("job topology definition mismatch")
	}
	if err := validateMarkers(job.NeedsReassignment, job.JobID); err != nil {
		return err
	}
	activeInvalidations, err := validateInvalidationHistory(machine, job)
	if err != nil {
		return err
	}
	client, ok := machine.clients[job.DefiningRequest.ClientID]
	if !ok || client.sequence < job.DefiningRequest.Sequence {
		return errors.New("job defining request has no retained client history")
	}
	if client.sequence == job.DefiningRequest.Sequence && client.digest != model.PublicSubmitCommandDigest(job.DefiningRequest, job.TopologyBytes) {
		return errors.New("job defining request digest mismatch")
	}
	if job.Assignment != nil || job.Lifecycle == JobDeploying || job.Lifecycle == JobRunning || job.Lifecycle == JobDraining || job.Lifecycle == JobSucceeded || job.Lifecycle == JobFailed {
		history, exists := machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: job.JobID}]
		external := activeInvalidations
		if job.Lifecycle == JobCanceled {
			external++
		}
		if !exists || history.appliedRevision == 0 || history.appliedRevision > job.JobControlRevision || job.JobControlRevision-history.appliedRevision != external {
			return errors.New("job state lacks required applied job-control history")
		}
	}
	if job.Assignment != nil {
		if err := job.Assignment.Validate(topology); err != nil {
			return fmt.Errorf("%w: assignment: %w", ErrInvalidCommandSubject, err)
		}
	}
	if job.Lifecycle == JobPending && job.Assignment != nil || (job.Lifecycle == JobDeploying || job.Lifecycle == JobRunning || job.Lifecycle == JobDraining || job.Lifecycle == JobSucceeded) && job.Assignment == nil {
		return errors.New("job lifecycle assignment mismatch")
	}
	for source, eof := range job.SourceEOFs {
		want, sourceErr := model.SourceEOF(topology, source)
		if sourceErr != nil || want != eof.EOF || eof.Revision != 1 {
			return errors.New("source EOF cross-reference mismatch")
		}
		if err := requireAppliedSubject(machine, SubjectKey{Kind: SubjectSourceEOF, JobID: job.JobID, TaskID: source}, eof.Revision); err != nil {
			return fmt.Errorf("source EOF history: %w", err)
		}
	}
	for source, checkpoint := range job.Checkpoints {
		eof, ok := job.SourceEOFs[source]
		if !ok || checkpoint.Revision == 0 || checkpoint.Watermark > eof.EOF {
			return errors.New("checkpoint cross-reference mismatch")
		}
		if err := requireAppliedSubject(machine, SubjectKey{Kind: SubjectSourceCheckpoint, JobID: job.JobID, TaskID: source}, checkpoint.Revision); err != nil {
			return fmt.Errorf("checkpoint history: %w", err)
		}
	}
	var artifactBytes uint64
	for sink, manifest := range job.Manifests {
		if manifest.Validate() != nil || manifest.JobID != job.JobID || manifest.SinkTask != sink || manifest.SpecificationHash != job.TopologyDigest || job.Assignment == nil {
			return errors.New("manifest cross-reference mismatch")
		}
		replica, ok := resultReplica(job.Assignment, sink)
		if !ok || replica != manifest.Replicas {
			return errors.New("manifest assignment mismatch")
		}
		if manifest.TotalBytes > model.LimitsV1().MaxResultRecordsBytesPerJob-artifactBytes {
			return errors.New("aggregate result artifact bytes exceed per-job bound")
		}
		artifactBytes += manifest.TotalBytes
		if err := requireAppliedSubject(machine, SubjectKey{Kind: SubjectResultManifest, JobID: job.JobID, TaskID: sink}, manifest.ManifestRevision); err != nil {
			return fmt.Errorf("manifest history: %w", err)
		}
	}
	if job.Failure != nil {
		failureToken, tokenExists := assignmentToken(job.Assignment, job.Failure.Task.Task)
		if job.Failure.Validate() != nil || job.Failure.JobID != job.JobID || job.Lifecycle != JobFailed || job.Assignment == nil || !tokenExists || failureToken != job.Failure.Task || job.Failure.AssignmentRevision != job.Assignment.Revision || job.Failure.JobControlRevision == ^uint64(0) || job.Failure.JobControlRevision+1 != job.JobControlRevision {
			return errors.New("failure cross-reference mismatch")
		}
	} else if job.Lifecycle == JobFailed {
		return errors.New("failed job has no failure report")
	}
	if job.Assignment != nil {
		if !machine.allSourceEOFsPresent(job) {
			return errors.New("assigned job lacks complete source EOF state")
		}
	}
	if job.Lifecycle == JobDraining || job.Lifecycle == JobSucceeded {
		if !allCheckpointsFinal(job) {
			return errors.New("draining or succeeded job lacks final checkpoints")
		}
	}
	if job.Lifecycle == JobSucceeded && (!allManifestsCurrent(job) || job.Failure != nil) {
		return errors.New("succeeded job lacks current manifests")
	}
	if job.Assignment != nil {
		for _, token := range job.Assignment.Tasks {
			worker, ok := machine.workers[token.WorkerID]
			if !ok {
				return errors.New("assignment references an unknown worker")
			}
			if job.Lifecycle.terminal() {
				continue
			}
			if snapshotTaskTargetMarked(job.NeedsReassignment, token) {
				continue
			}
			if worker.Epoch != token.WorkerEpoch || worker.State == WorkerOffline {
				return errors.New("active assignment references unavailable worker")
			}
			usedSlots[token.WorkerID]++
		}
		for _, replica := range job.Assignment.ResultReplicas {
			if err := validateSnapshotReplicaEndpoint(machine, job, replica, model.PrimaryReplica, replica.PrimaryNodeID, replica.PrimaryEpoch); err != nil {
				return err
			}
			if err := validateSnapshotReplicaEndpoint(machine, job, replica, model.SecondaryReplica, replica.SecondaryNodeID, replica.SecondaryEpoch); err != nil {
				return err
			}
		}
	}
	for _, marker := range job.NeedsReassignment {
		if !snapshotMarkerTargetsAssignment(marker, job.Assignment) {
			return errors.New("reassignment marker has no matching assignment target")
		}
	}
	return nil
}

func validateInvalidationHistory(machine *Machine, job JobRecord) (uint64, error) {
	if uint64(len(job.invalidationHistory)) > model.StateCommandMaxInvalidationProvenanceV1 {
		return 0, fmt.Errorf("%w: invalidation provenance count exceeds bound", ErrSnapshotCrossReference)
	}
	type repairGroup struct {
		provenance []invalidationProvenance
		retained   bool
	}
	repairs := make(map[uint64]*repairGroup)
	active := make([]invalidationProvenance, 0)
	seenEvent := make(map[[27]byte]struct{}, len(job.invalidationHistory))
	var priorJobRevision uint64
	var currentRepairRevision uint64
	var completedRepairRevision uint64
	seenActive := false
	for _, provenance := range job.invalidationHistory {
		if provenance.Kind < workerInvalidationDeactivate || provenance.Kind > workerInvalidationReplaceEpoch || provenance.WorkerID == 0 || provenance.WorkerEpoch.Validate() != nil || provenance.WorkerRevision == 0 || provenance.JobControlRevision == 0 || provenance.AssignmentRevision == 0 || provenance.AssignmentDigest == ([32]byte{}) || len(provenance.Markers) == 0 {
			return 0, fmt.Errorf("%w: invalid invalidation provenance identity or fence", ErrSnapshotCrossReference)
		}
		if priorJobRevision != 0 && compareSnapshotInvalidation(invalidationProvenance{JobControlRevision: priorJobRevision}, provenance) >= 0 {
			return 0, fmt.Errorf("%w: invalidation provenance is not chronological", ErrSnapshotCrossReference)
		}
		priorJobRevision = provenance.JobControlRevision
		if err := validateMarkers(provenance.Markers, job.JobID); err != nil {
			return 0, fmt.Errorf("%w: invalidation provenance markers: %w", ErrSnapshotCrossReference, err)
		}
		for _, marker := range provenance.Markers {
			if marker.OldWorkerID != provenance.WorkerID || marker.OldWorkerEpoch != provenance.WorkerEpoch {
				return 0, fmt.Errorf("%w: invalidation marker does not identify its worker incarnation", ErrSnapshotCrossReference)
			}
		}
		worker, exists := machine.workers[provenance.WorkerID]
		if !exists || worker.Revision < provenance.WorkerRevision {
			return 0, fmt.Errorf("%w: invalidation provenance has no reachable worker revision", ErrSnapshotCrossReference)
		}
		var event [27]byte
		event[0] = byte(provenance.Kind)
		copy(event[1:17], provenance.WorkerEpoch[:])
		event[17] = byte(provenance.WorkerID >> 8)
		event[18] = byte(provenance.WorkerID)
		for index := 0; index < 8; index++ {
			event[19+index] = byte(provenance.WorkerRevision >> (56 - 8*index))
		}
		if _, duplicate := seenEvent[event]; duplicate {
			return 0, fmt.Errorf("%w: duplicate invalidation provenance event", ErrSnapshotCrossReference)
		}
		seenEvent[event] = struct{}{}
		retained := machine.subjectRetainsInvalidation(job.JobID, provenance)
		if worker.Revision == provenance.WorkerRevision {
			if !retained || (provenance.Kind == workerInvalidationDeactivate && (worker.Epoch != provenance.WorkerEpoch || worker.State != WorkerOffline)) || (provenance.Kind == workerInvalidationReplaceEpoch && worker.Epoch == provenance.WorkerEpoch) {
				return 0, fmt.Errorf("%w: current worker does not prove invalidation outcome", ErrSnapshotCrossReference)
			}
		} else if provenance.Kind == workerInvalidationReplaceEpoch && worker.Epoch == provenance.WorkerEpoch {
			return 0, fmt.Errorf("%w: replaced worker epoch became current again", ErrSnapshotCrossReference)
		}
		activeRepair := provenance.RepairJobControlRevision == 0 && provenance.RepairAssignmentRevision == 0 && provenance.RepairAssignmentDigest == ([32]byte{}) && provenance.RepairMarkersDigest == ([32]byte{})
		completeRepair := provenance.RepairJobControlRevision != 0 && provenance.RepairAssignmentRevision != 0 && provenance.RepairAssignmentDigest != ([32]byte{}) && provenance.RepairMarkersDigest != ([32]byte{})
		if !activeRepair && !completeRepair {
			return 0, fmt.Errorf("%w: partial invalidation repair provenance", ErrSnapshotCrossReference)
		}
		if currentRepairRevision != 0 && provenance.RepairJobControlRevision != currentRepairRevision {
			completedRepairRevision = currentRepairRevision
			currentRepairRevision = 0
		}
		if provenance.JobControlRevision < completedRepairRevision || seenActive && completeRepair {
			return 0, fmt.Errorf("%w: invalidation repair generations are not chronological", ErrSnapshotCrossReference)
		}
		if activeRepair {
			if job.Assignment == nil || provenance.AssignmentRevision != job.Assignment.Revision || provenance.AssignmentDigest != job.Assignment.Digest {
				return 0, fmt.Errorf("%w: active invalidation does not fence current assignment", ErrSnapshotCrossReference)
			}
			active = append(active, provenance)
			seenActive = true
			continue
		}
		if currentRepairRevision == 0 {
			currentRepairRevision = provenance.RepairJobControlRevision
		}
		if provenance.RepairAssignmentRevision != provenance.AssignmentRevision+1 || provenance.RepairJobControlRevision <= provenance.JobControlRevision+1 || provenance.RepairJobControlRevision > job.JobControlRevision || job.Assignment == nil || job.Assignment.Revision < provenance.RepairAssignmentRevision || job.Assignment.Revision == provenance.RepairAssignmentRevision && job.Assignment.Digest != provenance.RepairAssignmentDigest {
			return 0, fmt.Errorf("%w: invalidation repair fence is unreachable", ErrSnapshotCrossReference)
		}
		group := repairs[provenance.RepairJobControlRevision]
		if group == nil {
			group = &repairGroup{}
			repairs[provenance.RepairJobControlRevision] = group
		}
		group.provenance = append(group.provenance, provenance)
		group.retained = group.retained || retained
	}

	for repairRevision, group := range repairs {
		first := group.provenance[0]
		markers := make([]NeedsReassignment, 0)
		for index, provenance := range group.provenance {
			if provenance.AssignmentRevision != first.AssignmentRevision || provenance.AssignmentDigest != first.AssignmentDigest || provenance.RepairAssignmentRevision != first.RepairAssignmentRevision || provenance.RepairAssignmentDigest != first.RepairAssignmentDigest || provenance.RepairMarkersDigest != first.RepairMarkersDigest || provenance.JobControlRevision != first.JobControlRevision+uint64(index) {
				return 0, fmt.Errorf("%w: inconsistent invalidation repair group", ErrSnapshotCrossReference)
			}
			markers = sortedMarkerUnion(markers, provenance.Markers)
		}
		if !group.retained || repairRevision != first.JobControlRevision+uint64(len(group.provenance))+1 || NeedsReassignmentDigest(markers) != first.RepairMarkersDigest {
			return 0, fmt.Errorf("%w: invalidation repair group lacks retained causal proof", ErrSnapshotCrossReference)
		}
		if history, exists := machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: job.JobID}]; exists && history.appliedRevision == repairRevision {
			replacement, ok := decodeSnapshotReplacementTarget(history.appliedTarget)
			if !ok || replacement.ExpectedRevision != first.AssignmentRevision || replacement.ExpectedDigest != first.AssignmentDigest || replacement.ExpectedMarkersDigest != first.RepairMarkersDigest || replacement.Target.Revision != first.RepairAssignmentRevision || replacement.Target.Digest != first.RepairAssignmentDigest {
				return 0, fmt.Errorf("%w: retained repair target does not match provenance", ErrSnapshotCrossReference)
			}
		}
	}

	markers := make([]NeedsReassignment, 0)
	for index, provenance := range active {
		if index > 0 && provenance.JobControlRevision != active[index-1].JobControlRevision+1 {
			return 0, fmt.Errorf("%w: active invalidations are not contiguous", ErrSnapshotCrossReference)
		}
		markers = sortedMarkerUnion(markers, provenance.Markers)
	}
	if len(active) != 0 {
		history, exists := machine.subjects[SubjectKey{Kind: SubjectJobControl, JobID: job.JobID}]
		if !exists || history.appliedRevision != active[0].JobControlRevision {
			return 0, fmt.Errorf("%w: active invalidation does not follow retained job-control authority", ErrSnapshotCrossReference)
		}
	}
	if !sameReassignmentMarkers(markers, job.NeedsReassignment) {
		return 0, fmt.Errorf("%w: active invalidation provenance does not exactly explain markers", ErrSnapshotCrossReference)
	}
	return uint64(len(active)), nil
}

func sameReassignmentMarkers(left, right []NeedsReassignment) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func snapshotTaskTargetMarked(markers []NeedsReassignment, token model.AssignmentToken) bool {
	for _, marker := range markers {
		if marker.Kind == TaskTarget && marker.Task == token.Task && marker.OldWorkerID == token.WorkerID && marker.OldWorkerEpoch == token.WorkerEpoch {
			return true
		}
	}
	return false
}

func validateSnapshotReplicaEndpoint(machine *Machine, job JobRecord, replica model.ResultReplicaSet, role model.ResultReplicaRole, workerID uint16, epoch model.WorkerEpoch) error {
	worker, ok := machine.workers[workerID]
	if !ok {
		return errors.New("result replica references an unknown worker")
	}
	if job.Lifecycle.terminal() || snapshotReplicaTargetMarked(job.NeedsReassignment, replica.SinkTask, role, workerID, epoch) {
		return nil
	}
	if worker.Epoch != epoch || worker.State == WorkerOffline {
		return errors.New("active result replica references unavailable worker")
	}
	return nil
}

func snapshotReplicaTargetMarked(markers []NeedsReassignment, sink model.TaskID, role model.ResultReplicaRole, workerID uint16, epoch model.WorkerEpoch) bool {
	for _, marker := range markers {
		if marker.Kind == ResultReplicaTarget && marker.SinkTask == sink && marker.ReplicaRole == role && marker.OldWorkerID == workerID && marker.OldWorkerEpoch == epoch {
			return true
		}
	}
	return false
}

func snapshotMarkerTargetsAssignment(marker NeedsReassignment, assignment *model.AssignmentSet) bool {
	if assignment == nil {
		return false
	}
	if marker.Kind == TaskTarget {
		for _, token := range assignment.Tasks {
			if marker.Task == token.Task && marker.OldWorkerID == token.WorkerID && marker.OldWorkerEpoch == token.WorkerEpoch {
				return true
			}
		}
		return false
	}
	for _, replica := range assignment.ResultReplicas {
		if replica.SinkTask != marker.SinkTask {
			continue
		}
		if marker.ReplicaRole == model.PrimaryReplica {
			return marker.OldWorkerID == replica.PrimaryNodeID && marker.OldWorkerEpoch == replica.PrimaryEpoch
		}
		if marker.ReplicaRole == model.SecondaryReplica {
			return marker.OldWorkerID == replica.SecondaryNodeID && marker.OldWorkerEpoch == replica.SecondaryEpoch
		}
	}
	return false
}
