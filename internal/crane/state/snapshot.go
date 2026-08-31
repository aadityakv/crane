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
		return nil, fmt.Errorf("%w: live state: %v", ErrInvalidSnapshot, err)
	}
	estimated, ok := machine.estimateCanonicalSnapshotBytesLocked()
	if !ok {
		return nil, fmt.Errorf("%w: canonical state exceeds snapshot bound", ErrInvalidSnapshot)
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
	if schemaVersion == 0 || schemaVersion == 1 {
		if len(encoded) != 0 {
			return fmt.Errorf("%w: legacy schema %d has a payload", ErrInvalidSnapshot, schemaVersion)
		}
		empty := NewMachine()
		machine.replaceWith(empty)
		return nil
	}
	if schemaVersion != SnapshotSchemaVersion {
		return fmt.Errorf("%w: unsupported schema %d", ErrInvalidSnapshot, schemaVersion)
	}
	if len(encoded) == 0 || uint64(len(encoded)) > model.StateCommandMaxSnapshotBytesV1 {
		return fmt.Errorf("%w: payload length %d", ErrInvalidSnapshot, len(encoded))
	}
	temporary, err := decodeSnapshot(encoded)
	if err != nil {
		return err
	}
	canonical := temporary.appendCanonicalSnapshotLocked(make([]byte, 0, len(encoded)), nil)
	if !bytes.Equal(canonical, encoded) {
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
	workerIDs := make([]int, 0, len(machine.workers))
	for id := range machine.workers {
		workerIDs = append(workerIDs, int(id))
	}
	sort.Ints(workerIDs)
	for _, id := range workerIDs {
		encoded = appendWorkerEntry(encoded, uint16(id), machine.workers[uint16(id)], nil)
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
	var fixed [4]byte
	binary.BigEndian.PutUint32(fixed[:], uint32(len(value)))
	encoded = append(encoded, fixed[:]...)
	return append(encoded, value...)
}

func sortedClientIDs(values map[model.ClientID]clientHistory) []model.ClientID {
	keys := make([]model.ClientID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}
func sortedSubjectKeys(values map[SubjectKey]subjectHistory) []SubjectKey {
	keys := make([]SubjectKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(appendSubject(nil, keys[i]), appendSubject(nil, keys[j])) < 0
	})
	return keys
}
func sortedJobIDs(values map[model.JobID]JobRecord) []model.JobID {
	keys := make([]model.JobID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}
func sortedWorkerEventKeys(values map[workerEventKey]workerEventCursor) []workerEventKey {
	keys := make([]workerEventKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].WorkerID != keys[j].WorkerID {
			return keys[i].WorkerID < keys[j].WorkerID
		}
		return bytes.Compare(keys[i].WorkerEpoch[:], keys[j].WorkerEpoch[:]) < 0
	})
	return keys
}
func sortedTaskKeysEOF(values map[model.TaskID]SourceEOFRecord) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(taskBytes(keys[i]), taskBytes(keys[j])) < 0 })
	return keys
}
func sortedTaskKeysCheckpoint(values map[model.TaskID]CheckpointRecord) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(taskBytes(keys[i]), taskBytes(keys[j])) < 0 })
	return keys
}
func sortedTaskKeysManifest(values map[model.TaskID]ResultManifest) []model.TaskID {
	keys := make([]model.TaskID, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(taskBytes(keys[i]), taskBytes(keys[j])) < 0 })
	return keys
}

type snapshotDecoder struct{ commandDecoder }

func decodeSnapshot(encoded []byte) (*Machine, error) {
	decoder := snapshotDecoder{commandDecoder{input: encoded}}
	magic, err := decoder.take(4)
	if err != nil || string(magic) != snapshotMagic {
		return nil, snapshotDecodeError("magic", err)
	}
	version, err := decoder.u16()
	if err != nil || version != uint16(SnapshotSchemaVersion) {
		return nil, snapshotDecodeError("schema", err)
	}
	fingerprint, err := decoder.array32()
	if err != nil || fingerprint != model.ConsensusFingerprint() {
		return nil, snapshotDecodeError("consensus fingerprint", err)
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
		return nil, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	return temporary, nil
}

func snapshotDecodeError(field string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: invalid %s", ErrInvalidSnapshot, field)
	}
	return fmt.Errorf("%w: %s: %v", ErrInvalidSnapshot, field, err)
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
	fixed, err := decoder.take(4)
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
		if id.Validate() != nil || index > 0 && bytes.Compare(previous[:], id[:]) >= 0 {
			return fmt.Errorf("%w: client keys are invalid or unsorted", ErrInvalidSnapshot)
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
	var previous []byte
	for index := uint64(0); index < count; index++ {
		subject, err := decoder.subject()
		if err != nil {
			return snapshotDecodeError("subject key", err)
		}
		keyBytes := appendSubject(nil, subject)
		if subject.Validate() != nil || index > 0 && bytes.Compare(previous, keyBytes) >= 0 {
			return fmt.Errorf("%w: subject keys are invalid or unsorted", ErrInvalidSnapshot)
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
		previous = keyBytes
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
		if key == 0 || index > 0 && key <= previous || worker.NodeID != key || worker.Validate() != nil {
			return fmt.Errorf("%w: invalid or unsorted worker entry", ErrInvalidSnapshot)
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
		if key.Validate() != nil || index > 0 && bytes.Compare(previous[:], key[:]) >= 0 {
			return fmt.Errorf("%w: invalid or unsorted job key", ErrInvalidSnapshot)
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
	var previous []byte
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
		key := taskBytes(task)
		if task.JobID != job || index > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, fmt.Errorf("%w: source EOF keys", ErrInvalidSnapshot)
		}
		result[task] = SourceEOFRecord{EOF: eof, Revision: revision}
		previous = key
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
	var previous []byte
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
		key := taskBytes(task)
		if task.JobID != job || index > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, fmt.Errorf("%w: checkpoint keys", ErrInvalidSnapshot)
		}
		result[task] = CheckpointRecord{Watermark: watermark, Revision: revision}
		previous = key
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
	var previous []byte
	for index := 0; index < int(count); index++ {
		task, err := decoder.taskID()
		if err != nil {
			return nil, err
		}
		manifest, err := decoder.manifest()
		if err != nil {
			return nil, err
		}
		key := taskBytes(task)
		if task.JobID != job || manifest.SinkTask != task || index > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, fmt.Errorf("%w: manifest keys", ErrInvalidSnapshot)
		}
		result[task] = manifest
		previous = key
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
		if worker == 0 || index > 0 && !workerEventLess(previous, key) {
			return fmt.Errorf("%w: worker event keys", ErrInvalidSnapshot)
		}
		machine.workerEvents[key] = workerEventCursor{TransactionID: transaction, Digest: digest}
		previous = key
	}
	return nil
}

func workerEventLess(left, right workerEventKey) bool {
	if left.WorkerID != right.WorkerID {
		return left.WorkerID < right.WorkerID
	}
	return bytes.Compare(left.WorkerEpoch[:], right.WorkerEpoch[:]) < 0
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
		if id.Validate() != nil || history.sequence == 0 || history.digest == ([32]byte{}) || uint64(len(history.result)) > model.StateCommandMaxCachedResultBytesV1 {
			return errors.New("invalid client history")
		}
	}
	for id, worker := range machine.workers {
		if id == 0 || worker.NodeID != id || worker.Validate() != nil {
			return errors.New("invalid worker entry")
		}
	}
	for _, subject := range sortedSubjectKeys(machine.subjects) {
		if err := validateSubjectHistory(machine, subject, machine.subjects[subject]); err != nil {
			return err
		}
	}
	usedSlots := make(map[uint16]uint64)
	for id, job := range machine.jobs {
		if err := validateSnapshotJob(machine, id, job, usedSlots); err != nil {
			return err
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
	if subject.Validate() != nil || history.id == (InternalCommandID{}) || history.digest == ([32]byte{}) || uint64(len(history.result)) > model.StateCommandMaxCachedResultBytesV1 || uint64(len(history.appliedResult)) > model.StateCommandMaxCachedResultBytesV1 || uint64(len(history.target)) > model.LimitsV1().MaxSubmitJobBytes || uint64(len(history.appliedTarget)) > model.LimitsV1().MaxSubmitJobBytes {
		return errors.New("invalid subject history")
	}
	if history.appliedRevision > history.revision || history.appliedRevision == 0 && (len(history.appliedTarget) != 0 || len(history.appliedResult) != 0) || history.appliedRevision != 0 && (len(history.appliedTarget) == 0 || len(history.appliedResult) == 0) || history.applied && history.appliedRevision != history.revision {
		return errors.New("invalid applied subject history")
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

func validateSnapshotJob(machine *Machine, key model.JobID, job JobRecord, usedSlots map[uint16]uint64) error {
	if job.JobID != key || key.Validate() != nil || job.DefiningRequest.Validate() != nil || job.JobControlRevision == 0 || job.Lifecycle < JobPending || job.Lifecycle > JobCanceled {
		return errors.New("invalid job identity, revision, or lifecycle")
	}
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil || topology.Digest() != job.TopologyDigest || model.DeriveJobID(job.DefiningRequest, job.TopologyDigest) != job.JobID {
		return errors.New("job topology definition mismatch")
	}
	if err := validateMarkers(job.NeedsReassignment, job.JobID); err != nil {
		return err
	}
	if job.Assignment != nil {
		if err := job.Assignment.Validate(topology); err != nil {
			return err
		}
	}
	if job.Lifecycle == JobPending && job.Assignment != nil || (job.Lifecycle == JobDeploying || job.Lifecycle == JobRunning || job.Lifecycle == JobDraining || job.Lifecycle == JobSucceeded) && job.Assignment == nil {
		return errors.New("job lifecycle assignment mismatch")
	}
	for source, eof := range job.SourceEOFs {
		want, sourceErr := model.SourceEOF(topology, source)
		if sourceErr != nil || want != eof.EOF || eof.Revision == 0 {
			return errors.New("source EOF cross-reference mismatch")
		}
	}
	for source, checkpoint := range job.Checkpoints {
		eof, ok := job.SourceEOFs[source]
		if !ok || checkpoint.Revision == 0 || checkpoint.Watermark > eof.EOF {
			return errors.New("checkpoint cross-reference mismatch")
		}
	}
	for sink, manifest := range job.Manifests {
		if manifest.Validate() != nil || manifest.JobID != job.JobID || manifest.SinkTask != sink || manifest.SpecificationHash != job.TopologyDigest || job.Assignment == nil {
			return errors.New("manifest cross-reference mismatch")
		}
		replica, ok := resultReplica(job.Assignment, sink)
		if !ok || replica != manifest.Replicas {
			return errors.New("manifest assignment mismatch")
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
	if job.Lifecycle == JobDeploying || job.Lifecycle == JobRunning || job.Lifecycle == JobDraining || job.Lifecycle == JobSucceeded {
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
