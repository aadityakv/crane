package state

import "github.com/aaditya/cs425mp3/internal/crane/model"

const (
	workerRecordEstimatedBytes        int64  = int64(model.StateCommandWorkerRecordBytesV1)
	jobRecordFixedEstimatedBytes      int64  = int64(model.StateCommandJobRecordFixedBytesV1)
	assignmentTokenEstimatedBytes     uint64 = model.StateCommandAssignmentTokenBytesV1
	resultReplicaEstimatedBytes       uint64 = model.StateCommandResultReplicaBytesV1
	reassignmentMarkerEstimatedBytes  uint64 = model.StateCommandReassignmentBytesV1
	sourceEOFEntryEstimatedBytes      uint64 = model.StateCommandSourceEOFEntryBytesV1
	checkpointEntryEstimatedBytes     uint64 = model.StateCommandCheckpointEntryBytesV1
	resultManifestEntryEstimatedBytes uint64 = model.StateCommandManifestEntryBytesV1
	jobFailureEstimatedBytes          uint64 = model.StateCommandFailureBytesV1
	workerEventEntryEstimatedBytes    uint64 = model.StateCommandWorkerEventBytesV1
)

// estimateCanonicalSnapshotBytesLocked recomputes the exact v1 accounting shape
// that Task 11's canonical encoder must use. Collection-count and optional-value
// selectors are included in the root and per-job fixed portions respectively.
func (machine *Machine) estimateCanonicalSnapshotBytesLocked() (uint64, bool) {
	total := uint64(estimatedSnapshotBaseBytes)
	add := func(value uint64) bool {
		var ok bool
		total, ok = checkedSnapshotAdd(total, value)
		return ok
	}
	for _, history := range machine.clients {
		value, ok := clientHistoryEstimatedBytes(history)
		if !ok || !add(value) {
			return 0, false
		}
	}
	for _, history := range machine.subjects {
		value, ok := subjectHistoryEstimatedBytes(history)
		if !ok || !add(value) {
			return 0, false
		}
	}
	for range machine.workers {
		if !add(uint64(workerRecordEstimatedBytes)) {
			return 0, false
		}
	}
	for _, job := range machine.jobs {
		value, ok := estimateJobRecordBytes(job)
		if !ok || !add(value) {
			return 0, false
		}
	}
	if !add(uint64(len(machine.workerEvents)) * workerEventEntryEstimatedBytes) {
		return 0, false
	}
	return total, true
}

func estimateJobRecordBytes(job JobRecord) (uint64, bool) {
	total, ok := checkedAddMany(uint64(jobRecordFixedEstimatedBytes), uint64(len(job.TopologyBytes)))
	if !ok {
		return 0, false
	}
	if job.Assignment != nil {
		total, ok = checkedAddMany(total, assignmentEncodedBytes(*job.Assignment))
		if !ok {
			return 0, false
		}
	}
	return checkedAddMany(
		total,
		uint64(len(job.NeedsReassignment))*reassignmentMarkerEstimatedBytes,
		uint64(len(job.SourceEOFs))*sourceEOFEntryEstimatedBytes,
		uint64(len(job.Checkpoints))*checkpointEntryEstimatedBytes,
		uint64(len(job.Manifests))*resultManifestEntryEstimatedBytes,
		func() uint64 {
			if job.Failure != nil {
				return jobFailureEstimatedBytes
			}
			return 0
		}(),
	)
}

func assignmentEncodedBytes(set model.AssignmentSet) uint64 {
	return 16 + 8 + 32 + 2 + uint64(len(set.Tasks))*assignmentTokenEstimatedBytes + 2 + uint64(len(set.ResultReplicas))*resultReplicaEstimatedBytes
}
