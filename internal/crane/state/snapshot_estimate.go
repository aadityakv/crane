package state

import "crane/internal/crane/model"

const (
	workerRecordEstimatedBytes        int64  = int64(model.StateCommandWorkerRecordBytesV1)
	jobRecordFixedEstimatedBytes      int64  = int64(model.StateCommandJobRecordFixedBytesV1)
	assignmentTokenEstimatedBytes     uint64 = model.StateCommandAssignmentTokenBytesV1
	resultReplicaEstimatedBytes       uint64 = model.StateCommandResultReplicaBytesV1
	reassignmentMarkerEstimatedBytes  uint64 = model.StateCommandReassignmentBytesV1
	invalidationProvenanceFixedBytes  uint64 = model.StateCommandInvalidationProvenanceFixedBytesV1
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
	provenanceBytes, ok := invalidationProvenanceEstimatedBytes(job.invalidationHistory)
	if !ok {
		return 0, false
	}
	return checkedAddMany(
		total,
		uint64(len(job.NeedsReassignment))*reassignmentMarkerEstimatedBytes,
		provenanceBytes,
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

func invalidationProvenanceEstimatedBytes(history []invalidationProvenance) (uint64, bool) {
	var total uint64
	for _, provenance := range history {
		entry, ok := checkedAddMany(invalidationProvenanceFixedBytes, uint64(len(provenance.Markers))*reassignmentMarkerEstimatedBytes)
		if !ok {
			return 0, false
		}
		total, ok = checkedSnapshotAdd(total, entry)
		if !ok {
			return 0, false
		}
	}
	return total, true
}

func assignmentEncodedBytes(set model.AssignmentSet) uint64 {
	return model.StateCommandAssignmentFixedBytesV1 + uint64(len(set.Tasks))*assignmentTokenEstimatedBytes + uint64(len(set.ResultReplicas))*resultReplicaEstimatedBytes
}

// SnapshotEstimatorConstants returns every production-used fixed accounting value.
func SnapshotEstimatorConstants() []model.StateCommandConstantDescriptor {
	return []model.StateCommandConstantDescriptor{
		{Name: "SnapshotBaseFixed", Value: model.StateCommandSnapshotBaseBytesV1},
		{Name: "ClientHistoryFixed", Value: model.StateCommandClientHistoryFixedV1},
		{Name: "SubjectHistoryFixed", Value: model.StateCommandSubjectHistoryFixedV1},
		{Name: "WorkerEntryFixed", Value: model.StateCommandWorkerRecordBytesV1},
		{Name: "JobRecordFixed", Value: model.StateCommandJobRecordFixedBytesV1},
		{Name: "AssignmentFixed", Value: model.StateCommandAssignmentFixedBytesV1},
		{Name: "AssignmentTokenFixed", Value: model.StateCommandAssignmentTokenBytesV1},
		{Name: "ResultReplicaFixed", Value: model.StateCommandResultReplicaBytesV1},
		{Name: "ReassignmentMarkerFixed", Value: model.StateCommandReassignmentBytesV1},
		{Name: "InvalidationProvenanceFixed", Value: model.StateCommandInvalidationProvenanceFixedBytesV1},
		{Name: "InvalidationProvenanceMaxCount", Value: model.StateCommandMaxInvalidationProvenanceV1},
		{Name: "SourceEOFEntryFixed", Value: model.StateCommandSourceEOFEntryBytesV1},
		{Name: "CheckpointEntryFixed", Value: model.StateCommandCheckpointEntryBytesV1},
		{Name: "ManifestEntryFixed", Value: model.StateCommandManifestEntryBytesV1},
		{Name: "FailurePresentFixed", Value: model.StateCommandFailureBytesV1},
		{Name: "WorkerEventEntryFixed", Value: model.StateCommandWorkerEventBytesV1},
		{Name: "Bytes32LengthPrefix", Value: model.StateCommandSnapshotBytes32PrefixV2},
		{Name: "Bytes64LengthPrefix", Value: model.StateCommandSnapshotBytes64PrefixV2},
		{Name: "RootCollectionCount", Value: model.StateCommandSnapshotRootCountBytesV2},
		{Name: "NestedCollectionCount", Value: model.StateCommandSnapshotNestedCountBytesV2},
		{Name: "OptionalSelector", Value: model.StateCommandSnapshotOptionalSelectorBytesV2},
	}
}
