package state

import (
	"errors"
	"fmt"
	"math"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

// ResultManifest seals one sink artifact and its two current durable replicas.
type ResultManifest struct {
	JobID             model.JobID            // JobID identifies the producing job.
	SinkTask          model.TaskID           // SinkTask identifies the collecting task.
	ManifestRevision  uint64                 // ManifestRevision is the independent sink revision.
	SpecificationHash [32]byte               // SpecificationHash binds the immutable topology.
	RecordCount       uint64                 // RecordCount is bounded by canonical record sizes.
	TotalBytes        uint64                 // TotalBytes is the complete artifact byte count.
	Checksum          [32]byte               // Checksum authenticates the artifact stream.
	Replicas          model.ResultReplicaSet // Replicas are the exact two current copies.
}

func (manifest ResultManifest) Validate() error {
	if err := manifest.JobID.Validate(); err != nil {
		return err
	}
	if err := manifest.SinkTask.Validate(); err != nil || manifest.SinkTask.JobID != manifest.JobID || manifest.ManifestRevision == 0 || manifest.SpecificationHash == ([32]byte{}) || manifest.TotalBytes > model.LimitsV1().MaxResultRecordsBytesPerJob || manifest.Checksum == ([32]byte{}) {
		return errors.New("invalid result manifest identity or bounds")
	}
	if err := manifest.Replicas.Validate(); err != nil || manifest.Replicas.SinkTask != manifest.SinkTask {
		return errors.New("manifest replica set mismatch")
	}
	if manifest.RecordCount == 0 {
		if manifest.TotalBytes != 0 {
			return errors.New("empty result manifest has nonzero artifact bytes")
		}
		return nil
	}
	if manifest.RecordCount > model.ResultArtifactMaxRecordCountV1 || manifest.TotalBytes == 0 {
		return errors.New("result manifest record count outside artifact bounds")
	}
	if manifest.RecordCount > math.MaxUint64/model.ResultArtifactMinRecordBytesV1 || manifest.RecordCount*model.ResultArtifactMinRecordBytesV1 > manifest.TotalBytes {
		return errors.New("result manifest bytes cannot contain the declared records")
	}
	if manifest.RecordCount <= math.MaxUint64/model.ResultArtifactMaxRecordBytesV1 && manifest.TotalBytes > manifest.RecordCount*model.ResultArtifactMaxRecordBytesV1 {
		return errors.New("result manifest bytes exceed the declared records")
	}
	return nil
}

// SealManifest commits one complete current result artifact description.
type SealManifest struct {
	Envelope Envelope       // Envelope carries manifest and coordinator fences.
	Manifest ResultManifest // Manifest is the complete successor record.
}

// TransitionJob conditionally applies one legal nonterminal lifecycle edge.
type TransitionJob struct {
	Envelope Envelope     // Envelope carries job-control and coordinator fences.
	JobID    model.JobID  // JobID selects the retained job.
	From     JobLifecycle // From is the required current lifecycle.
	To       JobLifecycle // To is the legal successor lifecycle.
}

// FailJob commits one current-token failure and terminal Failed state atomically.
type FailJob struct {
	Envelope Envelope               // Envelope carries job-control and coordinator fences.
	Report   model.JobFailureReport // Report binds the worker cursor and current token.
}

func NewSealManifest(id InternalCommandID, expectedRevision uint64, manifest ResultManifest, fence ...model.CoordinatorEpoch) (SealManifest, error) {
	command := SealManifest{Manifest: manifest}
	command.Envelope = newInternalEnvelope(CommandSealManifest, SubjectKey{Kind: SubjectResultManifest, JobID: manifest.JobID, TaskID: manifest.SinkTask}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, sealManifestTarget(command))
	return command, command.Validate()
}

func NewTransitionJob(id InternalCommandID, expectedRevision uint64, job model.JobID, from, to JobLifecycle, fence ...model.CoordinatorEpoch) (TransitionJob, error) {
	command := TransitionJob{JobID: job, From: from, To: to}
	command.Envelope = newInternalEnvelope(CommandTransitionJob, SubjectKey{Kind: SubjectJobControl, JobID: job}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, transitionJobTarget(command))
	return command, command.Validate()
}

func NewFailJob(id InternalCommandID, expectedRevision uint64, report model.JobFailureReport, fence ...model.CoordinatorEpoch) (FailJob, error) {
	command := FailJob{Report: report}
	command.Envelope = newInternalEnvelope(CommandFailJob, SubjectKey{Kind: SubjectJobControl, JobID: report.JobID}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, failJobTarget(command))
	return command, command.Validate()
}

func (command SealManifest) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	key := SubjectKey{Kind: SubjectResultManifest, JobID: command.Manifest.JobID, TaskID: command.Manifest.SinkTask}
	if command.Envelope.Kind != CommandSealManifest || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != key || command.Manifest.ManifestRevision != command.Envelope.Internal.ExpectedRevision+1 {
		return fmt.Errorf("%w: manifest subject/revision mismatch", ErrInvalidCommandSubject)
	}
	if err := command.Manifest.Validate(); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, sealManifestTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command TransitionJob) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	if command.Envelope.Kind != CommandTransitionJob || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != (SubjectKey{Kind: SubjectJobControl, JobID: command.JobID}) || command.JobID.Validate() != nil || command.From < JobPending || command.From > JobCanceled || command.To < JobPending || command.To > JobCanceled || command.From == command.To {
		return fmt.Errorf("%w: invalid lifecycle transition command", ErrInvalidCommandSubject)
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, transitionJobTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command FailJob) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	if command.Envelope.Kind != CommandFailJob || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != (SubjectKey{Kind: SubjectJobControl, JobID: command.Report.JobID}) {
		return fmt.Errorf("%w: fail-job subject mismatch", ErrInvalidCommandSubject)
	}
	if err := command.Report.Validate(); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, failJobTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (machine *Machine) applySealManifestLocked(command SealManifest) ([]byte, error) {
	manifest := command.Manifest
	record, exists := machine.jobs[manifest.JobID]
	currentRevision := uint64(0)
	manifestExists := false
	if exists {
		currentManifest, ok := record.Manifests[manifest.SinkTask]
		currentRevision, manifestExists = currentManifest.ManifestRevision, ok
	}
	key := command.Envelope.Internal.Subject
	target := sealManifestTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		currentReplica, replicaExists := resultReplica(record.Assignment, manifest.SinkTask)
		valid := record.Lifecycle == JobDraining && record.Assignment != nil && len(record.NeedsReassignment) == 0 && allCheckpointsFinal(record) && manifest.ManifestRevision == nextRevision && manifest.SpecificationHash == record.TopologyDigest && replicaExists && currentReplica == manifest.Replicas && machine.replicaWorkersCurrent(currentReplica) && manifestTotalWithinLimit(record.Manifests, manifest)
		if !valid {
			result, err := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		candidate := cloneJobRecord(record)
		candidate.Manifests[manifest.SinkTask] = manifest
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		delta := int64(0)
		if !manifestExists {
			delta = int64(resultManifestEntryEstimatedBytes)
		}
		return mutationPlan{result: result, stateDelta: delta, commit: func() { machine.jobs[candidate.JobID] = candidate }}, err
	})
}

func (machine *Machine) applyTransitionJobLocked(command TransitionJob) ([]byte, error) {
	record, exists := machine.jobs[command.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.JobControlRevision
	}
	key := command.Envelope.Internal.Subject
	target := transitionJobTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		valid := record.Lifecycle == command.From && record.Assignment != nil && len(record.NeedsReassignment) == 0
		switch {
		case command.From == JobDeploying && command.To == JobRunning:
			valid = valid && machine.allSourceEOFsPresent(record)
		case command.From == JobRunning && command.To == JobDraining:
			valid = valid && allCheckpointsFinal(record)
		case command.From == JobDraining && command.To == JobSucceeded:
			valid = valid && allCheckpointsFinal(record) && allManifestsCurrent(record) && record.Failure == nil
		default:
			valid = false
		}
		if !valid {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		candidate := cloneJobRecord(record)
		candidate.Lifecycle = command.To
		candidate.JobControlRevision = nextRevision
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, commit: func() { machine.jobs[candidate.JobID] = candidate }}, err
	})
}

func (machine *Machine) applyFailJobLocked(command FailJob) ([]byte, error) {
	report := command.Report
	record, exists := machine.jobs[report.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.JobControlRevision
	}
	key := command.Envelope.Internal.Subject
	target := failJobTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		currentToken, tokenExists := assignmentToken(record.Assignment, report.Task.Task)
		worker, workerExists := machine.workers[report.Task.WorkerID]
		valid := (record.Lifecycle == JobDeploying || record.Lifecycle == JobRunning || record.Lifecycle == JobDraining) && record.Assignment != nil && len(record.NeedsReassignment) == 0 && report.JobControlRevision == record.JobControlRevision && report.AssignmentRevision == record.Assignment.Revision && report.Epoch == machine.coordinatorEpoch && tokenExists && currentToken == report.Task && workerExists && worker.Epoch == report.Task.WorkerEpoch && worker.State != WorkerOffline
		if !valid {
			result, err := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		digest := failureEventDigest(report)
		cursorKey := workerEventKey{WorkerID: report.Task.WorkerID, WorkerEpoch: report.Task.WorkerEpoch}
		if err := validateNextWorkerEvent(machine.workerEvents, cursorKey.WorkerID, cursorKey.WorkerEpoch, report.TransactionID, digest); err != nil {
			result, resultErr := marshalBusinessResult(ResultStaleWorkerEvent, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		candidate := cloneJobRecord(record)
		failure := report
		candidate.Failure = &failure
		candidate.Lifecycle = JobFailed
		candidate.JobControlRevision = nextRevision
		_, cursorExists := machine.workerEvents[cursorKey]
		delta := int64(jobFailureEstimatedBytes)
		if !cursorExists {
			delta += int64(workerEventEntryEstimatedBytes)
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.jobs[candidate.JobID] = candidate
			machine.workerEvents[cursorKey] = workerEventCursor{TransactionID: report.TransactionID, Digest: digest}
		}}, err
	})
}

func manifestTotalWithinLimit(current map[model.TaskID]ResultManifest, target ResultManifest) bool {
	total := target.TotalBytes
	limit := model.LimitsV1().MaxResultRecordsBytesPerJob
	if total > limit {
		return false
	}
	for task, manifest := range current {
		if task == target.SinkTask {
			continue
		}
		if manifest.TotalBytes > limit-total {
			return false
		}
		total += manifest.TotalBytes
	}
	return true
}

func resultReplica(set *model.AssignmentSet, sink model.TaskID) (model.ResultReplicaSet, bool) {
	if set == nil {
		return model.ResultReplicaSet{}, false
	}
	for _, replica := range set.ResultReplicas {
		if replica.SinkTask == sink {
			return replica, true
		}
	}
	return model.ResultReplicaSet{}, false
}

func (machine *Machine) replicaWorkersCurrent(replica model.ResultReplicaSet) bool {
	primary, primaryOK := machine.workers[replica.PrimaryNodeID]
	secondary, secondaryOK := machine.workers[replica.SecondaryNodeID]
	return primaryOK && secondaryOK && primary.State != WorkerOffline && secondary.State != WorkerOffline && primary.Epoch == replica.PrimaryEpoch && secondary.Epoch == replica.SecondaryEpoch
}

func allCheckpointsFinal(record JobRecord) bool {
	if len(record.SourceEOFs) == 0 || len(record.Checkpoints) != len(record.SourceEOFs) {
		return false
	}
	for source, eof := range record.SourceEOFs {
		checkpoint, ok := record.Checkpoints[source]
		if !ok || checkpoint.Revision == 0 || checkpoint.Watermark != eof.EOF {
			return false
		}
	}
	return true
}

func allManifestsCurrent(record JobRecord) bool {
	if record.Assignment == nil || len(record.Manifests) != len(record.Assignment.ResultReplicas) {
		return false
	}
	for _, replica := range record.Assignment.ResultReplicas {
		manifest, ok := record.Manifests[replica.SinkTask]
		if !ok || manifest.SpecificationHash != record.TopologyDigest || manifest.Replicas != replica || !machineReplicaCurrent(record, manifest) {
			return false
		}
	}
	return true
}

func machineReplicaCurrent(record JobRecord, manifest ResultManifest) bool {
	current, ok := resultReplica(record.Assignment, manifest.SinkTask)
	return ok && current == manifest.Replicas
}
