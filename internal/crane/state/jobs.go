package state

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

// JobLifecycle is the complete replicated finite-job lifecycle.
type JobLifecycle uint8

const (
	JobPending   JobLifecycle = iota + 1 // JobPending awaits a complete assignment set.
	JobDeploying                         // JobDeploying has assignments awaiting activation.
	JobRunning                           // JobRunning accepts worker progress.
	JobDraining                          // JobDraining has observed all source EOFs.
	JobSucceeded                         // JobSucceeded is terminal successful retention.
	JobFailed                            // JobFailed is terminal failure retention.
	JobCanceled                          // JobCanceled is terminal client cancellation.
)

func (state JobLifecycle) terminal() bool {
	return state == JobSucceeded || state == JobFailed || state == JobCanceled
}

// JobRecord is one retained immutable definition and its replicated control fence.
type JobRecord struct {
	JobID              model.JobID                       // JobID is derived from request and topology.
	DefiningRequest    model.ClientRequestID             // DefiningRequest defends ID collisions.
	TopologyDigest     [32]byte                          // TopologyDigest binds the immutable plan.
	TopologyBytes      []byte                            // TopologyBytes owns the canonical plan bytes.
	Lifecycle          JobLifecycle                      // Lifecycle is the finite control state.
	JobControlRevision uint64                            // JobControlRevision fences job-wide changes.
	Assignment         *model.AssignmentSet              // Assignment is the complete current set.
	NeedsReassignment  []NeedsReassignment               // NeedsReassignment is sorted and complete.
	SourceEOFs         map[model.TaskID]SourceEOFRecord  // SourceEOFs retain immutable source bounds.
	Checkpoints        map[model.TaskID]CheckpointRecord // Checkpoints advance independently by source.
	Manifests          map[model.TaskID]ResultManifest   // Manifests seal independently by sink.
	Failure            *model.JobFailureReport           // Failure retains the terminal failure event.
}

// SubmitJob creates an immutable job definition under a client identity.
type SubmitJob struct {
	Envelope Envelope           // Envelope carries client identity and coordinator fence.
	Topology model.TopologySpec // Topology is the validated immutable logical plan.
}

// CancelJob conditionally moves a nonterminal job to Canceled.
type CancelJob struct {
	Envelope         Envelope    // Envelope carries client identity and coordinator fence.
	Job              model.JobID // Job selects the retained job.
	ExpectedRevision uint64      // ExpectedRevision fences job control.
}

func NewSubmitJob(request model.ClientRequestID, topology model.TopologySpec, fences ...model.CoordinatorEpoch) (SubmitJob, error) {
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		return SubmitJob{}, err
	}
	command := SubmitJob{Topology: validated.Spec()}
	var fence model.CoordinatorEpoch
	if len(fences) == 1 {
		fence = fences[0]
	}
	command.Envelope = Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: CommandSubmitJob, CoordinatorEpoch: fence, Client: &ClientEnvelope{Request: request}}
	command.Envelope.Client.Digest = model.PublicSubmitCommandDigest(request, validated.CanonicalBytes())
	return command, command.Validate()
}

func (command SubmitJob) JobID() model.JobID {
	validated, err := model.ValidateTopology(command.Topology)
	if err != nil || command.Envelope.Client == nil {
		return model.JobID{}
	}
	return model.DeriveJobID(command.Envelope.Client.Request, validated.Digest())
}

func NewCancelJob(request model.ClientRequestID, job model.JobID, expectedRevision uint64, fences ...model.CoordinatorEpoch) (CancelJob, error) {
	command := CancelJob{Job: job, ExpectedRevision: expectedRevision}
	var fence model.CoordinatorEpoch
	if len(fences) == 1 {
		fence = fences[0]
	}
	command.Envelope = Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: CommandCancelJob, CoordinatorEpoch: fence, Client: &ClientEnvelope{Request: request}}
	command.Envelope.Client.Digest = model.PublicCancelCommandDigest(request, job, expectedRevision)
	return command, command.Validate()
}

func validateClientCommandEnvelope(envelope Envelope, kind CommandKind) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	if envelope.Kind != kind || envelope.Client == nil || envelope.Internal != nil {
		return fmt.Errorf("%w: client command envelope mismatch", ErrInvalidCommandSubject)
	}
	return nil
}

func (command SubmitJob) Validate() error {
	if err := validateClientCommandEnvelope(command.Envelope, CommandSubmitJob); err != nil {
		return err
	}
	validated, err := model.ValidateTopology(command.Topology)
	if err != nil {
		return err
	}
	canonical := validated.CanonicalBytes()
	if _, err := model.CompleteSubmitJobBytes(uint64(len(canonical))); err != nil {
		return err
	}
	if command.Envelope.Client.Digest != model.PublicSubmitCommandDigest(command.Envelope.Client.Request, canonical) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command CancelJob) Validate() error {
	if err := validateClientCommandEnvelope(command.Envelope, CommandCancelJob); err != nil {
		return err
	}
	if err := command.Job.Validate(); err != nil {
		return err
	}
	if command.ExpectedRevision == 0 || command.ExpectedRevision == ^uint64(0) {
		return errors.New("invalid expected job revision")
	}
	if command.Envelope.Client.Digest != model.PublicCancelCommandDigest(command.Envelope.Client.Request, command.Job, command.ExpectedRevision) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func sameJobDefinition(record JobRecord, request model.ClientRequestID, digest [32]byte, canonical []byte) bool {
	return record.DefiningRequest == request && record.TopologyDigest == digest && bytes.Equal(record.TopologyBytes, canonical)
}

func (machine *Machine) applySubmitJobLocked(command SubmitJob) ([]byte, error) {
	validated, err := model.ValidateTopology(command.Topology)
	if err != nil {
		return nil, err
	}
	canonical := validated.CanonicalBytes()
	jobID := command.JobID()
	request := command.Envelope.Client.Request
	key := SubjectKey{Kind: SubjectJobControl, JobID: jobID}
	return machine.applyClientCommandLocked(command.Envelope, key, func() (mutationPlan, error) {
		if existing, ok := machine.jobs[jobID]; ok {
			if !sameJobDefinition(existing, request, validated.Digest(), canonical) {
				result, resultErr := marshalBusinessResult(ResultIdentityCollision, key, existing.JobControlRevision, model.CoordinatorEpoch{})
				return mutationPlan{result: result, reject: true}, resultErr
			}
			result, resultErr := marshalBusinessResult(ResultSuccess, key, existing.JobControlRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result}, resultErr
		}
		if uint64(len(machine.jobs)) >= model.LimitsV1().MaxRetainedJobs || machine.activeJobCount() >= model.LimitsV1().MaxActiveJobs {
			result, resultErr := marshalBusinessResult(ResultCapacityExhausted, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true, capacity: true}, resultErr
		}
		record := JobRecord{JobID: jobID, DefiningRequest: request, TopologyDigest: validated.Digest(), TopologyBytes: append([]byte(nil), canonical...), Lifecycle: JobPending, JobControlRevision: 1}
		result, resultErr := marshalBusinessResult(ResultSuccess, key, 1, model.CoordinatorEpoch{})
		if resultErr != nil {
			return mutationPlan{}, resultErr
		}
		delta := jobRecordFixedEstimatedBytes + int64(len(canonical))
		return mutationPlan{result: result, stateDelta: delta, commit: func() { machine.jobs[jobID] = record }}, nil
	})
}

func (machine *Machine) applyCancelJobLocked(command CancelJob) ([]byte, error) {
	key := SubjectKey{Kind: SubjectJobControl, JobID: command.Job}
	return machine.applyClientCommandLocked(command.Envelope, key, func() (mutationPlan, error) {
		record, exists := machine.jobs[command.Job]
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if record.Lifecycle == JobCanceled && record.JobControlRevision == command.ExpectedRevision+1 {
			result, err := marshalBusinessResult(ResultSuccess, key, record.JobControlRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result}, err
		}
		if record.JobControlRevision != command.ExpectedRevision || record.Lifecycle.terminal() || record.JobControlRevision == ^uint64(0) {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, record.JobControlRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		record.Lifecycle = JobCanceled
		record.JobControlRevision++
		result, err := marshalBusinessResult(ResultSuccess, key, record.JobControlRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, commit: func() { machine.jobs[command.Job] = record }}, err
	})
}

func (machine *Machine) activeJobCount() uint64 {
	var count uint64
	for _, job := range machine.jobs {
		if !job.Lifecycle.terminal() {
			count++
		}
	}
	return count
}

func cloneJobRecord(record JobRecord) JobRecord {
	clone := record
	clone.TopologyBytes = append([]byte(nil), record.TopologyBytes...)
	clone.NeedsReassignment = append([]NeedsReassignment(nil), record.NeedsReassignment...)
	if record.Assignment != nil {
		assignment := cloneAssignment(*record.Assignment)
		clone.Assignment = &assignment
	}
	clone.SourceEOFs = make(map[model.TaskID]SourceEOFRecord, len(record.SourceEOFs))
	for key, value := range record.SourceEOFs {
		clone.SourceEOFs[key] = value
	}
	clone.Checkpoints = make(map[model.TaskID]CheckpointRecord, len(record.Checkpoints))
	for key, value := range record.Checkpoints {
		clone.Checkpoints[key] = value
	}
	clone.Manifests = make(map[model.TaskID]ResultManifest, len(record.Manifests))
	for key, value := range record.Manifests {
		clone.Manifests[key] = value
	}
	if record.Failure != nil {
		failure := *record.Failure
		clone.Failure = &failure
	}
	return clone
}
