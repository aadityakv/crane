package state

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

// WorkerState is the replicated assignment-eligibility state.
type WorkerState uint8

const (
	WorkerEligible WorkerState = iota + 1
	WorkerDraining
	WorkerOffline
)

// WorkerRecord is the exact current durable identity and capacity of one node.
type WorkerRecord struct {
	NodeID               uint16
	Epoch                model.WorkerEpoch
	State                WorkerState
	Revision             uint64
	Slots                uint16
	ConsensusFingerprint [32]byte
	RegistryFingerprint  [32]byte
}

func (record WorkerRecord) Validate() error {
	if record.NodeID == 0 || record.Revision == 0 || record.Slots == 0 || uint64(record.Slots) > model.LimitsV1().MaxWorkerSlots {
		return errors.New("invalid worker identity, revision, or slots")
	}
	if err := record.Epoch.Validate(); err != nil {
		return err
	}
	if record.State < WorkerEligible || record.State > WorkerOffline {
		return errors.New("unknown worker state")
	}
	if record.ConsensusFingerprint != model.ConsensusFingerprint() || record.RegistryFingerprint != model.RegistryFingerprint() {
		return errors.New("worker compatibility fingerprint mismatch")
	}
	return nil
}

// AffectedAssignment binds a worker invalidation to every impacted complete set.
type AffectedAssignment struct {
	JobID              model.JobID
	JobControlRevision uint64
	AssignmentRevision uint64
	AssignmentDigest   [32]byte
}

func (affected AffectedAssignment) validate() error {
	if err := affected.JobID.Validate(); err != nil {
		return err
	}
	if affected.JobControlRevision == 0 || affected.AssignmentRevision == 0 || affected.AssignmentDigest == ([32]byte{}) {
		return errors.New("zero affected assignment fence")
	}
	return nil
}

func validateAffected(input []AffectedAssignment) error {
	if uint64(len(input)) > model.LimitsV1().MaxActiveJobs {
		return errors.New("affected assignment count exceeds active-job bound")
	}
	for index, affected := range input {
		if err := affected.validate(); err != nil {
			return fmt.Errorf("affected assignment %d: %w", index, err)
		}
		if index > 0 && bytes.Compare(input[index-1].JobID[:], affected.JobID[:]) >= 0 {
			return errors.New("affected assignments are not sorted and unique")
		}
	}
	return nil
}

type RegisterWorker struct {
	Envelope Envelope
	Worker   WorkerRecord
}

type DrainWorker struct {
	Envelope    Envelope
	WorkerID    uint16
	WorkerEpoch model.WorkerEpoch
}

type DeactivateWorker struct {
	Envelope    Envelope
	WorkerID    uint16
	WorkerEpoch model.WorkerEpoch
	Affected    []AffectedAssignment
}

type ReplaceWorkerEpoch struct {
	Envelope Envelope
	WorkerID uint16
	OldEpoch model.WorkerEpoch
	Target   WorkerRecord
	Affected []AffectedAssignment
}

func NewRegisterWorker(id InternalCommandID, expectedRevision uint64, target WorkerRecord) (RegisterWorker, error) {
	command := RegisterWorker{Worker: target}
	command.Envelope = newInternalEnvelope(CommandRegisterWorker, SubjectKey{Kind: SubjectWorker, WorkerID: target.NodeID}, id, expectedRevision)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, registerWorkerTarget(command))
	return command, command.Validate()
}

func NewDrainWorker(id InternalCommandID, expectedRevision uint64, workerID uint16, epoch model.WorkerEpoch) (DrainWorker, error) {
	command := DrainWorker{WorkerID: workerID, WorkerEpoch: epoch}
	command.Envelope = newInternalEnvelope(CommandDrainWorker, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, drainWorkerTarget(command))
	return command, command.Validate()
}

func NewDeactivateWorker(id InternalCommandID, expectedRevision uint64, workerID uint16, epoch model.WorkerEpoch, affected []AffectedAssignment) (DeactivateWorker, error) {
	command := DeactivateWorker{WorkerID: workerID, WorkerEpoch: epoch, Affected: cloneAffected(affected)}
	command.Envelope = newInternalEnvelope(CommandDeactivateWorker, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, deactivateWorkerTarget(command))
	return command, command.Validate()
}

func NewReplaceWorkerEpoch(id InternalCommandID, expectedRevision uint64, workerID uint16, oldEpoch model.WorkerEpoch, target WorkerRecord, affected []AffectedAssignment) (ReplaceWorkerEpoch, error) {
	command := ReplaceWorkerEpoch{WorkerID: workerID, OldEpoch: oldEpoch, Target: target, Affected: cloneAffected(affected)}
	command.Envelope = newInternalEnvelope(CommandReplaceWorkerEpoch, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, replaceWorkerEpochTarget(command))
	return command, command.Validate()
}

func newInternalEnvelope(kind CommandKind, subject SubjectKey, id InternalCommandID, expected uint64) Envelope {
	return Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: kind, Internal: &InternalEnvelope{ID: id, Subject: subject, ExpectedRevision: expected}}
}

func validateWorkerCommandEnvelope(envelope Envelope, kind CommandKind, workerID uint16) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	if envelope.Kind != kind || envelope.Internal == nil || envelope.Client != nil || workerID == 0 || envelope.Internal.Subject != (SubjectKey{Kind: SubjectWorker, WorkerID: workerID}) {
		return fmt.Errorf("%w: worker command envelope mismatch", ErrInvalidCommandSubject)
	}
	return nil
}

func (command RegisterWorker) Validate() error {
	if err := validateWorkerCommandEnvelope(command.Envelope, CommandRegisterWorker, command.Worker.NodeID); err != nil {
		return err
	}
	if err := command.Worker.Validate(); err != nil {
		return err
	}
	if command.Worker.Revision != command.Envelope.Internal.ExpectedRevision+1 || command.Worker.Revision == 0 {
		return errors.New("worker target revision is not expected successor")
	}
	if command.Worker.State != WorkerEligible {
		return errors.New("registration target must be Eligible")
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, registerWorkerTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command DrainWorker) Validate() error {
	if err := validateWorkerCommandEnvelope(command.Envelope, CommandDrainWorker, command.WorkerID); err != nil {
		return err
	}
	if err := command.WorkerEpoch.Validate(); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, drainWorkerTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command DeactivateWorker) Validate() error {
	if err := validateWorkerCommandEnvelope(command.Envelope, CommandDeactivateWorker, command.WorkerID); err != nil {
		return err
	}
	if err := command.WorkerEpoch.Validate(); err != nil {
		return err
	}
	if err := validateAffected(command.Affected); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, deactivateWorkerTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command ReplaceWorkerEpoch) Validate() error {
	if err := validateWorkerCommandEnvelope(command.Envelope, CommandReplaceWorkerEpoch, command.WorkerID); err != nil {
		return err
	}
	if err := command.OldEpoch.Validate(); err != nil {
		return err
	}
	if err := command.Target.Validate(); err != nil {
		return err
	}
	if command.Target.NodeID != command.WorkerID || command.Target.Epoch == command.OldEpoch || command.Target.Revision != command.Envelope.Internal.ExpectedRevision+1 || command.Target.State == WorkerDraining {
		return errors.New("invalid replacement worker target")
	}
	if err := validateAffected(command.Affected); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, replaceWorkerEpochTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func cloneAffected(input []AffectedAssignment) []AffectedAssignment {
	return append([]AffectedAssignment(nil), input...)
}

func canonicalAffected(input []AffectedAssignment) []AffectedAssignment {
	result := cloneAffected(input)
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].JobID[:], result[j].JobID[:]) < 0 })
	return result
}

const workerRecordEstimatedBytes int64 = 93

func (machine *Machine) applyRegisterWorkerLocked(command RegisterWorker) ([]byte, error) {
	target := registerWorkerTarget(command)
	return machine.applyInternalLocked(command.Envelope, target, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		current, exists := machine.workers[command.Worker.NodeID]
		if !exists {
			if uint64(len(machine.workers)) >= model.LimitsV1().MaxRegisteredWorkers {
				result, err := marshalBusinessResult(ResultCapacityExhausted, key, 0, model.CoordinatorEpoch{})
				return mutationPlan{result: result, reject: true, capacity: true}, err
			}
		} else {
			preserved := current.State == WorkerOffline && current.Epoch == command.Worker.Epoch && current.Slots == command.Worker.Slots && current.ConsensusFingerprint == command.Worker.ConsensusFingerprint && current.RegistryFingerprint == command.Worker.RegistryFingerprint
			if !preserved {
				result, err := marshalBusinessResult(ResultInvalidTransition, key, current.Revision, model.CoordinatorEpoch{})
				return mutationPlan{result: result, reject: true}, err
			}
		}
		if command.Worker.Revision != nextRevision {
			return mutationPlan{}, errors.New("impossible worker target revision divergence")
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		if err != nil {
			return mutationPlan{}, err
		}
		delta := int64(0)
		if !exists {
			delta = workerRecordEstimatedBytes
		}
		return mutationPlan{result: result, stateDelta: delta, commit: func() { machine.workers[command.Worker.NodeID] = command.Worker }}, nil
	})
}

func (machine *Machine) applyDrainWorkerLocked(command DrainWorker) ([]byte, error) {
	target := drainWorkerTarget(command)
	return machine.applyInternalLocked(command.Envelope, target, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		current, exists := machine.workers[command.WorkerID]
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if current.Epoch != command.WorkerEpoch || current.State != WorkerEligible {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		current.State, current.Revision = WorkerDraining, nextRevision
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, commit: func() { machine.workers[command.WorkerID] = current }}, err
	})
}

func (machine *Machine) applyDeactivateWorkerLocked(command DeactivateWorker) ([]byte, error) {
	target := deactivateWorkerTarget(command)
	return machine.applyInternalLocked(command.Envelope, target, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		current, exists := machine.workers[command.WorkerID]
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if current.Epoch != command.WorkerEpoch || current.State != WorkerEligible {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		jobs, delta, ok := machine.prepareWorkerInvalidation(command.WorkerID, command.WorkerEpoch, command.Affected)
		if !ok {
			result, err := marshalBusinessResult(ResultRevisionMismatch, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		current.State, current.Revision = WorkerOffline, nextRevision
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.workers[command.WorkerID] = current
			for job, record := range jobs {
				machine.jobs[job] = record
			}
		}}, err
	})
}

func (machine *Machine) applyReplaceWorkerEpochLocked(command ReplaceWorkerEpoch) ([]byte, error) {
	target := replaceWorkerEpochTarget(command)
	return machine.applyInternalLocked(command.Envelope, target, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		current, exists := machine.workers[command.WorkerID]
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if current.Epoch != command.OldEpoch || command.Target.Revision != nextRevision {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		jobs, delta, ok := machine.prepareWorkerInvalidation(command.WorkerID, command.OldEpoch, command.Affected)
		if !ok {
			result, err := marshalBusinessResult(ResultRevisionMismatch, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		oldCursor := workerEventKey{WorkerID: command.WorkerID, WorkerEpoch: command.OldEpoch}
		if _, exists := machine.workerEvents[oldCursor]; exists {
			delta -= 58
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.workers[command.WorkerID] = command.Target
			delete(machine.workerEvents, oldCursor)
			for job, record := range jobs {
				machine.jobs[job] = record
			}
		}}, err
	})
}

func (machine *Machine) prepareWorkerInvalidation(workerID uint16, epoch model.WorkerEpoch, presented []AffectedAssignment) (map[model.JobID]JobRecord, int64, bool) {
	jobs := make(map[model.JobID]JobRecord)
	var actual []AffectedAssignment
	var delta int64
	for jobID, record := range machine.jobs {
		if record.Assignment == nil || record.Lifecycle.terminal() {
			continue
		}
		markers := markersForWorker(*record.Assignment, workerID, epoch)
		if len(markers) == 0 {
			continue
		}
		actual = append(actual, AffectedAssignment{JobID: jobID, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision, AssignmentDigest: record.Assignment.Digest})
		candidate := cloneJobRecord(record)
		before := len(candidate.NeedsReassignment)
		candidate.NeedsReassignment = sortedMarkerUnion(candidate.NeedsReassignment, markers)
		if err := validateMarkers(candidate.NeedsReassignment, jobID); err != nil || candidate.JobControlRevision == ^uint64(0) {
			return nil, 0, false
		}
		candidate.JobControlRevision++
		delta += int64(len(candidate.NeedsReassignment)-before) * 60
		jobs[jobID] = candidate
	}
	sort.Slice(actual, func(i, j int) bool { return bytes.Compare(actual[i].JobID[:], actual[j].JobID[:]) < 0 })
	if !reflect.DeepEqual(actual, presented) {
		return nil, 0, false
	}
	return jobs, delta, true
}

func markersForWorker(set model.AssignmentSet, workerID uint16, epoch model.WorkerEpoch) []NeedsReassignment {
	markers := make([]NeedsReassignment, 0)
	for _, token := range set.Tasks {
		if token.WorkerID == workerID && token.WorkerEpoch == epoch {
			markers = append(markers, NeedsReassignment{Kind: TaskTarget, Task: token.Task, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
	}
	for _, replica := range set.ResultReplicas {
		if replica.PrimaryNodeID == workerID && replica.PrimaryEpoch == epoch {
			markers = append(markers, NeedsReassignment{Kind: ResultReplicaTarget, SinkTask: replica.SinkTask, ReplicaRole: model.PrimaryReplica, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
		if replica.SecondaryNodeID == workerID && replica.SecondaryEpoch == epoch {
			markers = append(markers, NeedsReassignment{Kind: ResultReplicaTarget, SinkTask: replica.SinkTask, ReplicaRole: model.SecondaryReplica, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
	}
	sort.Slice(markers, func(i, j int) bool { return markerLess(markers[i], markers[j]) })
	return markers
}
