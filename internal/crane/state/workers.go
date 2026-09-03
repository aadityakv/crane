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
	WorkerEligible WorkerState = iota + 1 // WorkerEligible permits new task placement.
	WorkerDraining                        // WorkerDraining preserves work but forbids new placement.
	WorkerOffline                         // WorkerOffline marks the incarnation unavailable.
)

// WorkerRecord is the exact current durable identity and capacity of one node.
type WorkerRecord struct {
	NodeID               uint16            // NodeID is the stable cluster member ID.
	Epoch                model.WorkerEpoch // Epoch distinguishes worker process incarnations.
	State                WorkerState       // State records operator-selected availability.
	Revision             uint64            // Revision is this worker subject's successor revision.
	Slots                uint16            // Slots bounds concurrent task tokens cluster-wide.
	ConsensusFingerprint [32]byte          // ConsensusFingerprint proves state compatibility.
	RegistryFingerprint  [32]byte          // RegistryFingerprint proves protocol compatibility.
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
	JobID              model.JobID // JobID identifies an affected complete assignment set.
	JobControlRevision uint64      // JobControlRevision fences the atomic marker mutation.
	AssignmentRevision uint64      // AssignmentRevision fences the current set revision.
	AssignmentDigest   [32]byte    // AssignmentDigest fences the exact current set bytes.
}

type workerInvalidationKind uint8

const (
	workerInvalidationDeactivate workerInvalidationKind = iota + 1
	workerInvalidationReplaceEpoch
)

type invalidationRepairState uint8

const (
	invalidationRepairActive invalidationRepairState = iota + 1
	invalidationRepairAnchored
	invalidationRepairForgotten
)

type invalidationProvenance struct {
	Kind                     workerInvalidationKind
	WorkerID                 uint16
	WorkerEpoch              model.WorkerEpoch
	WorkerRevision           uint64
	JobControlRevision       uint64
	AssignmentRevision       uint64
	AssignmentDigest         [32]byte
	Markers                  []NeedsReassignment
	RepairState              invalidationRepairState
	RepairJobControlRevision uint64
	RepairAssignmentRevision uint64
	RepairAssignmentDigest   [32]byte
	RepairMarkersDigest      [32]byte
}

func cloneInvalidationProvenance(input []invalidationProvenance) []invalidationProvenance {
	result := make([]invalidationProvenance, len(input))
	for index, provenance := range input {
		result[index] = provenance
		result[index].Markers = append([]NeedsReassignment(nil), provenance.Markers...)
	}
	return result
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

// RegisterWorker adds a compatible worker or revives the same offline incarnation.
type RegisterWorker struct {
	Envelope Envelope     // Envelope carries worker subject and coordinator fences.
	Worker   WorkerRecord // Worker is the complete successor record.
}

// DrainWorker disables new placement without invalidating current assignments.
type DrainWorker struct {
	Envelope    Envelope          // Envelope carries worker subject and coordinator fences.
	WorkerID    uint16            // WorkerID selects the worker record.
	WorkerEpoch model.WorkerEpoch // WorkerEpoch prevents draining a replacement incarnation.
}

// DeactivateWorker atomically marks one eligible incarnation offline and affected work stale.
type DeactivateWorker struct {
	Envelope    Envelope             // Envelope carries worker subject and coordinator fences.
	WorkerID    uint16               // WorkerID selects the worker record.
	WorkerEpoch model.WorkerEpoch    // WorkerEpoch fences the exact incarnation.
	Affected    []AffectedAssignment // Affected is the sorted complete affected-job list.
}

// ReplaceWorkerEpoch atomically replaces an incarnation while preserving operator state.
type ReplaceWorkerEpoch struct {
	Envelope Envelope             // Envelope carries worker subject and coordinator fences.
	WorkerID uint16               // WorkerID selects the stable worker identity.
	OldEpoch model.WorkerEpoch    // OldEpoch conditionally fences the replaced incarnation.
	Target   WorkerRecord         // Target is the complete successor with preserved state.
	Affected []AffectedAssignment // Affected is the sorted complete affected-job list.
}

// NewRegisterWorker builds a digest-bound worker successor under one coordinator fence.
func NewRegisterWorker(id InternalCommandID, expectedRevision uint64, target WorkerRecord, fence ...model.CoordinatorEpoch) (RegisterWorker, error) {
	command := RegisterWorker{Worker: target}
	command.Envelope = newInternalEnvelope(CommandRegisterWorker, SubjectKey{Kind: SubjectWorker, WorkerID: target.NodeID}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, registerWorkerTarget(command))
	return command, command.Validate()
}

// NewDrainWorker builds a conditional operator drain for one exact incarnation.
func NewDrainWorker(id InternalCommandID, expectedRevision uint64, workerID uint16, epoch model.WorkerEpoch, fence ...model.CoordinatorEpoch) (DrainWorker, error) {
	command := DrainWorker{WorkerID: workerID, WorkerEpoch: epoch}
	command.Envelope = newInternalEnvelope(CommandDrainWorker, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, drainWorkerTarget(command))
	return command, command.Validate()
}

// NewDeactivateWorker owns the sorted complete affected-job list for atomic invalidation.
func NewDeactivateWorker(id InternalCommandID, expectedRevision uint64, workerID uint16, epoch model.WorkerEpoch, affected []AffectedAssignment, fence ...model.CoordinatorEpoch) (DeactivateWorker, error) {
	command := DeactivateWorker{WorkerID: workerID, WorkerEpoch: epoch, Affected: cloneAffected(affected)}
	command.Envelope = newInternalEnvelope(CommandDeactivateWorker, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, deactivateWorkerTarget(command))
	return command, command.Validate()
}

// NewReplaceWorkerEpoch builds an atomic incarnation replacement preserving operator state.
func NewReplaceWorkerEpoch(id InternalCommandID, expectedRevision uint64, workerID uint16, oldEpoch model.WorkerEpoch, target WorkerRecord, affected []AffectedAssignment, fence ...model.CoordinatorEpoch) (ReplaceWorkerEpoch, error) {
	command := ReplaceWorkerEpoch{WorkerID: workerID, OldEpoch: oldEpoch, Target: target, Affected: cloneAffected(affected)}
	command.Envelope = newInternalEnvelope(CommandReplaceWorkerEpoch, SubjectKey{Kind: SubjectWorker, WorkerID: workerID}, id, expectedRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, replaceWorkerEpochTarget(command))
	return command, command.Validate()
}

func newInternalEnvelope(kind CommandKind, subject SubjectKey, id InternalCommandID, expected uint64, fences ...model.CoordinatorEpoch) Envelope {
	var fence model.CoordinatorEpoch
	if len(fences) == 1 {
		fence = fences[0]
	}
	return Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: kind, CoordinatorEpoch: fence, Internal: &InternalEnvelope{ID: id, Subject: subject, ExpectedRevision: expected}}
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
	if command.Target.NodeID != command.WorkerID || command.Target.Epoch == command.OldEpoch || command.Target.Revision != command.Envelope.Internal.ExpectedRevision+1 {
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
		jobs, pruneDelta, ok := machine.prepareConsumedInvalidationPrune(command.Worker.NodeID)
		if !ok {
			return mutationPlan{}, errors.New("impossible invalidation provenance accounting")
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		if err != nil {
			return mutationPlan{}, err
		}
		delta := int64(0)
		if !exists {
			delta = workerRecordEstimatedBytes
		}
		delta += pruneDelta
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.workers[command.Worker.NodeID] = command.Worker
			for jobID, record := range jobs {
				machine.jobs[jobID] = record
			}
		}}, nil
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
		jobs, delta, ok := machine.prepareConsumedInvalidationPrune(command.WorkerID)
		if !ok {
			return mutationPlan{}, errors.New("impossible invalidation provenance accounting")
		}
		current.State, current.Revision = WorkerDraining, nextRevision
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: delta, commit: func() {
			machine.workers[command.WorkerID] = current
			for jobID, record := range jobs {
				machine.jobs[jobID] = record
			}
		}}, err
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
		jobs, delta, ok := machine.prepareWorkerInvalidation(workerInvalidationDeactivate, nextRevision, command.WorkerID, command.WorkerEpoch, command.Affected)
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
		if current.Epoch != command.OldEpoch || command.Target.Revision != nextRevision || command.Target.State != current.State {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		jobs, delta, ok := machine.prepareWorkerInvalidation(workerInvalidationReplaceEpoch, nextRevision, command.WorkerID, command.OldEpoch, command.Affected)
		if !ok {
			result, err := marshalBusinessResult(ResultRevisionMismatch, key, current.Revision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		oldCursor := workerEventKey{WorkerID: command.WorkerID, WorkerEpoch: command.OldEpoch}
		if _, exists := machine.workerEvents[oldCursor]; exists {
			delta -= int64(workerEventEntryEstimatedBytes)
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

func (machine *Machine) prepareWorkerInvalidation(kind workerInvalidationKind, workerRevision uint64, workerID uint16, epoch model.WorkerEpoch, presented []AffectedAssignment) (map[model.JobID]JobRecord, int64, bool) {
	jobs := make(map[model.JobID]JobRecord)
	var actual []AffectedAssignment
	var delta int64
	for jobID, record := range machine.jobs {
		candidate := cloneJobRecord(record)
		candidate.invalidationHistory = machine.pruneConsumedInvalidationHistory(jobID, candidate.invalidationHistory, workerID)
		if record.Assignment != nil && !record.Lifecycle.terminal() {
			markers := markersForWorker(*record.Assignment, workerID, epoch)
			union := sortedMarkerUnion(record.NeedsReassignment, markers)
			if len(union) != len(record.NeedsReassignment) {
				actual = append(actual, AffectedAssignment{JobID: jobID, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision, AssignmentDigest: record.Assignment.Digest})
				candidate.NeedsReassignment = union
				if err := validateMarkers(candidate.NeedsReassignment, jobID); err != nil || candidate.JobControlRevision == ^uint64(0) || len(candidate.invalidationHistory) == int(model.StateCommandMaxInvalidationProvenanceV1) {
					return nil, 0, false
				}
				candidate.invalidationHistory = append(candidate.invalidationHistory, invalidationProvenance{
					Kind: kind, WorkerID: workerID, WorkerEpoch: epoch, WorkerRevision: workerRevision,
					JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision,
					AssignmentDigest: record.Assignment.Digest, Markers: append([]NeedsReassignment(nil), markers...),
					RepairState: invalidationRepairActive,
				})
				candidate.JobControlRevision++
			}
		}
		if !reflect.DeepEqual(candidate, record) {
			before, beforeOK := estimateJobRecordBytes(record)
			after, afterOK := estimateJobRecordBytes(candidate)
			if !beforeOK || !afterOK {
				return nil, 0, false
			}
			delta += signedSizeDelta(after, before)
			jobs[jobID] = candidate
		}
	}
	sort.Slice(actual, func(i, j int) bool { return bytes.Compare(actual[i].JobID[:], actual[j].JobID[:]) < 0 })
	if !reflect.DeepEqual(actual, presented) {
		return nil, 0, false
	}
	return jobs, delta, true
}

func (machine *Machine) prepareConsumedInvalidationPrune(workerID uint16) (map[model.JobID]JobRecord, int64, bool) {
	jobs := make(map[model.JobID]JobRecord)
	var delta int64
	for jobID, record := range machine.jobs {
		candidate := cloneJobRecord(record)
		candidate.invalidationHistory = machine.pruneConsumedInvalidationHistory(jobID, candidate.invalidationHistory, workerID)
		if reflect.DeepEqual(candidate.invalidationHistory, record.invalidationHistory) {
			continue
		}
		before, beforeOK := estimateJobRecordBytes(record)
		after, afterOK := estimateJobRecordBytes(candidate)
		if !beforeOK || !afterOK {
			return nil, 0, false
		}
		delta += signedSizeDelta(after, before)
		jobs[jobID] = candidate
	}
	return jobs, delta, true
}

func (machine *Machine) pruneConsumedInvalidationHistory(jobID model.JobID, history []invalidationProvenance, overwrittenWorker uint16) []invalidationProvenance {
	normalized := cloneInvalidationProvenance(history)
	if overwrittenWorker != 0 {
		for index := range normalized {
			if normalized[index].WorkerID == overwrittenWorker {
				normalized[index].Kind = 0
				normalized[index].WorkerRevision = 0
			}
		}
	}
	result := make([]invalidationProvenance, 0, len(normalized))
	for _, provenance := range normalized {
		if provenance.RepairState == invalidationRepairForgotten && !machine.subjectRetainsInvalidation(jobID, provenance) {
			continue
		}
		clone := provenance
		clone.Markers = append([]NeedsReassignment(nil), provenance.Markers...)
		result = append(result, clone)
	}
	return result
}

func (machine *Machine) forgetJobControlInvalidationHistory(jobID model.JobID, history []invalidationProvenance) []invalidationProvenance {
	normalized := cloneInvalidationProvenance(history)
	result := make([]invalidationProvenance, 0, len(normalized))
	for _, provenance := range normalized {
		if provenance.RepairState == invalidationRepairAnchored {
			provenance.RepairState = invalidationRepairForgotten
			provenance.RepairJobControlRevision = 0
			provenance.RepairAssignmentRevision = 0
			provenance.RepairAssignmentDigest = [32]byte{}
			provenance.RepairMarkersDigest = [32]byte{}
		}
		if provenance.RepairState == invalidationRepairForgotten && !machine.subjectRetainsInvalidation(jobID, provenance) {
			continue
		}
		result = append(result, provenance)
	}
	return result
}

func (machine *Machine) subjectRetainsInvalidation(jobID model.JobID, provenance invalidationProvenance) bool {
	history, exists := machine.subjects[SubjectKey{Kind: SubjectWorker, WorkerID: provenance.WorkerID}]
	if !exists || history.appliedRevision != provenance.WorkerRevision {
		return false
	}
	kind, workerID, epoch, _, affected, ok := decodeWorkerInvalidationTarget(history.appliedTarget)
	if !ok || kind != provenance.Kind || workerID != provenance.WorkerID || epoch != provenance.WorkerEpoch {
		return false
	}
	want := AffectedAssignment{JobID: jobID, JobControlRevision: provenance.JobControlRevision, AssignmentRevision: provenance.AssignmentRevision, AssignmentDigest: provenance.AssignmentDigest}
	for _, item := range affected {
		if item == want {
			return true
		}
	}
	return false
}

func decodeWorkerInvalidationTarget(target []byte) (workerInvalidationKind, uint16, model.WorkerEpoch, WorkerRecord, []AffectedAssignment, bool) {
	decoder := commandDecoder{input: target}
	if len(target) >= 20 && (len(target)-20)%64 == 0 {
		workerID, workerErr := decoder.u16()
		epoch, epochErr := decoder.workerEpoch()
		affected, affectedErr := decoder.affected()
		return workerInvalidationDeactivate, workerID, epoch, WorkerRecord{}, affected, workerErr == nil && epochErr == nil && affectedErr == nil && decoder.done()
	}
	if len(target) >= 113 && (len(target)-113)%64 == 0 {
		workerID, workerErr := decoder.u16()
		epoch, epochErr := decoder.workerEpoch()
		record, recordErr := decoder.workerRecord()
		affected, affectedErr := decoder.affected()
		return workerInvalidationReplaceEpoch, workerID, epoch, record, affected, workerErr == nil && epochErr == nil && recordErr == nil && affectedErr == nil && decoder.done()
	}
	return 0, 0, model.WorkerEpoch{}, WorkerRecord{}, nil, false
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
