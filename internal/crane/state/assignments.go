package state

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
)

const needsReassignmentDigestDomain = "crane/needs-reassignment/v1\x00"

// ReassignmentTargetKind selects a task token or one result-replica role.
type ReassignmentTargetKind uint8

const (
	TaskTarget          ReassignmentTargetKind = iota + 1 // TaskTarget invalidates one task token.
	ResultReplicaTarget                                   // ResultReplicaTarget invalidates one replica role.
)

// NeedsReassignment identifies one exact target invalidated by a worker change.
type NeedsReassignment struct {
	Kind           ReassignmentTargetKind  // Kind selects the marker union member.
	Task           model.TaskID            // Task identifies a task-token target.
	SinkTask       model.TaskID            // SinkTask identifies a replica target.
	ReplicaRole    model.ResultReplicaRole // ReplicaRole selects primary or secondary.
	OldWorkerID    uint16                  // OldWorkerID binds the invalidated node.
	OldWorkerEpoch model.WorkerEpoch       // OldWorkerEpoch binds its exact incarnation.
}

func (marker NeedsReassignment) Validate() error {
	if marker.OldWorkerID == 0 {
		return errors.New("zero old worker ID")
	}
	if err := marker.OldWorkerEpoch.Validate(); err != nil {
		return err
	}
	switch marker.Kind {
	case TaskTarget:
		if err := marker.Task.Validate(); err != nil || marker.SinkTask != (model.TaskID{}) || marker.ReplicaRole != 0 {
			return errors.New("noncanonical task reassignment marker")
		}
	case ResultReplicaTarget:
		if err := marker.SinkTask.Validate(); err != nil || marker.Task != (model.TaskID{}) || marker.ReplicaRole < model.PrimaryReplica || marker.ReplicaRole > model.SecondaryReplica {
			return errors.New("noncanonical result-replica marker")
		}
	default:
		return errors.New("unknown reassignment target kind")
	}
	return nil
}

func markerLess(left, right NeedsReassignment) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	leftTask, rightTask := left.Task, right.Task
	if left.Kind == ResultReplicaTarget {
		leftTask, rightTask = left.SinkTask, right.SinkTask
	}
	if comparison := bytes.Compare(taskBytes(leftTask), taskBytes(rightTask)); comparison != 0 {
		return comparison < 0
	}
	if left.ReplicaRole != right.ReplicaRole {
		return left.ReplicaRole < right.ReplicaRole
	}
	if left.OldWorkerID != right.OldWorkerID {
		return left.OldWorkerID < right.OldWorkerID
	}
	return bytes.Compare(left.OldWorkerEpoch[:], right.OldWorkerEpoch[:]) < 0
}

func validateMarkers(markers []NeedsReassignment, job model.JobID) error {
	if uint64(len(markers)) > model.LimitsV1().MaxTasksPerJob+2*model.LimitsV1().MaxResultManifestsPerJob {
		return errors.New("reassignment marker count exceeds bound")
	}
	for index, marker := range markers {
		if err := marker.Validate(); err != nil {
			return err
		}
		task := marker.Task
		if marker.Kind == ResultReplicaTarget {
			task = marker.SinkTask
		}
		if task.JobID != job {
			return errors.New("foreign reassignment marker")
		}
		if index > 0 && !markerLess(markers[index-1], marker) {
			return errors.New("reassignment markers are not sorted and unique")
		}
	}
	return nil
}

// NeedsReassignmentDigest binds the exact sorted complete invalid-target list.
func NeedsReassignmentDigest(markers []NeedsReassignment) [32]byte {
	encoded := append([]byte(needsReassignmentDigestDomain), byte(len(markers)>>8), byte(len(markers)))
	for _, marker := range markers {
		encoded = appendMarker(encoded, marker)
	}
	return sha256.Sum256(encoded)
}

// InstallAssignments commits the first complete assignment set for a pending job.
type InstallAssignments struct {
	Envelope   Envelope            // Envelope carries job-control and coordinator fences.
	Assignment model.AssignmentSet // Assignment is the complete revision-one set.
}

// ReplaceAssignments conditionally replaces exactly the marked targets.
type ReplaceAssignments struct {
	Envelope                   Envelope            // Envelope carries job-control and coordinator fences.
	JobID                      model.JobID         // JobID selects the assigned job.
	ExpectedAssignmentRevision uint64              // ExpectedAssignmentRevision fences the old set.
	ExpectedDigest             [32]byte            // ExpectedDigest binds the old complete set.
	ExpectedMarkersDigest      [32]byte            // ExpectedMarkersDigest binds all invalid targets.
	Target                     model.AssignmentSet // Target is the complete successor set.
}

// NewInstallAssignments owns one complete initial assignment set.
func NewInstallAssignments(id InternalCommandID, expectedJobRevision uint64, assignment model.AssignmentSet, fence ...model.CoordinatorEpoch) (InstallAssignments, error) {
	command := InstallAssignments{Assignment: cloneAssignment(assignment)}
	command.Envelope = newInternalEnvelope(CommandInstallAssignments, SubjectKey{Kind: SubjectJobControl, JobID: assignment.JobID}, id, expectedJobRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, installAssignmentsTarget(command))
	return command, command.Validate()
}

// NewReplaceAssignments owns a complete successor and all old-set conditional fences.
func NewReplaceAssignments(id InternalCommandID, expectedJobRevision uint64, job model.JobID, expectedAssignmentRevision uint64, expectedDigest, expectedMarkersDigest [32]byte, target model.AssignmentSet, fence ...model.CoordinatorEpoch) (ReplaceAssignments, error) {
	command := ReplaceAssignments{JobID: job, ExpectedAssignmentRevision: expectedAssignmentRevision, ExpectedDigest: expectedDigest, ExpectedMarkersDigest: expectedMarkersDigest, Target: cloneAssignment(target)}
	command.Envelope = newInternalEnvelope(CommandReplaceAssignments, SubjectKey{Kind: SubjectJobControl, JobID: job}, id, expectedJobRevision, fence...)
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, replaceAssignmentsTarget(command))
	return command, command.Validate()
}

func validateAssignmentStructure(set model.AssignmentSet) error {
	if err := set.JobID.Validate(); err != nil {
		return err
	}
	if set.Revision == 0 || set.Digest == ([32]byte{}) || len(set.Tasks) == 0 || uint64(len(set.Tasks)) > model.LimitsV1().MaxTasksPerJob || uint64(len(set.ResultReplicas)) > model.LimitsV1().MaxResultManifestsPerJob {
		return errors.New("assignment set header/count outside bounds")
	}
	for index, token := range set.Tasks {
		if err := token.Validate(); err != nil || token.Task.JobID != set.JobID || token.AssignmentRevision != set.Revision {
			return fmt.Errorf("invalid assignment token %d", index)
		}
		if index > 0 && bytes.Compare(taskBytes(set.Tasks[index-1].Task), taskBytes(token.Task)) >= 0 {
			return errors.New("assignment tasks not sorted and unique")
		}
	}
	for index, replica := range set.ResultReplicas {
		if err := replica.Validate(); err != nil || replica.SinkTask.JobID != set.JobID {
			return fmt.Errorf("invalid result replica %d", index)
		}
		if index > 0 && bytes.Compare(taskBytes(set.ResultReplicas[index-1].SinkTask), taskBytes(replica.SinkTask)) >= 0 {
			return errors.New("result replicas not sorted and unique")
		}
	}
	return nil
}

func (command InstallAssignments) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	if command.Envelope.Kind != CommandInstallAssignments || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != (SubjectKey{Kind: SubjectJobControl, JobID: command.Assignment.JobID}) {
		return fmt.Errorf("%w: install assignment subject mismatch", ErrInvalidCommandSubject)
	}
	if err := validateAssignmentStructure(command.Assignment); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, installAssignmentsTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func (command ReplaceAssignments) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	if command.Envelope.Kind != CommandReplaceAssignments || command.Envelope.Internal == nil || command.Envelope.Internal.Subject != (SubjectKey{Kind: SubjectJobControl, JobID: command.JobID}) {
		return fmt.Errorf("%w: replace assignment subject mismatch", ErrInvalidCommandSubject)
	}
	if err := command.JobID.Validate(); err != nil {
		return err
	}
	if command.ExpectedAssignmentRevision == 0 || command.ExpectedDigest == ([32]byte{}) || command.ExpectedMarkersDigest == ([32]byte{}) || command.Target.JobID != command.JobID || command.Target.Revision != command.ExpectedAssignmentRevision+1 {
		return errors.New("invalid conditional assignment target")
	}
	if err := validateAssignmentStructure(command.Target); err != nil {
		return err
	}
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, replaceAssignmentsTarget(command)) {
		return ErrCommandDigestMismatch
	}
	return nil
}

func cloneAssignment(set model.AssignmentSet) model.AssignmentSet {
	clone := set
	clone.Tasks = append([]model.AssignmentToken(nil), set.Tasks...)
	clone.ResultReplicas = append([]model.ResultReplicaSet(nil), set.ResultReplicas...)
	return clone
}

func taskBytes(task model.TaskID) []byte {
	encoded := append([]byte(nil), task.JobID[:]...)
	encoded = appendU16(encoded, task.StageID)
	return appendU16(encoded, task.Partition)
}

func sortedMarkerUnion(current, additional []NeedsReassignment) []NeedsReassignment {
	result := append(append([]NeedsReassignment(nil), current...), additional...)
	sort.Slice(result, func(i, j int) bool { return markerLess(result[i], result[j]) })
	unique := result[:0]
	for _, marker := range result {
		if len(unique) == 0 || unique[len(unique)-1] != marker {
			unique = append(unique, marker)
		}
	}
	return unique
}

func (machine *Machine) applyInstallAssignmentsLocked(command InstallAssignments) ([]byte, error) {
	record, exists := machine.jobs[command.Assignment.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.JobControlRevision
	}
	target := installAssignmentsTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, target, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		if !exists {
			result, err := marshalBusinessResult(ResultNotFound, key, 0, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		candidate := cloneJobRecord(record)
		if candidate.Lifecycle != JobPending || candidate.Assignment != nil || len(candidate.NeedsReassignment) != 0 || !machine.allSourceEOFsPresent(candidate) {
			result, err := marshalBusinessResult(ResultInvalidTransition, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		topology, err := model.DecodeTopology(candidate.TopologyBytes)
		if err != nil {
			return mutationPlan{}, fmt.Errorf("impossible retained topology: %w", err)
		}
		if err := command.Assignment.Validate(topology); err != nil {
			result, resultErr := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		want, err := model.BuildAssignmentSet(candidate.JobID, candidate.TopologyDigest, 1, topology, machine.residualEligiblePlacements(candidate.JobID))
		if err != nil || !reflect.DeepEqual(want, command.Assignment) {
			result, resultErr := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		candidate.Assignment = assignmentPointer(command.Assignment)
		candidate.Lifecycle = JobDeploying
		candidate.JobControlRevision = nextRevision
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: int64(assignmentEncodedBytes(command.Assignment)), commit: func() { machine.jobs[candidate.JobID] = candidate }}, err
	})
}

func (machine *Machine) applyReplaceAssignmentsLocked(command ReplaceAssignments) ([]byte, error) {
	record, exists := machine.jobs[command.JobID]
	currentRevision := uint64(0)
	if exists {
		currentRevision = record.JobControlRevision
	}
	targetBytes := replaceAssignmentsTarget(command)
	return machine.applyInternalAtRevisionLocked(command.Envelope, targetBytes, currentRevision, func(nextRevision uint64) (mutationPlan, error) {
		key := command.Envelope.Internal.Subject
		if !exists || record.Assignment == nil {
			result, err := marshalBusinessResult(ResultNotFound, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		if record.Assignment.Revision != command.ExpectedAssignmentRevision || record.Assignment.Digest != command.ExpectedDigest || NeedsReassignmentDigest(record.NeedsReassignment) != command.ExpectedMarkersDigest || len(record.NeedsReassignment) == 0 {
			result, err := marshalBusinessResult(ResultRevisionMismatch, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, err
		}
		topology, err := model.DecodeTopology(record.TopologyBytes)
		if err != nil {
			return mutationPlan{}, fmt.Errorf("impossible retained topology: %w", err)
		}
		if err := command.Target.Validate(topology); err != nil || validateReplacement(*record.Assignment, command.Target, record.NeedsReassignment) != nil || !machine.replacementUsesCurrentWorkers(*record.Assignment, command.Target, command.JobID) {
			result, resultErr := marshalBusinessResult(ResultInvalidTarget, key, currentRevision, model.CoordinatorEpoch{})
			return mutationPlan{result: result, reject: true}, resultErr
		}
		candidate := cloneJobRecord(record)
		candidate.invalidationHistory = machine.forgetJobControlInvalidationHistory(candidate.JobID, candidate.invalidationHistory)
		for index := range candidate.invalidationHistory {
			provenance := &candidate.invalidationHistory[index]
			if provenance.RepairState != invalidationRepairActive {
				continue
			}
			provenance.RepairState = invalidationRepairAnchored
			provenance.RepairJobControlRevision = nextRevision
			provenance.RepairAssignmentRevision = command.Target.Revision
			provenance.RepairAssignmentDigest = command.Target.Digest
			provenance.RepairMarkersDigest = command.ExpectedMarkersDigest
		}
		candidate.Assignment = assignmentPointer(command.Target)
		candidate.NeedsReassignment = nil
		candidate.JobControlRevision = nextRevision
		beforeBytes, beforeOK := estimateJobRecordBytes(record)
		afterBytes, afterOK := estimateJobRecordBytes(candidate)
		if !beforeOK || !afterOK {
			return mutationPlan{}, errors.New("impossible assignment provenance accounting")
		}
		result, err := marshalBusinessResult(ResultSuccess, key, nextRevision, model.CoordinatorEpoch{})
		return mutationPlan{result: result, stateDelta: signedSizeDelta(afterBytes, beforeBytes), commit: func() { machine.jobs[candidate.JobID] = candidate }}, err
	})
}

func assignmentPointer(set model.AssignmentSet) *model.AssignmentSet {
	clone := cloneAssignment(set)
	return &clone
}

func signedSizeDelta(next, current uint64) int64 {
	if next >= current {
		return int64(next - current)
	}
	return -int64(current - next)
}

// residualEligiblePlacements returns task-execution capacity after subtracting
// every token in every other nonterminal job. Result replicas consume durable
// storage capacity, not logical worker execution slots, and are not subtracted.
func (machine *Machine) residualEligiblePlacements(excludeJob model.JobID) []model.WorkerPlacement {
	used := make(map[uint16]uint64)
	for jobID, job := range machine.jobs {
		if jobID == excludeJob || job.Lifecycle.terminal() || job.Assignment == nil {
			continue
		}
		for _, token := range job.Assignment.Tasks {
			used[token.WorkerID]++
		}
	}
	workers := make([]WorkerRecord, 0, len(machine.workers))
	for _, worker := range machine.workers {
		if worker.State == WorkerEligible {
			workers = append(workers, worker)
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].NodeID < workers[j].NodeID })
	placements := make([]model.WorkerPlacement, 0, len(workers))
	for _, worker := range workers {
		if used[worker.NodeID] >= uint64(worker.Slots) {
			continue
		}
		placements = append(placements, model.WorkerPlacement{NodeID: worker.NodeID, WorkerEpoch: worker.Epoch, SlotCapacity: worker.Slots - uint16(used[worker.NodeID])})
	}
	return placements
}

func (machine *Machine) replacementUsesCurrentWorkers(old, target model.AssignmentSet, excludeJob model.JobID) bool {
	usedByOtherJobs := make(map[uint16]uint64)
	for jobID, job := range machine.jobs {
		if jobID == excludeJob || job.Lifecycle.terminal() || job.Assignment == nil {
			continue
		}
		for _, token := range job.Assignment.Tasks {
			usedByOtherJobs[token.WorkerID]++
		}
	}
	usedByTarget := make(map[uint16]uint64)
	for index, token := range target.Tasks {
		before := old.Tasks[index]
		changed := before.WorkerID != token.WorkerID || before.WorkerEpoch != token.WorkerEpoch
		worker, ok := machine.workers[token.WorkerID]
		if !ok || worker.Epoch != token.WorkerEpoch || !replacementWorkerStateAllowed(worker.State, changed) {
			return false
		}
		usedByTarget[token.WorkerID]++
		if usedByOtherJobs[token.WorkerID] > uint64(worker.Slots) || usedByTarget[token.WorkerID] > uint64(worker.Slots)-usedByOtherJobs[token.WorkerID] {
			return false
		}
	}
	for index, replica := range target.ResultReplicas {
		before := old.ResultReplicas[index]
		if !machine.replacementEndpointCurrent(replica.PrimaryNodeID, replica.PrimaryEpoch, before.PrimaryNodeID != replica.PrimaryNodeID || before.PrimaryEpoch != replica.PrimaryEpoch) {
			return false
		}
		if !machine.replacementEndpointCurrent(replica.SecondaryNodeID, replica.SecondaryEpoch, before.SecondaryNodeID != replica.SecondaryNodeID || before.SecondaryEpoch != replica.SecondaryEpoch) {
			return false
		}
	}
	return true
}

func replacementWorkerStateAllowed(state WorkerState, changed bool) bool {
	if changed {
		return state == WorkerEligible
	}
	return state == WorkerEligible || state == WorkerDraining
}

func (machine *Machine) replacementEndpointCurrent(workerID uint16, epoch model.WorkerEpoch, changed bool) bool {
	worker, ok := machine.workers[workerID]
	return ok && worker.Epoch == epoch && replacementWorkerStateAllowed(worker.State, changed)
}

func validateReplacement(old, target model.AssignmentSet, markers []NeedsReassignment) error {
	if target.JobID != old.JobID || target.Revision != old.Revision+1 || len(target.Tasks) != len(old.Tasks) || len(target.ResultReplicas) != len(old.ResultReplicas) {
		return errors.New("replacement is not one complete successor set")
	}
	markedTasks := make(map[model.TaskID]NeedsReassignment)
	markedReplicas := make(map[[22]byte]NeedsReassignment)
	for _, marker := range markers {
		if marker.Kind == TaskTarget {
			markedTasks[marker.Task] = marker
		} else {
			var key [22]byte
			copy(key[:20], taskBytes(marker.SinkTask))
			key[20] = byte(marker.ReplicaRole)
			markedReplicas[key] = marker
		}
	}
	for index, before := range old.Tasks {
		after := target.Tasks[index]
		changed := before.WorkerID != after.WorkerID || before.WorkerEpoch != after.WorkerEpoch
		_, marked := markedTasks[before.Task]
		if changed != marked || changed && after.Attempt != before.Attempt+1 || !changed && after.Attempt != before.Attempt || after.AssignmentRevision != target.Revision {
			return errors.New("replacement task attempt/marker invariant failed")
		}
		if marked && after.WorkerID == markedTasks[before.Task].OldWorkerID && after.WorkerEpoch == markedTasks[before.Task].OldWorkerEpoch {
			return errors.New("marked task retained old worker")
		}
	}
	for index, before := range old.ResultReplicas {
		after := target.ResultReplicas[index]
		for _, role := range []model.ResultReplicaRole{model.PrimaryReplica, model.SecondaryReplica} {
			var key [22]byte
			copy(key[:20], taskBytes(before.SinkTask))
			key[20] = byte(role)
			_, marked := markedReplicas[key]
			beforeID, afterID := before.PrimaryNodeID, after.PrimaryNodeID
			beforeEpoch, afterEpoch := before.PrimaryEpoch, after.PrimaryEpoch
			if role == model.SecondaryReplica {
				beforeID, afterID = before.SecondaryNodeID, after.SecondaryNodeID
				beforeEpoch, afterEpoch = before.SecondaryEpoch, after.SecondaryEpoch
			}
			changed := beforeID != afterID || beforeEpoch != afterEpoch
			if changed != marked {
				return errors.New("replacement replica marker invariant failed")
			}
		}
	}
	return nil
}

func (machine *Machine) allSourceEOFsPresent(job JobRecord) bool {
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return false
	}
	for _, stage := range topology.Spec().Stages {
		if stage.Role != model.Source {
			continue
		}
		for partition := uint16(0); partition < stage.Parallelism; partition++ {
			task := model.TaskID{JobID: job.JobID, StageID: stage.StageID, Partition: partition}
			record, ok := job.SourceEOFs[task]
			if !ok || record.Revision != 1 {
				return false
			}
		}
	}
	return true
}
