package model

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
)

const rendezvousDomain = "cs425/crane/rendezvous/v1\x00"

// WorkerPlacement is one current eligible worker and its committed executor capacity.
type WorkerPlacement struct {
	NodeID       uint16
	WorkerEpoch  WorkerEpoch
	SlotCapacity uint16
}

// ResultReplicaSet assigns one collect partition to two current worker epochs.
type ResultReplicaSet struct {
	SinkTask                       TaskID
	PrimaryNodeID, SecondaryNodeID uint16
	PrimaryEpoch, SecondaryEpoch   WorkerEpoch
}

// AssignmentSet is the complete canonical placement for one job revision.
type AssignmentSet struct {
	JobID          JobID
	Revision       uint64
	Digest         [32]byte
	Tasks          []AssignmentToken
	ResultReplicas []ResultReplicaSet
}

// NewAssignmentSet owns one explicit complete placement, calculates its
// canonical digest, and validates it against the immutable topology.
func NewAssignmentSet(job JobID, revision uint64, tasks []AssignmentToken, replicas []ResultReplicaSet, topology ValidatedTopology) (AssignmentSet, error) {
	if uint64(len(tasks)) > LimitsV1().MaxTasksPerJob || uint64(len(replicas)) > LimitsV1().MaxResultManifestsPerJob {
		return AssignmentSet{}, errors.New("assignment collection exceeds bound before copy")
	}
	set := AssignmentSet{JobID: job, Revision: revision, Tasks: append([]AssignmentToken(nil), tasks...), ResultReplicas: append([]ResultReplicaSet(nil), replicas...)}
	set.Digest = assignmentDigest(set)
	if err := set.Validate(topology); err != nil {
		return AssignmentSet{}, err
	}
	return set, nil
}

// PlaceTasks assigns all explicit tasks by deterministic rendezvous score and slots.
func PlaceTasks(job JobID, specificationHash [32]byte, assignmentRevision uint64, tasks []TaskID, workers []WorkerPlacement) ([]AssignmentToken, error) {
	if err := job.Validate(); err != nil {
		return nil, err
	}
	if specificationHash == ([32]byte{}) {
		return nil, errors.New("zero specification hash")
	}
	if assignmentRevision == 0 {
		return nil, errors.New("zero assignment revision")
	}
	if len(tasks) == 0 || uint64(len(tasks)) > LimitsV1().MaxTasksPerJob {
		return nil, errors.New("task count outside bounds before copy")
	}
	if len(workers) == 0 || uint64(len(workers)) > LimitsV1().MaxRegisteredWorkers {
		return nil, errors.New("worker count outside bounds before copy")
	}
	canonicalTasks := append([]TaskID(nil), tasks...)
	sort.Slice(canonicalTasks, func(i, j int) bool {
		if canonicalTasks[i].StageID != canonicalTasks[j].StageID {
			return canonicalTasks[i].StageID < canonicalTasks[j].StageID
		}
		return canonicalTasks[i].Partition < canonicalTasks[j].Partition
	})
	for index, task := range canonicalTasks {
		if err := task.Validate(); err != nil || task.JobID != job {
			return nil, errors.New("invalid or foreign task")
		}
		if index > 0 && canonicalTasks[index-1] == task {
			return nil, errors.New("duplicate task")
		}
	}
	canonicalWorkers, err := validateWorkers(workers)
	if err != nil {
		return nil, err
	}
	used := make(map[uint16]uint16, len(canonicalWorkers))
	tokens := make([]AssignmentToken, 0, len(canonicalTasks))
	for _, task := range canonicalTasks {
		worker, ok := bestWorker(job, task, canonicalWorkers, func(candidate WorkerPlacement) bool { return used[candidate.NodeID] < candidate.SlotCapacity })
		if !ok {
			return nil, errors.New("insufficient committed worker slots")
		}
		used[worker.NodeID]++
		tokens = append(tokens, AssignmentToken{Task: task, WorkerID: worker.NodeID, WorkerEpoch: worker.WorkerEpoch, Attempt: 1, SpecificationHash: specificationHash, AssignmentRevision: assignmentRevision})
	}
	return tokens, nil
}

// BuildAssignmentSet derives every task and collect partition from one immutable topology.
func BuildAssignmentSet(job JobID, specificationHash [32]byte, assignmentRevision uint64, topology ValidatedTopology, workers []WorkerPlacement) (AssignmentSet, error) {
	if err := job.Validate(); err != nil {
		return AssignmentSet{}, err
	}
	if specificationHash == ([32]byte{}) || specificationHash != topology.digest || len(topology.canonical) == 0 {
		return AssignmentSet{}, errors.New("specification hash does not match validated topology")
	}
	if assignmentRevision == 0 {
		return AssignmentSet{}, errors.New("zero assignment revision")
	}
	tasks := topologyTasks(job, topology.spec)
	tokens, err := PlaceTasks(job, specificationHash, assignmentRevision, tasks, workers)
	if err != nil {
		return AssignmentSet{}, err
	}
	canonicalWorkers, err := validateWorkers(workers)
	if err != nil {
		return AssignmentSet{}, err
	}
	replicas := make([]ResultReplicaSet, 0, len(topology.collectPartitions))
	for _, partition := range topology.collectPartitions {
		task := TaskID{JobID: job, StageID: topology.collectStageID, Partition: partition}
		var primary AssignmentToken
		for _, token := range tokens {
			if token.Task == task {
				primary = token
				break
			}
		}
		secondary, ok := bestWorker(job, task, canonicalWorkers, func(candidate WorkerPlacement) bool { return candidate.NodeID != primary.WorkerID })
		if !ok {
			return AssignmentSet{}, errors.New("two distinct eligible result replica nodes are required")
		}
		replicas = append(replicas, ResultReplicaSet{SinkTask: task, PrimaryNodeID: primary.WorkerID, SecondaryNodeID: secondary.NodeID, PrimaryEpoch: primary.WorkerEpoch, SecondaryEpoch: secondary.WorkerEpoch})
	}
	set := AssignmentSet{JobID: job, Revision: assignmentRevision, Tasks: tokens, ResultReplicas: replicas}
	set.Digest = assignmentDigest(set)
	if err := set.Validate(topology); err != nil {
		return AssignmentSet{}, err
	}
	return set, nil
}

// Validate checks that a set is the exact complete topology placement it claims.
func (set AssignmentSet) Validate(topology ValidatedTopology) error {
	if err := set.JobID.Validate(); err != nil {
		return err
	}
	if set.Revision == 0 {
		return errors.New("zero assignment revision")
	}
	if len(topology.canonical) == 0 {
		return errors.New("unvalidated topology")
	}
	wantTasks := topologyTasks(set.JobID, topology.spec)
	if len(set.Tasks) != len(wantTasks) {
		return errors.New("assignment task set is incomplete")
	}
	epochs := make(map[uint16]WorkerEpoch)
	registerEpoch := func(nodeID uint16, epoch WorkerEpoch) error {
		if prior, ok := epochs[nodeID]; ok && prior != epoch {
			return errors.New("one worker ID carries contradictory epochs across assignment duties")
		}
		epochs[nodeID] = epoch
		return nil
	}
	for index, token := range set.Tasks {
		if err := token.Validate(); err != nil {
			return fmt.Errorf("task token %d: %w", index, err)
		}
		if token.Task != wantTasks[index] || token.Task.JobID != set.JobID || token.AssignmentRevision != set.Revision || token.SpecificationHash != topology.digest {
			return errors.New("assignment token does not match complete topology set")
		}
		if err := registerEpoch(token.WorkerID, token.WorkerEpoch); err != nil {
			return err
		}
	}
	var sink StageSpec
	for _, stage := range topology.spec.Stages {
		if stage.Role == StageSink {
			sink = stage
		}
	}
	if len(set.ResultReplicas) != int(sink.Parallelism) {
		return errors.New("result replica set is incomplete")
	}
	for index, replica := range set.ResultReplicas {
		want := TaskID{JobID: set.JobID, StageID: sink.StageID, Partition: uint16(index)}
		if replica.SinkTask != want {
			return errors.New("result replicas are not canonical and complete")
		}
		if err := replica.Validate(); err != nil {
			return err
		}
		if err := registerEpoch(replica.PrimaryNodeID, replica.PrimaryEpoch); err != nil {
			return err
		}
		if err := registerEpoch(replica.SecondaryNodeID, replica.SecondaryEpoch); err != nil {
			return err
		}
		var token AssignmentToken
		for _, candidate := range set.Tasks {
			if candidate.Task == want {
				token = candidate
				break
			}
		}
		if replica.PrimaryNodeID != token.WorkerID || replica.PrimaryEpoch != token.WorkerEpoch {
			return errors.New("result primary is not sink task assignee")
		}
	}
	if set.Digest == ([32]byte{}) || set.Digest != assignmentDigest(set) {
		return errors.New("assignment set digest mismatch")
	}
	return nil
}

// Validate checks the exact two-node result replica contract.
func (set ResultReplicaSet) Validate() error {
	if err := set.SinkTask.Validate(); err != nil {
		return err
	}
	if set.PrimaryNodeID == 0 || set.SecondaryNodeID == 0 || set.PrimaryNodeID == set.SecondaryNodeID {
		return errors.New("result replicas require distinct nonzero nodes")
	}
	if err := set.PrimaryEpoch.Validate(); err != nil {
		return err
	}
	if err := set.SecondaryEpoch.Validate(); err != nil {
		return err
	}
	return nil
}

func topologyTasks(job JobID, spec TopologySpec) []TaskID {
	tasks := make([]TaskID, 0)
	for _, stage := range spec.Stages {
		for partition := uint16(0); partition < stage.Parallelism; partition++ {
			tasks = append(tasks, TaskID{JobID: job, StageID: stage.StageID, Partition: partition})
		}
	}
	return tasks
}

func validateWorkers(input []WorkerPlacement) ([]WorkerPlacement, error) {
	if len(input) == 0 || uint64(len(input)) > LimitsV1().MaxRegisteredWorkers {
		return nil, errors.New("worker count outside bounds before copy")
	}
	workers := append([]WorkerPlacement(nil), input...)
	sort.Slice(workers, func(i, j int) bool { return workers[i].NodeID < workers[j].NodeID })
	for index, worker := range workers {
		if worker.NodeID == 0 || worker.SlotCapacity == 0 || uint64(worker.SlotCapacity) > LimitsV1().MaxWorkerSlots {
			return nil, errors.New("invalid worker placement")
		}
		if err := worker.WorkerEpoch.Validate(); err != nil {
			return nil, err
		}
		if index > 0 && workers[index-1].NodeID == worker.NodeID {
			return nil, errors.New("duplicate worker node ID")
		}
	}
	return workers, nil
}

func bestWorker(job JobID, task TaskID, workers []WorkerPlacement, eligible func(WorkerPlacement) bool) (WorkerPlacement, bool) {
	var best WorkerPlacement
	var bestScore [32]byte
	found := false
	for _, worker := range workers {
		if !eligible(worker) {
			continue
		}
		encoded := append([]byte(rendezvousDomain), job[:]...)
		encoded = appendTaskID(encoded, task)
		encoded = appendUint16(encoded, worker.NodeID)
		encoded = append(encoded, worker.WorkerEpoch[:]...)
		score := sha256.Sum256(encoded)
		if !found || preferRendezvous(score, worker.NodeID, bestScore, best.NodeID) {
			best, bestScore, found = worker, score, true
		}
	}
	return best, found
}

func preferRendezvous(candidate [32]byte, candidateNode uint16, current [32]byte, currentNode uint16) bool {
	comparison := compareDigest(candidate, current)
	return comparison > 0 || comparison == 0 && candidateNode < currentNode
}

func assignmentDigest(set AssignmentSet) [32]byte {
	encoded := append([]byte("cs425/crane/assignment-set/v1\x00"), set.JobID[:]...)
	encoded = appendUint64(encoded, set.Revision)
	encoded = appendUint16(encoded, uint16(len(set.Tasks)))
	for _, token := range set.Tasks {
		encoded = appendTaskID(encoded, token.Task)
		encoded = appendUint16(encoded, token.WorkerID)
		encoded = append(encoded, token.WorkerEpoch[:]...)
		encoded = appendUint64(encoded, token.Attempt)
		encoded = append(encoded, token.SpecificationHash[:]...)
		encoded = appendUint64(encoded, token.AssignmentRevision)
	}
	encoded = appendUint16(encoded, uint16(len(set.ResultReplicas)))
	for _, replica := range set.ResultReplicas {
		encoded = appendTaskID(encoded, replica.SinkTask)
		encoded = appendUint16(encoded, replica.PrimaryNodeID)
		encoded = appendUint16(encoded, replica.SecondaryNodeID)
		encoded = append(encoded, replica.PrimaryEpoch[:]...)
		encoded = append(encoded, replica.SecondaryEpoch[:]...)
	}
	return sha256.Sum256(encoded)
}
