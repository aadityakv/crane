package coordinator

import (
	"bytes"
	"context"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/swim"
)

// resolveWorkerFailures tracks continuous Dead/Left membership combined with
// worker-control failure and, after FailureGracePeriod, deactivates the exact
// observed incarnation. Suspect alone never starts or commits reassignment,
// and any refutation before the deactivation commits cancels the transition.
func (actor *Actor) resolveWorkerFailures(ctx context.Context, epoch model.CoordinatorEpoch, session *sessionState, converged *bool) {
	members := actor.options.Membership.View()
	view := actor.options.Machine.View()
	now := actor.options.Clock.Now()
	for _, worker := range view.Workers {
		if ctx.Err() != nil {
			return
		}
		if worker.State == state.WorkerOffline {
			delete(session.observations, worker.NodeID)
			continue
		}
		if !session.controlFailed[worker.NodeID] {
			delete(session.observations, worker.NodeID)
			continue
		}
		member, exists := findMember(members, worker.NodeID)
		deadOrLeft := !exists || member.Status == swim.Dead || member.Status == swim.Left
		if !deadOrLeft {
			// Membership refutes the failure, but the worker stayed
			// control-unreachable: reconciliation remains incomplete.
			delete(session.observations, worker.NodeID)
			*converged = false
			continue
		}
		since, observed := session.observations[worker.NodeID]
		if !observed {
			session.observations[worker.NodeID] = now
			*converged = false
			continue
		}
		if now.Sub(since) < actor.options.FailureGracePeriod {
			*converged = false
			continue
		}
		if !actor.deactivateWorker(ctx, epoch, view, worker, session) {
			*converged = false
		}
	}
}

// deactivateWorker first durably closes every affected job on all reachable
// current task and replica workers, then proposes the conditional
// DeactivateWorker for the exact incarnation and revision. Only a replicated
// view showing that exact epoch Offline lets the invalidation stand.
func (actor *Actor) deactivateWorker(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, worker state.WorkerRecord, session *sessionState) bool {
	// A refutation observed before proposing cancels the transition.
	if memberActive(actor.options.Membership.View(), worker.NodeID) {
		delete(session.observations, worker.NodeID)
		return true
	}
	affected := affectedForWorker(view, worker.NodeID, worker.Epoch)
	for _, item := range affected {
		job, ok := findJob(view, item.JobID)
		if !ok {
			return false
		}
		// Healthy receivers must durably reject a returning stale producer
		// before durable authority advances.
		if !actor.installAssignment(ctx, epoch, job, model.Closed, false, worker.NodeID) {
			return false
		}
	}
	subject := state.SubjectKey{Kind: state.SubjectWorker, WorkerID: worker.NodeID}
	id := internalCommandID(state.CommandDeactivateWorker, epoch, subject, worker.Revision, worker.Epoch[:])
	command, err := state.NewDeactivateWorker(id, worker.Revision, worker.NodeID, worker.Epoch, affected, epoch)
	if err != nil {
		return false
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	if err != nil || result.Code != state.ResultSuccess {
		return false
	}
	confirm := actor.options.Machine.View()
	record, ok := findWorker(confirm, worker.NodeID)
	if !ok || record.Epoch != worker.Epoch || record.State != state.WorkerOffline {
		return false
	}
	delete(session.observations, worker.NodeID)
	return true
}

// replaceAssignments resolves every committed reassignment marker with one
// conditional complete successor set fenced by the exact current set revision,
// digest, and canonical marker digest.
func (actor *Actor) replaceAssignments(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, job state.JobRecord) bool {
	if job.Assignment == nil {
		return false
	}
	old := *job.Assignment
	target, ok := buildReplacementSet(view, job)
	if !ok {
		return false
	}
	markersDigest := state.NeedsReassignmentDigest(job.NeedsReassignment)
	subject := state.SubjectKey{Kind: state.SubjectJobControl, JobID: job.JobID}
	id := internalCommandID(state.CommandReplaceAssignments, epoch, subject, job.JobControlRevision, old.Digest[:], markersDigest[:], target.Digest[:])
	command, err := state.NewReplaceAssignments(id, job.JobControlRevision, job.JobID, old.Revision, old.Digest, markersDigest, target, epoch)
	if err != nil {
		return false
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	return err == nil && result.Code == state.ResultSuccess
}

// buildReplacementSet deterministically recomputes one complete successor set
// that changes exactly the marked targets, advances marked task attempts by
// one, and leaves every unmarked attempt untouched.
func buildReplacementSet(view state.View, job state.JobRecord) (model.AssignmentSet, bool) {
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return model.AssignmentSet{}, false
	}
	old := *job.Assignment
	markedTasks := make(map[model.TaskID]state.NeedsReassignment)
	markedReplicas := make(map[replicaRoleKey]state.NeedsReassignment)
	for _, marker := range job.NeedsReassignment {
		if marker.Kind == state.TaskTarget {
			markedTasks[marker.Task] = marker
		} else {
			markedReplicas[replicaRoleKey{task: marker.SinkTask, role: marker.ReplicaRole}] = marker
		}
	}
	capacity := replacementCapacity(view, job, markedTasks)
	sinkTasks := make(map[model.TaskID]int, len(old.ResultReplicas))
	for index, replica := range old.ResultReplicas {
		sinkTasks[replica.SinkTask] = index
	}

	tasks := append([]model.AssignmentToken(nil), old.Tasks...)
	for index := range tasks {
		tasks[index].AssignmentRevision = old.Revision + 1
		marker, marked := markedTasks[tasks[index].Task]
		if !marked {
			continue
		}
		var avoid uint16
		if replicaIndex, isSink := sinkTasks[tasks[index].Task]; isSink {
			// The sink assignee becomes the primary replica: avoid colliding
			// with an unmarked surviving secondary.
			replica := old.ResultReplicas[replicaIndex]
			if _, secondaryMarked := markedReplicas[replicaRoleKey{task: replica.SinkTask, role: model.SecondaryReplica}]; !secondaryMarked {
				avoid = replica.SecondaryNodeID
			}
		}
		replacement, ok := pickReplacementWorker(view, capacity, marker, avoid)
		if !ok {
			return model.AssignmentSet{}, false
		}
		tasks[index].WorkerID = replacement.NodeID
		tasks[index].WorkerEpoch = replacement.Epoch
		tasks[index].Attempt++
		capacity[replacement.NodeID]--
	}

	newTokens := make(map[model.TaskID]model.AssignmentToken, len(tasks))
	for _, token := range tasks {
		newTokens[token.Task] = token
	}
	replicas := append([]model.ResultReplicaSet(nil), old.ResultReplicas...)
	for index := range replicas {
		replica := &replicas[index]
		sinkToken := newTokens[replica.SinkTask]
		if _, marked := markedReplicas[replicaRoleKey{task: replica.SinkTask, role: model.PrimaryReplica}]; marked {
			replica.PrimaryNodeID, replica.PrimaryEpoch = sinkToken.WorkerID, sinkToken.WorkerEpoch
		} else if replica.PrimaryNodeID != sinkToken.WorkerID || replica.PrimaryEpoch != sinkToken.WorkerEpoch {
			return model.AssignmentSet{}, false
		}
		if marker, marked := markedReplicas[replicaRoleKey{task: replica.SinkTask, role: model.SecondaryReplica}]; marked {
			secondary, ok := pickSecondaryWorker(view, marker, replica.PrimaryNodeID)
			if !ok {
				return model.AssignmentSet{}, false
			}
			replica.SecondaryNodeID, replica.SecondaryEpoch = secondary.NodeID, secondary.Epoch
		}
		if replica.SecondaryNodeID == replica.PrimaryNodeID {
			return model.AssignmentSet{}, false
		}
	}
	set, err := model.NewAssignmentSet(job.JobID, old.Revision+1, tasks, replicas, topology)
	if err != nil {
		return model.AssignmentSet{}, false
	}
	return set, true
}

// replicaRoleKey identifies one replica-role reassignment target.
type replicaRoleKey struct {
	task model.TaskID
	role model.ResultReplicaRole
}

// replacementCapacity computes residual eligible slots after other jobs'
// tokens and this job's kept (unmarked) tokens.
func replacementCapacity(view state.View, job state.JobRecord, markedTasks map[model.TaskID]state.NeedsReassignment) map[uint16]int {
	usedByOther := make(map[uint16]uint64)
	for _, other := range view.Jobs {
		if other.JobID == job.JobID || terminalLifecycle(other.Lifecycle) || other.Assignment == nil {
			continue
		}
		for _, token := range other.Assignment.Tasks {
			usedByOther[token.WorkerID]++
		}
	}
	kept := make(map[uint16]uint64)
	for _, token := range job.Assignment.Tasks {
		if _, marked := markedTasks[token.Task]; !marked {
			kept[token.WorkerID]++
		}
	}
	capacity := make(map[uint16]int)
	for _, worker := range view.Workers {
		if worker.State != state.WorkerEligible {
			continue
		}
		residual := int64(worker.Slots) - int64(usedByOther[worker.NodeID]) - int64(kept[worker.NodeID])
		if residual > 0 {
			capacity[worker.NodeID] = int(residual)
		}
	}
	return capacity
}

// pickReplacementWorker chooses the lowest eligible node with residual
// capacity that is neither the invalidated incarnation nor the avoided node.
func pickReplacementWorker(view state.View, capacity map[uint16]int, marker state.NeedsReassignment, avoid uint16) (state.WorkerRecord, bool) {
	for _, worker := range view.Workers {
		if worker.State != state.WorkerEligible || capacity[worker.NodeID] <= 0 {
			continue
		}
		if worker.NodeID == avoid {
			continue
		}
		if worker.NodeID == marker.OldWorkerID && worker.Epoch == marker.OldWorkerEpoch {
			continue
		}
		return worker, true
	}
	return state.WorkerRecord{}, false
}

// pickSecondaryWorker chooses the lowest eligible node distinct from the
// primary that is not the invalidated incarnation. Result replicas consume
// durable storage, not execution slots.
func pickSecondaryWorker(view state.View, marker state.NeedsReassignment, primary uint16) (state.WorkerRecord, bool) {
	for _, worker := range view.Workers {
		if worker.State != state.WorkerEligible || worker.NodeID == primary {
			continue
		}
		if worker.NodeID == marker.OldWorkerID && worker.Epoch == marker.OldWorkerEpoch {
			continue
		}
		return worker, true
	}
	return state.WorkerRecord{}, false
}

// affectedForWorker mirrors the replicated invalidation computation: the
// sorted complete list of nonterminal assigned jobs on which the exact worker
// incarnation would gain new reassignment markers.
func affectedForWorker(view state.View, workerID uint16, epoch model.WorkerEpoch) []state.AffectedAssignment {
	var affected []state.AffectedAssignment
	for _, job := range view.Jobs {
		if job.Assignment == nil || terminalLifecycle(job.Lifecycle) {
			continue
		}
		markers := markersForWorkerSet(*job.Assignment, workerID, epoch)
		if len(markers) == 0 {
			continue
		}
		union := markerUnion(job.NeedsReassignment, markers)
		if len(union) == len(job.NeedsReassignment) {
			continue
		}
		affected = append(affected, state.AffectedAssignment{
			JobID: job.JobID, JobControlRevision: job.JobControlRevision,
			AssignmentRevision: job.Assignment.Revision, AssignmentDigest: job.Assignment.Digest,
		})
	}
	sort.Slice(affected, func(i, j int) bool {
		return bytes.Compare(affected[i].JobID[:], affected[j].JobID[:]) < 0
	})
	return affected
}

// markersForWorkerSet lists every task token and replica role held by the
// exact worker incarnation in one complete assignment set, sorted canonically.
func markersForWorkerSet(set model.AssignmentSet, workerID uint16, epoch model.WorkerEpoch) []state.NeedsReassignment {
	markers := make([]state.NeedsReassignment, 0)
	for _, token := range set.Tasks {
		if token.WorkerID == workerID && token.WorkerEpoch == epoch {
			markers = append(markers, state.NeedsReassignment{Kind: state.TaskTarget, Task: token.Task, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
	}
	for _, replica := range set.ResultReplicas {
		if replica.PrimaryNodeID == workerID && replica.PrimaryEpoch == epoch {
			markers = append(markers, state.NeedsReassignment{Kind: state.ResultReplicaTarget, SinkTask: replica.SinkTask, ReplicaRole: model.PrimaryReplica, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
		if replica.SecondaryNodeID == workerID && replica.SecondaryEpoch == epoch {
			markers = append(markers, state.NeedsReassignment{Kind: state.ResultReplicaTarget, SinkTask: replica.SinkTask, ReplicaRole: model.SecondaryReplica, OldWorkerID: workerID, OldWorkerEpoch: epoch})
		}
	}
	sort.Slice(markers, func(i, j int) bool { return markerBefore(markers[i], markers[j]) })
	return markers
}

// markerUnion mirrors the replicated sorted unique marker union.
func markerUnion(current, additional []state.NeedsReassignment) []state.NeedsReassignment {
	result := append(append([]state.NeedsReassignment(nil), current...), additional...)
	sort.Slice(result, func(i, j int) bool { return markerBefore(result[i], result[j]) })
	unique := result[:0]
	for _, marker := range result {
		if len(unique) == 0 || unique[len(unique)-1] != marker {
			unique = append(unique, marker)
		}
	}
	return unique
}

// markerBefore mirrors the replicated canonical marker ordering.
func markerBefore(left, right state.NeedsReassignment) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	leftTask, rightTask := left.Task, right.Task
	if left.Kind == state.ResultReplicaTarget {
		leftTask, rightTask = left.SinkTask, right.SinkTask
	}
	if comparison := bytes.Compare(taskOrderBytes(leftTask), taskOrderBytes(rightTask)); comparison != 0 {
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

// taskOrderBytes yields the canonical comparable task encoding.
func taskOrderBytes(task model.TaskID) []byte {
	encoded := append([]byte(nil), task.JobID[:]...)
	encoded = append(encoded, byte(task.StageID>>8), byte(task.StageID))
	return append(encoded, byte(task.Partition>>8), byte(task.Partition))
}
