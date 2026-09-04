package coordinator

import (
	"context"
	"errors"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/swim"
)

// reconcile performs one complete full-view pass in the exact required order:
// the cluster phase (worker registration, worker fences, status draining,
// failure resolution) followed by per-job convergence (reassignment,
// distribution, checkpoint notices, result repair, replay activation). The
// cluster phase gates admission-opening; job convergence does not.
func (actor *Actor) reconcile(ctx context.Context, epoch model.CoordinatorEpoch, session *sessionState) bool {
	clusterConverged := actor.reconcileCluster(ctx, epoch, session)
	jobsConverged := actor.reconcileJobs(ctx, epoch, session)
	return clusterConverged && jobsConverged
}

// reconcileCluster performs the cluster phase: worker registration, worker
// fences, status draining, and failure resolution. It reports whether the
// phase converged.
func (actor *Actor) reconcileCluster(ctx context.Context, epoch model.CoordinatorEpoch, session *sessionState) bool {
	converged := true
	session.controlFailed = make(map[uint16]bool)
	actor.registerWorkers(ctx, epoch, &converged)
	view := actor.options.Machine.View()
	reachable := actor.fenceWorkers(ctx, epoch, view, session, &converged)
	actor.drainWorkerEvents(ctx, reachable, session, &converged)
	actor.resolveWorkerFailures(ctx, epoch, session, &converged)
	return converged && ctx.Err() == nil
}

// reconcileJobs performs every per-job convergence pass and session pruning.
func (actor *Actor) reconcileJobs(ctx context.Context, epoch model.CoordinatorEpoch, session *sessionState) bool {
	converged := true
	view := actor.options.Machine.View()
	jobs := make([]model.JobID, 0, len(view.Jobs))
	for _, job := range view.Jobs {
		jobs = append(jobs, job.JobID)
	}
	for _, jobID := range jobs {
		actor.reconcileJob(ctx, epoch, jobID, session, &converged)
	}
	session.pruneJobs(view)
	return converged && ctx.Err() == nil
}

// registerWorkers scans the complete owned Alive/Suspect view in NodeID order
// and converges every replicated worker record through authenticated
// handshakes. An absent or failed handshake never creates a record.
func (actor *Actor) registerWorkers(ctx context.Context, epoch model.CoordinatorEpoch, converged *bool) {
	members := actor.options.Membership.View()
	for _, member := range members.Members {
		if ctx.Err() != nil {
			return
		}
		if !activeMemberStatus(member.Status) {
			continue
		}
		actor.registerWorker(ctx, epoch, member, converged)
	}
}

func (actor *Actor) registerWorker(ctx context.Context, epoch model.CoordinatorEpoch, member swim.Member, converged *bool) {
	identity, err := actor.options.Workers.Handshake(ctx, member)
	if err != nil {
		return
	}
	if verifyWorkerIdentity(member, identity) != nil {
		return
	}
	if !memberActive(actor.options.Membership.View(), member.NodeID) {
		return
	}
	view := actor.options.Machine.View()
	record, exists := findWorker(view, member.NodeID)
	switch {
	case !exists:
		actor.proposeRegisterWorker(ctx, epoch, identity, 0, converged)
	case record.Epoch == identity.WorkerEpoch:
		// A store-preserving same-epoch rejoin re-registers Eligible with a
		// higher revision. Eligible and Draining records are left untouched:
		// re-registration must never override Draining.
		if record.State == state.WorkerOffline {
			actor.proposeRegisterWorker(ctx, epoch, identity, record.Revision, converged)
		}
	default:
		actor.proposeReplaceWorkerEpoch(ctx, epoch, view, record, identity, converged)
	}
}

// verifyWorkerIdentity checks node identity, epoch validity, the nonzero exact
// compiled full Crane consensus fingerprint, registry equality, and slots.
func verifyWorkerIdentity(member swim.Member, identity WorkerIdentity) error {
	if identity.NodeID != member.NodeID {
		return ErrWorkerUnauthorized
	}
	if identity.WorkerEpoch.Validate() != nil {
		return ErrWorkerUnauthorized
	}
	if identity.ConsensusFingerprint == ([32]byte{}) || identity.ConsensusFingerprint != model.ConsensusFingerprint() {
		return ErrWorkerUnauthorized
	}
	if identity.RegistryFingerprint != model.RegistryFingerprint() {
		return ErrWorkerUnauthorized
	}
	if identity.Slots == 0 || uint64(identity.Slots) > model.LimitsV1().MaxWorkerSlots {
		return ErrWorkerUnauthorized
	}
	return nil
}

func (actor *Actor) proposeRegisterWorker(ctx context.Context, epoch model.CoordinatorEpoch, identity WorkerIdentity, expectedRevision uint64, converged *bool) {
	subject := state.SubjectKey{Kind: state.SubjectWorker, WorkerID: identity.NodeID}
	target := state.WorkerRecord{
		NodeID: identity.NodeID, Epoch: identity.WorkerEpoch, State: state.WorkerEligible,
		Revision: expectedRevision + 1, Slots: identity.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	id := internalCommandID(state.CommandRegisterWorker, epoch, subject, expectedRevision, identity.WorkerEpoch[:], uint16Bytes(identity.Slots))
	command, err := state.NewRegisterWorker(id, expectedRevision, target, epoch)
	if err != nil {
		return
	}
	if _, err := actor.proposeResolved(ctx, id, subject, command); err != nil {
		*converged = false
	}
}

func (actor *Actor) proposeReplaceWorkerEpoch(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, record state.WorkerRecord, identity WorkerIdentity, converged *bool) {
	subject := state.SubjectKey{Kind: state.SubjectWorker, WorkerID: record.NodeID}
	target := state.WorkerRecord{
		NodeID: record.NodeID, Epoch: identity.WorkerEpoch, State: record.State,
		Revision: record.Revision + 1, Slots: identity.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	affected := affectedForWorker(view, record.NodeID, record.Epoch)
	id := internalCommandID(state.CommandReplaceWorkerEpoch, epoch, subject, record.Revision, record.Epoch[:], identity.WorkerEpoch[:], uint16Bytes(identity.Slots))
	command, err := state.NewReplaceWorkerEpoch(id, record.Revision, record.NodeID, record.Epoch, target, affected, epoch)
	if err != nil {
		return
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	if err != nil {
		*converged = false
		return
	}
	if result.Code == state.ResultSuccess && record.State == state.WorkerOffline {
		// The replacement preserved Offline; revive the new incarnation.
		actor.proposeRegisterWorker(ctx, epoch, identity, record.Revision+1, converged)
	}
}

// fenceWorkers installs the leadership fence on every current non-Offline
// worker before any status, install, or repair command. Ambiguous fence
// outcomes are resolved by one idempotent retry; persistent failure marks the
// worker control-unreachable for the failure tracker.
func (actor *Actor) fenceWorkers(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, session *sessionState, converged *bool) map[uint16]bool {
	reachable := make(map[uint16]bool)
	for _, worker := range view.Workers {
		if ctx.Err() != nil {
			return reachable
		}
		if worker.State == state.WorkerOffline {
			continue
		}
		err := actor.options.Workers.Fence(ctx, worker.NodeID, epoch)
		if err != nil && ctx.Err() == nil {
			err = actor.options.Workers.Fence(ctx, worker.NodeID, epoch)
		}
		if err != nil {
			// Convergence for control-unreachable workers is decided by the
			// failure tracker, which may deactivate them this very pass.
			session.controlFailed[worker.NodeID] = true
			continue
		}
		reachable[worker.NodeID] = true
	}
	return reachable
}

// drainWorkerEvents drains every fenced worker's bounded event pages with
// explicit cursors, resolving each event's replicated effect before advancing
// the leader-local cursor.
func (actor *Actor) drainWorkerEvents(ctx context.Context, reachable map[uint16]bool, session *sessionState, converged *bool) {
	nodes := make([]uint16, 0, len(reachable))
	for node := range reachable {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	for _, node := range nodes {
		if ctx.Err() != nil {
			return
		}
		actor.drainWorker(ctx, node, session, converged)
	}
}

// drainWorker fully handles one worker's durable events. A transport failure
// feeds the failure tracker; a validation or handling failure only leaves the
// pass unconverged so the periodic rescan retries it. Each drained page also
// refreshes the session's observation of the worker's process admission gate.
func (actor *Actor) drainWorker(ctx context.Context, node uint16, session *sessionState, converged *bool) {
	if err := actor.pollWorkerEvents(ctx, node, session); err != nil {
		if errors.Is(err, ErrWorkerUnavailable) {
			session.controlFailed[node] = true
			return
		}
		*converged = false
	}
}

// reconcileJob converges one job: terminal propagation, initial scheduling,
// conditional reassignment, fenced distribution, checkpoint notices, result
// repair, and replay activation, in that order.
func (actor *Actor) reconcileJob(ctx context.Context, epoch model.CoordinatorEpoch, jobID model.JobID, session *sessionState, converged *bool) {
	if ctx.Err() != nil {
		return
	}
	view := actor.options.Machine.View()
	job, ok := findJob(view, jobID)
	if !ok {
		return
	}
	if terminalLifecycle(job.Lifecycle) {
		actor.propagateTerminal(ctx, epoch, job, session)
		return
	}
	if job.Lifecycle == state.JobPending && job.Assignment == nil {
		if !actor.scheduleJob(ctx, epoch, view, job) {
			*converged = false
			return
		}
		view = actor.options.Machine.View()
		job, ok = findJob(view, jobID)
		if !ok || job.Assignment == nil {
			*converged = false
			return
		}
	}
	if len(job.NeedsReassignment) > 0 {
		if !actor.replaceAssignments(ctx, epoch, view, job) {
			*converged = false
			return
		}
		view = actor.options.Machine.View()
		job, ok = findJob(view, jobID)
		if !ok || job.Assignment == nil || len(job.NeedsReassignment) > 0 {
			*converged = false
			return
		}
	}
	if job.Assignment == nil {
		*converged = false
		return
	}
	fence := jobFence{jobControlRevision: job.JobControlRevision, assignmentRevision: job.Assignment.Revision}
	if session.reconciled[jobID] == fence {
		// Convergence derives from worker-observed state, not committed
		// revisions alone: a same-epoch worker restart retains the durable
		// Running assignment but starts with the process admission gate
		// closed, so the cached fence alone proves nothing about execution.
		// Re-drive the idempotent Running install until every assigned worker
		// is observed admitted under this leadership epoch.
		if job.Lifecycle == state.JobRunning && !actor.assignmentAdmissionCurrent(epoch, job, session) {
			if !actor.activateJob(ctx, epoch, job) {
				*converged = false
			}
		}
		return
	}
	if job.Lifecycle == state.JobDeploying {
		if !actor.installAssignment(ctx, epoch, job, model.Closed, true, 0) {
			*converged = false
			return
		}
		if !actor.transitionJob(ctx, epoch, job, state.JobDeploying, state.JobRunning) {
			*converged = false
			return
		}
		view = actor.options.Machine.View()
		job, ok = findJob(view, jobID)
		if !ok || job.Assignment == nil {
			*converged = false
			return
		}
		fence = jobFence{jobControlRevision: job.JobControlRevision, assignmentRevision: job.Assignment.Revision}
	}
	if job.Lifecycle != state.JobRunning && job.Lifecycle != state.JobDraining {
		*converged = false
		return
	}
	if !actor.activateJob(ctx, epoch, job) {
		*converged = false
		return
	}
	session.reconciled[jobID] = fence
}

// assignmentAdmissionCurrent reports whether every current worker of one
// Running job's assignment was observed this leadership session with its
// process admission gate open under exactly the session's coordinator epoch.
// A missing observation (an unreachable or never-polled worker) or a closed
// gate (a same-epoch restart) leaves the job unconverged so the idempotent
// Running install is re-driven.
func (actor *Actor) assignmentAdmissionCurrent(epoch model.CoordinatorEpoch, job state.JobRecord, session *sessionState) bool {
	if job.Assignment == nil {
		return false
	}
	for _, node := range assignmentNodes(*job.Assignment) {
		if session.admission[node] != epoch {
			return false
		}
	}
	return true
}

// propagateTerminal durably installs the exact committed Closed/terminal job
// revision on every reachable current task and result worker. Failures retry
// idempotently on later passes and leadership changes without holding the
// admission gate closed.
func (actor *Actor) propagateTerminal(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord, session *sessionState) {
	if job.Assignment == nil {
		return
	}
	fence := jobFence{jobControlRevision: job.JobControlRevision, assignmentRevision: job.Assignment.Revision}
	if session.reconciled[job.JobID] == fence {
		return
	}
	install, ok := actor.buildInstall(job, model.Closed, epoch)
	if !ok {
		return
	}
	progress := session.terminal[job.JobID]
	if progress.fence != fence || progress.nodes == nil {
		progress = terminalProgress{fence: fence, nodes: make(map[uint16]bool)}
	}
	members := actor.options.Membership.View()
	complete := true
	for _, node := range assignmentNodes(*job.Assignment) {
		if progress.nodes[node] {
			continue
		}
		if !memberActive(members, node) {
			// A worker that is not currently reachable has not confirmed
			// the terminal install; it is retried on a later pass once it
			// is Alive again instead of being silently dropped for the
			// rest of the session (a sealed-result replica that missed its
			// re-install could otherwise never serve fetches until the next
			// leadership change).
			complete = false
			continue
		}
		if err := actor.options.Workers.Install(ctx, node, install); err != nil {
			complete = false
			continue
		}
		progress.nodes[node] = true
	}
	session.terminal[job.JobID] = progress
	if complete {
		session.reconciled[job.JobID] = fence
	}
}

// activateJob closes the fenced set on every current worker, resends the
// committed checkpoint notices, verifies and repairs the covered result
// replicas, and only then installs the exact committed scheduling state so
// source emission and replay above committed watermarks may resume.
func (actor *Actor) activateJob(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord) bool {
	// A Running job is re-fenced through Closed before re-verification. A
	// Draining job is re-fenced through the Draining install itself: its
	// workers durably hold Draining at the lifecycle-bumped
	// JobControlRevision, and the durable store admits exactly the
	// Closed<->Running admission progressions at an equal fence (Task 24
	// defect #1 ruling), so a Closed pre-install would be rejected as
	// identity reuse and the job could never be re-driven. Installing
	// Draining first also carries the current JobControlRevision (and a new
	// leadership epoch's rebind) to every worker before the checkpoint
	// notices that are validated against it. Draining sources sit at EOF,
	// so no producer can mutate during the verification.
	scheduling, fence := model.Running, model.Closed
	if job.Lifecycle == state.JobDraining {
		scheduling, fence = model.Draining, model.Draining
	}
	if !actor.installAssignment(ctx, epoch, job, fence, true, 0) {
		return false
	}
	if !actor.resendCheckpointNotices(ctx, job) {
		return false
	}
	if !actor.repairResults(ctx, epoch, job) {
		return false
	}
	if scheduling == fence {
		return true
	}
	return actor.installAssignment(ctx, epoch, job, scheduling, true, 0)
}

// scheduleJob commits every deterministic source EOF and the initial complete
// assignment set for one committed pending job.
func (actor *Actor) scheduleJob(ctx context.Context, epoch model.CoordinatorEpoch, _ state.View, job state.JobRecord) bool {
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return false
	}
	for _, source := range sourceTasks(topology, job.JobID) {
		if _, ok := job.SourceEOFs[source]; ok {
			continue
		}
		eof, eofErr := model.SourceEOF(topology, source)
		if eofErr != nil {
			return false
		}
		subject := state.SubjectKey{Kind: state.SubjectSourceEOF, JobID: job.JobID, TaskID: source}
		id := internalCommandID(state.CommandRecordSourceEOF, epoch, subject, 0, uint64Bytes(eof))
		command, commandErr := state.NewRecordSourceEOF(id, 0, source, eof, epoch)
		if commandErr != nil {
			return false
		}
		if _, proposeErr := actor.proposeResolved(ctx, id, subject, command); proposeErr != nil {
			return false
		}
	}
	view := actor.options.Machine.View()
	job, ok := findJob(view, job.JobID)
	if !ok || job.Assignment != nil {
		return ok
	}
	placements := eligiblePlacements(view, job.JobID)
	if len(placements) == 0 {
		return false
	}
	assignment, err := model.BuildAssignmentSet(job.JobID, job.TopologyDigest, 1, topology, placements)
	if err != nil {
		return false
	}
	subject := state.SubjectKey{Kind: state.SubjectJobControl, JobID: job.JobID}
	id := internalCommandID(state.CommandInstallAssignments, epoch, subject, job.JobControlRevision, assignment.Digest[:])
	command, err := state.NewInstallAssignments(id, job.JobControlRevision, assignment, epoch)
	if err != nil {
		return false
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	return err == nil && result.Code == state.ResultSuccess
}

// transitionJob applies one conditional legal lifecycle edge.
func (actor *Actor) transitionJob(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord, from, to state.JobLifecycle) bool {
	subject := state.SubjectKey{Kind: state.SubjectJobControl, JobID: job.JobID}
	id := internalCommandID(state.CommandTransitionJob, epoch, subject, job.JobControlRevision, []byte{byte(from), byte(to)})
	command, err := state.NewTransitionJob(id, job.JobControlRevision, job.JobID, from, to, epoch)
	if err != nil {
		return false
	}
	result, err := actor.proposeResolved(ctx, id, subject, command)
	return err == nil && result.Code == state.ResultSuccess
}

// installAssignment installs one fenced assignment set at one scheduling
// state on the job's distinct current workers. With mustReachAll, an inactive
// current worker fails the installation; otherwise inactive workers are
// skipped and only attempted installs must acknowledge. skip excludes one
// invalidated worker from distribution.
func (actor *Actor) installAssignment(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord, scheduling model.SchedulingState, mustReachAll bool, skip uint16) bool {
	install, ok := actor.buildInstall(job, scheduling, epoch)
	if !ok {
		return false
	}
	members := actor.options.Membership.View()
	success := true
	for _, node := range assignmentNodes(*job.Assignment) {
		if node == skip {
			continue
		}
		if !memberActive(members, node) {
			if mustReachAll {
				success = false
			}
			continue
		}
		if err := actor.options.Workers.Install(ctx, node, install); err != nil {
			success = false
		}
	}
	return success
}

// buildInstall assembles one complete fenced AssignmentSetInstall.
func (actor *Actor) buildInstall(job state.JobRecord, scheduling model.SchedulingState, epoch model.CoordinatorEpoch) (protocol.AssignmentSetInstall, bool) {
	if job.Assignment == nil {
		return protocol.AssignmentSetInstall{}, false
	}
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return protocol.AssignmentSetInstall{}, false
	}
	return protocol.AssignmentSetInstall{
		Assignment: *job.Assignment, Specification: topology.Spec(), SpecificationDigest: job.TopologyDigest,
		JobControlRevision: job.JobControlRevision, SchedulingState: scheduling, CoordinatorEpoch: epoch,
	}, true
}

// resendCheckpointNotices redelivers the committed checkpoint watermark of
// every source to every current worker of the job through the validated
// committed-notice path.
func (actor *Actor) resendCheckpointNotices(ctx context.Context, job state.JobRecord) bool {
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return false
	}
	appliedIndex := actor.options.Machine.View().AppliedIndex
	if appliedIndex == 0 {
		return false
	}
	for _, entry := range checkpointVector(topology, job) {
		if err := actor.ApplyCommittedCheckpoint(ctx, job.JobID, entry.Source, entry.Watermark, appliedIndex); err != nil {
			return false
		}
	}
	return true
}

// repairResults verifies every assigned sink's covered result inventory on
// both current replicas and performs record-level repair where a copy is
// absent or differs. The admission gate never opens until every sink's two
// current replica summaries match the expected covered set.
func (actor *Actor) repairResults(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord) bool {
	topology, err := model.DecodeTopology(job.TopologyBytes)
	if err != nil {
		return false
	}
	vector := checkpointVector(topology, job)
	view := actor.options.Machine.View()
	set := *job.Assignment
	for _, replica := range set.ResultReplicas {
		if !actor.repairSink(ctx, epoch, view, job, set, replica, vector) {
			return false
		}
	}
	return true
}

// repairEndpoint identifies one exact worker incarnation.
type repairEndpoint struct {
	node  uint16
	epoch model.WorkerEpoch
}

func (actor *Actor) repairSink(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, job state.JobRecord, set model.AssignmentSet, replica model.ResultReplicaSet, vector []protocol.SourceCheckpoint) bool {
	query := buildInventoryQuery(job, set, replica.SinkTask, vector)
	primary := repairEndpoint{node: replica.PrimaryNodeID, epoch: replica.PrimaryEpoch}
	secondary := repairEndpoint{node: replica.SecondaryNodeID, epoch: replica.SecondaryEpoch}
	primarySummary, primaryErr := actor.queryInventory(ctx, epoch, primary.node, query)
	secondarySummary, secondaryErr := actor.queryInventory(ctx, epoch, secondary.node, query)
	if primaryErr != nil || secondaryErr != nil {
		return false
	}
	if summariesEqual(primarySummary, secondarySummary) {
		if primarySummary.RecordCount == 0 && coveredWatermarkNonzero(vector) {
			return actor.scanRetainedHolders(ctx, epoch, view, job, set, replica, vector, query, primary, secondary)
		}
		return true
	}
	primaryHolds := primarySummary.RecordCount > 0
	secondaryHolds := secondarySummary.RecordCount > 0
	switch {
	case primaryHolds && secondaryHolds:
		// Multiple disagreeing survivors leave admission closed.
		return false
	case primaryHolds:
		return actor.repairAndVerify(ctx, epoch, job, replica, vector, query, primary, []repairEndpoint{secondary}, primarySummary)
	case secondaryHolds:
		return actor.repairAndVerify(ctx, epoch, job, replica, vector, query, secondary, []repairEndpoint{primary}, secondarySummary)
	default:
		return false
	}
}

// scanRetainedHolders scans every replicated registered worker in NodeID
// order for retained old-provenance inventory so actor restart needs no local
// history. Disagreeing holders leave admission closed; an agreed empty state
// on both current replicas stands when nobody retains records.
func (actor *Actor) scanRetainedHolders(ctx context.Context, epoch model.CoordinatorEpoch, view state.View, job state.JobRecord, set model.AssignmentSet, replica model.ResultReplicaSet, vector []protocol.SourceCheckpoint, query protocol.ResultInventoryQuery, primary, secondary repairEndpoint) bool {
	install, ok := actor.buildInstall(job, model.Closed, epoch)
	if !ok {
		return false
	}
	members := actor.options.Membership.View()
	var holderSummary *protocol.ResultInventorySummary
	var holder repairEndpoint
	for _, worker := range view.Workers {
		if worker.NodeID == primary.node || worker.NodeID == secondary.node || worker.State == state.WorkerOffline {
			continue
		}
		if !memberActive(members, worker.NodeID) {
			continue
		}
		// The candidate must hold the current fenced assignment before it may
		// answer; only retained old-provenance holders accept this install.
		if err := actor.options.Workers.Install(ctx, worker.NodeID, install); err != nil {
			continue
		}
		summary, err := actor.queryInventory(ctx, epoch, worker.NodeID, query)
		if err != nil || summary.RecordCount == 0 {
			continue
		}
		if holderSummary != nil && !summariesEqual(*holderSummary, summary) {
			return false
		}
		if holderSummary == nil {
			copied := summary
			holderSummary = &copied
			holder = repairEndpoint{node: worker.NodeID, epoch: worker.Epoch}
		}
	}
	if holderSummary == nil {
		return true
	}
	return actor.repairAndVerify(ctx, epoch, job, replica, vector, query, holder, []repairEndpoint{primary, secondary}, *holderSummary)
}

// repairAndVerify repairs every listed destination from one surviving
// authorized copy, then re-queries both current distinct replicas and demands
// the expected covered summary from each.
func (actor *Actor) repairAndVerify(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord, replica model.ResultReplicaSet, vector []protocol.SourceCheckpoint, query protocol.ResultInventoryQuery, source repairEndpoint, destinations []repairEndpoint, expected protocol.ResultInventorySummary) bool {
	for _, destination := range destinations {
		if !actor.executeRepair(ctx, epoch, job, replica.SinkTask, vector, query, source, destination, expected) {
			return false
		}
	}
	primarySummary, primaryErr := actor.queryInventory(ctx, epoch, replica.PrimaryNodeID, query)
	secondarySummary, secondaryErr := actor.queryInventory(ctx, epoch, replica.SecondaryNodeID, query)
	return primaryErr == nil && secondaryErr == nil &&
		summariesEqual(primarySummary, expected) && summariesEqual(secondarySummary, expected)
}

// executeRepair installs the identical deterministic grant on the destination
// first and then the source, then polls both durable statuses to completion.
func (actor *Actor) executeRepair(ctx context.Context, epoch model.CoordinatorEpoch, job state.JobRecord, sink model.TaskID, vector []protocol.SourceCheckpoint, query protocol.ResultInventoryQuery, source, destination repairEndpoint, expected protocol.ResultInventorySummary) bool {
	set := *job.Assignment
	instruction := protocol.RepairResultPartition{
		CoordinatorEpoch: epoch, JobID: job.JobID,
		AssignmentRevision: set.Revision, AssignmentDigest: set.Digest,
		SourceNodeID: source.node, SourceWorkerEpoch: source.epoch,
		DestinationNodeID: destination.node, DestinationWorkerEpoch: destination.epoch,
		SinkTask: sink, SpecificationHash: job.TopologyDigest,
		Checkpoints: append([]protocol.SourceCheckpoint(nil), vector...), CheckpointDigest: protocol.CheckpointVectorDigest(vector),
		InventoryQueryDigest: query.QueryDigest,
		ExpectedRecordCount:  expected.RecordCount, ExpectedTotalBytes: expected.TotalBytes, ExpectedContentDigest: expected.ContentDigest,
	}
	instruction.RepairID = protocol.DeriveRepairID(instruction)
	instruction.InstructionDigest = protocol.RepairInstructionDigest(instruction)

	destinationStatus, err := actor.sendRepairGrant(ctx, epoch, destination.node, protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairDestination})
	if err != nil {
		return false
	}
	sourceStatus, err := actor.sendRepairGrant(ctx, epoch, source.node, protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairSource})
	if err != nil {
		return false
	}
	for poll := 0; poll < repairPollLimit; poll++ {
		if destinationStatus.State == protocol.RepairFailed || sourceStatus.State == protocol.RepairFailed {
			return false
		}
		if destinationStatus.State == protocol.RepairComplete && sourceStatus.State == protocol.RepairComplete {
			return destinationStatus.RecordCount == expected.RecordCount &&
				destinationStatus.TotalBytes == expected.TotalBytes &&
				destinationStatus.ContentDigest == expected.ContentDigest
		}
		destinationStatus, err = actor.sendRepairGrant(ctx, epoch, destination.node, protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairDestination})
		if err != nil {
			return false
		}
		sourceStatus, err = actor.sendRepairGrant(ctx, epoch, source.node, protocol.RepairGrant{Instruction: instruction, Role: protocol.RepairSource})
		if err != nil {
			return false
		}
	}
	return false
}

// sendRepairGrant delivers one idempotent grant and returns its durable state.
func (actor *Actor) sendRepairGrant(ctx context.Context, epoch model.CoordinatorEpoch, node uint16, grant protocol.RepairGrant) (protocol.ResultRepairStatus, error) {
	request := protocol.WorkerStatusRequest{CoordinatorEpoch: epoch, MaxEvents: 1, Repair: &grant}
	status, err := actor.options.Workers.Status(ctx, node, request)
	if err != nil {
		return protocol.ResultRepairStatus{}, err
	}
	if status.Repair == nil {
		return protocol.ResultRepairStatus{}, ErrWorkerUnavailable
	}
	return *status.Repair, nil
}

// queryInventory performs one query-bound inventory exchange with one worker.
func (actor *Actor) queryInventory(ctx context.Context, epoch model.CoordinatorEpoch, node uint16, query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
	request := protocol.WorkerStatusRequest{CoordinatorEpoch: epoch, MaxEvents: 1, Inventory: &query}
	status, err := actor.options.Workers.Status(ctx, node, request)
	if err != nil {
		return protocol.ResultInventorySummary{}, err
	}
	if status.Inventory == nil || status.Inventory.QueryDigest != query.QueryDigest {
		return protocol.ResultInventorySummary{}, ErrWorkerUnavailable
	}
	return *status.Inventory, nil
}

// buildInventoryQuery binds one sink's covered checkpoint vector.
func buildInventoryQuery(job state.JobRecord, set model.AssignmentSet, sink model.TaskID, vector []protocol.SourceCheckpoint) protocol.ResultInventoryQuery {
	query := protocol.ResultInventoryQuery{
		JobID: job.JobID, SinkTask: sink, SpecificationHash: job.TopologyDigest,
		AssignmentRevision: set.Revision, AssignmentDigest: set.Digest,
		Checkpoints:      append([]protocol.SourceCheckpoint(nil), vector...),
		CheckpointDigest: protocol.CheckpointVectorDigest(vector),
	}
	query.QueryDigest = protocol.InventoryQueryDigest(query)
	return query
}

// summariesEqual compares the query-bound count, bytes, and content digests.
func summariesEqual(left, right protocol.ResultInventorySummary) bool {
	return left.RecordCount == right.RecordCount && left.TotalBytes == right.TotalBytes && left.ContentDigest == right.ContentDigest
}

// coveredWatermarkNonzero reports whether any committed watermark covers data.
func coveredWatermarkNonzero(vector []protocol.SourceCheckpoint) bool {
	for _, entry := range vector {
		if entry.Watermark > 0 {
			return true
		}
	}
	return false
}

// checkpointVector builds the exact bounded, sorted committed checkpoint
// vector across every source of the job; sources without a committed record
// carry the zero watermark.
func checkpointVector(topology model.ValidatedTopology, job state.JobRecord) []protocol.SourceCheckpoint {
	vector := make([]protocol.SourceCheckpoint, 0)
	for _, source := range sourceTasks(topology, job.JobID) {
		vector = append(vector, protocol.SourceCheckpoint{Source: source, Watermark: job.Checkpoints[source].Watermark})
	}
	return vector
}

// sourceTasks lists every source task of one topology in canonical order.
func sourceTasks(topology model.ValidatedTopology, job model.JobID) []model.TaskID {
	tasks := make([]model.TaskID, 0)
	for _, stage := range topology.Spec().Stages {
		if stage.Role != model.StageSource {
			continue
		}
		for partition := uint16(0); partition < stage.Parallelism; partition++ {
			tasks = append(tasks, model.TaskID{JobID: job, StageID: stage.StageID, Partition: partition})
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].StageID != tasks[j].StageID {
			return tasks[i].StageID < tasks[j].StageID
		}
		return tasks[i].Partition < tasks[j].Partition
	})
	return tasks
}

// assignmentNodes lists the distinct sorted task and replica worker nodes.
func assignmentNodes(set model.AssignmentSet) []uint16 {
	seen := make(map[uint16]struct{})
	for _, token := range set.Tasks {
		seen[token.WorkerID] = struct{}{}
	}
	for _, replica := range set.ResultReplicas {
		seen[replica.PrimaryNodeID] = struct{}{}
		seen[replica.SecondaryNodeID] = struct{}{}
	}
	nodes := make([]uint16, 0, len(seen))
	for node := range seen {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// eligiblePlacements mirrors the replicated residual-capacity computation:
// task-execution capacity of every Eligible worker after subtracting every
// token in every other nonterminal job.
func eligiblePlacements(view state.View, exclude model.JobID) []model.WorkerPlacement {
	used := make(map[uint16]uint64)
	for _, job := range view.Jobs {
		if job.JobID == exclude || terminalLifecycle(job.Lifecycle) || job.Assignment == nil {
			continue
		}
		for _, token := range job.Assignment.Tasks {
			used[token.WorkerID]++
		}
	}
	placements := make([]model.WorkerPlacement, 0, len(view.Workers))
	for _, worker := range view.Workers {
		if worker.State != state.WorkerEligible {
			continue
		}
		if used[worker.NodeID] >= uint64(worker.Slots) {
			continue
		}
		placements = append(placements, model.WorkerPlacement{
			NodeID: worker.NodeID, WorkerEpoch: worker.Epoch,
			SlotCapacity: worker.Slots - uint16(used[worker.NodeID]),
		})
	}
	return placements
}

func uint16Bytes(value uint16) []byte {
	return []byte{byte(value >> 8), byte(value)}
}

func uint64Bytes(value uint64) []byte {
	return []byte{
		byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32),
		byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value),
	}
}
