package coordinator

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/swim"
)

// failWorker scripts one node as continuously unreachable over worker control.
func (h *harness) failWorker(node uint16) {
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.handshakeErr = errors.New("worker unreachable")
	script.fenceErr = errors.New("worker unreachable")
	script.statusErr = errors.New("worker unreachable")
	h.workers.mu.Unlock()
}

func (h *harness) reviveWorker(node uint16) {
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.handshakeErr = nil
	script.fenceErr = nil
	script.statusErr = nil
	h.workers.mu.Unlock()
}

func TestSuspectAloneNeverStartsReassignment(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	h.setMemberStatus(3, swim.Suspect)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	h.clk.Advance(time.Second)
	h.actor.Wake()
	// pass-exchange elimination: settled workers are no longer re-fenced
	// each pass, so the second pass over the suspect member is pinned by
	// its event drain instead of its fence dial.
	h.waitFor(func() bool { return h.log.count("status:3") >= 2 }, "second pass over suspect worker")
	if h.log.contains("propose:deactivate") || h.log.contains("propose:replace-assignments") {
		t.Fatalf("suspicion alone drove reassignment: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	if record.Assignment.Revision != assignment.Revision || len(record.NeedsReassignment) != 0 {
		t.Fatalf("suspect mutated placement: %#v", record)
	}
	worker, _ := h.workerRecord(3)
	if worker.State != state.WorkerEligible {
		t.Fatalf("suspect worker state = %#v", worker)
	}
}

func TestFailureGraceDeactivatesAndRecomputesAssignments(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	// A spare eligible worker that holds no duties for the seeded job.
	h.seedWorker(4, model.WorkerEpoch{4}, 4)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	h.setMemberStatus(3, swim.Dead)
	h.failWorker(3)

	h.start()
	h.markReady()
	h.lead(2)
	// The first pass observed the control failure and moved on to job
	// distribution; the grace period is measured from that observation.
	h.waitFor(func() bool { return h.log.count("install:2:closed") >= 1 }, "first pass past the failure tracker")
	if h.log.contains("propose:deactivate") {
		t.Fatal("deactivation before the grace period")
	}
	h.clk.Advance(150 * time.Millisecond)
	h.actor.Wake()
	h.waitFor(func() bool {
		record, ok := h.workerRecord(3)
		return ok && record.State == state.WorkerOffline
	}, "worker deactivated")
	h.waitFor(func() bool {
		record, ok := h.job(job)
		return ok && record.Assignment.Revision == assignment.Revision+1 && len(record.NeedsReassignment) == 0
	}, "assignments recomputed")
	h.waitGateOpen()

	// Healthy receivers are closed before the conditional deactivation commits.
	assertSubsequence(t, h.log.snapshot(), "install:2:closed", "propose:deactivate", "propose:replace-assignments")
	record, _ := h.job(job)
	for _, token := range record.Assignment.Tasks {
		if token.WorkerID == 3 {
			t.Fatalf("failed worker retained a task: %#v", token)
		}
	}
	for _, replica := range record.Assignment.ResultReplicas {
		if replica.PrimaryNodeID == 3 || replica.SecondaryNodeID == 3 {
			t.Fatalf("failed worker retained a replica: %#v", replica)
		}
	}
	worker, _ := h.workerRecord(3)
	if worker.Epoch != (model.WorkerEpoch{3}) {
		t.Fatalf("deactivation fenced the wrong epoch: %#v", worker)
	}
}

func TestFailureRefutationBeforeCommitCancelsDeactivation(t *testing.T) {
	h, _, _, _ := runningHarness(t)
	h.seedWorker(4, model.WorkerEpoch{4}, 4)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	h.setMemberStatus(3, swim.Dead)
	h.failWorker(3)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { return h.log.count("install:2:closed") >= 1 }, "failure observed by a complete pass")

	// The worker refutes its death before the grace period elapses.
	h.setMemberStatus(3, swim.Alive)
	h.reviveWorker(3)
	h.clk.Advance(150 * time.Millisecond)
	h.actor.Wake()
	h.waitGateOpen()
	if h.log.contains("propose:deactivate") {
		t.Fatalf("refuted worker was deactivated: %v", h.log.snapshot())
	}
	record, _ := h.workerRecord(3)
	if record.State != state.WorkerEligible {
		t.Fatalf("refuted worker state = %#v", record)
	}
}

func TestAmbiguousDeactivationResolvedByBarrierAndView(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	h.seedWorker(4, model.WorkerEpoch{4}, 4)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	h.setMemberStatus(3, swim.Dead)
	h.failWorker(3)
	injected := false
	h.raft.setProposeHook(func(command any) (bool, error) {
		if _, ok := command.(state.DeactivateWorker); ok && !injected {
			injected = true
			return true, errors.New("injected ambiguous deactivation")
		}
		return true, nil
	})

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { return h.log.count("install:2:closed") >= 1 }, "failure observed by a complete pass")
	h.clk.Advance(150 * time.Millisecond)
	h.actor.Wake()
	h.waitFor(func() bool {
		record, ok := h.workerRecord(3)
		return ok && record.State == state.WorkerOffline
	}, "worker deactivated")
	h.waitFor(func() bool {
		record, ok := h.job(job)
		return ok && record.Assignment.Revision == assignment.Revision+1
	}, "recomputation after resolution")
	if got := h.log.count("propose:deactivate"); got != 1 {
		t.Fatalf("deactivation proposed %d times: %v", got, h.log.snapshot())
	}
	worker, _ := h.workerRecord(3)
	if worker.Revision != 2 {
		t.Fatalf("deactivation applied more than once: %#v", worker)
	}
}

// secondaryOnlyPlacement finds worker epochs whose deterministic placement
// leaves the secondary result replica with no task token.
func secondaryOnlyPlacement(t *testing.T) ([3]model.WorkerEpoch, uint16) {
	t.Helper()
	topology, err := model.ValidateTopology(testTopologySpec(1))
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	request := model.ClientRequestID{ClientID: model.ClientID{0x51}, Sequence: 1}
	job := model.DeriveJobID(request, topology.Digest())
	for variant := byte(0); variant < 32; variant++ {
		epochs := [3]model.WorkerEpoch{{2, variant}, {3, variant}, {4, variant}}
		placements := []model.WorkerPlacement{
			{NodeID: 2, WorkerEpoch: epochs[0], SlotCapacity: 4},
			{NodeID: 3, WorkerEpoch: epochs[1], SlotCapacity: 4},
			{NodeID: 4, WorkerEpoch: epochs[2], SlotCapacity: 4},
		}
		assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, placements)
		if err != nil {
			t.Fatalf("build assignment: %v", err)
		}
		secondary := assignment.ResultReplicas[0].SecondaryNodeID
		holdsToken := false
		for _, token := range assignment.Tasks {
			if token.WorkerID == secondary {
				holdsToken = true
			}
		}
		if !holdsToken {
			return epochs, secondary
		}
	}
	t.Skip("no epoch variant produced a token-free secondary")
	return [3]model.WorkerEpoch{}, 0
}

func TestSecondaryOnlyFailureReplacesReplicaWithoutTouchingAttempts(t *testing.T) {
	epochs, secondary := secondaryOnlyPlacement(t)
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, epochs[0], 4)
	h.seedWorker(3, epochs[1], 4)
	h.seedWorker(4, epochs[2], 4)
	h.addWorkerMember(2, epochs[0], 4)
	h.addWorkerMember(3, epochs[1], 4)
	h.addWorkerMember(4, epochs[2], 4)
	job, _, assignment := h.seedRunningJob(1)
	if assignment.ResultReplicas[0].SecondaryNodeID != secondary {
		t.Fatalf("placement drifted: %#v", assignment.ResultReplicas)
	}
	h.setMemberStatus(secondary, swim.Dead)
	h.failWorker(secondary)

	h.start()
	h.markReady()
	h.lead(2)
	survivor := uint16(2)
	if secondary == survivor {
		survivor = 3
	}
	h.waitFor(func() bool { return h.log.count(fmt.Sprintf("install:%d:closed", survivor)) >= 1 }, "first pass past the failure tracker")
	h.clk.Advance(150 * time.Millisecond)
	h.actor.Wake()
	h.waitFor(func() bool {
		record, ok := h.job(job)
		return ok && record.Assignment.Revision == assignment.Revision+1
	}, "replica replaced")
	record, _ := h.job(job)
	if !reflect.DeepEqual(stripRevision(record.Assignment.Tasks), stripRevision(assignment.Tasks)) {
		t.Fatalf("task attempts changed: %#v vs %#v", record.Assignment.Tasks, assignment.Tasks)
	}
	replica := record.Assignment.ResultReplicas[0]
	if replica.SecondaryNodeID == secondary {
		t.Fatalf("secondary not replaced: %#v", replica)
	}
	if replica.PrimaryNodeID != assignment.ResultReplicas[0].PrimaryNodeID {
		t.Fatalf("primary changed without a marker: %#v", replica)
	}
}

// stripRevision compares tokens ignoring only the set revision they carry.
func stripRevision(tokens []model.AssignmentToken) []model.AssignmentToken {
	result := append([]model.AssignmentToken(nil), tokens...)
	for index := range result {
		result[index].AssignmentRevision = 0
	}
	return result
}

func TestNextLeaderCompletesReplacementAfterReplaceWorkerEpochCrash(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	// The previous leader crashed immediately after ReplaceWorkerEpoch: the
	// markers are committed but no ReplaceAssignments followed.
	view := h.view()
	oldEpoch := model.WorkerEpoch{3}
	newEpoch := model.WorkerEpoch{3, 0xB}
	affected := affectedForWorker(view, 3, oldEpoch)
	target := state.WorkerRecord{
		NodeID: 3, Epoch: newEpoch, State: state.WorkerEligible, Revision: 2, Slots: 4,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-replace", job[:]), 1, 3, oldEpoch, target, affected, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed replace epoch: %v", err)
	}
	h.raft.applySeed(t, replace)
	seeded, _ := h.job(job)
	if len(seeded.NeedsReassignment) == 0 {
		t.Fatalf("seed produced no markers: %#v", seeded)
	}
	h.addWorkerMember(3, newEpoch, 4)

	h.start()
	h.markReady()
	h.lead(2)
	// gate decoupling: the gate opens before the replacement converges, so
	// wait for the pass's final artifact — the completed replacement and the
	// Running installs that follow it.
	h.waitFor(func() bool {
		record, ok := h.job(job)
		return ok && len(record.NeedsReassignment) == 0 && record.Assignment != nil &&
			record.Assignment.Revision == assignment.Revision+1 &&
			len(h.workers.installsFor(2, model.Running)) > 0 && len(h.workers.installsFor(3, model.Running)) > 0
	}, "markers cleared by conditional replacement")
	record, _ := h.job(job)
	if len(record.NeedsReassignment) != 0 || record.Assignment.Revision != assignment.Revision+1 {
		t.Fatalf("markers not cleared by conditional replacement: %#v", record)
	}
	for _, token := range record.Assignment.Tasks {
		if token.WorkerEpoch == oldEpoch {
			t.Fatalf("stale incarnation retained: %#v", token)
		}
	}
	assertSubsequence(t, h.log.snapshot(),
		"propose:replace-assignments",
		"install:2:closed", "install:3:closed",
		"install:2:running", "install:3:running",
	)
}

func TestSameEpochRejoinRegistersEligibleWithHigherRevision(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	view := h.view()
	deactivate, err := state.NewDeactivateWorker(testCommandID("seed-deactivate", []byte{3}), 1, 3, model.WorkerEpoch{3}, nil, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed deactivate: %v", err)
	}
	h.raft.applySeed(t, deactivate)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		record, ok := h.workerRecord(3)
		return ok && record.State == state.WorkerEligible
	}, "offline worker revived")
	record, _ := h.workerRecord(3)
	if record.Revision != 3 || record.Epoch != (model.WorkerEpoch{3}) {
		t.Fatalf("rejoin record = %#v", record)
	}
	if !h.log.contains("propose:register") {
		t.Fatalf("rejoin without fresh registration: %v", h.log.snapshot())
	}
	assertSubsequence(t, h.log.snapshot(), "handshake:3", "propose:register")
}

func TestReRegistrationNeverOverridesDraining(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	view := h.view()
	drain, err := state.NewDrainWorker(testCommandID("seed-drain", []byte{2}), 1, 2, model.WorkerEpoch{2}, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed drain: %v", err)
	}
	h.raft.applySeed(t, drain)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	record, _ := h.workerRecord(2)
	if record.State != state.WorkerDraining || record.Revision != 2 {
		t.Fatalf("draining overridden: %#v", record)
	}
	if h.log.contains("propose:register") {
		t.Fatalf("draining worker re-registered: %v", h.log.snapshot())
	}

	// A restarted incarnation replaces the epoch but preserves Draining.
	// pass-exchange elimination: a fresh worker store means a fresh storage
	// directory and therefore a fresh durable SWIM incarnation, so the
	// restart is modeled with the incarnation bump the membership view
	// observes; the settled-identity handshake skip relies on that delta.
	h.addWorkerMember(2, model.WorkerEpoch{2, 0xB}, 4)
	h.members.setMember(swim.Member{NodeID: 2, Host: "127.0.0.1", BasePort: 9000, Incarnation: 2, Status: swim.Alive})
	h.actor.Wake()
	h.waitFor(func() bool {
		record, ok := h.workerRecord(2)
		return ok && record.Epoch == (model.WorkerEpoch{2, 0xB})
	}, "epoch replaced")
	record, _ = h.workerRecord(2)
	if record.State != state.WorkerDraining {
		t.Fatalf("replacement overrode draining: %#v", record)
	}
	if h.log.contains("propose:register") {
		t.Fatalf("draining replacement registered eligible: %v", h.log.snapshot())
	}
}

func TestTerminalPropagationInstallsClosedIdempotently(t *testing.T) {
	h, job, _, _ := runningHarness(t)
	view := h.view()
	cancel, err := state.NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0x52}, Sequence: 1}, job, 3, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed cancel: %v", err)
	}
	h.raft.applySeed(t, cancel)
	script := h.workers.script(2)
	h.workers.mu.Lock()
	script.installErrs = 1
	h.workers.mu.Unlock()

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { return h.log.count("install:2:closed") >= 1 }, "first terminal install attempt")
	h.actor.Wake()
	h.waitFor(func() bool { return len(h.workers.installsFor(2, model.Closed)) >= 1 }, "lost acknowledgment retried")
	// The retry re-drives only the worker whose acknowledgment was lost; the
	// worker that already confirmed the identical terminal install is not
	// asked to commit it again within the same leadership session.
	if got := len(h.workers.installsFor(3, model.Closed)); got != 1 {
		t.Fatalf("converged worker 3 received %d terminal installs during the retry, want 1", got)
	}

	record, _ := h.job(job)
	if record.Lifecycle != state.JobCanceled {
		t.Fatalf("job lifecycle = %#v", record)
	}
	for node := uint16(2); node <= 3; node++ {
		installs := h.workers.installsFor(node, model.Closed)
		if len(installs) == 0 {
			t.Fatalf("worker %d missing terminal install", node)
		}
		last := installs[len(installs)-1]
		if last.JobControlRevision != record.JobControlRevision || last.SchedulingState != model.Closed {
			t.Fatalf("terminal install fence = %#v want revision %d", last, record.JobControlRevision)
		}
	}
	if len(h.workers.installsFor(2, model.Running))+len(h.workers.installsFor(3, model.Running)) != 0 {
		t.Fatal("terminal job received running installs")
	}

	// A leadership change retries the idempotent terminal install again.
	before := h.log.count("install:3:closed")
	h.follow(2)
	h.waitGateClosed()
	h.lead(3)
	h.waitFor(func() bool { return h.log.count("install:3:closed") > before }, "terminal install after leadership change")
}

// TestTerminalPropagationRetriesWorkersThatWereUnreachable pins the Task 27
// real-process finding: a terminal Closed install is not confirmed for a
// worker that was Dead in membership when the leader's pass ran; once the
// worker is Alive again the same leadership session installs it, so a
// sealed-result replica that missed its re-install can serve fetches without
// waiting for the next leadership change.
func TestTerminalPropagationRetriesWorkersThatWereUnreachable(t *testing.T) {
	h, job, _, _ := runningHarness(t)
	view := h.view()
	cancel, err := state.NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0x53}, Sequence: 1}, job, 3, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed cancel: %v", err)
	}
	h.raft.applySeed(t, cancel)
	h.setMemberStatus(3, swim.Dead)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { return len(h.workers.installsFor(2, model.Closed)) >= 1 }, "terminal install on the reachable worker")
	if got := len(h.workers.installsFor(3, model.Closed)); got != 0 {
		t.Fatalf("unreachable worker 3 received %d terminal installs", got)
	}

	h.setMemberStatus(3, swim.Alive)
	h.actor.Wake()
	h.waitFor(func() bool { return len(h.workers.installsFor(3, model.Closed)) >= 1 }, "terminal install once worker 3 is reachable again")
	if got := len(h.workers.installsFor(2, model.Closed)); got != 1 {
		t.Fatalf("confirmed worker 2 received %d terminal installs, want exactly 1", got)
	}
}
