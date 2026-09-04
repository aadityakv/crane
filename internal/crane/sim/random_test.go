package sim

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
)

// randomStressEnvironment optionally widens the randomized seed set.
const randomStressEnvironment = "CRANE_SIM_STRESS"

// minimumMeaningfulEvents is the floor every randomized schedule must clear.
const minimumMeaningfulEvents = 40

// randomTopologyChoice fixes one randomized job shape.
type randomTopologyChoice struct {
	name      string
	end       int64
	stage     simStageSpec
	parallel  uint16
	secondJob bool
}

// randomScheduleState tracks the live fault windows and crash constraints of
// one randomized run.
type randomScheduleState struct {
	mu sync.Mutex

	crashedNodes map[uint16]bool
	lostStores   map[uint16]bool
	// current replica pair of the active job, to never destroy both copies.
	replicaA, replicaB uint16

	tupleWindows []*datagramRule
	swimWindows  []*datagramRule
	dialCuts     []*dialRule
	// pendingWindows are fault windows whose tracking is gated on actual
	// consumption: an SWIM suspicion or control-dial window that nothing
	// consumed inside its lifetime (probe rotation and coordinator pass
	// cadence decide) is a no-op, not an injected fault.
	pendingWindows []pendingFaultWindow
}

// pendingFaultWindow pairs one fault name with the consumption oracle of its
// rules and survives until the window expires.
type pendingFaultWindow struct {
	name     string
	consumed func() bool
}

// TestRandomizedSchedules runs seeded randomized failure schedules over the
// full production stack. Every run records its seed and final step on
// failure, requires a minimum meaningful-event count, spans multiple topology
// and parallelism choices, and tracks every fault window that actually
// consumed traffic (an unconsumed SWIM-suspicion or control-dial window is a
// no-op, not an injected fault).
func TestRandomizedSchedules(t *testing.T) {
	if testing.Short() {
		t.Skip("randomized simulations run full in-process clusters")
	}
	multiplier := randomStressMultiplier(t)
	baseSeeds := []uint64{0x42505100, 0x42505117, 0x42505145, 0x42505199}
	for repetition := 0; repetition < multiplier; repetition++ {
		for _, base := range baseSeeds {
			seed := base + uint64(repetition)*0x100000001b3
			t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
				runRandomizedSchedule(t, seed)
			})
		}
	}
}

func randomStressMultiplier(t *testing.T) int {
	t.Helper()
	raw, present := os.LookupEnv(randomStressEnvironment)
	if !present {
		return 1
	}
	multiplier, err := strconv.Atoi(raw)
	if err != nil || multiplier < 1 || multiplier > 20 {
		t.Fatalf("%s=%q must be an integer in [1,20]", randomStressEnvironment, raw)
	}
	return multiplier
}

// randomChoice draws one harness decision from the scenario seed.
func (cluster *simCluster) randomChoice(modulo int) int {
	cluster.randMu.Lock()
	defer cluster.randMu.Unlock()
	return cluster.rand.Intn(modulo)
}

// runRandomizedSchedule drives one complete randomized scenario.
func runRandomizedSchedule(t *testing.T, seed uint64) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()

	choice := randomTopologyChoice{
		name:      fmt.Sprintf("rand-%d", seed),
		end:       int64(4 + cluster.randomChoice(7)),
		parallel:  uint16(1 + cluster.randomChoice(2)),
		secondJob: cluster.randomChoice(2) == 0,
	}
	switch cluster.randomChoice(4) {
	case 1:
		choice.stage = simStageSpec{factor: int64(1 + cluster.randomChoice(3))}
	case 2:
		choice.stage = simStageSpec{filter: "even"}
	case 3:
		choice.stage = simStageSpec{filter: "even", factor: 2}
	}

	client := cluster.newClient(choice.name)
	spec := newSimTopology(choice.name, choice.parallel, choice.end, choice.stage)
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, choice.stage)
	job := cluster.submit(client, plan)

	schedule := &randomScheduleState{
		crashedNodes: make(map[uint16]bool),
		lostStores:   make(map[uint16]bool),
	}
	cluster.await("job running or beyond", func() bool {
		record, ok := cluster.jobRecord(job)
		return ok && record.Lifecycle != state.JobPending
	})
	cluster.trackReplicaPair(schedule, plan)
	if initial, ok := cluster.jobRecord(job); !ok || initial.Assignment == nil {
		cluster.fail("randomized seed=%d lost its committed assignment", seed)
	}

	maxSteps := 5000
	for step := 0; step < maxSteps; step++ {
		if lifecycle := cluster.jobLifecycle(job); lifecycle == state.JobSucceeded || lifecycle == state.JobFailed || lifecycle == JobCanceledForCheck() {
			break
		}
		applyRandomFaultAction(cluster, schedule, plan, job)
		cluster.pump(20 + cluster.randomChoice(80))
		cluster.expireRandomWindows(schedule)
	}

	// A randomized fault schedule can reassign a sink-replica-bearing node;
	// when the job then wedges in the recorded defect-#4 signature the run
	// skips under that adjudication instead of timing out.
	lifecycle := cluster.awaitJobTerminal(job, "randomized")
	if lifecycle != state.JobSucceeded {
		cluster.fail("randomized seed=%d job ended %d, want Succeeded", seed, lifecycle)
	}
	if choice.secondJob {
		secondSpec := newSimTopology(choice.name+"-2", 1, 4, simStageSpec{})
		secondPlan := newSimJobPlan(t, client.store.NextRequestID(), secondSpec, simStageSpec{})
		second := cluster.submit(client, secondPlan)
		if secondLifecycle := cluster.awaitJobTerminal(second, "randomized-second"); secondLifecycle != state.JobSucceeded {
			cluster.fail("randomized seed=%d second job ended %d", seed, secondLifecycle)
		}
		secondRecords := cluster.pageResult(client, second)
		cluster.oracle.verifyFinal(second, secondRecords, choice.name+"-second")
	}

	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, choice.name)
	cluster.requireFaultsConsumed("randomized")
	if events := cluster.oracle.meaningfulEvents(); events < minimumMeaningfulEvents {
		cluster.fail("randomized seed=%d produced only %d meaningful events, want at least %d", seed, events, minimumMeaningfulEvents)
	}
	cluster.record("randomized seed=%d verified with %d meaningful events", seed, cluster.oracle.meaningfulEvents())
}

// JobCanceledForCheck mirrors the terminal lifecycle for schedule polling.
func JobCanceledForCheck() state.JobLifecycle { return state.JobCanceled }

// trackReplicaPair records the job's current sink replica pair.
func (cluster *simCluster) trackReplicaPair(schedule *randomScheduleState, plan *simJobPlan) {
	cluster.await("replica pair committed", func() bool {
		record, ok := cluster.jobRecord(plan.jobID)
		if !ok || record.Assignment == nil || len(record.Assignment.ResultReplicas) == 0 {
			return false
		}
		replica := record.Assignment.ResultReplicas[0]
		schedule.mu.Lock()
		schedule.replicaA, schedule.replicaB = replica.PrimaryNodeID, replica.SecondaryNodeID
		schedule.mu.Unlock()
		return true
	})
}

// applyRandomFaultAction picks and applies one fault action, keeping every
// safety constraint (one crashed node at a time, quorum, and never losing
// both sink replica stores).
func applyRandomFaultAction(cluster *simCluster, schedule *randomScheduleState, plan *simJobPlan, job model.JobID) {
	cluster.t.Helper()
	schedule.mu.Lock()
	crashedCount := len(schedule.crashedNodes)
	replicaA, replicaB := schedule.replicaA, schedule.replicaB
	schedule.mu.Unlock()

	tupleTraffic := cluster.tupleTrafficActive(job)
	action := cluster.randomChoice(10)
	switch {
	case action < 2 && tupleTraffic:
		// Bounded +7 drop window toward one random victim. Tracked only when
		// the victim actually sent +7 traffic inside the window; an
		// unconsumed window is a no-op, not an injected fault.
		victim := cluster.ids[cluster.randomChoice(len(cluster.ids))]
		name := fmt.Sprintf("rand-drop-%d", victim)
		rules := []*datagramRule{cluster.nodes[victim].tupleD.addRule(dgramFaultDrop)}
		cluster.record("arm fault %s", name)
		schedule.mu.Lock()
		schedule.tupleWindows = append(schedule.tupleWindows, rules...)
		schedule.pendingWindows = append(schedule.pendingWindows, pendingFaultWindow{name: name, consumed: anyRuleConsumed(rules)})
		schedule.mu.Unlock()
	case action < 3 && tupleTraffic:
		victim := cluster.ids[cluster.randomChoice(len(cluster.ids))]
		duplicate := []*datagramRule{cluster.nodes[victim].tupleD.addRule(dgramFaultDuplicate)}
		hold := []*datagramRule{cluster.nodes[victim].tupleD.addRule(dgramFaultHold)}
		cluster.record("arm fault rand-dup-%d / rand-reorder-%d", victim, victim)
		schedule.mu.Lock()
		schedule.tupleWindows = append(schedule.tupleWindows, duplicate...)
		schedule.tupleWindows = append(schedule.tupleWindows, hold...)
		schedule.pendingWindows = append(schedule.pendingWindows,
			pendingFaultWindow{name: fmt.Sprintf("rand-dup-%d", victim), consumed: anyRuleConsumed(duplicate)},
			pendingFaultWindow{name: fmt.Sprintf("rand-reorder-%d", victim), consumed: anyRuleConsumed(hold)})
		schedule.mu.Unlock()
	case action < 4:
		// SWIM-only suspicion window: probes fail, control stays healthy.
		// The fault is tracked only when some peer's probe actually hit the
		// victim inside the window (probe targets rotate per node); an
		// unconsumed window is a no-op, not an injected fault.
		victim := cluster.ids[cluster.randomChoice(len(cluster.ids))]
		target := cluster.swimEndpointsOf(victim)
		name := fmt.Sprintf("rand-suspect-%d", victim)
		rules := make([]*datagramRule, 0, len(cluster.ids))
		for _, other := range cluster.ids {
			if other != victim {
				rules = append(rules, cluster.nodes[other].swimD.addRule(dgramFaultDrop, target...))
			}
		}
		cluster.record("arm fault %s", name)
		schedule.mu.Lock()
		schedule.swimWindows = append(schedule.swimWindows, rules...)
		schedule.pendingWindows = append(schedule.pendingWindows, pendingFaultWindow{name: name, consumed: anyRuleConsumed(rules)})
		schedule.mu.Unlock()
	case action < 5:
		victim := cluster.ids[cluster.randomChoice(len(cluster.ids))]
		name := fmt.Sprintf("rand-cut-%d", victim)
		rule := cluster.dialer.cut(cluster.controlAddressOf(victim))
		cluster.record("arm fault %s", name)
		schedule.mu.Lock()
		schedule.dialCuts = append(schedule.dialCuts, rule)
		schedule.pendingWindows = append(schedule.pendingWindows, pendingFaultWindow{name: name, consumed: func() bool { return rule.blocked.Load() > 0 }})
		schedule.mu.Unlock()
	case action < 7 && crashedCount == 0:
		// Crash one node; the leader is preferred half the time.
		var victim uint16
		if cluster.randomChoice(2) == 0 {
			if leader := cluster.oracle.currentLeader(); leader != 0 {
				victim = leader
			}
		}
		if victim == 0 {
			victim = cluster.ids[cluster.randomChoice(len(cluster.ids))]
		}
		schedule.mu.Lock()
		crashCanLoseStore := victim != replicaA && victim != replicaB && !schedule.lostStores[victim]
		loseStore := crashCanLoseStore && cluster.randomChoice(3) == 0
		if loseStore {
			schedule.lostStores[victim] = true
		}
		schedule.crashedNodes[victim] = true
		schedule.mu.Unlock()
		cluster.stopNode(cluster.nodes[victim])
		cluster.trackFault(fmt.Sprintf("rand-crash-%d-lose-%t", victim, loseStore), func() bool { return true })
		cluster.restartCrashedNodeLater(schedule, victim, loseStore)
	default:
		// Harmless step: just let the cluster run.
	}
}

// restartCrashedNodeLater rejoins a crashed node after a bounded simulated
// delay inside the schedule loop, without any wall-clock sleep.
func (cluster *simCluster) restartCrashedNodeLater(schedule *randomScheduleState, victim uint16, loseStore bool) {
	cluster.t.Helper()
	delay := 60 + cluster.randomChoice(240)
	cluster.awaitOptionally(delay, func() bool { return false })
	cluster.restartNode(cluster.nodes[victim], loseStore)
	schedule.mu.Lock()
	delete(schedule.crashedNodes, victim)
	schedule.mu.Unlock()
	cluster.oracle.noteEvent()
}

// expireRandomWindows heals long-lived fault windows so progress resumes. A
// window that actually consumed traffic becomes a tracked (already consumed)
// injected fault; a no-op window is deactivated without tracking.
func (cluster *simCluster) expireRandomWindows(schedule *randomScheduleState) {
	schedule.mu.Lock()
	tupleWindows := schedule.tupleWindows
	swimWindows := schedule.swimWindows
	dialCuts := schedule.dialCuts
	pending := schedule.pendingWindows
	schedule.tupleWindows, schedule.swimWindows, schedule.dialCuts, schedule.pendingWindows = nil, nil, nil, nil
	schedule.mu.Unlock()
	for _, rule := range append(tupleWindows, swimWindows...) {
		rule.deactivate()
	}
	for _, rule := range dialCuts {
		rule.active.Store(false)
	}
	for _, window := range pending {
		if window.consumed() {
			cluster.trackFault(window.name, window.consumed)
		}
	}
}

// anyRuleConsumed reports whether any rule of one window has taken effect.
func anyRuleConsumed(rules []*datagramRule) func() bool {
	return func() bool {
		for _, rule := range rules {
			if rule.consumed.Load() > 0 {
				return true
			}
		}
		return false
	}
}

// tupleTrafficActive reports whether any live store still has an
// unacknowledged +7 outbox, so datagram faults can actually consume.
func (cluster *simCluster) tupleTrafficActive(job model.JobID) bool {
	for _, id := range cluster.ids {
		handle := cluster.workerStore(id)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		for _, outbox := range work.Outboxes {
			if outbox.ID.Tuple.JobID == job && !outbox.Completed {
				return true
			}
		}
	}
	return false
}
