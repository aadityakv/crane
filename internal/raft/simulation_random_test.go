package raft

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
)

const simulationStressEnvironment = "RAFT_SIM_STRESS"

type simulationScheduleCoverage struct {
	persistenceBeforeRelease       int
	crashAfterPersistBeforeRelease int
	crashAfterApplyBeforeAdvance   int
}

func TestSimulationRandomizedSchedules(t *testing.T) {
	multiplier := simulationStressMultiplier(t)
	baseSeeds := []uint64{0x42500001, 0x42500017, 0x42500101, 0x42501007}
	for repetition := 0; repetition < multiplier; repetition++ {
		for _, base := range baseSeeds {
			seed := base + uint64(repetition)*0x100000001b3
			t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
				runSimulationSchedule(t, seed, 300)
			})
		}
	}
}

func TestSimulationRandomDeliverableSequenceExcludesBlockedAndDownTargets(t *testing.T) {
	cluster := newSimulationCluster(t, 3, 0x4250d311)
	cluster.partitioned[simulationLink{from: 1, to: 2}] = true
	cluster.crash(t, 3)
	cluster.queue = []simulationEnvelope{
		{sequence: 1, from: 1, to: 2, rpc: PreVoteRequest{CandidateID: 1, ProspectiveTerm: 1}},
		{sequence: 2, from: 1, to: 3, rpc: PreVoteRequest{CandidateID: 1, ProspectiveTerm: 1}},
		{sequence: 3, from: 2, to: 1, rpc: PreVoteRequest{CandidateID: 2, ProspectiveTerm: 1}},
	}
	for sample := uint64(0); sample < 6; sample++ {
		if got := simulationRandomDeliverableSequence(cluster, sample); got != 3 {
			t.Fatalf("sample %d selected blocked/down sequence %d, want live unblocked sequence 3", sample, got)
		}
	}
}

func simulationStressMultiplier(t *testing.T) int {
	t.Helper()
	raw, present := os.LookupEnv(simulationStressEnvironment)
	if !present {
		return 1
	}
	multiplier, err := strconv.Atoi(raw)
	if err != nil || multiplier < 1 || multiplier > 20 {
		t.Fatalf("%s=%q must be an integer in [1,20]", simulationStressEnvironment, raw)
	}
	return multiplier
}

func runSimulationSchedule(t *testing.T, seed uint64, steps int) {
	t.Helper()
	voterCount := 3
	if seed&1 != 0 {
		voterCount = 5
	}
	cluster := newSimulationCluster(t, voterCount, seed)
	random := &simulationRandom{state: seed ^ 0x726166742d73696d}
	cluster.elect(t, 1, cluster.ids[1:]...)
	cluster.proposeAndCommit(t, 1, fmt.Sprintf("seed-%d-initial", seed), cluster.ids[1:]...)
	coverage := simulationScheduleCoverage{}
	simulationExerciseRequiredPhases(t, cluster, seed, &coverage)

	for step := 0; step < steps; step++ {
		choice := random.Uint64() % 100
		switch {
		case choice < 10:
			if id := simulationRandomNodeID(cluster, random.Uint64(), simulationCanAcceptInput); id != 0 {
				node := cluster.nodes[id]
				now := node.core.ElectionDeadline()
				if node.core.Status().Role == RoleLeader || now <= node.core.now {
					now = node.core.now + 1
				}
				simulationRecordSchedule(cluster, step, choice, "tick-input voter=%d now=%d", id, now)
				if err := node.core.Tick(now); err != nil {
					cluster.fail(t, "random tick voter=%d: %v", id, err)
				}
				cluster.check(t, "random tick input without Ready settlement")
			}
		case choice < 20:
			if sequence := simulationRandomDeliverableSequence(cluster, random.Uint64()); sequence != 0 {
				simulationRecordSchedule(cluster, step, choice, "rpc-input sequence=%d", sequence)
				cluster.stepOnly(t, func(envelope simulationEnvelope) bool { return envelope.sequence == sequence })
			}
		case choice < 27:
			leader := simulationHighestTermLeader(cluster)
			if leader != 0 && simulationCanAcceptInput(cluster.nodes[leader]) && cluster.nodes[leader].core.hasCommittedCurrentTerm() {
				simulationRecordSchedule(cluster, step, choice, "proposal-input leader=%d", leader)
				if _, entry, err := cluster.propose(leader, EntryCommand, []byte(fmt.Sprintf("seed-%d-step-%d", seed, step))); err == nil {
					cluster.record("random proposal left Ready outstanding leader=%d index=%d term=%d", leader, entry.Index, entry.Term)
					cluster.check(t, "random proposal input without Ready settlement")
				}
			}
		case choice < 36:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending == nil && node.core.hasPending
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "take-ready voter=%d", id)
				if !cluster.takeReady(t, id) {
					cluster.fail(t, "scheduled Ready missing voter=%d", id)
				}
			}
		case choice < 45:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending != nil && !node.pending.persisted
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "persist-ready voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
				if err := cluster.persistReady(t, id); err != nil {
					cluster.fail(t, "random persist Ready voter=%d: %v", id, err)
				}
				coverage.persistenceBeforeRelease++
			}
		case choice < 54:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending != nil && node.pending.persisted && !node.pending.released
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "release-ready voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
				if err := cluster.releaseReady(t, id); err != nil {
					cluster.fail(t, "random release Ready voter=%d: %v", id, err)
				}
			}
		case choice < 63:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending != nil && node.pending.released && !node.pending.applied
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "apply-ready voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
				cluster.applyReady(t, id)
			}
		case choice < 70:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending != nil && node.pending.applied && !node.pending.published
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "publish-results voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
				if err := cluster.publishReadyResults(t, id); err != nil {
					cluster.fail(t, "random publish Ready voter=%d: %v", id, err)
				}
			}
		case choice < 77:
			if id := simulationRandomNodeID(cluster, random.Uint64(), func(node *simulationNode) bool {
				return node.live && node.pending != nil && node.pending.published
			}); id != 0 {
				simulationRecordSchedule(cluster, step, choice, "advance-ready voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
				cluster.advanceReady(t, id)
			}
		case choice < 84:
			id := cluster.ids[int(random.Uint64()%uint64(len(cluster.ids)))]
			node := cluster.nodes[id]
			if node.live {
				simulationRecordSchedule(cluster, step, choice, "crash voter=%d phase=%s", id, simulationPendingPhase(node))
				if node.pending != nil && node.pending.persisted && !node.pending.released {
					coverage.crashAfterPersistBeforeRelease++
				}
				if node.pending != nil && node.pending.applied {
					coverage.crashAfterApplyBeforeAdvance++
				}
				cluster.crash(t, id)
			} else {
				simulationRecordSchedule(cluster, step, choice, "restart voter=%d", id)
				cluster.restart(t, id)
			}
		case choice < 89:
			from, to := simulationDistinctPair(cluster, random.Uint64(), random.Uint64())
			if from != 0 {
				link := simulationLink{from: from, to: to}
				simulationRecordSchedule(cluster, step, choice, "toggle-link %d->%d blocked=%t", from, to, !cluster.partitioned[link])
				cluster.partitioned[link] = !cluster.partitioned[link]
				cluster.check(t, "toggle directed partition")
			}
		case choice < 93:
			simulationRandomDuplicate(t, cluster, random.Uint64(), step, choice)
		case choice < 96:
			simulationRandomReorder(t, cluster, random.Uint64(), random.Uint64(), step, choice)
		case choice < 99:
			simulationInjectPendingPersistenceFault(t, cluster, random.Uint64(), step, choice)
		default:
			simulationRecordSchedule(cluster, step, choice, "capture-safe-snapshot")
			simulationCaptureSafeSnapshot(t, cluster)
		}
		if len(cluster.queue) > 512 {
			drop := len(cluster.queue) - 512
			simulationRecordSchedule(cluster, step, choice, "bound-queue drop-oldest=%d", drop)
			cluster.queue = append([]simulationEnvelope(nil), cluster.queue[drop:]...)
			cluster.check(t, "bound randomized queue")
		}
	}
	if coverage.persistenceBeforeRelease == 0 || coverage.crashAfterPersistBeforeRelease == 0 || coverage.crashAfterApplyBeforeAdvance == 0 {
		cluster.fail(t, "required phase coverage missing: %+v", coverage)
	}

	simulationRecoverLiveness(t, cluster, seed)
}

func simulationRecordSchedule(cluster *simulationCluster, step int, choice uint64, format string, arguments ...any) {
	cluster.record("schedule step=%d choice=%d event=%s", step, choice, fmt.Sprintf(format, arguments...))
}

func simulationCanAcceptInput(node *simulationNode) bool {
	return node != nil && node.live && node.core != nil && node.pending == nil && !node.core.hasPending
}

func simulationRandomNodeID(cluster *simulationCluster, sample uint64, predicate func(*simulationNode) bool) uint16 {
	if len(cluster.ids) == 0 {
		return 0
	}
	start := int(sample % uint64(len(cluster.ids)))
	for offset := 0; offset < len(cluster.ids); offset++ {
		id := cluster.ids[(start+offset)%len(cluster.ids)]
		if predicate(cluster.nodes[id]) {
			return id
		}
	}
	return 0
}

func simulationRandomDeliverableSequence(cluster *simulationCluster, sample uint64) uint64 {
	candidates := make([]uint64, 0, len(cluster.queue))
	for _, envelope := range cluster.queue {
		if !cluster.partitioned[simulationLink{from: envelope.from, to: envelope.to}] && simulationCanAcceptInput(cluster.nodes[envelope.to]) {
			candidates = append(candidates, envelope.sequence)
		}
	}
	if len(candidates) == 0 {
		return 0
	}
	return candidates[int(sample%uint64(len(candidates)))]
}

func simulationPendingPhase(node *simulationNode) string {
	if node.pending == nil {
		if node.core != nil && node.core.hasPending {
			return "ready-untaken"
		}
		return "idle"
	}
	switch {
	case !node.pending.persisted:
		return "ready-taken"
	case !node.pending.released:
		return "persisted-before-release"
	case !node.pending.applied:
		return "released-before-apply"
	case !node.pending.published:
		return "applied-before-result"
	default:
		return "result-before-advance"
	}
}

func simulationRandomDuplicate(t *testing.T, cluster *simulationCluster, sample uint64, step int, choice uint64) {
	t.Helper()
	if len(cluster.queue) == 0 || len(cluster.queue) >= 512 {
		return
	}
	original := cluster.queue[int(sample%uint64(len(cluster.queue)))]
	simulationRecordSchedule(cluster, step, choice, "duplicate sequence=%d %d->%d type=%T", original.sequence, original.from, original.to, original.rpc)
	cluster.nextSequence++
	duplicate := original
	duplicate.sequence = cluster.nextSequence
	duplicate.rpc = CloneRPC(original.rpc)
	cluster.queue = append(cluster.queue, duplicate)
	cluster.record("duplicate seq=%d as seq=%d %d->%d type=%T", original.sequence, duplicate.sequence, duplicate.from, duplicate.to, duplicate.rpc)
	cluster.check(t, "duplicate message")
}

func simulationRandomReorder(t *testing.T, cluster *simulationCluster, leftSample, rightSample uint64, step int, choice uint64) {
	t.Helper()
	if len(cluster.queue) < 2 {
		return
	}
	left := int(leftSample % uint64(len(cluster.queue)))
	right := int(rightSample % uint64(len(cluster.queue)))
	simulationRecordSchedule(cluster, step, choice, "reorder positions=%d,%d sequences=%d,%d", left, right, cluster.queue[left].sequence, cluster.queue[right].sequence)
	cluster.queue[left], cluster.queue[right] = cluster.queue[right], cluster.queue[left]
	cluster.check(t, "reorder messages")
}

func simulationInjectPendingPersistenceFault(t *testing.T, cluster *simulationCluster, sample uint64, step int, choice uint64) {
	t.Helper()
	id := simulationRandomNodeID(cluster, sample, func(node *simulationNode) bool {
		return node.live && node.pending != nil && !node.pending.persisted && persistenceBatchHasEffects(node.pending.batch)
	})
	if id == 0 {
		return
	}
	simulationRecordSchedule(cluster, step, choice, "fault-persist voter=%d token=%d", id, cluster.nodes[id].pending.ready.Token)
	cluster.nodes[id].store.FailNext(StorageOperationPersist, errors.New("seeded persistence fault"))
	if err := cluster.persistReady(t, id); err == nil {
		cluster.fail(t, "random persistence fault voter=%d succeeded", id)
	}
	cluster.check(t, "random failed persistence left Ready outstanding")
}

func simulationExerciseRequiredPhases(t *testing.T, cluster *simulationCluster, seed uint64, coverage *simulationScheduleCoverage) {
	t.Helper()
	cluster.queue = nil
	persistCrashID := cluster.ids[len(cluster.ids)-1]
	persistCrashNode := cluster.nodes[persistCrashID]
	candidate := cluster.ids[1]
	if candidate == persistCrashID {
		candidate = cluster.ids[0]
	}
	request := RequestVoteRequest{
		CandidateID:  candidate,
		Term:         persistCrashNode.core.HardState().Term + 1,
		LastLogIndex: persistCrashNode.core.Status().LastIndex,
		LastLogTerm:  persistCrashNode.core.log.LastTerm(),
	}
	simulationRecordSchedule(cluster, -9, 0, "required rpc-input voter=%d candidate=%d term=%d", persistCrashID, candidate, request.Term)
	if err := persistCrashNode.core.Step(candidate, request); err != nil {
		cluster.fail(t, "required persisted-before-release setup: %v", err)
	}
	simulationRecordSchedule(cluster, -8, 0, "required take-ready voter=%d", persistCrashID)
	if !cluster.takeReady(t, persistCrashID) {
		cluster.fail(t, "required persisted-before-release Ready missing")
	}
	simulationRecordSchedule(cluster, -7, 0, "required persist-ready voter=%d", persistCrashID)
	if err := cluster.persistReady(t, persistCrashID); err != nil {
		cluster.fail(t, "required persisted-before-release persist: %v", err)
	}
	coverage.persistenceBeforeRelease++
	simulationRecordSchedule(cluster, -6, 0, "required crash voter=%d phase=%s", persistCrashID, simulationPendingPhase(persistCrashNode))
	coverage.crashAfterPersistBeforeRelease++
	cluster.crash(t, persistCrashID)
	cluster.restart(t, persistCrashID)

	cluster.queue = nil
	applyCrashID := uint16(1)
	simulationRecordSchedule(cluster, -5, 0, "required proposal-input leader=%d", applyCrashID)
	_, entry, err := cluster.propose(applyCrashID, EntryCommand, []byte(fmt.Sprintf("seed-%d-required-apply-crash", seed)))
	if err != nil {
		cluster.fail(t, "required apply-before-advance proposal: %v", err)
	}
	cluster.settle(t, applyCrashID)
	for _, supporter := range cluster.ids[1 : len(cluster.ids)/2+1] {
		simulationRecordSchedule(cluster, -4, 0, "required rpc-input append leader=%d follower=%d index=%d", applyCrashID, supporter, entry.Index)
		if !cluster.deliver(t, applyCrashID, supporter, AppendEntriesRequest{}) {
			cluster.fail(t, "required apply-before-advance append missing follower=%d", supporter)
		}
		if _, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			response, responseOK := envelope.rpc.(AppendEntriesResponse)
			return responseOK && envelope.from == supporter && envelope.to == applyCrashID && response.MatchIndex == entry.Index
		}); !ok {
			cluster.fail(t, "required apply-before-advance response missing follower=%d", supporter)
		}
	}
	simulationRecordSchedule(cluster, -3, 0, "required take/persist/release/apply voter=%d", applyCrashID)
	if !cluster.takeReady(t, applyCrashID) {
		cluster.fail(t, "required apply-before-advance Ready missing")
	}
	if err := cluster.persistReady(t, applyCrashID); err != nil {
		cluster.fail(t, "required apply-before-advance persist: %v", err)
	}
	coverage.persistenceBeforeRelease++
	if err := cluster.releaseReady(t, applyCrashID); err != nil {
		cluster.fail(t, "required apply-before-advance release: %v", err)
	}
	cluster.applyReady(t, applyCrashID)
	simulationRecordSchedule(cluster, -2, 0, "required crash voter=%d phase=%s", applyCrashID, simulationPendingPhase(cluster.nodes[applyCrashID]))
	coverage.crashAfterApplyBeforeAdvance++
	cluster.crash(t, applyCrashID)
	cluster.restart(t, applyCrashID)
	simulationRecordSchedule(cluster, -1, 0, "required recovery")
	simulationRecoverLiveness(t, cluster, seed^0x706861736573)
}

func simulationHighestTermLeader(cluster *simulationCluster) uint16 {
	var leader uint16
	var term uint64
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if !simulationCanAcceptInput(node) {
			continue
		}
		status := node.core.Status()
		if status.Role == RoleLeader && (leader == 0 || status.Term > term || status.Term == term && id < leader) {
			leader, term = id, status.Term
		}
	}
	return leader
}

func simulationDistinctPair(cluster *simulationCluster, left, right uint64) (uint16, uint16) {
	if len(cluster.ids) < 2 {
		return 0, 0
	}
	from := cluster.ids[int(left%uint64(len(cluster.ids)))]
	to := cluster.ids[int(right%uint64(len(cluster.ids)))]
	if to == from {
		to = cluster.ids[(int(right%uint64(len(cluster.ids)))+1)%len(cluster.ids)]
	}
	return from, to
}

func simulationCaptureSafeSnapshot(t *testing.T, cluster *simulationCluster) {
	t.Helper()
	leader := simulationHighestTermLeader(cluster)
	if leader == 0 {
		return
	}
	leaderState := cluster.nodes[leader].core.LogState()
	if leaderState.AppliedIndex == 0 || leaderState.AppliedIndex != leaderState.CommitIndex || leaderState.AppliedIndex == leaderState.SnapshotIndex {
		return
	}
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if !simulationCanAcceptInput(node) || node.core.Status().CommitIndex != leaderState.CommitIndex || node.core.Status().LastIndex < leaderState.CommitIndex {
			return
		}
	}
	cluster.captureSnapshot(t, leader)
}

func simulationRecoverLiveness(t *testing.T, cluster *simulationCluster, seed uint64) {
	t.Helper()
	cluster.healAll(t)
	cluster.queue = nil
	for _, id := range cluster.ids {
		if cluster.nodes[id].live {
			cluster.crash(t, id)
		}
	}
	for _, id := range cluster.ids {
		cluster.restart(t, id)
	}
	type candidateState struct {
		id        uint16
		term      uint64
		lastTerm  uint64
		lastIndex uint64
	}
	candidates := make([]candidateState, 0, len(cluster.ids))
	var maximumTerm uint64
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		state := candidateState{id: id, term: node.core.HardState().Term, lastTerm: node.core.log.LastTerm(), lastIndex: node.core.Status().LastIndex}
		candidates = append(candidates, state)
		if state.term > maximumTerm {
			maximumTerm = state.term
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lastTerm != candidates[j].lastTerm {
			return candidates[i].lastTerm > candidates[j].lastTerm
		}
		if candidates[i].lastIndex != candidates[j].lastIndex {
			return candidates[i].lastIndex > candidates[j].lastIndex
		}
		return candidates[i].id < candidates[j].id
	})
	candidate := candidates[0].id
	if cluster.nodes[candidate].core.HardState().Term < maximumTerm {
		other := cluster.ids[0]
		if other == candidate {
			other = cluster.ids[1]
		}
		request := RequestVoteRequest{CandidateID: other, Term: maximumTerm, LastLogIndex: cluster.nodes[candidate].core.Status().LastIndex, LastLogTerm: cluster.nodes[candidate].core.log.LastTerm()}
		if err := cluster.nodes[candidate].core.Step(other, request); err != nil {
			cluster.fail(t, "normalize final candidate term: %v", err)
		}
		cluster.settle(t, candidate)
	}
	supporters := make([]uint16, 0, len(cluster.ids)-1)
	for _, state := range candidates {
		if state.id != candidate {
			supporters = append(supporters, state.id)
		}
	}
	cluster.queue = nil
	cluster.elect(t, candidate, supporters...)
	entry := cluster.proposeAndCommit(t, candidate, fmt.Sprintf("seed-%d-final", seed), supporters...)
	if cluster.nodes[candidate].core.Status().CommitIndex < entry.Index {
		cluster.fail(t, "final liveness proposal index=%d did not commit", entry.Index)
	}
}
