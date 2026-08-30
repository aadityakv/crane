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

	for step := 0; step < steps; step++ {
		choice := random.Uint64() % 100
		switch {
		case choice < 22:
			if leader := simulationHighestTermLeader(cluster); leader != 0 {
				cluster.heartbeatTick(t, leader)
			} else if id := simulationRandomLiveID(cluster, random.Uint64()); id != 0 {
				cluster.tick(t, id)
			}
		case choice < 44:
			if len(cluster.queue) != 0 {
				sequence := cluster.queue[int(random.Uint64()%uint64(len(cluster.queue)))].sequence
				cluster.deliverMatching(t, func(envelope simulationEnvelope) bool { return envelope.sequence == sequence })
			}
		case choice < 52:
			if len(cluster.queue) != 0 && len(cluster.queue) < 512 {
				original := cluster.queue[int(random.Uint64()%uint64(len(cluster.queue)))]
				cluster.nextSequence++
				duplicate := original
				duplicate.sequence = cluster.nextSequence
				duplicate.rpc = CloneRPC(original.rpc)
				cluster.queue = append(cluster.queue, duplicate)
				cluster.record("duplicate seq=%d as seq=%d %d->%d type=%T", original.sequence, duplicate.sequence, duplicate.from, duplicate.to, duplicate.rpc)
				cluster.check(t, "duplicate message")
			}
		case choice < 58:
			if len(cluster.queue) >= 2 {
				left := int(random.Uint64() % uint64(len(cluster.queue)))
				right := int(random.Uint64() % uint64(len(cluster.queue)))
				cluster.queue[left], cluster.queue[right] = cluster.queue[right], cluster.queue[left]
				cluster.record("reorder queue positions=%d,%d", left, right)
				cluster.check(t, "reorder messages")
			}
		case choice < 68:
			from, to := simulationDistinctPair(cluster, random.Uint64(), random.Uint64())
			if from != 0 {
				link := simulationLink{from: from, to: to}
				cluster.partitioned[link] = !cluster.partitioned[link]
				cluster.record("directed link %d->%d blocked=%t", from, to, cluster.partitioned[link])
				cluster.check(t, "toggle directed partition")
			}
		case choice < 76:
			leader := simulationHighestTermLeader(cluster)
			if leader != 0 {
				node := cluster.nodes[leader]
				if node.pending == nil && node.core.hasCommittedCurrentTerm() {
					if entry, err := node.core.ProposeEntry([]byte(fmt.Sprintf("seed-%d-step-%d", seed, step))); err == nil {
						cluster.record("random propose leader=%d index=%d term=%d", leader, entry.Index, entry.Term)
						cluster.settle(t, leader)
					}
				}
			}
		case choice < 84:
			id := cluster.ids[int(random.Uint64()%uint64(len(cluster.ids)))]
			if cluster.nodes[id].live {
				cluster.crash(t, id)
			} else {
				cluster.restart(t, id)
			}
		case choice < 91:
			simulationInjectPersistenceFault(t, cluster, random.Uint64())
		case choice < 96:
			simulationCaptureSafeSnapshot(t, cluster)
		default:
			simulationCrashBeforePersistence(t, cluster, random.Uint64())
		}
		if len(cluster.queue) > 512 {
			drop := len(cluster.queue) - 512
			cluster.queue = append([]simulationEnvelope(nil), cluster.queue[drop:]...)
			cluster.record("bound queue dropped oldest=%d", drop)
			cluster.check(t, "bound randomized queue")
		}
	}

	simulationRecoverLiveness(t, cluster, seed)
}

func simulationHighestTermLeader(cluster *simulationCluster) uint16 {
	var leader uint16
	var term uint64
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if !node.live || node.pending != nil {
			continue
		}
		status := node.core.Status()
		if status.Role == RoleLeader && (leader == 0 || status.Term > term || status.Term == term && id < leader) {
			leader, term = id, status.Term
		}
	}
	return leader
}

func simulationRandomLiveID(cluster *simulationCluster, sample uint64) uint16 {
	for offset := 0; offset < len(cluster.ids); offset++ {
		id := cluster.ids[(int(sample%uint64(len(cluster.ids)))+offset)%len(cluster.ids)]
		if cluster.nodes[id].live && cluster.nodes[id].pending == nil {
			return id
		}
	}
	return 0
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

func simulationInjectPersistenceFault(t *testing.T, cluster *simulationCluster, sample uint64) {
	t.Helper()
	id := simulationRandomLiveID(cluster, sample)
	if id == 0 {
		return
	}
	node := cluster.nodes[id]
	term := node.core.HardState().Term + 1
	candidate := cluster.ids[0]
	if candidate == id {
		candidate = cluster.ids[1]
	}
	request := RequestVoteRequest{CandidateID: candidate, Term: term, LastLogIndex: node.core.Status().LastIndex, LastLogTerm: node.core.log.LastTerm()}
	if err := node.core.Step(candidate, request); err != nil {
		cluster.fail(t, "random fault setup voter=%d: %v", id, err)
	}
	if !cluster.takeReady(t, id) {
		cluster.fail(t, "random fault voter=%d had no Ready", id)
	}
	node.store.FailNext(StorageOperationPersist, errors.New("seeded persistence fault"))
	if err := cluster.persistReady(t, id); err == nil {
		cluster.fail(t, "random persistence fault voter=%d succeeded", id)
	}
	cluster.crash(t, id)
	cluster.restart(t, id)
}

func simulationCrashBeforePersistence(t *testing.T, cluster *simulationCluster, sample uint64) {
	t.Helper()
	id := simulationRandomLiveID(cluster, sample)
	if id == 0 {
		return
	}
	node := cluster.nodes[id]
	term := node.core.HardState().Term + 1
	candidate := cluster.ids[len(cluster.ids)-1]
	if candidate == id {
		candidate = cluster.ids[0]
	}
	request := RequestVoteRequest{CandidateID: candidate, Term: term, LastLogIndex: node.core.Status().LastIndex, LastLogTerm: node.core.log.LastTerm()}
	if err := node.core.Step(candidate, request); err != nil {
		cluster.fail(t, "random crash setup voter=%d: %v", id, err)
	}
	if cluster.takeReady(t, id) {
		cluster.crash(t, id)
		cluster.restart(t, id)
	}
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
		if !node.live || node.pending != nil || node.core.Status().CommitIndex != leaderState.CommitIndex || node.core.Status().LastIndex < leaderState.CommitIndex {
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
