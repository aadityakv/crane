package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"crane/internal/config"
)

var (
	errSimulationUndurableMessage = errors.New("simulation released message before durability")
	errSimulationSafety           = errors.New("simulation safety violation")
)

type simulationRandom struct{ state uint64 }

func (random *simulationRandom) Uint64() uint64 {
	random.state += 0x9e3779b97f4a7c15
	value := random.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

type simulationEntryIdentity struct {
	Term    uint64
	Kind    EntryKind
	Command string
}

func simulationIdentity(entry Entry) simulationEntryIdentity {
	return simulationEntryIdentity{Term: entry.Term, Kind: entry.Kind, Command: string(entry.CommandBytes())}
}

type simulationLink struct{ from, to uint16 }

type simulationEnvelope struct {
	sequence uint64
	from     uint16
	to       uint16
	rpc      RPC
	requires DurabilityPrerequisite
}

type simulationPending struct {
	ready     Ready
	batch     PersistenceBatch
	before    RecoveredState
	prospect  RecoveredState
	persisted bool
	applied   bool
	published bool
	released  bool
	results   map[uint64]ProposalResult
}

type simulationNode struct {
	id          uint16
	identity    StorageIdentity
	store       *MemoryStore
	core        *Core
	live        bool
	incarnation uint64
	pending     *simulationPending
	application map[uint64]simulationEntryIdentity
	appliedSeen map[uint64]struct{}
	proposals   map[ProposalID]Entry
	published   map[ProposalID]ProposalResult
	failed      map[ProposalID]error
	lastApplied uint64
	applyOrder  []uint64

	maxDurableTerm   uint64
	maxDurableCommit uint64
	maxDurableApply  uint64
	maxSnapshotBase  uint64
	maxCoreApplied   uint64
	lastVoteTerm     uint64
	lastVotedFor     uint16
}

type simulationCluster struct {
	t              *testing.T
	seed           uint64
	voters         VoterSet
	nodes          map[uint16]*simulationNode
	ids            []uint16
	queue          []simulationEnvelope
	partitioned    map[simulationLink]bool
	nextSequence   uint64
	trace          []string
	committed      map[uint64]simulationEntryIdentity
	leaderByTerm   map[uint64]uint16
	leaderRequired map[uint64]map[uint64]simulationEntryIdentity
	releasedCount  uint64
}

func newSimulationCluster(t *testing.T, voterCount int, seed uint64) *simulationCluster {
	t.Helper()
	configured := make([]config.RaftVoter, voterCount)
	for index := range configured {
		configured[index] = config.RaftVoter{NodeID: uint16(index + 1), Endpoint: fmt.Sprintf("127.0.0.1:%d", 32008+index)}
	}
	voters, err := NewVoterSet(configured)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &simulationCluster{
		t: t, seed: seed, voters: voters, nodes: make(map[uint16]*simulationNode, voterCount),
		partitioned: make(map[simulationLink]bool), committed: make(map[uint64]simulationEntryIdentity),
		leaderByTerm: make(map[uint64]uint16), leaderRequired: make(map[uint64]map[uint64]simulationEntryIdentity),
	}
	for _, voter := range voters.Voters() {
		cluster.ids = append(cluster.ids, voter.ID)
		var clusterID [16]byte
		binary.BigEndian.PutUint64(clusterID[:8], 0x5241465453494d31)
		binary.BigEndian.PutUint64(clusterID[8:], seed)
		identity, identityErr := NewStorageIdentity(StorageFormatVersion1, clusterID, voter.ID, voters)
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		store, storeErr := NewMemoryStore(identity, voters)
		if storeErr != nil {
			t.Fatal(storeErr)
		}
		node := &simulationNode{id: voter.ID, identity: identity, store: store, application: make(map[uint64]simulationEntryIdentity), appliedSeen: make(map[uint64]struct{}), proposals: make(map[ProposalID]Entry), published: make(map[ProposalID]ProposalResult), failed: make(map[ProposalID]error)}
		cluster.nodes[voter.ID] = node
		cluster.restart(t, voter.ID)
	}
	cluster.check(t, "cluster initialized")
	return cluster
}

func (cluster *simulationCluster) record(format string, arguments ...any) {
	cluster.trace = append(cluster.trace, fmt.Sprintf(format, arguments...))
	if len(cluster.trace) > 160 {
		cluster.trace = append([]string(nil), cluster.trace[len(cluster.trace)-160:]...)
	}
}

func (cluster *simulationCluster) traceText() string {
	return fmt.Sprintf("seed=%d voters=%v partitions=%v\n%s", cluster.seed, cluster.ids, cluster.partitionList(), strings.Join(cluster.trace, "\n"))
}

func (cluster *simulationCluster) partitionList() []simulationLink {
	links := make([]simulationLink, 0, len(cluster.partitioned))
	for link, blocked := range cluster.partitioned {
		if blocked {
			links = append(links, link)
		}
	}
	sort.Slice(links, func(i, j int) bool {
		if links[i].from != links[j].from {
			return links[i].from < links[j].from
		}
		return links[i].to < links[j].to
	})
	return links
}

func (cluster *simulationCluster) fail(t *testing.T, format string, arguments ...any) {
	t.Helper()
	t.Fatalf(format+"\n%s", append(arguments, cluster.traceText())...)
}

func (cluster *simulationCluster) restart(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.nodes[id]
	if node.live {
		cluster.fail(t, "restart voter %d while live", id)
	}
	recovered, err := node.store.Recover()
	if err != nil {
		cluster.fail(t, "recover voter %d: %v", id, err)
	}
	application, err := decodeSimulationApplication(recovered.Snapshot)
	if err != nil {
		cluster.fail(t, "decode voter %d snapshot: %v", id, err)
	}
	for _, entry := range recovered.Entries {
		if entry.Index <= recovered.HardState.CommitIndex && entry.Kind == EntryCommand {
			application[entry.Index] = simulationIdentity(entry)
		}
	}
	log, err := NewLog(recovered.SnapshotBase.LastIncludedIndex, recovered.SnapshotBase.LastIncludedTerm, recovered.HardState.CommitIndex, recovered.SnapshotBase.LastIncludedIndex, recovered.Entries)
	if err != nil {
		cluster.fail(t, "rebuild voter %d log: %v", id, err)
	}
	random := &simulationRandom{state: cluster.seed ^ (uint64(id) << 32) ^ (node.incarnation + 1)}
	core, err := NewCore(CoreOptions{
		LocalID: id, Voters: cluster.voters, HardState: recovered.HardState, Log: log,
		AppliedIndex: recovered.SnapshotBase.LastIncludedIndex, ElectionTimeoutMin: 5,
		ElectionTimeoutMax: 10, HeartbeatInterval: 1, Random: random,
	})
	if err != nil {
		cluster.fail(t, "rebuild voter %d core: %v", id, err)
	}
	if err := core.log.AdvanceApplied(recovered.HardState.CommitIndex); err != nil {
		cluster.fail(t, "establish voter %d recovered apply seam: %v", id, err)
	}
	node.core = core
	node.live = true
	node.incarnation++
	node.pending = nil
	node.application = application
	node.appliedSeen = make(map[uint64]struct{})
	node.proposals = make(map[ProposalID]Entry)
	node.published = make(map[ProposalID]ProposalResult)
	node.failed = make(map[ProposalID]error)
	node.lastApplied = recovered.SnapshotBase.LastIncludedIndex
	node.applyOrder = nil
	expectedReplay := recovered.SnapshotBase.LastIncludedIndex
	for _, entry := range recovered.Entries {
		if entry.Index > recovered.HardState.CommitIndex {
			break
		}
		expectedReplay++
		if entry.Index != expectedReplay {
			cluster.fail(t, "voter %d replay index=%d want=%d", id, entry.Index, expectedReplay)
		}
		node.applyOrder = append(node.applyOrder, entry.Index)
		node.lastApplied = entry.Index
	}
	cluster.record("restart voter=%d incarnation=%d term=%d commit=%d last=%d", id, node.incarnation, recovered.HardState.Term, recovered.HardState.CommitIndex, core.Status().LastIndex)
	cluster.check(t, fmt.Sprintf("restart voter %d", id))
}

func (cluster *simulationCluster) crash(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.nodes[id]
	if !node.live {
		return
	}
	cluster.record("crash voter=%d pending=%t persisted=%t applied=%t", id, node.pending != nil, node.pending != nil && node.pending.persisted, node.pending != nil && node.pending.applied)
	node.live = false
	node.core = nil
	node.pending = nil
	if err := node.store.AbortSnapshotStage(); err != nil {
		cluster.fail(t, "abort voter %d snapshot stage on crash: %v", id, err)
	}
	cluster.check(t, fmt.Sprintf("crash voter %d", id))
}

func (cluster *simulationCluster) requireIdleLive(t *testing.T, id uint16) *simulationNode {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.core == nil || node.pending != nil {
		cluster.fail(t, "voter %d is not live and idle", id)
	}
	return node
}

func (cluster *simulationCluster) tick(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.requireIdleLive(t, id)
	now := node.core.ElectionDeadline()
	if now <= node.core.now {
		now = node.core.now + 1
	}
	cluster.record("tick voter=%d now=%d", id, now)
	if err := node.core.Tick(now); err != nil {
		cluster.fail(t, "tick voter %d: %v", id, err)
	}
	cluster.settle(t, id)
}

func (cluster *simulationCluster) heartbeatTick(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.requireIdleLive(t, id)
	now := node.core.now + 1
	cluster.record("heartbeat tick voter=%d now=%d", id, now)
	if err := node.core.Tick(now); err != nil {
		cluster.fail(t, "heartbeat tick voter %d: %v", id, err)
	}
	cluster.settle(t, id)
}

func (cluster *simulationCluster) takeReady(t *testing.T, id uint16) bool {
	t.Helper()
	ok, err := cluster.tryTakeReady(id)
	if err != nil {
		cluster.fail(t, "validate voter %d Ready: %v", id, err)
	}
	if !ok {
		return false
	}
	node := cluster.nodes[id]
	ready := node.pending.ready
	cluster.record("ready voter=%d token=%d hard=%t entries=%d messages=%d committed=%d actions=%d", id, ready.Token, ready.HardState != nil, len(ready.Entries), len(ready.Messages), len(ready.CommittedEntries), len(ready.SnapshotActions))
	cluster.check(t, fmt.Sprintf("take Ready voter %d", id))
	return true
}

func (cluster *simulationCluster) tryTakeReady(id uint16) (bool, error) {
	node := cluster.nodes[id]
	if node == nil || !node.live || node.core == nil || node.pending != nil {
		return false, fmt.Errorf("voter %d is not live and idle", id)
	}
	ready, ok := node.core.Ready()
	if !ok {
		return false, nil
	}
	before, err := node.store.Recover()
	if err != nil {
		return false, fmt.Errorf("recover voter %d before Ready: %w", id, err)
	}
	pendingLocal := make(map[ProposalID]pendingLocalRequest, len(node.proposals))
	for proposalID, entry := range node.proposals {
		pendingLocal[proposalID] = pendingLocalRequest{entry: entry.Clone(), response: make(chan localRequestResponse, 1)}
	}
	validator := &Node{
		options: NodeOptions{LocalID: id, Identity: node.identity, Voters: cluster.voters, MaxSnapshotBytes: node.store.SnapshotLimit()},
		core:    node.core, durableState: before.Clone(), pendingLocal: pendingLocal,
	}
	batch, prospective, err := validator.validateReadyAndDerive(ready)
	if err != nil {
		return false, err
	}
	node.pending = &simulationPending{ready: ready, batch: batch, before: before, prospect: prospective}
	return true, nil
}

func (cluster *simulationCluster) persistReady(t *testing.T, id uint16) error {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending == nil || node.pending.persisted {
		return fmt.Errorf("voter %d has no unpersisted Ready", id)
	}
	batch := node.pending.batch
	if persistenceBatchHasEffects(batch) {
		if err := node.store.Persist(batch); err != nil {
			cluster.record("persist voter=%d token=%d failed=%v", id, node.pending.ready.Token, err)
			return err
		}
	}
	node.pending.persisted = true
	if durable, err := node.store.Recover(); err != nil || !reflect.DeepEqual(durable, node.pending.prospect) {
		return fmt.Errorf("persist voter %d Ready recovered=%+v want=%+v: %w", id, durable, node.pending.prospect, err)
	}
	cluster.record("persist voter=%d token=%d", id, node.pending.ready.Token)
	cluster.check(t, fmt.Sprintf("persist Ready voter %d", id))
	return nil
}

func (cluster *simulationCluster) releaseReady(t *testing.T, id uint16) error {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending == nil || node.pending.released {
		return fmt.Errorf("voter %d has no unreleased Ready", id)
	}
	ready := node.pending.ready
	if persistenceBatchHasEffects(node.pending.batch) && !node.pending.persisted {
		return fmt.Errorf("%w: voter=%d token=%d Ready persistence is incomplete", errSimulationUndurableMessage, id, ready.Token)
	}
	for _, message := range ready.Messages {
		validator := &Node{options: NodeOptions{Identity: node.identity, Voters: cluster.voters}, core: node.core, durableState: node.pending.before}
		if err := validator.validateMessageDurability(message, node.pending.before, node.pending.prospect); err != nil {
			return fmt.Errorf("%w: voter=%d Ready declaration: %v", errSimulationUndurableMessage, id, err)
		}
		if (message.Requires.HardState || message.Requires.EntriesThrough != 0) && !node.pending.persisted {
			return fmt.Errorf("%w: voter=%d token=%d requires=%+v", errSimulationUndurableMessage, id, ready.Token, message.Requires)
		}
		if err := cluster.checkMessageDurable(node, message); err != nil {
			return err
		}
		cluster.enqueue(id, message)
	}
	for _, action := range ready.SnapshotActions {
		message, err := cluster.executeSnapshotAction(node, ready.Token, action)
		if err != nil {
			return err
		}
		cluster.enqueue(id, message)
	}
	node.pending.released = true
	cluster.record("release voter=%d token=%d messages=%d", id, ready.Token, len(ready.Messages)+len(ready.SnapshotActions))
	cluster.check(t, fmt.Sprintf("release Ready voter %d", id))
	return nil
}

func (cluster *simulationCluster) checkMessageDurable(node *simulationNode, message PeerMessage) error {
	state, err := node.store.Recover()
	if err != nil {
		return err
	}
	if message.Requires.HardState && node.pending.ready.HardState != nil && state.HardState != *node.pending.ready.HardState {
		return fmt.Errorf("%w: voter=%d hard state is not exact", errSimulationUndurableMessage, node.id)
	}
	if message.Requires.EntriesThrough != 0 {
		entry, exists := recoveredEntryAt(state, message.Requires.EntriesThrough)
		if !exists {
			return fmt.Errorf("%w: voter=%d missing entry=%d", errSimulationUndurableMessage, node.id, message.Requires.EntriesThrough)
		}
		found := false
		for _, unstable := range node.pending.ready.Entries {
			if unstable.Index == entry.Index && sameEntry(unstable, entry) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: voter=%d wrong durable entry=%d", errSimulationUndurableMessage, node.id, entry.Index)
		}
	}
	return nil
}

func (cluster *simulationCluster) enqueue(from uint16, message PeerMessage) {
	cluster.nextSequence++
	cluster.releasedCount++
	cluster.queue = append(cluster.queue, simulationEnvelope{sequence: cluster.nextSequence, from: from, to: message.To, rpc: CloneRPC(message.RPC), requires: message.Requires})
}

func (cluster *simulationCluster) executeSnapshotAction(node *simulationNode, token ReadyToken, action SnapshotAction) (PeerMessage, error) {
	if action.Reset {
		if err := node.store.AbortSnapshotStage(); err != nil {
			return PeerMessage{}, err
		}
	}
	if action.Kind == SnapshotActionAbort {
		if err := node.store.AbortSnapshotStage(); err != nil {
			return PeerMessage{}, err
		}
		return node.core.CompleteSnapshotAction(token, SnapshotActionResult{Rejected: true})
	}
	result, err := node.store.StageSnapshotChunk(action.Request)
	if err != nil {
		if errors.Is(err, ErrSnapshotRejected) {
			return node.core.CompleteSnapshotAction(token, SnapshotActionResult{Rejected: true})
		}
		return PeerMessage{}, err
	}
	actionResult := SnapshotActionResult{NextOffset: result.NextOffset, Done: result.Done, State: result.State}
	if result.Done {
		application, decodeErr := decodeSimulationApplication(result.State.Snapshot)
		if decodeErr != nil {
			return PeerMessage{}, decodeErr
		}
		for _, entry := range result.State.Entries {
			if entry.Index <= result.State.HardState.CommitIndex && entry.Kind == EntryCommand {
				application[entry.Index] = simulationIdentity(entry)
			}
		}
		node.application = application
		node.appliedSeen = make(map[uint64]struct{})
		node.applyOrder = nil
		node.lastApplied = result.State.SnapshotBase.LastIncludedIndex
		for _, entry := range result.State.Entries {
			if entry.Index > result.State.HardState.CommitIndex {
				break
			}
			expected, ok := checkedNextIndex(node.lastApplied)
			if !ok || entry.Index != expected {
				return PeerMessage{}, fmt.Errorf("%w: installed replay index=%d want=%d", errSimulationSafety, entry.Index, expected)
			}
			node.applyOrder = append(node.applyOrder, entry.Index)
			node.lastApplied = entry.Index
		}
	}
	return node.core.CompleteSnapshotAction(token, actionResult)
}

func (cluster *simulationCluster) applyReady(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending == nil || node.pending.applied || !node.pending.persisted || !node.pending.released {
		cluster.fail(t, "voter %d Ready cannot apply", id)
	}
	durable, err := node.store.Recover()
	if err != nil {
		cluster.fail(t, "recover before voter %d apply: %v", id, err)
	}
	node.pending.results = make(map[uint64]ProposalResult, len(node.pending.ready.CommittedEntries))
	for _, entry := range node.pending.ready.CommittedEntries {
		expected, ok := checkedNextIndex(node.lastApplied)
		if !ok || entry.Index != expected {
			cluster.fail(t, "voter %d application sequence index=%d want=%d", id, entry.Index, expected)
		}
		if entry.Index > durable.HardState.CommitIndex {
			cluster.fail(t, "voter %d applied index %d before durable commit %d", id, entry.Index, durable.HardState.CommitIndex)
		}
		if _, duplicate := node.appliedSeen[entry.Index]; duplicate {
			cluster.fail(t, "voter %d duplicate effect at index %d in incarnation %d", id, entry.Index, node.incarnation)
		}
		if entry.Index > durable.SnapshotBase.LastIncludedIndex {
			durableEntry, exists := recoveredEntryAt(durable, entry.Index)
			if !exists || !sameEntry(durableEntry, entry) {
				cluster.fail(t, "voter %d applied index %d before identical persistence", id, entry.Index)
			}
		}
		node.appliedSeen[entry.Index] = struct{}{}
		node.lastApplied = entry.Index
		node.applyOrder = append(node.applyOrder, entry.Index)
		result := []byte(nil)
		if entry.Kind == EntryCommand {
			node.application[entry.Index] = simulationIdentity(entry)
			result = entry.CommandBytes()
		}
		node.pending.results[entry.Index] = ProposalResult{Index: entry.Index, Term: entry.Term, Result: result}
	}
	node.pending.applied = true
	cluster.record("apply voter=%d token=%d entries=%d", id, node.pending.ready.Token, len(node.pending.ready.CommittedEntries))
	cluster.check(t, fmt.Sprintf("apply Ready voter %d", id))
}

func (cluster *simulationCluster) publishReadyResults(t *testing.T, id uint16) error {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending == nil || node.pending.published {
		return fmt.Errorf("voter %d has no unpublished Ready", id)
	}
	if !node.pending.persisted || !node.pending.released || !node.pending.applied {
		return fmt.Errorf("%w: voter=%d token=%d proposal publication before durable apply", errSimulationUndurableMessage, id, node.pending.ready.Token)
	}
	for _, committed := range node.pending.ready.CommittedProposals {
		waiter, exists := node.proposals[committed.ID]
		if !exists || !sameEntry(waiter, committed.Entry) {
			return fmt.Errorf("%w: committed proposal %d waiter mismatch", errSimulationSafety, committed.ID)
		}
		result, exists := node.pending.results[committed.Entry.Index]
		if !exists || result.Index != committed.Entry.Index || result.Term != committed.Entry.Term || !bytes.Equal(result.Result, committed.Entry.CommandBytes()) {
			return fmt.Errorf("%w: committed proposal %d result mismatch", errSimulationSafety, committed.ID)
		}
		if _, duplicate := node.published[committed.ID]; duplicate {
			return fmt.Errorf("%w: duplicate proposal result %d", errSimulationSafety, committed.ID)
		}
		result.Result = cloneBytes(result.Result)
		node.published[committed.ID] = result
		delete(node.proposals, committed.ID)
	}
	for _, failed := range node.pending.ready.FailedProposals {
		waiter, exists := node.proposals[failed.ID]
		if !exists || !sameEntry(waiter, failed.Entry) || failed.Err == nil {
			return fmt.Errorf("%w: failed proposal %d waiter mismatch", errSimulationSafety, failed.ID)
		}
		if _, duplicate := node.failed[failed.ID]; duplicate {
			return fmt.Errorf("%w: duplicate failed proposal %d", errSimulationSafety, failed.ID)
		}
		node.failed[failed.ID] = failed.Err
		delete(node.proposals, failed.ID)
	}
	node.pending.published = true
	cluster.record("publish voter=%d token=%d committed=%d failed=%d", id, node.pending.ready.Token, len(node.pending.ready.CommittedProposals), len(node.pending.ready.FailedProposals))
	cluster.check(t, fmt.Sprintf("publish Ready voter %d", id))
	return nil
}

func (cluster *simulationCluster) advanceReady(t *testing.T, id uint16) {
	t.Helper()
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending == nil || !node.pending.persisted || !node.pending.released || !node.pending.applied || !node.pending.published {
		cluster.fail(t, "voter %d Ready cannot advance", id)
	}
	token := node.pending.ready.Token
	if err := node.core.Advance(token); err != nil {
		cluster.fail(t, "advance voter %d token %d: %v", id, token, err)
	}
	node.pending = nil
	cluster.record("advance voter=%d token=%d", id, token)
	cluster.check(t, fmt.Sprintf("advance Ready voter %d", id))
}

func (cluster *simulationCluster) settle(t *testing.T, id uint16) {
	t.Helper()
	for iterations := 0; iterations < 64; iterations++ {
		node := cluster.nodes[id]
		if !node.live {
			return
		}
		if node.pending == nil && !cluster.takeReady(t, id) {
			return
		}
		if err := cluster.persistReady(t, id); err != nil {
			cluster.fail(t, "persist voter %d Ready: %v", id, err)
		}
		if err := cluster.releaseReady(t, id); err != nil {
			cluster.fail(t, "release voter %d Ready: %v", id, err)
		}
		cluster.applyReady(t, id)
		if err := cluster.publishReadyResults(t, id); err != nil {
			cluster.fail(t, "publish voter %d Ready: %v", id, err)
		}
		cluster.advanceReady(t, id)
	}
	cluster.fail(t, "voter %d exceeded Ready drain bound", id)
}

func (cluster *simulationCluster) deliverMatching(t *testing.T, predicate func(simulationEnvelope) bool) bool {
	t.Helper()
	for index, envelope := range cluster.queue {
		if !predicate(envelope) {
			continue
		}
		cluster.queue = append(cluster.queue[:index], cluster.queue[index+1:]...)
		if cluster.partitioned[simulationLink{from: envelope.from, to: envelope.to}] || !cluster.nodes[envelope.to].live {
			cluster.record("drop seq=%d %d->%d type=%T", envelope.sequence, envelope.from, envelope.to, envelope.rpc)
			cluster.check(t, "drop message")
			return true
		}
		target := cluster.requireIdleLive(t, envelope.to)
		cluster.record("deliver seq=%d %d->%d type=%T", envelope.sequence, envelope.from, envelope.to, envelope.rpc)
		if err := target.core.Step(envelope.from, CloneRPC(envelope.rpc)); err != nil {
			cluster.fail(t, "deliver seq %d %d->%d %T: %v", envelope.sequence, envelope.from, envelope.to, envelope.rpc, err)
		}
		cluster.settle(t, envelope.to)
		cluster.check(t, "deliver message")
		return true
	}
	return false
}

func (cluster *simulationCluster) stepOnly(t *testing.T, predicate func(simulationEnvelope) bool) (simulationEnvelope, bool) {
	t.Helper()
	for index, envelope := range cluster.queue {
		if !predicate(envelope) {
			continue
		}
		cluster.queue = append(cluster.queue[:index], cluster.queue[index+1:]...)
		if cluster.partitioned[simulationLink{from: envelope.from, to: envelope.to}] || !cluster.nodes[envelope.to].live {
			cluster.record("drop seq=%d %d->%d type=%T", envelope.sequence, envelope.from, envelope.to, envelope.rpc)
			cluster.check(t, "drop message without settle")
			return envelope, true
		}
		target := cluster.requireIdleLive(t, envelope.to)
		cluster.record("step-only seq=%d %d->%d type=%T", envelope.sequence, envelope.from, envelope.to, envelope.rpc)
		if err := target.core.Step(envelope.from, CloneRPC(envelope.rpc)); err != nil {
			cluster.fail(t, "step-only seq %d %d->%d %T: %v", envelope.sequence, envelope.from, envelope.to, envelope.rpc, err)
		}
		cluster.check(t, "step message without settle")
		return envelope, true
	}
	return simulationEnvelope{}, false
}

func (cluster *simulationCluster) deliver(t *testing.T, from, to uint16, prototype RPC) bool {
	t.Helper()
	return cluster.deliverMatching(t, func(envelope simulationEnvelope) bool {
		return envelope.from == from && envelope.to == to && fmt.Sprintf("%T", envelope.rpc) == fmt.Sprintf("%T", prototype)
	})
}

func (cluster *simulationCluster) dropMatching(t *testing.T, predicate func(simulationEnvelope) bool) int {
	t.Helper()
	kept := cluster.queue[:0]
	dropped := 0
	for _, envelope := range cluster.queue {
		if predicate(envelope) {
			dropped++
			cluster.record("explicit drop seq=%d %d->%d type=%T", envelope.sequence, envelope.from, envelope.to, envelope.rpc)
			continue
		}
		kept = append(kept, envelope)
	}
	cluster.queue = kept
	cluster.check(t, "explicit drop")
	return dropped
}

func (cluster *simulationCluster) partitionPair(t *testing.T, left, right uint16) {
	t.Helper()
	cluster.partitioned[simulationLink{from: left, to: right}] = true
	cluster.partitioned[simulationLink{from: right, to: left}] = true
	cluster.record("partition %d<->%d", left, right)
	cluster.check(t, "partition link")
}

func (cluster *simulationCluster) healAll(t *testing.T) {
	t.Helper()
	cluster.partitioned = make(map[simulationLink]bool)
	cluster.record("heal all")
	cluster.check(t, "heal all")
}

func (cluster *simulationCluster) isolate(t *testing.T, id uint16) {
	t.Helper()
	for _, other := range cluster.ids {
		if other != id {
			cluster.partitionPair(t, id, other)
		}
	}
}

func (cluster *simulationCluster) elect(t *testing.T, candidate uint16, supporters ...uint16) {
	t.Helper()
	cluster.tick(t, candidate)
	for _, supporter := range supporters {
		if !cluster.deliver(t, candidate, supporter, PreVoteRequest{}) {
			cluster.fail(t, "missing pre-vote request %d->%d", candidate, supporter)
		}
		if !cluster.deliver(t, supporter, candidate, PreVoteResponse{}) {
			cluster.fail(t, "missing pre-vote response %d->%d", supporter, candidate)
		}
		if cluster.nodes[candidate].core.Status().Role == RoleCandidate {
			break
		}
	}
	for _, supporter := range supporters {
		if cluster.nodes[candidate].core.Status().Role == RoleLeader {
			break
		}
		if !cluster.deliver(t, candidate, supporter, RequestVoteRequest{}) {
			cluster.fail(t, "missing vote request %d->%d", candidate, supporter)
		}
		if !cluster.deliver(t, supporter, candidate, RequestVoteResponse{}) {
			cluster.fail(t, "missing vote response %d->%d", supporter, candidate)
		}
	}
	if status := cluster.nodes[candidate].core.Status(); status.Role != RoleLeader {
		cluster.fail(t, "voter %d role=%d term=%d, want leader", candidate, status.Role, status.Term)
	}
	cluster.replicateForQuorum(t, candidate, supporters)
	if !cluster.nodes[candidate].core.hasCommittedCurrentTerm() {
		cluster.fail(t, "leader %d current-term no-op did not commit", candidate)
	}
}

func (cluster *simulationCluster) replicateForQuorum(t *testing.T, leader uint16, followers []uint16) {
	t.Helper()
	needed := cluster.voters.Majority() - 1
	for _, follower := range followers {
		if needed == 0 {
			break
		}
		matched := false
		for attempts := 0; attempts < 16; attempts++ {
			if !cluster.deliver(t, leader, follower, AppendEntriesRequest{}) {
				cluster.heartbeatTick(t, leader)
				continue
			}
			if !cluster.deliver(t, follower, leader, AppendEntriesResponse{}) {
				cluster.fail(t, "missing append response %d->%d", follower, leader)
			}
			progress, exists := cluster.nodes[leader].core.Progress(follower)
			if exists && progress.MatchIndex >= cluster.nodes[leader].core.Status().LastIndex {
				matched = true
				break
			}
		}
		if matched {
			needed--
		}
	}
	if needed != 0 {
		cluster.fail(t, "leader %d lacked quorum supporters", leader)
	}
}

func (cluster *simulationCluster) proposeAndCommit(t *testing.T, leader uint16, command string, followers ...uint16) Entry {
	t.Helper()
	_, entry, err := cluster.propose(leader, EntryCommand, []byte(command))
	if err != nil {
		cluster.fail(t, "propose %q on leader %d: %v", command, leader, err)
	}
	node := cluster.nodes[leader]
	cluster.record("propose leader=%d index=%d term=%d command=%q", leader, entry.Index, entry.Term, command)
	cluster.settle(t, leader)
	cluster.replicateForQuorum(t, leader, followers)
	state, err := node.store.Recover()
	if err != nil || state.HardState.CommitIndex < entry.Index {
		cluster.fail(t, "proposal %q index=%d not durably committed: state=%+v error=%v", command, entry.Index, state.HardState, err)
	}
	return entry
}

func (cluster *simulationCluster) propose(id uint16, kind EntryKind, command []byte) (ProposalID, Entry, error) {
	node := cluster.nodes[id]
	if node == nil || !node.live || node.pending != nil {
		return 0, Entry{}, fmt.Errorf("voter %d cannot propose", id)
	}
	proposalID, entry, err := node.core.proposeTracked(kind, command)
	if err != nil {
		return 0, Entry{}, err
	}
	if proposalID == 0 {
		return 0, Entry{}, fmt.Errorf("%w: zero proposal identity", errSimulationSafety)
	}
	if _, duplicate := node.proposals[proposalID]; duplicate {
		return 0, Entry{}, fmt.Errorf("%w: duplicate proposal identity %d", errSimulationSafety, proposalID)
	}
	node.proposals[proposalID] = entry.Clone()
	return proposalID, entry, nil
}

func (cluster *simulationCluster) syncFollowers(t *testing.T, leader uint16, followers ...uint16) {
	t.Helper()
	cluster.heartbeatTick(t, leader)
	for _, follower := range followers {
		for attempts := 0; attempts < 16; attempts++ {
			if cluster.deliver(t, leader, follower, AppendEntriesRequest{}) {
				_ = cluster.deliver(t, follower, leader, AppendEntriesResponse{})
				if cluster.nodes[follower].core.Status().CommitIndex == cluster.nodes[leader].core.Status().CommitIndex {
					break
				}
				cluster.heartbeatTick(t, leader)
				continue
			}
			cluster.heartbeatTick(t, leader)
		}
		if cluster.nodes[follower].core.Status().CommitIndex != cluster.nodes[leader].core.Status().CommitIndex {
			cluster.fail(t, "follower %d commit=%d want leader %d commit=%d", follower, cluster.nodes[follower].core.Status().CommitIndex, leader, cluster.nodes[leader].core.Status().CommitIndex)
		}
	}
}

func (cluster *simulationCluster) check(t *testing.T, event string) {
	t.Helper()
	states := make([]string, 0, len(cluster.ids))
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if !node.live {
			states = append(states, fmt.Sprintf("n%d=down", id))
			continue
		}
		status := node.core.Status()
		states = append(states, fmt.Sprintf("n%d=r%d/t%d/c%d/a%d/l%d", id, status.Role, status.Term, status.CommitIndex, status.AppliedIndex, status.LastIndex))
	}
	cluster.record("state after %s: %s", event, strings.Join(states, " "))
	if err := cluster.oracle(); err != nil {
		cluster.fail(t, "%s: %v", event, err)
	}
}

func (cluster *simulationCluster) oracle() error {
	states := make(map[uint16]RecoveredState, len(cluster.nodes))
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		state, err := node.store.Recover()
		if err != nil {
			return fmt.Errorf("%w: voter %d recover: %v", errSimulationSafety, id, err)
		}
		if err := validateRecoveredStateWithSnapshotLimit(state, node.identity, cluster.voters, node.store.SnapshotLimit()); err != nil {
			return fmt.Errorf("%w: voter %d durable state: %v", errSimulationSafety, id, err)
		}
		if state.HardState.Term < node.maxDurableTerm || state.HardState.CommitIndex < node.maxDurableCommit || state.AppliedIndex < node.maxDurableApply || state.SnapshotBase.LastIncludedIndex < node.maxSnapshotBase {
			return fmt.Errorf("%w: voter %d durable regression term=%d/%d commit=%d/%d applied=%d/%d snapshot=%d/%d", errSimulationSafety, id, state.HardState.Term, node.maxDurableTerm, state.HardState.CommitIndex, node.maxDurableCommit, state.AppliedIndex, node.maxDurableApply, state.SnapshotBase.LastIncludedIndex, node.maxSnapshotBase)
		}
		if state.HardState.Term == node.lastVoteTerm && node.lastVotedFor != 0 && state.HardState.VotedFor != node.lastVotedFor {
			return fmt.Errorf("%w: voter %d vote changed in term %d from %d to %d", errSimulationSafety, id, state.HardState.Term, node.lastVotedFor, state.HardState.VotedFor)
		}
		node.maxDurableTerm = state.HardState.Term
		node.maxDurableCommit = state.HardState.CommitIndex
		node.maxDurableApply = state.AppliedIndex
		node.maxSnapshotBase = state.SnapshotBase.LastIncludedIndex
		node.lastVoteTerm = state.HardState.Term
		node.lastVotedFor = state.HardState.VotedFor
		states[id] = state
		for _, entry := range state.Entries {
			if entry.Index > state.HardState.CommitIndex {
				break
			}
			identity := simulationIdentity(entry)
			if previous, exists := cluster.committed[entry.Index]; exists && previous != identity {
				return fmt.Errorf("%w: committed index %d changed from %+v to %+v", errSimulationSafety, entry.Index, previous, identity)
			}
			cluster.committed[entry.Index] = identity
		}
	}
	for _, id := range cluster.ids {
		if err := cluster.validateSnapshotLedger(id, states[id]); err != nil {
			return err
		}
	}

	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if !node.live {
			continue
		}
		status := node.core.Status()
		logState := node.core.LogState()
		if logState.SnapshotIndex > status.AppliedIndex || status.AppliedIndex > status.CommitIndex || status.CommitIndex > status.LastIndex {
			return fmt.Errorf("%w: voter %d indices snapshot=%d applied=%d commit=%d last=%d", errSimulationSafety, id, logState.SnapshotIndex, status.AppliedIndex, status.CommitIndex, status.LastIndex)
		}
		if status.AppliedIndex < node.maxCoreApplied {
			return fmt.Errorf("%w: voter %d applied seam regressed from %d to %d", errSimulationSafety, id, node.maxCoreApplied, status.AppliedIndex)
		}
		node.maxCoreApplied = status.AppliedIndex
		if status.Role == RoleLeader {
			prior, exists := cluster.leaderByTerm[status.Term]
			if exists && prior != id {
				return fmt.Errorf("%w: term %d leaders %d and %d", errSimulationSafety, status.Term, prior, id)
			}
			if !exists {
				cluster.leaderByTerm[status.Term] = id
				cluster.leaderRequired[status.Term] = cloneSimulationIdentityMap(cluster.committed)
			}
			for _, index := range sortedSimulationIdentityIndices(cluster.leaderRequired[status.Term]) {
				identity := cluster.leaderRequired[status.Term][index]
				if index <= logState.SnapshotIndex {
					if err := cluster.snapshotCoversIdentity(states[id], index, identity); err != nil {
						return fmt.Errorf("%w: term %d leader %d compacted prefix: %v", errSimulationSafety, status.Term, id, err)
					}
					continue
				}
				entry, err := node.core.log.Entry(index)
				if err != nil || simulationIdentity(entry) != identity {
					return fmt.Errorf("%w: term %d leader %d lacks committed index %d", errSimulationSafety, status.Term, id, index)
				}
			}
		}
		for _, index := range sortedSimulationIdentityIndices(node.application) {
			identity := node.application[index]
			state := states[id]
			if index > state.HardState.CommitIndex {
				return fmt.Errorf("%w: voter %d application index %d exceeds durable commit %d", errSimulationSafety, id, index, state.HardState.CommitIndex)
			}
			if index > state.SnapshotBase.LastIncludedIndex {
				entry, exists := recoveredEntryAt(state, index)
				if !exists || simulationIdentity(entry) != identity {
					return fmt.Errorf("%w: voter %d application index %d lacks identical durable entry", errSimulationSafety, id, index)
				}
			}
		}
	}
	return nil
}

func (cluster *simulationCluster) validateSnapshotLedger(id uint16, state RecoveredState) error {
	if state.SnapshotBase.LastIncludedIndex == 0 {
		return nil
	}
	if state.Snapshot == nil || state.Snapshot.Metadata != state.SnapshotBase {
		return fmt.Errorf("%w: voter %d compacted base lacks exact snapshot", errSimulationSafety, id)
	}
	baseIdentity, exists := cluster.committed[state.SnapshotBase.LastIncludedIndex]
	if !exists || baseIdentity.Term != state.SnapshotBase.LastIncludedTerm {
		return fmt.Errorf("%w: voter %d snapshot base %d/%d does not match committed ledger", errSimulationSafety, id, state.SnapshotBase.LastIncludedIndex, state.SnapshotBase.LastIncludedTerm)
	}
	application, err := decodeSimulationApplication(state.Snapshot)
	if err != nil {
		return fmt.Errorf("%w: voter %d decode application snapshot: %v", errSimulationSafety, id, err)
	}
	for _, index := range sortedSimulationIdentityIndices(cluster.committed) {
		if index > state.SnapshotBase.LastIncludedIndex {
			break
		}
		if err := cluster.snapshotCoversIdentityWithApplication(state, application, index, cluster.committed[index]); err != nil {
			return fmt.Errorf("%w: voter %d: %v", errSimulationSafety, id, err)
		}
	}
	for _, index := range sortedSimulationIdentityIndices(application) {
		identity, committed := cluster.committed[index]
		if index > state.SnapshotBase.LastIncludedIndex || !committed || identity.Kind != EntryCommand || application[index] != identity {
			return fmt.Errorf("%w: voter %d snapshot contains uncommitted or changed command index %d", errSimulationSafety, id, index)
		}
	}
	return nil
}

func (cluster *simulationCluster) snapshotCoversIdentity(state RecoveredState, index uint64, identity simulationEntryIdentity) error {
	application, err := decodeSimulationApplication(state.Snapshot)
	if err != nil {
		return err
	}
	return cluster.snapshotCoversIdentityWithApplication(state, application, index, identity)
}

func (*simulationCluster) snapshotCoversIdentityWithApplication(state RecoveredState, application map[uint64]simulationEntryIdentity, index uint64, identity simulationEntryIdentity) error {
	if state.Snapshot == nil || index > state.SnapshotBase.LastIncludedIndex {
		return fmt.Errorf("snapshot does not cover committed index %d", index)
	}
	if identity.Kind == EntryCommand && application[index] != identity {
		return fmt.Errorf("snapshot command index %d=%+v want %+v", index, application[index], identity)
	}
	return nil
}

func encodeSimulationApplication(application map[uint64]simulationEntryIdentity) []byte {
	indices := sortedSimulationIdentityIndices(application)
	var encoded bytes.Buffer
	_ = binary.Write(&encoded, binary.BigEndian, uint32(len(indices)))
	for _, index := range indices {
		identity := application[index]
		_ = binary.Write(&encoded, binary.BigEndian, index)
		_ = binary.Write(&encoded, binary.BigEndian, identity.Term)
		_ = encoded.WriteByte(byte(identity.Kind))
		_ = binary.Write(&encoded, binary.BigEndian, uint32(len(identity.Command)))
		_, _ = encoded.WriteString(identity.Command)
	}
	return encoded.Bytes()
}

func sortedSimulationIdentityIndices(values map[uint64]simulationEntryIdentity) []uint64 {
	indices := make([]uint64, 0, len(values))
	for index := range values {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	return indices
}

func cloneSimulationIdentityMap(values map[uint64]simulationEntryIdentity) map[uint64]simulationEntryIdentity {
	cloned := make(map[uint64]simulationEntryIdentity, len(values))
	for index, identity := range values {
		cloned[index] = identity
	}
	return cloned
}

func decodeSimulationApplication(snapshot *Snapshot) (map[uint64]simulationEntryIdentity, error) {
	application := make(map[uint64]simulationEntryIdentity)
	if snapshot == nil {
		return application, nil
	}
	reader := bytes.NewReader(snapshot.StateBytes())
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	for item := uint32(0); item < count; item++ {
		var index, term uint64
		var kind byte
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &kind); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil || uint64(length) > uint64(reader.Len()) {
			return nil, fmt.Errorf("invalid simulation snapshot command length %d: %v", length, err)
		}
		command := make([]byte, length)
		if _, err := reader.Read(command); err != nil {
			return nil, err
		}
		application[index] = simulationEntryIdentity{Term: term, Kind: EntryKind(kind), Command: string(command)}
	}
	if reader.Len() != 0 {
		return nil, errors.New("simulation snapshot has trailing bytes")
	}
	return application, nil
}

func (cluster *simulationCluster) captureSnapshot(t *testing.T, id uint16) Snapshot {
	t.Helper()
	node := cluster.requireIdleLive(t, id)
	state := node.core.LogState()
	if state.AppliedIndex == 0 || state.AppliedIndex != state.CommitIndex {
		cluster.fail(t, "voter %d cannot capture at applied=%d commit=%d", id, state.AppliedIndex, state.CommitIndex)
	}
	term, err := node.core.log.Term(state.AppliedIndex)
	if err != nil {
		cluster.fail(t, "snapshot term voter %d: %v", id, err)
	}
	snapshot, err := NewSnapshot(node.identity, SnapshotMetadata{LastIncludedIndex: state.AppliedIndex, LastIncludedTerm: term, StateMachineSchemaVersion: 1}, encodeSimulationApplication(node.application), node.store.SnapshotLimit())
	if err != nil {
		cluster.fail(t, "new voter %d snapshot: %v", id, err)
	}
	if err := node.store.PersistSnapshot(snapshot); err != nil {
		cluster.fail(t, "persist voter %d snapshot: %v", id, err)
	}
	if err := node.core.CompactSnapshot(snapshot.Metadata); err != nil {
		cluster.fail(t, "compact voter %d snapshot: %v", id, err)
	}
	cluster.record("snapshot voter=%d base=%d term=%d bytes=%d", id, snapshot.Metadata.LastIncludedIndex, snapshot.Metadata.LastIncludedTerm, len(snapshot.StateBytes()))
	cluster.check(t, "capture snapshot")
	return snapshot
}

func TestSimulationSafetyOracleMutations(t *testing.T) {
	t.Run("snapshot action cannot execute before enclosing Ready persistence", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1100)
		cluster.elect(t, 1, 2)
		committed := cluster.proposeAndCommit(t, 1, "snapshot-action-durability", 2)
		stateBytes := encodeSimulationApplication(cluster.nodes[1].application)
		snapshot, err := NewSnapshot(cluster.nodes[1].identity, SnapshotMetadata{LastIncludedIndex: committed.Index, LastIncludedTerm: committed.Term, StateMachineSchemaVersion: 1}, stateBytes, cluster.nodes[1].store.SnapshotLimit())
		if err != nil {
			t.Fatal(err)
		}
		cluster.queue = nil
		var transferID TransferID
		transferID[0] = 1
		chunkLength := len(stateBytes) / 2
		if chunkLength == 0 {
			chunkLength = 1
		}
		request := InstallSnapshotRequest{
			LeaderID: 1, Term: committed.Term, TransferID: transferID, SnapshotID: snapshot.ID,
			LastIncludedIndex: committed.Index, LastIncludedTerm: committed.Term, StateMachineSchemaVersion: 1,
			TotalLength: uint64(len(stateBytes)), Checksum: snapshot.StateChecksum,
			Chunk: cloneBytes(stateBytes[:chunkLength]), Done: false,
		}
		before, err := cluster.nodes[3].store.Recover()
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.nodes[3].core.Step(1, request); err != nil {
			t.Fatal(err)
		}
		if !cluster.takeReady(t, 3) {
			t.Fatal("higher-term snapshot did not produce Ready")
		}
		ready := cluster.nodes[3].pending.ready
		if ready.HardState == nil || len(ready.SnapshotActions) != 1 {
			cluster.fail(t, "snapshot Ready hard=%v actions=%d", ready.HardState != nil, len(ready.SnapshotActions))
		}
		queueBefore := len(cluster.queue)
		if err := cluster.releaseReady(t, 3); !errors.Is(err, errSimulationUndurableMessage) {
			cluster.fail(t, "early snapshot action release error=%v, want durability rejection", err)
		}
		after, err := cluster.nodes[3].store.Recover()
		if err != nil || !reflect.DeepEqual(after, before) || cluster.nodes[3].store.stage != nil || len(cluster.queue) != queueBefore {
			cluster.fail(t, "early snapshot action mutated state: after=%+v stage=%v queue=%d/%d error=%v", after, cluster.nodes[3].store.stage != nil, len(cluster.queue), queueBefore, err)
		}
		if err := cluster.persistReady(t, 3); err != nil {
			cluster.fail(t, "persist snapshot Ready: %v", err)
		}
		if err := cluster.releaseReady(t, 3); err != nil {
			cluster.fail(t, "release persisted snapshot Ready: %v", err)
		}
		installed, err := cluster.nodes[3].store.Recover()
		if err != nil || installed.Snapshot != nil || cluster.nodes[3].store.stage == nil || !bytes.Equal(cluster.nodes[3].store.stage.bytes, request.Chunk) || len(cluster.queue) != queueBefore+1 {
			cluster.fail(t, "persisted snapshot action result state=%+v stage=%v queue=%d/%d error=%v", installed.SnapshotBase, cluster.nodes[3].store.stage != nil, len(cluster.queue), queueBefore+1, err)
		}
		response, ok := cluster.queue[len(cluster.queue)-1].rpc.(InstallSnapshotResponse)
		if !ok || !response.Success || response.Done || response.NextOffset != uint64(chunkLength) {
			cluster.fail(t, "persisted snapshot response=%#v", cluster.queue[len(cluster.queue)-1].rpc)
		}
	})

	t.Run("production Ready validation rejects missing proposal waiter", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1105)
		cluster.elect(t, 1, 2)
		proposalID, entry, err := cluster.nodes[1].core.proposeTracked(EntryCommand, []byte("missing-waiter"))
		if err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		for cluster.nodes[2].core.Status().LastIndex < entry.Index {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "proposal append missing")
			}
		}
		_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			response, responseOK := envelope.rpc.(AppendEntriesResponse)
			return responseOK && envelope.from == 2 && envelope.to == 1 && response.Success && response.MatchIndex >= entry.Index
		})
		if !ok {
			cluster.fail(t, "proposal success response missing")
		}
		if ready, exists := cluster.nodes[1].core.Ready(); !exists || len(ready.CommittedProposals) != 1 || ready.CommittedProposals[0].ID != proposalID {
			cluster.fail(t, "committed proposal Ready=%#v", ready)
		}
		if _, err := cluster.tryTakeReady(1); !errors.Is(err, ErrInvalidCoreState) {
			cluster.fail(t, "missing waiter validation error=%v, want ErrInvalidCoreState", err)
		}
	})

	t.Run("production Ready validation rejects mismatched proposal waiter", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1108)
		cluster.elect(t, 1, 2)
		proposalID, entry, err := cluster.propose(1, EntryCommand, []byte("exact-waiter"))
		if err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		mismatched := entry.Clone()
		mismatched.command = []byte("wrong-waiter")
		cluster.nodes[1].proposals[proposalID] = mismatched
		for cluster.nodes[2].core.Status().LastIndex < entry.Index {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "proposal append missing")
			}
		}
		_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			response, responseOK := envelope.rpc.(AppendEntriesResponse)
			return responseOK && envelope.from == 2 && envelope.to == 1 && response.Success && response.MatchIndex >= entry.Index
		})
		if !ok {
			cluster.fail(t, "proposal success response missing")
		}
		if _, err := cluster.tryTakeReady(1); !errors.Is(err, ErrInvalidCoreState) {
			cluster.fail(t, "mismatched waiter validation error=%v, want ErrInvalidCoreState", err)
		}
	})

	t.Run("proposal result cannot publish before durable exact apply", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1106)
		cluster.elect(t, 1, 2)
		proposalID, entry, err := cluster.propose(1, EntryCommand, []byte("exact-result"))
		if err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		for cluster.nodes[2].core.Status().LastIndex < entry.Index {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "proposal append missing")
			}
		}
		_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			response, responseOK := envelope.rpc.(AppendEntriesResponse)
			return responseOK && envelope.from == 2 && envelope.to == 1 && response.Success && response.MatchIndex >= entry.Index
		})
		if !ok || !cluster.takeReady(t, 1) {
			cluster.fail(t, "proposal commit Ready missing")
		}
		if err := cluster.publishReadyResults(t, 1); !errors.Is(err, errSimulationUndurableMessage) {
			cluster.fail(t, "early proposal publication error=%v", err)
		}
		if _, exists := cluster.nodes[1].published[proposalID]; exists {
			cluster.fail(t, "proposal %d published before persistence/application", proposalID)
		}
		if err := cluster.persistReady(t, 1); err != nil {
			cluster.fail(t, "persist proposal Ready: %v", err)
		}
		if err := cluster.releaseReady(t, 1); err != nil {
			cluster.fail(t, "release proposal Ready: %v", err)
		}
		cluster.applyReady(t, 1)
		if err := cluster.publishReadyResults(t, 1); err != nil {
			cluster.fail(t, "publish exact proposal result: %v", err)
		}
		result := cluster.nodes[1].published[proposalID]
		if result.Index != entry.Index || result.Term != entry.Term || !bytes.Equal(result.Result, entry.CommandBytes()) {
			cluster.fail(t, "proposal result=%+v want index=%d term=%d value=%q", result, entry.Index, entry.Term, entry.CommandBytes())
		}
	})

	t.Run("production Ready validation rejects reversed or skipped committed apply sequence", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1107)
		cluster.elect(t, 1, 2)
		_, first, err := cluster.propose(1, EntryCommand, []byte("first"))
		if err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		_, second, err := cluster.propose(1, EntryCommand, []byte("second"))
		if err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		for cluster.nodes[2].core.Status().LastIndex < second.Index {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "batched proposal append missing")
			}
		}
		_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			response, responseOK := envelope.rpc.(AppendEntriesResponse)
			return responseOK && envelope.from == 2 && envelope.to == 1 && response.Success && response.MatchIndex >= second.Index
		})
		if !ok {
			cluster.fail(t, "batched proposal success response missing")
		}
		ready, exists := cluster.nodes[1].core.Ready()
		if !exists || len(ready.CommittedEntries) != 2 || ready.CommittedEntries[0].Index != first.Index || ready.CommittedEntries[1].Index != second.Index {
			cluster.fail(t, "batched committed Ready=%#v", ready.CommittedEntries)
		}
		cluster.nodes[1].core.pendingReady.CommittedEntries[0], cluster.nodes[1].core.pendingReady.CommittedEntries[1] = cluster.nodes[1].core.pendingReady.CommittedEntries[1], cluster.nodes[1].core.pendingReady.CommittedEntries[0]
		if _, err := cluster.tryTakeReady(1); !errors.Is(err, ErrLogInvariant) {
			cluster.fail(t, "reversed committed sequence error=%v, want ErrLogInvariant", err)
		}
		cluster.nodes[1].core.pendingReady.CommittedEntries[0], cluster.nodes[1].core.pendingReady.CommittedEntries[1] = cluster.nodes[1].core.pendingReady.CommittedEntries[1], cluster.nodes[1].core.pendingReady.CommittedEntries[0]
		cluster.nodes[1].core.pendingReady.CommittedEntries = cluster.nodes[1].core.pendingReady.CommittedEntries[1:]
		if _, err := cluster.tryTakeReady(1); !errors.Is(err, ErrLogInvariant) {
			cluster.fail(t, "skipped committed sequence error=%v, want ErrLogInvariant", err)
		}
	})

	t.Run("response before persistence", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1101)
		candidate := cluster.nodes[1]
		if err := candidate.core.Step(2, RequestVoteRequest{CandidateID: 2, Term: 1}); err != nil {
			t.Fatal(err)
		}
		if !cluster.takeReady(t, 1) {
			t.Fatal("vote did not produce Ready")
		}
		cluster.nodes[1].pending.ready.Messages[0].Requires = DurabilityPrerequisite{}
		if err := cluster.releaseReady(t, 1); !errors.Is(err, errSimulationUndurableMessage) {
			cluster.fail(t, "early release with falsified declaration error=%v, want durability rejection", err)
		}
	})

	t.Run("unpersisted entry treated as durable", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1102)
		cluster.elect(t, 1, 2)
		_, entry, err := cluster.propose(1, EntryCommand, []byte("not-durable"))
		if err != nil {
			cluster.fail(t, "propose mutation: %v", err)
		}
		if !cluster.takeReady(t, 1) {
			cluster.fail(t, "proposal index %d lacked Ready", entry.Index)
		}
		cluster.nodes[1].pending.persisted = true
		if err := cluster.releaseReady(t, 1); !errors.Is(err, errSimulationUndurableMessage) {
			cluster.fail(t, "false persisted marker error=%v, want exact store rejection", err)
		}
	})

	t.Run("two leaders in one term", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1103)
		cluster.leaderByTerm[7] = 1
		cluster.nodes[2].core.role = RoleLeader
		cluster.nodes[2].core.hardState.Term = 7
		if err := cluster.oracle(); !errors.Is(err, errSimulationSafety) {
			cluster.fail(t, "two-leader mutation error=%v, want safety violation", err)
		}
	})

	t.Run("committed identity changed", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1104)
		cluster.elect(t, 1, 2)
		entry := cluster.proposeAndCommit(t, 1, "immutable", 2)
		cluster.committed[entry.Index] = simulationEntryIdentity{Term: entry.Term, Kind: EntryCommand, Command: "changed"}
		if err := cluster.oracle(); !errors.Is(err, errSimulationSafety) {
			cluster.fail(t, "committed mutation error=%v, want safety violation", err)
		}
	})

	t.Run("durable snapshot payload cannot change compacted committed prefix", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1108)
		cluster.elect(t, 1, 2)
		entry := cluster.proposeAndCommit(t, 1, "snapshotted-immutable", 2)
		snapshot := cluster.captureSnapshot(t, 1)
		wrongApplication := map[uint64]simulationEntryIdentity{entry.Index: {Term: entry.Term, Kind: entry.Kind, Command: "changed-in-snapshot"}}
		wrong, err := NewSnapshot(cluster.nodes[1].identity, snapshot.Metadata, encodeSimulationApplication(wrongApplication), cluster.nodes[1].store.SnapshotLimit())
		if err != nil {
			t.Fatal(err)
		}
		store := cluster.nodes[1].store
		store.mu.Lock()
		store.state.Snapshot = &wrong
		store.mu.Unlock()
		if err := cluster.oracle(); !errors.Is(err, errSimulationSafety) {
			cluster.fail(t, "changed durable snapshot payload error=%v, want safety violation", err)
		}
	})
}

func TestSimulationDeterministicScenarios(t *testing.T) {
	t.Run("split vote and repeated split vote recovery", func(t *testing.T) {
		cluster := newSimulationCluster(t, 5, 1901)
		for round := uint64(1); round <= 2; round++ {
			cluster.tick(t, 1)
			cluster.tick(t, 2)
			for _, candidate := range []uint16{1, 2} {
				for _, supporter := range []uint16{3, 5} {
					if candidate == 2 && supporter == 3 {
						supporter = 4
					}
					if !cluster.deliver(t, candidate, supporter, PreVoteRequest{}) || !cluster.deliver(t, supporter, candidate, PreVoteResponse{}) {
						cluster.fail(t, "round %d missing pre-vote exchange candidate=%d supporter=%d", round, candidate, supporter)
					}
				}
			}
			for _, ballot := range []simulationLink{{1, 3}, {2, 4}, {1, 2}, {2, 1}} {
				if !cluster.deliver(t, ballot.from, ballot.to, RequestVoteRequest{}) || !cluster.deliver(t, ballot.to, ballot.from, RequestVoteResponse{}) {
					cluster.fail(t, "round %d missing split ballot %d->%d", round, ballot.from, ballot.to)
				}
			}
			for _, id := range []uint16{1, 2} {
				status := cluster.nodes[id].core.Status()
				if status.Role == RoleLeader || status.Term != round {
					cluster.fail(t, "round %d voter %d status=%+v, want split candidate", round, id, status)
				}
			}
			cluster.dropMatching(t, func(simulationEnvelope) bool { return true })
		}
		cluster.elect(t, 1, 3, 5)
		if status := cluster.nodes[1].core.Status(); status.Role != RoleLeader || status.Term != 3 {
			cluster.fail(t, "split recovery status=%+v, want leader term 3", status)
		}
	})

	t.Run("stale and wrong replication generation with delayed responses", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1902)
		cluster.elect(t, 1, 2)
		cluster.heartbeatTick(t, 1)
		if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
			cluster.fail(t, "missing first heartbeat")
		}
		var stale AppendEntriesResponse
		for _, envelope := range cluster.queue {
			if response, ok := envelope.rpc.(AppendEntriesResponse); ok && envelope.from == 2 && envelope.to == 1 {
				stale = response
				break
			}
		}
		if stale.Generation == 0 || !cluster.deliver(t, 2, 1, AppendEntriesResponse{}) {
			cluster.fail(t, "missing first correlated response")
		}
		cluster.dropMatching(t, func(envelope simulationEnvelope) bool {
			_, request := envelope.rpc.(AppendEntriesRequest)
			_, response := envelope.rpc.(AppendEntriesResponse)
			return (request || response) && envelope.from <= 2 && envelope.to <= 2
		})
		_, entry, err := cluster.propose(1, EntryCommand, []byte("generation-progress"))
		if err != nil {
			cluster.fail(t, "propose generation command: %v", err)
		}
		cluster.settle(t, 1)
		if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
			cluster.fail(t, "missing active replication request")
		}
		var active AppendEntriesResponse
		activeIndex := -1
		for index, envelope := range cluster.queue {
			response, ok := envelope.rpc.(AppendEntriesResponse)
			if ok && envelope.from == 2 && envelope.to == 1 && response.MatchIndex == entry.Index {
				active, activeIndex = response, index
				break
			}
		}
		if activeIndex < 0 || active.Generation == stale.Generation {
			cluster.fail(t, "missing distinct active generation response stale=%d active=%d", stale.Generation, active.Generation)
		}
		cluster.queue = append(cluster.queue[:activeIndex], cluster.queue[activeIndex+1:]...)
		before, _ := cluster.nodes[1].core.Progress(2)
		beforeStatus := cluster.nodes[1].core.Status()
		beforeLog := cluster.nodes[1].core.LogState()
		beforeHard := cluster.nodes[1].core.HardState()
		if err := cluster.nodes[1].core.Step(2, stale); err != nil {
			cluster.fail(t, "step delayed stale response: %v", err)
		}
		afterStale, _ := cluster.nodes[1].core.Progress(2)
		if afterStale != before || cluster.nodes[1].core.Status() != beforeStatus || !reflect.DeepEqual(cluster.nodes[1].core.LogState(), beforeLog) || cluster.nodes[1].core.HardState() != beforeHard {
			cluster.fail(t, "stale issued generation was not atomic no-op before=%+v after=%+v", before, afterStale)
		}
		if _, ok := cluster.nodes[1].core.Ready(); ok {
			cluster.fail(t, "stale issued generation exposed Ready")
		}
		wrong := active
		wrong.Generation++
		if err := cluster.nodes[1].core.Step(2, wrong); err != nil {
			cluster.fail(t, "step never-issued generation response: %v", err)
		}
		afterWrong, _ := cluster.nodes[1].core.Progress(2)
		if afterWrong != before || cluster.nodes[1].core.Status() != beforeStatus || !reflect.DeepEqual(cluster.nodes[1].core.LogState(), beforeLog) || cluster.nodes[1].core.HardState() != beforeHard {
			cluster.fail(t, "never-issued generation was not atomic no-op before=%+v after=%+v", before, afterWrong)
		}
		if _, ok := cluster.nodes[1].core.Ready(); ok {
			cluster.fail(t, "never-issued generation exposed Ready")
		}
		if err := cluster.nodes[1].core.Step(2, active); err != nil {
			cluster.fail(t, "step valid active generation response: %v", err)
		}
		cluster.settle(t, 1)
		afterActive, _ := cluster.nodes[1].core.Progress(2)
		if afterActive.MatchIndex != entry.Index || afterActive.ActiveGeneration != 0 {
			cluster.fail(t, "valid active response did not progress before=%+v after=%+v entry=%+v", before, afterActive, entry)
		}
	})

	t.Run("crash before persistence and after persistence before send", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1903)
		request := RequestVoteRequest{CandidateID: 2, Term: 1}
		if err := cluster.nodes[1].core.Step(2, request); err != nil {
			t.Fatal(err)
		}
		cluster.takeReady(t, 1)
		before := cluster.releasedCount
		cluster.crash(t, 1)
		if cluster.releasedCount != before {
			cluster.fail(t, "crash-before-persist released a vote")
		}
		cluster.restart(t, 1)
		if state, _ := cluster.nodes[1].store.Recover(); state.HardState.Term != 0 || state.HardState.VotedFor != 0 {
			cluster.fail(t, "unpersisted vote survived crash: %+v", state.HardState)
		}
		if err := cluster.nodes[1].core.Step(2, request); err != nil {
			t.Fatal(err)
		}
		cluster.takeReady(t, 1)
		if err := cluster.persistReady(t, 1); err != nil {
			t.Fatal(err)
		}
		before = cluster.releasedCount
		cluster.crash(t, 1)
		if cluster.releasedCount != before {
			cluster.fail(t, "crash-after-persist-before-send released a vote")
		}
		cluster.restart(t, 1)
		if state, _ := cluster.nodes[1].store.Recover(); state.HardState != (HardState{Term: 1, VotedFor: 2}) {
			cluster.fail(t, "persisted vote missing after crash: %+v", state.HardState)
		}
		if err := cluster.nodes[1].core.Step(2, request); err != nil {
			t.Fatal(err)
		}
		cluster.settle(t, 1)
		if cluster.releasedCount != before+1 {
			cluster.fail(t, "retry did not release durable idempotent vote response")
		}
	})

	t.Run("crash after Apply before Advance", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1904)
		cluster.elect(t, 1, 2)
		_, entry, err := cluster.propose(1, EntryCommand, []byte("apply-before-advance"))
		if err != nil {
			cluster.fail(t, "propose: %v", err)
		}
		cluster.settle(t, 1)
		for cluster.nodes[2].core.Status().LastIndex < entry.Index {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "missing proposal append")
			}
		}
		_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
			_, response := envelope.rpc.(AppendEntriesResponse)
			return response && envelope.from == 2 && envelope.to == 1
		})
		if !ok || !cluster.takeReady(t, 1) {
			cluster.fail(t, "commit response did not expose phased Ready")
		}
		if err := cluster.persistReady(t, 1); err != nil {
			cluster.fail(t, "persist commit Ready: %v", err)
		}
		if err := cluster.releaseReady(t, 1); err != nil {
			cluster.fail(t, "release commit Ready: %v", err)
		}
		cluster.applyReady(t, 1)
		if err := cluster.publishReadyResults(t, 1); err != nil {
			cluster.fail(t, "publish before crash: %v", err)
		}
		if got := cluster.nodes[1].application[entry.Index]; got != simulationIdentity(entry) {
			cluster.fail(t, "pre-crash application=%+v want %+v", got, simulationIdentity(entry))
		}
		cluster.crash(t, 1)
		cluster.restart(t, 1)
		if got := cluster.nodes[1].application[entry.Index]; got != simulationIdentity(entry) {
			cluster.fail(t, "restart replay application=%+v want %+v", got, simulationIdentity(entry))
		}
	})

	t.Run("repeated election crashes", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1905)
		for attempt := 0; attempt < 3; attempt++ {
			cluster.tick(t, 1)
			if !cluster.deliver(t, 1, 2, PreVoteRequest{}) {
				cluster.fail(t, "attempt %d missing pre-vote request", attempt)
			}
			_, ok := cluster.stepOnly(t, func(envelope simulationEnvelope) bool {
				_, response := envelope.rpc.(PreVoteResponse)
				return response && envelope.from == 2 && envelope.to == 1
			})
			if !ok || !cluster.takeReady(t, 1) {
				cluster.fail(t, "attempt %d missing election Ready", attempt)
			}
			cluster.crash(t, 1)
			cluster.restart(t, 1)
			if state, _ := cluster.nodes[1].store.Recover(); state.HardState.Term != 0 {
				cluster.fail(t, "attempt %d unpersisted election term survived: %+v", attempt, state.HardState)
			}
			cluster.dropMatching(t, func(simulationEnvelope) bool { return true })
		}
	})

	t.Run("persistence fault preserves exact nontrivial last-good transaction", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 1906)
		cluster.elect(t, 1, 2)
		committed := cluster.proposeAndCommit(t, 1, "durable-before-fault", 2)
		lastGood, err := cluster.nodes[1].store.Recover()
		if err != nil || lastGood.HardState.CommitIndex < committed.Index || cluster.nodes[1].application[committed.Index] != simulationIdentity(committed) {
			cluster.fail(t, "nontrivial last-good state=%+v error=%v", lastGood, err)
		}
		lastGoodApplication := make(map[uint64]simulationEntryIdentity, len(cluster.nodes[1].application))
		for index, identity := range cluster.nodes[1].application {
			lastGoodApplication[index] = identity
		}
		_, failedEntry, err := cluster.propose(1, EntryCommand, []byte("must-not-survive-fault"))
		if err != nil || !cluster.takeReady(t, 1) {
			cluster.fail(t, "faulted transaction setup entry=%+v error=%v", failedEntry, err)
		}
		cluster.nodes[1].store.FailNext(StorageOperationPersist, errors.New("injected persistence fault"))
		if err := cluster.persistReady(t, 1); err == nil {
			cluster.fail(t, "persistence fault unexpectedly succeeded")
		}
		cluster.crash(t, 1)
		cluster.restart(t, 1)
		recovered, err := cluster.nodes[1].store.Recover()
		if err != nil || !reflect.DeepEqual(recovered, lastGood) || !reflect.DeepEqual(cluster.nodes[1].application, lastGoodApplication) {
			cluster.fail(t, "fault restart recovered=%+v want=%+v application=%v want=%v error=%v", recovered, lastGood, cluster.nodes[1].application, lastGoodApplication, err)
		}
		if _, exists := recoveredEntryAt(recovered, failedEntry.Index); exists {
			cluster.fail(t, "failed transaction entry survived at index %d", failedEntry.Index)
		}
	})

	t.Run("stale-term leader and follower rejoin with higher-term step-down", func(t *testing.T) {
		cluster := newSimulationCluster(t, 5, 2001)
		cluster.elect(t, 1, 2, 3)
		oldTerm := cluster.nodes[1].core.Status().Term
		cluster.isolate(t, 1)
		cluster.elect(t, 2, 4, 5)
		newTerm := cluster.nodes[2].core.Status().Term
		if newTerm <= oldTerm {
			cluster.fail(t, "replacement term=%d did not exceed old term=%d", newTerm, oldTerm)
		}
		cluster.healAll(t)
		cluster.heartbeatTick(t, 2)
		if !cluster.deliver(t, 2, 1, AppendEntriesRequest{}) {
			cluster.fail(t, "new leader had no AppendEntries for stale leader")
		}
		status := cluster.nodes[1].core.Status()
		if status.Role != RoleFollower || status.Term != newTerm || status.LeaderID != 2 {
			cluster.fail(t, "stale leader status=%+v, want follower of voter 2 in term %d", status, newTerm)
		}
	})

	t.Run("stale follower rejoins higher-term leader and catches up", func(t *testing.T) {
		cluster := newSimulationCluster(t, 5, 2006)
		cluster.elect(t, 1, 2, 3)
		baseline := cluster.proposeAndCommit(t, 1, "before-follower-isolation", 2, 3)
		cluster.syncFollowers(t, 1, 3)
		staleTerm := cluster.nodes[3].core.Status().Term
		staleCommit := cluster.nodes[3].core.Status().CommitIndex
		cluster.isolate(t, 3)
		cluster.isolate(t, 1)
		cluster.elect(t, 2, 4, 5)
		newEntry := cluster.proposeAndCommit(t, 2, "missed-by-stale-follower", 4, 5)
		newTerm := cluster.nodes[2].core.Status().Term
		if newTerm <= staleTerm || newEntry.Index <= staleCommit || cluster.nodes[3].core.Status().Term != staleTerm || cluster.nodes[3].core.Status().CommitIndex != staleCommit {
			cluster.fail(t, "isolated follower changed or replacement did not advance stale=%d/%d new=%d/%d", staleTerm, staleCommit, newTerm, newEntry.Index)
		}
		cluster.healAll(t)
		cluster.queue = nil
		cluster.syncFollowers(t, 2, 3)
		status := cluster.nodes[3].core.Status()
		if status.Role != RoleFollower || status.Term != newTerm || status.LeaderID != 2 || status.CommitIndex < newEntry.Index {
			cluster.fail(t, "rejoined follower status=%+v want follower leader=2 term=%d commit>=%d", status, newTerm, newEntry.Index)
		}
		if got := cluster.nodes[3].application[baseline.Index]; got != simulationIdentity(baseline) {
			cluster.fail(t, "rejoined follower changed committed prefix got=%+v want=%+v", got, simulationIdentity(baseline))
		}
		if got := cluster.nodes[3].application[newEntry.Index]; got != simulationIdentity(newEntry) {
			cluster.fail(t, "rejoined follower missing new command got=%+v want=%+v", got, simulationIdentity(newEntry))
		}
	})

	t.Run("divergent uncommitted suffix repair without committed overwrite", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 2002)
		cluster.elect(t, 1, 2)
		cluster.syncFollowers(t, 1, 2, 3)
		committedOne := cluster.committed[1]
		cluster.isolate(t, 1)
		_, staleEntry, err := cluster.propose(1, EntryCommand, []byte("uncommitted-old-leader"))
		if err != nil {
			cluster.fail(t, "old leader proposal: %v", err)
		}
		cluster.settle(t, 1)
		cluster.crash(t, 3)
		cluster.restart(t, 3)
		cluster.elect(t, 2, 3)
		cluster.healAll(t)
		cluster.syncFollowers(t, 2, 1, 3)
		repaired, err := cluster.nodes[1].core.log.Entry(staleEntry.Index)
		if err != nil {
			cluster.fail(t, "read repaired index %d: %v", staleEntry.Index, err)
		}
		if sameEntry(repaired, staleEntry) || repaired.Kind != EntryNoOp || repaired.Term <= staleEntry.Term {
			cluster.fail(t, "repaired entry=%+v still matches stale entry=%+v", simulationIdentity(repaired), simulationIdentity(staleEntry))
		}
		if got := cluster.committed[1]; got != committedOne {
			cluster.fail(t, "committed prefix index 1 changed from %+v to %+v", committedOne, got)
		}
	})

	t.Run("old-term entry committed only indirectly by a current-term entry", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 2003)
		cluster.elect(t, 1, 2)
		cluster.syncFollowers(t, 1, 2, 3)
		_, oldEntry, err := cluster.propose(1, EntryCommand, []byte("term-one-uncommitted"))
		if err != nil {
			cluster.fail(t, "old-term proposal: %v", err)
		}
		cluster.settle(t, 1)
		for attempts := 0; attempts < 8 && cluster.nodes[2].core.Status().LastIndex < oldEntry.Index; attempts++ {
			if !cluster.deliver(t, 1, 2, AppendEntriesRequest{}) {
				cluster.fail(t, "old-term append not delivered to future leader")
			}
			cluster.dropMatching(t, func(envelope simulationEnvelope) bool {
				_, response := envelope.rpc.(AppendEntriesResponse)
				return response && envelope.from == 2 && envelope.to == 1
			})
		}
		if cluster.nodes[2].core.Status().LastIndex < oldEntry.Index {
			cluster.fail(t, "future leader never persisted old-term index %d", oldEntry.Index)
		}
		if state, _ := cluster.nodes[1].store.Recover(); state.HardState.CommitIndex >= oldEntry.Index {
			cluster.fail(t, "old leader directly committed old-term entry")
		}
		cluster.crash(t, 1)
		cluster.crash(t, 3)
		cluster.restart(t, 3)
		cluster.elect(t, 2, 3)
		state, err := cluster.nodes[2].store.Recover()
		if err != nil || state.HardState.CommitIndex <= oldEntry.Index {
			cluster.fail(t, "current-term no-op did not indirectly commit old entry: hard=%+v error=%v", state.HardState, err)
		}
		if got := cluster.committed[oldEntry.Index]; got != simulationIdentity(oldEntry) {
			cluster.fail(t, "old-term committed identity=%+v want %+v", got, simulationIdentity(oldEntry))
		}
	})

	t.Run("minority isolation versus majority election and commit", func(t *testing.T) {
		cluster := newSimulationCluster(t, 5, 2101)
		cluster.isolate(t, 4)
		cluster.isolate(t, 5)
		cluster.elect(t, 1, 2, 3)
		entry := cluster.proposeAndCommit(t, 1, "majority-only", 2, 3)
		if got := cluster.nodes[4].core.Status().CommitIndex; got >= entry.Index {
			cluster.fail(t, "isolated minority voter 4 commit=%d reached entry=%d", got, entry.Index)
		}
		cluster.healAll(t)
		cluster.syncFollowers(t, 1, 2, 3, 4, 5)
	})

	t.Run("quorum loss prevents commit and healing permits progress", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 2102)
		cluster.elect(t, 1, 2)
		before := cluster.nodes[1].core.Status().CommitIndex
		cluster.isolate(t, 1)
		_, entry, err := cluster.propose(1, EntryCommand, []byte("requires-quorum"))
		if err != nil {
			cluster.fail(t, "isolated leader proposal: %v", err)
		}
		cluster.settle(t, 1)
		for cluster.deliverMatching(t, func(envelope simulationEnvelope) bool {
			_, appendRequest := envelope.rpc.(AppendEntriesRequest)
			return appendRequest && envelope.from == 1
		}) {
		}
		if got := cluster.nodes[1].core.Status().CommitIndex; got != before {
			cluster.fail(t, "minority leader commit advanced from %d to %d", before, got)
		}
		cluster.healAll(t)
		cluster.replicateForQuorum(t, 1, []uint16{2})
		if got := cluster.nodes[1].core.Status().CommitIndex; got < entry.Index {
			cluster.fail(t, "healed quorum commit=%d want at least %d", got, entry.Index)
		}
	})

	t.Run("follower snapshot catch-up rejects changed duplicate chunks then resumes Append", func(t *testing.T) {
		cluster := newSimulationCluster(t, 3, 2103)
		cluster.elect(t, 1, 2)
		cluster.proposeAndCommit(t, 1, "snap-a", 2)
		cluster.proposeAndCommit(t, 1, "snap-b", 2)
		snapshot := cluster.captureSnapshot(t, 1)
		var transferID TransferID
		transferID[0] = 1
		if err := cluster.nodes[1].core.StartSnapshotTransfer(3, snapshot, transferID, 3); err != nil {
			cluster.fail(t, "start snapshot transfer: %v", err)
		}
		cluster.settle(t, 1)
		var first InstallSnapshotRequest
		for _, envelope := range cluster.queue {
			if request, ok := envelope.rpc.(InstallSnapshotRequest); ok && envelope.from == 1 && envelope.to == 3 {
				first = request
				break
			}
		}
		if first.TransferID.IsZero() || !cluster.deliver(t, 1, 3, InstallSnapshotRequest{}) {
			cluster.fail(t, "first snapshot chunk missing")
		}
		if cluster.nodes[3].store.stage == nil || !bytes.Equal(cluster.nodes[3].store.stage.bytes, first.Chunk) {
			cluster.fail(t, "first snapshot chunk was not durably staged")
		}
		beforeChanged, _ := cluster.nodes[3].store.Recover()
		applicationBeforeChanged := make(map[uint64]simulationEntryIdentity, len(cluster.nodes[3].application))
		for index, identity := range cluster.nodes[3].application {
			applicationBeforeChanged[index] = identity
		}
		cluster.dropMatching(t, func(envelope simulationEnvelope) bool {
			_, response := envelope.rpc.(InstallSnapshotResponse)
			return response && envelope.from == 3 && envelope.to == 1
		})
		changed := first
		changed.Chunk = cloneBytes(first.Chunk)
		changed.Chunk[0] ^= 0xff
		if err := cluster.nodes[3].core.Step(1, changed); err != nil {
			cluster.fail(t, "step changed duplicate chunk: %v", err)
		}
		cluster.settle(t, 3)
		var rejection InstallSnapshotResponse
		for _, envelope := range cluster.queue {
			if response, ok := envelope.rpc.(InstallSnapshotResponse); ok && response.TransferID == transferID && envelope.from == 3 {
				rejection = response
				break
			}
		}
		afterChanged, _ := cluster.nodes[3].store.Recover()
		if rejection.Success || rejection.Done || rejection.NextOffset != 0 || !reflect.DeepEqual(afterChanged, beforeChanged) || !reflect.DeepEqual(cluster.nodes[3].application, applicationBeforeChanged) || cluster.nodes[3].store.stage != nil {
			cluster.fail(t, "changed duplicate response=%+v durableChanged=%t applicationChanged=%t stage=%v", rejection, !reflect.DeepEqual(afterChanged, beforeChanged), !reflect.DeepEqual(cluster.nodes[3].application, applicationBeforeChanged), cluster.nodes[3].store.stage != nil)
		}
		if !cluster.deliver(t, 3, 1, InstallSnapshotResponse{}) {
			cluster.fail(t, "changed duplicate rejection missing")
		}
		for attempts := 0; attempts < 256; attempts++ {
			state, err := cluster.nodes[3].store.Recover()
			if err != nil {
				cluster.fail(t, "recover snapshot follower: %v", err)
			}
			if state.SnapshotBase == snapshot.Metadata {
				break
			}
			if !cluster.deliver(t, 1, 3, InstallSnapshotRequest{}) {
				cluster.fail(t, "snapshot transfer stalled before install")
			}
			if !cluster.deliver(t, 3, 1, InstallSnapshotResponse{}) {
				cluster.fail(t, "snapshot response missing")
			}
		}
		state, _ := cluster.nodes[3].store.Recover()
		if state.SnapshotBase != snapshot.Metadata || state.Snapshot == nil || state.Snapshot.ID != snapshot.ID {
			cluster.fail(t, "follower snapshot state=%+v snapshot=%v want base=%+v id=%x", state.SnapshotBase, state.Snapshot != nil, snapshot.Metadata, snapshot.ID)
		}
		var staleTransferID TransferID
		staleTransferID[0] = 9
		stale := InstallSnapshotRequest{
			LeaderID: 1, Term: cluster.nodes[1].core.Status().Term, TransferID: staleTransferID, SnapshotID: snapshot.ID,
			LastIncludedIndex: snapshot.Metadata.LastIncludedIndex, LastIncludedTerm: snapshot.Metadata.LastIncludedTerm,
			StateMachineSchemaVersion: snapshot.Metadata.StateMachineSchemaVersion, TotalLength: uint64(len(snapshot.StateBytes())),
			Checksum: snapshot.StateChecksum, Chunk: snapshot.StateBytes(), Done: true,
		}
		beforeStale, _ := cluster.nodes[3].store.Recover()
		applicationBeforeStale := make(map[uint64]simulationEntryIdentity, len(cluster.nodes[3].application))
		for index, identity := range cluster.nodes[3].application {
			applicationBeforeStale[index] = identity
		}
		if err := cluster.nodes[3].core.Step(1, stale); err != nil {
			cluster.fail(t, "step stale installed snapshot: %v", err)
		}
		cluster.settle(t, 3)
		var staleResponse InstallSnapshotResponse
		for _, envelope := range cluster.queue {
			if response, ok := envelope.rpc.(InstallSnapshotResponse); ok && response.TransferID == staleTransferID {
				staleResponse = response
				break
			}
		}
		afterStale, _ := cluster.nodes[3].store.Recover()
		if !staleResponse.Success || !staleResponse.Done || staleResponse.NextOffset != stale.TotalLength || !reflect.DeepEqual(afterStale, beforeStale) || !reflect.DeepEqual(cluster.nodes[3].application, applicationBeforeStale) {
			cluster.fail(t, "stale snapshot response=%+v durableChanged=%t applicationChanged=%t", staleResponse, !reflect.DeepEqual(afterStale, beforeStale), !reflect.DeepEqual(cluster.nodes[3].application, applicationBeforeStale))
		}
		cluster.dropMatching(t, func(envelope simulationEnvelope) bool {
			_, request := envelope.rpc.(AppendEntriesRequest)
			_, response := envelope.rpc.(AppendEntriesResponse)
			return (request || response) && ((envelope.from == 1 && envelope.to == 3) || (envelope.from == 3 && envelope.to == 1))
		})
		post := cluster.proposeAndCommit(t, 1, "post-snapshot-append", 2)
		if !cluster.deliver(t, 1, 3, AppendEntriesRequest{}) || !cluster.deliver(t, 3, 1, AppendEntriesResponse{}) {
			cluster.fail(t, "ordinary post-snapshot AppendEntries exchange missing")
		}
		cluster.heartbeatTick(t, 1)
		if !cluster.deliver(t, 1, 3, AppendEntriesRequest{}) || !cluster.deliver(t, 3, 1, AppendEntriesResponse{}) {
			cluster.fail(t, "post-snapshot commit heartbeat exchange missing")
		}
		if got := cluster.nodes[3].application[post.Index]; got != simulationIdentity(post) {
			cluster.fail(t, "post-snapshot follower command=%+v want %+v", got, simulationIdentity(post))
		}
		progress, exists := cluster.nodes[1].core.Progress(3)
		if !exists || progress.SnapshotNeeded || !progress.ActiveTransferID.IsZero() || progress.MatchIndex < post.Index {
			cluster.fail(t, "post-snapshot follower progress=%+v exists=%t", progress, exists)
		}
		if got, want := cluster.nodes[3].application, cluster.nodes[1].application; !reflect.DeepEqual(got, want) {
			cluster.fail(t, "snapshot follower application=%v want %v", got, want)
		}
	})

	t.Run("complete three-voter and five-voter disk restart with equal committed state", func(t *testing.T) {
		for _, voterCount := range []int{3, 5} {
			t.Run(fmt.Sprintf("%d voters", voterCount), func(t *testing.T) {
				cluster := newSimulationCluster(t, voterCount, 2200+uint64(voterCount))
				followers := append([]uint16(nil), cluster.ids[1:]...)
				cluster.elect(t, 1, followers...)
				cluster.proposeAndCommit(t, 1, "disk-restart", followers...)
				cluster.syncFollowers(t, 1, followers...)
				for _, id := range cluster.ids {
					cluster.crash(t, id)
				}
				for _, id := range cluster.ids {
					cluster.restart(t, id)
				}
				want := cluster.nodes[1].application
				for _, id := range cluster.ids[1:] {
					if !reflect.DeepEqual(cluster.nodes[id].application, want) {
						cluster.fail(t, "voter %d application=%v want %v", id, cluster.nodes[id].application, want)
					}
				}
			})
		}
	})
}
