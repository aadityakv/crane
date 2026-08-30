package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	internalrandom "github.com/aaditya/cs425mp3/internal/random"
)

func TestTask10StateMachineCachesDistinctIndexRetryWithoutSecondMutation(t *testing.T) {
	tracker := newTask10KVTracker()
	machine := newTask10KVStateMachine(tracker)
	command := task10KVCommand{ID: "ambiguous-client-retry", Key: "key", Value: "value"}

	first, err := machine.Apply(2, 1, encodeTask10KVCommand(command))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := machine.Apply(4, 3, encodeTask10KVCommand(command))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retry, first) {
		t.Fatalf("retry result = %q, want cached first result %q", retry, first)
	}
	if got := machine.mutationCount(command.ID); got != 1 {
		t.Fatalf("state mutation count = %d, want 1", got)
	}
	if err := validateTask10DuplicateEvidence(tracker.duplicateEvidence()); err != nil {
		t.Fatalf("distinct-index retry evidence: %v", err)
	}

	sameIndex := tracker.duplicateEvidence()[0]
	sameIndex.Index = sameIndex.FirstIndex
	if err := validateTask10DuplicateEvidence([]task10DuplicateApply{sameIndex}); err == nil {
		t.Fatal("same-index duplicate Apply evidence was accepted")
	}
}

func TestRealTCPClusterFailoverRestartAndSnapshotCatchUp(t *testing.T) {
	configurations, secret, reservations := task10ClusterConfigurations(t)
	nodes := make([]*task10ClusterNode, 3)
	trackers := []*task10KVTracker{newTask10KVTracker(), newTask10KVTracker(), newTask10KVTracker()}
	for index := range nodes {
		nodes[index] = startTask10ClusterNode(t, configurations[index], secret, int64(index+1), reservations[index], trackers[index])
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.stop(t)
			}
		}
	}()

	leader := awaitTask10Leader(t, nodes, 5*time.Second)
	expected := make(map[string]string)
	setTask10Phase(trackers, "initial proposals")
	for index := 1; index <= 2; index++ {
		command := task10KVCommand{ID: fmt.Sprintf("initial-%02d", index), Key: fmt.Sprintf("key-%02d", index), Value: fmt.Sprintf("value-%02d", index)}
		leader = proposeTask10Command(t, nodes, command)
		expected[command.Key] = command.Value
	}
	barrierContext, cancelBarrier := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := leader.service.Barrier(barrierContext); err != nil {
		cancelBarrier()
		t.Fatalf("initial Barrier: %v; statuses=%s", err, task10Statuses(nodes))
	}
	cancelBarrier()
	awaitTask10State(t, nodes, expected, 5*time.Second)
	assertTask10IdempotencyEvidence(t, nodes, trackers, "initial commits")

	failedID := leader.configuration.NodeID
	leader.stop(t)
	leader = awaitTask10Leader(t, task10RunningNodes(nodes), 5*time.Second)
	if leader.configuration.NodeID == failedID {
		t.Fatalf("stopped leader %d remained leader", failedID)
	}
	setTask10Phase(trackers, "failover proposals")
	for index := 1; index <= 2; index++ {
		command := task10KVCommand{ID: fmt.Sprintf("failover-%02d", index), Key: fmt.Sprintf("failover-key-%02d", index), Value: fmt.Sprintf("failover-value-%02d", index)}
		leader = proposeTask10Command(t, task10RunningNodes(nodes), command)
		expected[command.Key] = command.Value
	}

	failedIndex := int(failedID - 1)
	nodes[failedIndex] = startTask10ClusterNode(t, configurations[failedIndex], secret, 100+int64(failedIndex), nil, trackers[failedIndex])
	awaitTask10State(t, nodes, expected, 5*time.Second)
	assertTask10IdempotencyEvidence(t, nodes, trackers, "old leader catch-up")

	leader = awaitTask10Leader(t, nodes, 5*time.Second)
	lagging := task10FollowerOtherThan(nodes, leader.configuration.NodeID)
	laggingIndex := int(lagging.configuration.NodeID - 1)
	if _, err := os.Stat(filepath.Join(lagging.configuration.StorageDir, RaftStorageDirectoryName, RaftSnapshotFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lagging voter already had a snapshot before isolation: %v", err)
	}
	lagging.stop(t)
	configurations[laggingIndex].Raft.SnapshotEntryThreshold = math.MaxUint64
	configurations[laggingIndex].Raft.SnapshotByteThreshold = math.MaxUint64
	remoteRestoresBefore := trackers[laggingIndex].snapshotRestoreCount()
	setTask10Phase(trackers, "snapshot threshold proposals")
	for index := 1; index <= 20; index++ {
		command := task10KVCommand{ID: fmt.Sprintf("snapshot-%02d", index), Key: fmt.Sprintf("snapshot-key-%02d", index), Value: fmt.Sprintf("snapshot-value-%02d", index)}
		leader = proposeTask10Command(t, task10RunningNodes(nodes), command)
		expected[command.Key] = command.Value
	}
	awaitTask10File(t, filepath.Join(leader.configuration.StorageDir, RaftStorageDirectoryName, RaftSnapshotFilename), 5*time.Second)

	setTask10Phase(trackers, "lagging snapshot restart")
	nodes[laggingIndex] = startTask10ClusterNode(t, configurations[laggingIndex], secret, 200+int64(laggingIndex), nil, trackers[laggingIndex])
	awaitTask10(t, nodes, 8*time.Second, "remote InstallSnapshot Restore on restarted lagging voter", func() bool {
		return trackers[laggingIndex].snapshotRestoreCount() > remoteRestoresBefore
	})
	awaitTask10State(t, nodes, expected, 8*time.Second)
	assertTask10IdempotencyEvidence(t, nodes, trackers, "lagging snapshot catch-up")
	awaitTask10File(t, filepath.Join(configurations[laggingIndex].StorageDir, RaftStorageDirectoryName, RaftSnapshotFilename), 5*time.Second)
	postSnapshot := task10KVCommand{ID: "post-snapshot", Key: "post-snapshot-key", Value: "post-snapshot-value"}
	setTask10Phase(trackers, "post-snapshot append")
	leader = proposeTask10Command(t, nodes, postSnapshot)
	expected[postSnapshot.Key] = postSnapshot.Value
	awaitTask10State(t, nodes, expected, 5*time.Second)
	if got := nodes[laggingIndex].machine.applyCount(postSnapshot.ID); got != 1 {
		t.Fatalf("lagging voter post-snapshot Append apply count = %d, want exactly one", got)
	}
	assertTask10IdempotencyEvidence(t, nodes, trackers, "post-snapshot append")

	setTask10Phase(trackers, "complete disk restart")
	for _, node := range nodes {
		node.stop(t)
	}
	for index := range nodes {
		nodes[index] = startTask10ClusterNode(t, configurations[index], secret, 300+int64(index), nil, trackers[index])
	}
	_ = awaitTask10Leader(t, nodes, 5*time.Second)
	awaitTask10State(t, nodes, expected, 8*time.Second)
	awaitTask10(t, nodes, 5*time.Second, "equal recovered commit and applied indexes", func() bool {
		commit := nodes[0].service.Status().CommitIndex
		if commit == 0 {
			return false
		}
		for _, node := range nodes {
			status := node.service.Status()
			if status.CommitIndex != commit || status.AppliedIndex != commit {
				return false
			}
		}
		return true
	})
	commit := nodes[0].service.Status().CommitIndex
	for _, node := range nodes {
		status := node.service.Status()
		if status.CommitIndex != commit || status.AppliedIndex != commit {
			t.Fatalf("restart status node=%d got=%#v want commit/applied=%d; all=%s", node.configuration.NodeID, status, commit, task10Statuses(nodes))
		}
	}
	assertTask10IdempotencyEvidence(t, nodes, trackers, "complete disk recovery")
}

type task10ClusterNode struct {
	configuration config.NodeConfig
	service       *Service
	machine       *task10KVStateMachine
	cancel        context.CancelFunc
	done          chan error
	stopOnce      sync.Once
}

func startTask10ClusterNode(t *testing.T, configuration config.NodeConfig, secret []byte, seed int64, reservation net.Listener, tracker *task10KVTracker) *task10ClusterNode {
	t.Helper()
	machine := newTask10KVStateMachine(tracker)
	service, err := NewService(ServiceOptions{
		Config: configuration, Secret: secret, Clock: clock.NewReal(),
		Random: internalrandom.NewLockedSource(seed), StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reservation != nil {
		service.listen = func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != reservation.Addr().String() {
				return nil, fmt.Errorf("reserved listener mismatch network=%s address=%s reserved=%s", network, address, reservation.Addr())
			}
			return reservation, nil
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	node := &task10ClusterNode{configuration: configuration, service: service, machine: machine, cancel: cancel, done: make(chan error, 1)}
	go func() { node.done <- service.Run(ctx) }()
	select {
	case <-service.Ready():
		return node
	case err := <-node.done:
		t.Fatalf("node %d failed before Ready: %v", configuration.NodeID, err)
	case <-time.After(3 * time.Second):
		t.Fatalf("node %d did not become lifecycle-ready", configuration.NodeID)
	}
	return nil
}

func (node *task10ClusterNode) stop(t *testing.T) {
	t.Helper()
	if node == nil {
		return
	}
	node.stopOnce.Do(func() {
		node.cancel()
		select {
		case err := <-node.done:
			if err != nil {
				t.Errorf("stop node %d: %v", node.configuration.NodeID, err)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("node %d did not stop", node.configuration.NodeID)
		}
	})
}

func task10ClusterConfigurations(t *testing.T) ([]config.NodeConfig, []byte, []net.Listener) {
	t.Helper()
	secret := []byte("task10-real-tcp-hmac-secret-key-0001")
	secretPath := filepath.Join(t.TempDir(), "cluster.secret")
	if err := os.WriteFile(secretPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	reservations := make([]net.Listener, 3)
	voters := make([]config.RaftVoter, 3)
	bases := make([]uint16, 3)
	for index := range reservations {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if port <= 8 {
			_ = listener.Close()
			t.Fatalf("reserved port %d cannot model +8", port)
		}
		reservations[index] = listener
		bases[index] = uint16(port - 8)
		voters[index] = config.RaftVoter{NodeID: uint16(index + 1), Endpoint: listener.Addr().String()}
	}
	configurations := make([]config.NodeConfig, 3)
	for index := range configurations {
		raftConfig := config.DefaultRaftConfig()
		raftConfig.HeartbeatInterval = config.Duration(20 * time.Millisecond)
		raftConfig.ElectionTimeoutMin = config.Duration(80 * time.Millisecond)
		raftConfig.ElectionTimeoutMax = config.Duration(160 * time.Millisecond)
		raftConfig.RPCTimeout = config.Duration(50 * time.Millisecond)
		raftConfig.SnapshotEntryThreshold = 12
		raftConfig.SnapshotByteThreshold = math.MaxUint64
		raftConfig.MaxSnapshotBytes = 1 << 20
		configuration := config.NodeConfig{
			NodeID: uint16(index + 1), ClusterID: "10112233-4455-6677-8899-aabbccddeeff",
			BindHost: "127.0.0.1", AdvertiseHost: "127.0.0.1", BasePort: bases[index],
			Introducer: "127.0.0.1:1", StorageDir: t.TempDir(), ClusterSecretFile: secretPath,
			Timing: config.DefaultTimingConfig(), Raft: raftConfig, RaftVoters: append([]config.RaftVoter(nil), voters...),
		}
		if err := configuration.Validate(); err != nil {
			t.Fatal(err)
		}
		configurations[index] = configuration
	}
	return configurations, secret, reservations
}

func awaitTask10Leader(t *testing.T, nodes []*task10ClusterNode, timeout time.Duration) *task10ClusterNode {
	t.Helper()
	var leader *task10ClusterNode
	awaitTask10(t, nodes, timeout, "one leader", func() bool {
		leader = nil
		for _, node := range nodes {
			if node != nil && node.service.Status().Role == RoleLeader {
				if leader != nil {
					return false
				}
				leader = node
			}
		}
		return leader != nil
	})
	return leader
}

func proposeTask10Command(t *testing.T, nodes []*task10ClusterNode, command task10KVCommand) *task10ClusterNode {
	t.Helper()
	encoded := encodeTask10KVCommand(command)
	var leader *task10ClusterNode
	awaitTask10(t, nodes, 5*time.Second, "proposal "+command.ID, func() bool {
		leader = nil
		for _, node := range nodes {
			if node != nil && node.service.Status().Role == RoleLeader {
				leader = node
				break
			}
		}
		if leader == nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		result, err := leader.service.Propose(ctx, encoded)
		cancel()
		leader.machine.tracker.recordProposalAttempt(task10ProposalAttempt{
			ID: command.ID, VoterID: leader.configuration.NodeID, ResultIndex: result.Index, ResultTerm: result.Term,
			Err: fmt.Sprint(err),
		})
		return err == nil
	})
	return leader
}

func awaitTask10State(t *testing.T, nodes []*task10ClusterNode, want map[string]string, timeout time.Duration) {
	t.Helper()
	awaitTask10(t, nodes, timeout, "replicated application state", func() bool {
		for _, node := range nodes {
			if node == nil || !reflect.DeepEqual(node.machine.valuesSnapshot(), want) {
				return false
			}
		}
		return true
	})
}

func awaitTask10File(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	awaitTask10(t, nil, timeout, "file "+path, func() bool {
		info, err := os.Stat(path)
		return err == nil && info.Mode().IsRegular()
	})
}

func awaitTask10(t *testing.T, nodes []*task10ClusterNode, timeout time.Duration, description string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; statuses=%s", description, task10Statuses(nodes))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func task10Statuses(nodes []*task10ClusterNode) string {
	statuses := ""
	for _, node := range nodes {
		if node != nil {
			statuses += fmt.Sprintf(" node%d=%#v", node.configuration.NodeID, node.service.Status())
		}
	}
	return statuses
}

func setTask10Phase(trackers []*task10KVTracker, phase string) {
	for _, tracker := range trackers {
		tracker.mu.Lock()
		tracker.phase = phase
		tracker.mu.Unlock()
	}
}

func assertTask10IdempotencyEvidence(t *testing.T, nodes []*task10ClusterNode, trackers []*task10KVTracker, checkpoint string) {
	t.Helper()
	for index, tracker := range trackers {
		duplicates := tracker.duplicateEvidence()
		tracker.mu.Lock()
		attempts := append([]task10ProposalAttempt(nil), tracker.proposalAttempts...)
		tracker.mu.Unlock()
		if err := validateTask10DuplicateEvidence(duplicates); err != nil {
			t.Fatalf("checkpoint %q voter %d invalid duplicate evidence: %v; duplicate applies=%#v; proposal attempts=%#v; statuses=%s", checkpoint, index+1, err, duplicates, attempts, task10Statuses(nodes))
		}
	}
}

func validateTask10DuplicateEvidence(duplicates []task10DuplicateApply) error {
	for _, duplicate := range duplicates {
		if duplicate.FirstIndex == 0 {
			return fmt.Errorf("command %q retry at index %d has no persisted origin", duplicate.ID, duplicate.Index)
		}
		if duplicate.FirstIndex == duplicate.Index {
			return fmt.Errorf("command %q was applied twice at log index %d", duplicate.ID, duplicate.Index)
		}
		if duplicate.CachedResult != duplicate.CommandValue {
			return fmt.Errorf("command %q retry returned %q, want cached result %q", duplicate.ID, duplicate.CachedResult, duplicate.CommandValue)
		}
		if duplicate.MutationCount > 1 {
			return fmt.Errorf("command %q caused %d mutations in incarnation %d", duplicate.ID, duplicate.MutationCount, duplicate.Incarnation)
		}
	}
	return nil
}

func task10RunningNodes(nodes []*task10ClusterNode) []*task10ClusterNode {
	result := make([]*task10ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		if node != nil && node.service.state.Load() == raftServiceRunning {
			result = append(result, node)
		}
	}
	return result
}

func task10FollowerOtherThan(nodes []*task10ClusterNode, leaderID uint16) *task10ClusterNode {
	for _, node := range nodes {
		if node != nil && node.configuration.NodeID != leaderID && node.service.state.Load() == raftServiceRunning {
			return node
		}
	}
	return nil
}

type task10KVCommand struct {
	ID    string
	Key   string
	Value string
}

type task10KVStateMachine struct {
	mu               sync.Mutex
	values           map[string]string
	results          map[string]string
	applies          map[string]int
	mutations        map[string]int
	tracker          *task10KVTracker
	incarnation      int
	restoredBase     uint64
	restoredBaseTerm uint64
	firstApplies     map[string]task10ApplyRecord
}

type task10KVTracker struct {
	mu               sync.Mutex
	phase            string
	nextIncarnation  int
	duplicates       []task10DuplicateApply
	proposalAttempts []task10ProposalAttempt
	snapshotRestores int
}

type task10ApplyRecord struct {
	Index uint64
	Term  uint64
}

type task10ProposalAttempt struct {
	ID          string
	VoterID     uint16
	ResultIndex uint64
	ResultTerm  uint64
	Err         string
}

type task10DuplicateApply struct {
	ID               string
	Index            uint64
	Term             uint64
	Incarnation      int
	Phase            string
	RestoredBase     uint64
	RestoredBaseTerm uint64
	FirstIndex       uint64
	FirstTerm        uint64
	CommandValue     string
	CachedResult     string
	MutationCount    int
}

func newTask10KVTracker() *task10KVTracker { return &task10KVTracker{} }

func newTask10KVStateMachine(tracker *task10KVTracker) *task10KVStateMachine {
	tracker.mu.Lock()
	tracker.nextIncarnation++
	incarnation := tracker.nextIncarnation
	tracker.mu.Unlock()
	return &task10KVStateMachine{
		values: make(map[string]string), results: make(map[string]string), applies: make(map[string]int), mutations: make(map[string]int), firstApplies: make(map[string]task10ApplyRecord),
		tracker: tracker, incarnation: incarnation,
	}
}

func (machine *task10KVStateMachine) Apply(index uint64, term uint64, encoded []byte) ([]byte, error) {
	command, err := decodeTask10KVCommand(encoded)
	if err != nil {
		return nil, err
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.applies[command.ID]++
	if result, duplicate := machine.results[command.ID]; duplicate {
		first := machine.firstApplies[command.ID]
		machine.tracker.mu.Lock()
		machine.tracker.duplicates = append(machine.tracker.duplicates, task10DuplicateApply{
			ID: command.ID, Index: index, Term: term, Incarnation: machine.incarnation,
			Phase: machine.tracker.phase, RestoredBase: machine.restoredBase, RestoredBaseTerm: machine.restoredBaseTerm,
			FirstIndex: first.Index, FirstTerm: first.Term, CommandValue: command.Value, CachedResult: result,
			MutationCount: machine.mutations[command.ID],
		})
		machine.tracker.mu.Unlock()
		return []byte(result), nil
	}
	machine.values[command.Key] = command.Value
	machine.results[command.ID] = command.Value
	machine.mutations[command.ID]++
	machine.firstApplies[command.ID] = task10ApplyRecord{Index: index, Term: term}
	return []byte(command.Value), nil
}

func (machine *task10KVStateMachine) Capture(index uint64, term uint64) (SnapshotCapture, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return task10KVSnapshot{encoded: encodeTask10KVSnapshot(index, term, machine.values, machine.results, machine.firstApplies)}, nil
}

func (machine *task10KVStateMachine) Restore(schema uint32, encoded []byte) error {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	machine.applies = make(map[string]int)
	machine.mutations = make(map[string]int)
	machine.firstApplies = make(map[string]task10ApplyRecord)
	if schema == 0 && len(encoded) == 0 {
		machine.values = make(map[string]string)
		machine.results = make(map[string]string)
		machine.restoredBase, machine.restoredBaseTerm = 0, 0
		return nil
	}
	if schema != 1 {
		return fmt.Errorf("unknown test KV schema %d", schema)
	}
	machine.tracker.mu.Lock()
	machine.tracker.snapshotRestores++
	machine.tracker.mu.Unlock()
	base, baseTerm, values, results, firstApplies, err := decodeTask10KVSnapshot(encoded)
	if err != nil {
		return err
	}
	machine.values, machine.results, machine.firstApplies = values, results, firstApplies
	machine.restoredBase, machine.restoredBaseTerm = base, baseTerm
	return nil
}

func (machine *task10KVStateMachine) valuesSnapshot() map[string]string {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return cloneTask10StringMap(machine.values)
}

func (machine *task10KVStateMachine) applyCount(id string) int {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.applies[id]
}

func (machine *task10KVStateMachine) mutationCount(id string) int {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.mutations[id]
}

func (tracker *task10KVTracker) duplicateEvidence() []task10DuplicateApply {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return append([]task10DuplicateApply(nil), tracker.duplicates...)
}

func (tracker *task10KVTracker) snapshotRestoreCount() int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.snapshotRestores
}

func (tracker *task10KVTracker) recordProposalAttempt(attempt task10ProposalAttempt) {
	tracker.mu.Lock()
	tracker.proposalAttempts = append(tracker.proposalAttempts, attempt)
	tracker.mu.Unlock()
}

type task10KVSnapshot struct{ encoded []byte }

func (task10KVSnapshot) SchemaVersion() uint32 { return 1 }
func (snapshot task10KVSnapshot) MarshalBinary() ([]byte, error) {
	return append([]byte(nil), snapshot.encoded...), nil
}

func encodeTask10KVCommand(command task10KVCommand) []byte {
	encoded := make([]byte, 0, 12+len(command.ID)+len(command.Key)+len(command.Value))
	encoded = appendTask10String(encoded, command.ID)
	encoded = appendTask10String(encoded, command.Key)
	return appendTask10String(encoded, command.Value)
}

func decodeTask10KVCommand(encoded []byte) (task10KVCommand, error) {
	decoder := task10StringDecoder{encoded: encoded}
	id, err := decoder.next()
	if err != nil {
		return task10KVCommand{}, err
	}
	key, err := decoder.next()
	if err != nil {
		return task10KVCommand{}, err
	}
	value, err := decoder.next()
	if err != nil || decoder.offset != len(encoded) || id == "" || key == "" {
		return task10KVCommand{}, fmt.Errorf("malformed test KV command")
	}
	return task10KVCommand{ID: id, Key: key, Value: value}, nil
}

func encodeTask10KVSnapshot(index, term uint64, values, results map[string]string, firstApplies map[string]task10ApplyRecord) []byte {
	encoded := make([]byte, 16)
	binary.BigEndian.PutUint64(encoded[0:8], index)
	binary.BigEndian.PutUint64(encoded[8:16], term)
	encoded = appendTask10Map(encoded, values)
	encoded = appendTask10Map(encoded, results)
	return appendTask10ApplyRecords(encoded, firstApplies)
}

func decodeTask10KVSnapshot(encoded []byte) (uint64, uint64, map[string]string, map[string]string, map[string]task10ApplyRecord, error) {
	if len(encoded) < 16 {
		return 0, 0, nil, nil, nil, fmt.Errorf("malformed test KV snapshot base")
	}
	base, term := binary.BigEndian.Uint64(encoded[0:8]), binary.BigEndian.Uint64(encoded[8:16])
	decoder := task10StringDecoder{encoded: encoded, offset: 16}
	values, err := decoder.nextMap()
	if err != nil {
		return 0, 0, nil, nil, nil, err
	}
	results, err := decoder.nextMap()
	if err != nil {
		return 0, 0, nil, nil, nil, err
	}
	firstApplies, err := decoder.nextApplyRecords()
	if err != nil || decoder.offset != len(encoded) {
		return 0, 0, nil, nil, nil, fmt.Errorf("malformed test KV snapshot")
	}
	return base, term, values, results, firstApplies, nil
}

func appendTask10ApplyRecords(encoded []byte, records map[string]task10ApplyRecord) []byte {
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var word [8]byte
	binary.BigEndian.PutUint64(word[:], uint64(len(keys)))
	encoded = append(encoded, word[:]...)
	for _, key := range keys {
		encoded = appendTask10String(encoded, key)
		binary.BigEndian.PutUint64(word[:], records[key].Index)
		encoded = append(encoded, word[:]...)
		binary.BigEndian.PutUint64(word[:], records[key].Term)
		encoded = append(encoded, word[:]...)
	}
	return encoded
}

func appendTask10Map(encoded []byte, values map[string]string) []byte {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(keys)))
	encoded = append(encoded, count[:]...)
	for _, key := range keys {
		encoded = appendTask10String(encoded, key)
		encoded = appendTask10String(encoded, values[key])
	}
	return encoded
}

func appendTask10String(encoded []byte, value string) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	encoded = append(encoded, length[:]...)
	return append(encoded, value...)
}

type task10StringDecoder struct {
	encoded []byte
	offset  int
}

func (decoder *task10StringDecoder) next() (string, error) {
	if len(decoder.encoded)-decoder.offset < 4 {
		return "", fmt.Errorf("missing string length")
	}
	length := binary.BigEndian.Uint32(decoder.encoded[decoder.offset : decoder.offset+4])
	decoder.offset += 4
	if uint64(length) > uint64(len(decoder.encoded)-decoder.offset) {
		return "", fmt.Errorf("invalid string length")
	}
	value := string(decoder.encoded[decoder.offset : decoder.offset+int(length)])
	decoder.offset += int(length)
	return value, nil
}

func (decoder *task10StringDecoder) nextMap() (map[string]string, error) {
	if len(decoder.encoded)-decoder.offset < 4 {
		return nil, fmt.Errorf("missing map count")
	}
	count := binary.BigEndian.Uint32(decoder.encoded[decoder.offset : decoder.offset+4])
	decoder.offset += 4
	values := make(map[string]string, count)
	for index := uint32(0); index < count; index++ {
		key, err := decoder.next()
		if err != nil {
			return nil, err
		}
		value, err := decoder.next()
		if err != nil {
			return nil, err
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("duplicate snapshot key")
		}
		values[key] = value
	}
	return values, nil
}

func (decoder *task10StringDecoder) nextApplyRecords() (map[string]task10ApplyRecord, error) {
	if len(decoder.encoded)-decoder.offset < 8 {
		return nil, fmt.Errorf("missing apply record count")
	}
	count := binary.BigEndian.Uint64(decoder.encoded[decoder.offset : decoder.offset+8])
	decoder.offset += 8
	if count > uint64(len(decoder.encoded)-decoder.offset) {
		return nil, fmt.Errorf("invalid apply record count")
	}
	records := make(map[string]task10ApplyRecord, count)
	for index := uint64(0); index < count; index++ {
		key, err := decoder.next()
		if err != nil {
			return nil, err
		}
		if len(decoder.encoded)-decoder.offset < 16 {
			return nil, fmt.Errorf("missing apply record")
		}
		record := task10ApplyRecord{
			Index: binary.BigEndian.Uint64(decoder.encoded[decoder.offset : decoder.offset+8]),
			Term:  binary.BigEndian.Uint64(decoder.encoded[decoder.offset+8 : decoder.offset+16]),
		}
		decoder.offset += 16
		if _, duplicate := records[key]; duplicate {
			return nil, fmt.Errorf("duplicate apply record")
		}
		records[key] = record
	}
	return records, nil
}

func cloneTask10StringMap(values map[string]string) map[string]string {
	owned := make(map[string]string, len(values))
	for key, value := range values {
		owned[key] = value
	}
	return owned
}
