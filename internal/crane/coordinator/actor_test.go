package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/membership"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/raft"
	"github.com/aaditya/cs425mp3/internal/swim"
)

const testWaitBudget = 5 * time.Second

// opLog records the exact cross-fake operation order for sequencing assertions.
type opLog struct {
	mu      sync.Mutex
	entries []string
}

func (log *opLog) add(entry string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = append(log.entries, entry)
}

func (log *opLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.entries...)
}

func (log *opLog) contains(entry string) bool {
	for _, candidate := range log.snapshot() {
		if candidate == entry {
			return true
		}
	}
	return false
}

func (log *opLog) count(entry string) int {
	total := 0
	for _, candidate := range log.snapshot() {
		if candidate == entry {
			total++
		}
	}
	return total
}

// assertSubsequence requires every expected entry to appear in order.
func assertSubsequence(t *testing.T, entries []string, expected ...string) {
	t.Helper()
	position := 0
	for _, want := range expected {
		found := false
		for position < len(entries) {
			if entries[position] == want {
				found = true
				position++
				break
			}
			position++
		}
		if !found {
			t.Fatalf("missing %q in order within %v", want, entries)
		}
	}
}

// fakeLeadership is an in-memory LeadershipSubscription driven by tests.
type fakeLeadership struct {
	mu       sync.Mutex
	sequence uint64
	current  raft.LeadershipEvent
	events   chan raft.LeadershipEvent
	done     chan struct{}
}

func newFakeLeadership() *fakeLeadership {
	return &fakeLeadership{events: make(chan raft.LeadershipEvent, 64), done: make(chan struct{})}
}

func (leadership *fakeLeadership) Snapshot() raft.LeadershipEvent {
	leadership.mu.Lock()
	defer leadership.mu.Unlock()
	return leadership.current
}

func (leadership *fakeLeadership) Events() <-chan raft.LeadershipEvent { return leadership.events }
func (leadership *fakeLeadership) Done() <-chan struct{}               { return leadership.done }
func (leadership *fakeLeadership) Err() error                          { return nil }
func (leadership *fakeLeadership) Unsubscribe()                        {}

func (leadership *fakeLeadership) emit(event raft.LeadershipEvent) {
	leadership.mu.Lock()
	leadership.sequence++
	event.Sequence = leadership.sequence
	leadership.current = event
	leadership.mu.Unlock()
	leadership.events <- event
}

// fakeRaft applies proposals to a real Machine and can inject ambiguity.
type fakeRaft struct {
	mu            sync.Mutex
	machine       *state.Machine
	log           *opLog
	ready         chan struct{}
	leadership    *fakeLeadership
	subscribeOnce sync.Once
	subscribed    chan struct{}
	index         uint64
	term          uint64
	barrierErr    error
	// proposeHook may suppress application or inject a transport error after
	// application, simulating an ambiguous proposal outcome.
	proposeHook func(command any) (apply bool, err error)
}

func newFakeRaft(machine *state.Machine, log *opLog) *fakeRaft {
	return &fakeRaft{
		machine: machine, log: log, ready: make(chan struct{}),
		leadership: newFakeLeadership(), subscribed: make(chan struct{}), term: 1,
	}
}

func (r *fakeRaft) Ready() <-chan struct{} { return r.ready }

func (r *fakeRaft) Barrier(context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.add("barrier")
	if r.barrierErr != nil {
		return 0, r.barrierErr
	}
	return r.index, nil
}

func (r *fakeRaft) Propose(_ context.Context, encoded []byte) (raft.ProposalResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	decoded, err := state.UnmarshalCommand(encoded)
	if err != nil {
		return raft.ProposalResult{}, err
	}
	r.log.add("propose:" + commandName(decoded))
	apply, injected := true, error(nil)
	if r.proposeHook != nil {
		apply, injected = r.proposeHook(decoded)
	}
	if !apply {
		if injected == nil {
			injected = errors.New("injected proposal drop")
		}
		return raft.ProposalResult{}, injected
	}
	r.index++
	result, err := r.machine.Apply(r.index, r.term, encoded)
	if err != nil {
		return raft.ProposalResult{}, err
	}
	if injected != nil {
		return raft.ProposalResult{}, injected
	}
	return raft.ProposalResult{Index: r.index, Term: r.term, Result: result}, nil
}

func (r *fakeRaft) SubscribeLeadership(context.Context, int) (LeadershipSubscription, error) {
	r.subscribeOnce.Do(func() { close(r.subscribed) })
	return r.leadership, nil
}

func (r *fakeRaft) setTerm(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.term = term
}

func (r *fakeRaft) setProposeHook(hook func(command any) (bool, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposeHook = hook
}

// applySeed applies one command directly, bypassing the actor and the log.
func (r *fakeRaft) applySeed(t *testing.T, command any) state.CommandResult {
	t.Helper()
	encoded, err := state.MarshalCommand(command)
	if err != nil {
		t.Fatalf("marshal seed command: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index++
	resultBytes, err := r.machine.Apply(r.index, r.term, encoded)
	if err != nil {
		t.Fatalf("apply seed command: %v", err)
	}
	result, err := state.UnmarshalCommandResult(resultBytes)
	if err != nil {
		t.Fatalf("decode seed result: %v", err)
	}
	if result.Code != state.ResultSuccess {
		t.Fatalf("seed command rejected: %#v", result)
	}
	return result
}

func commandName(command any) string {
	switch command.(type) {
	case state.BeginCoordinatorEpoch:
		return "begin"
	case state.RegisterWorker:
		return "register"
	case state.DrainWorker:
		return "drain"
	case state.DeactivateWorker:
		return "deactivate"
	case state.ReplaceWorkerEpoch:
		return "replace-epoch"
	case state.SubmitJob:
		return "submit"
	case state.CancelJob:
		return "cancel"
	case state.RecordSourceEOF:
		return "eof"
	case state.InstallAssignments:
		return "install-assignments"
	case state.ReplaceAssignments:
		return "replace-assignments"
	case state.AdvanceCheckpoint:
		return "advance"
	case state.SealManifest:
		return "seal"
	case state.TransitionJob:
		return "transition"
	case state.FailJob:
		return "fail"
	default:
		return "unknown"
	}
}

// fakeMemberSubscription is a bounded scripted authorizer subscription.
type fakeMemberSubscription struct {
	source *fakeMembership
	events chan membership.Event
}

func (subscription *fakeMemberSubscription) Events() <-chan membership.Event {
	return subscription.events
}

func (subscription *fakeMemberSubscription) Snapshot(context.Context) (membership.View, error) {
	subscription.source.mu.Lock()
	subscription.source.snapshots++
	subscription.source.mu.Unlock()
	return subscription.source.View(), nil
}

// fakeMembership implements the coordinator Membership seam.
type fakeMembership struct {
	mu        sync.Mutex
	revision  uint64
	members   map[uint16]swim.Member
	subs      []*fakeMemberSubscription
	snapshots int
}

func newFakeMembership() *fakeMembership {
	return &fakeMembership{revision: 1, members: make(map[uint16]swim.Member)}
}

func (m *fakeMembership) View() membership.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	view := membership.View{Revision: m.revision}
	for node := uint16(0); node < 128; node++ {
		if member, ok := m.members[node]; ok {
			view.Members = append(view.Members, member)
		}
	}
	return view
}

func (m *fakeMembership) Subscribe(context.Context, int) (MembershipSubscription, error) {
	subscription := &fakeMemberSubscription{source: m, events: make(chan membership.Event, 64)}
	m.mu.Lock()
	m.subs = append(m.subs, subscription)
	m.mu.Unlock()
	return subscription, nil
}

func (m *fakeMembership) setMember(member swim.Member) {
	m.mu.Lock()
	m.revision++
	m.members[member.NodeID] = member
	m.mu.Unlock()
}

func (m *fakeMembership) removeMember(node uint16) {
	m.mu.Lock()
	m.revision++
	delete(m.members, node)
	m.mu.Unlock()
}

func (m *fakeMembership) emit(event membership.Event) {
	m.mu.Lock()
	subs := append([]*fakeMemberSubscription(nil), m.subs...)
	m.mu.Unlock()
	for _, subscription := range subs {
		subscription.events <- event
	}
}

func (m *fakeMembership) snapshotCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshots
}

// workerScript configures one fake worker node's scripted behavior.
type workerScript struct {
	identity     WorkerIdentity
	handshakeErr error
	fenceErr     error
	statusErr    error
	installErrs  int
	blockFence   chan struct{}
	events       []model.WorkerEvent
	pageSize     int
	inventory    func(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error)
	repair       func(grant protocol.RepairGrant) (protocol.ResultRepairStatus, error)
}

type recordedInstall struct {
	node    uint16
	install protocol.AssignmentSetInstall
}

// fakeWorkers implements WorkerClient against scripted per-node behavior.
type fakeWorkers struct {
	mu          sync.Mutex
	log         *opLog
	scripts     map[uint16]*workerScript
	installs    []recordedInstall
	checkpoints []protocol.CheckpointNotice
	fences      map[uint16]model.CoordinatorEpoch
	acks        map[uint16]uint64
	grants      []protocol.RepairGrant
}

func newFakeWorkers(log *opLog) *fakeWorkers {
	return &fakeWorkers{
		log: log, scripts: make(map[uint16]*workerScript),
		fences: make(map[uint16]model.CoordinatorEpoch), acks: make(map[uint16]uint64),
	}
}

func (w *fakeWorkers) script(node uint16) *workerScript {
	w.mu.Lock()
	defer w.mu.Unlock()
	script, ok := w.scripts[node]
	if !ok {
		script = &workerScript{}
		w.scripts[node] = script
	}
	return script
}

func (w *fakeWorkers) Handshake(_ context.Context, member swim.Member) (WorkerIdentity, error) {
	w.log.add(fmt.Sprintf("handshake:%d", member.NodeID))
	w.mu.Lock()
	defer w.mu.Unlock()
	script, ok := w.scripts[member.NodeID]
	if !ok {
		return WorkerIdentity{}, errors.New("no scripted worker")
	}
	if script.handshakeErr != nil {
		return WorkerIdentity{}, script.handshakeErr
	}
	return script.identity, nil
}

func (w *fakeWorkers) Fence(ctx context.Context, node uint16, epoch model.CoordinatorEpoch) error {
	w.log.add(fmt.Sprintf("fence:%d", node))
	w.mu.Lock()
	script, ok := w.scripts[node]
	if !ok {
		w.mu.Unlock()
		return errors.New("no scripted worker")
	}
	blocked := script.blockFence
	script.blockFence = nil
	fenceErr := script.fenceErr
	w.mu.Unlock()
	if blocked != nil {
		close(blocked)
		<-ctx.Done()
		return ctx.Err()
	}
	if fenceErr != nil {
		return fenceErr
	}
	w.mu.Lock()
	w.fences[node] = epoch
	w.mu.Unlock()
	return nil
}

func (w *fakeWorkers) Status(_ context.Context, node uint16, request protocol.WorkerStatusRequest) (protocol.WorkerStatus, error) {
	w.mu.Lock()
	script, ok := w.scripts[node]
	if !ok {
		w.mu.Unlock()
		w.log.add(fmt.Sprintf("status:%d", node))
		return protocol.WorkerStatus{}, errors.New("no scripted worker")
	}
	statusErr := script.statusErr
	epoch := script.identity.WorkerEpoch
	events := append([]model.WorkerEvent(nil), script.events...)
	pageSize := script.pageSize
	w.mu.Unlock()
	switch {
	case request.Inventory != nil:
		w.log.add(fmt.Sprintf("inventory:%d", node))
	case request.Repair != nil:
		role := "destination"
		if request.Repair.Role == protocol.RepairSource {
			role = "source"
		}
		w.log.add(fmt.Sprintf("repair:%d:%s", node, role))
	default:
		w.log.add(fmt.Sprintf("status:%d", node))
	}
	if statusErr != nil {
		return protocol.WorkerStatus{}, statusErr
	}
	w.mu.Lock()
	w.acks[node] = request.AfterTransactionID
	w.mu.Unlock()

	status := protocol.WorkerStatus{
		NodeID: node, WorkerEpoch: epoch, CoordinatorEpoch: request.CoordinatorEpoch,
		AfterTransactionID: request.AfterTransactionID, LastTransactionID: request.AfterTransactionID,
		StoreTransactionID: 1,
	}
	remaining := make([]model.WorkerEvent, 0, len(events))
	for _, event := range events {
		if event.TransactionID > request.AfterTransactionID {
			remaining = append(remaining, event)
		}
		if event.TransactionID > status.StoreTransactionID {
			status.StoreTransactionID = event.TransactionID
		}
	}
	limit := len(remaining)
	if pageSize > 0 && pageSize < limit {
		limit = pageSize
	}
	if int(request.MaxEvents) < limit {
		limit = int(request.MaxEvents)
	}
	status.Events = remaining[:limit]
	if limit > 0 {
		status.LastTransactionID = status.Events[limit-1].TransactionID
	}
	status.HasMore = limit < len(remaining)
	if request.Inventory != nil {
		summary, err := w.inventoryFor(script, *request.Inventory)
		if err != nil {
			return protocol.WorkerStatus{}, err
		}
		status.Inventory = &summary
	}
	if request.Repair != nil {
		w.mu.Lock()
		w.grants = append(w.grants, *request.Repair)
		repair := script.repair
		w.mu.Unlock()
		if repair == nil {
			return protocol.WorkerStatus{}, errors.New("no scripted repair")
		}
		repairStatus, err := repair(*request.Repair)
		if err != nil {
			return protocol.WorkerStatus{}, err
		}
		status.Repair = &repairStatus
	}
	return status, nil
}

func (w *fakeWorkers) inventoryFor(script *workerScript, query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
	w.mu.Lock()
	inventory := script.inventory
	w.mu.Unlock()
	if inventory != nil {
		return inventory(query)
	}
	return emptySummary(query), nil
}

func emptySummary(query protocol.ResultInventoryQuery) protocol.ResultInventorySummary {
	return protocol.ResultInventorySummary{
		QueryDigest:   query.QueryDigest,
		ContentDigest: model.EmptyResultInventoryDigest(query.QueryDigest),
	}
}

func (w *fakeWorkers) Install(_ context.Context, node uint16, install protocol.AssignmentSetInstall) error {
	name := "closed"
	switch install.SchedulingState {
	case model.Running:
		name = "running"
	case model.Draining:
		name = "draining"
	}
	w.log.add(fmt.Sprintf("install:%d:%s", node, name))
	w.mu.Lock()
	defer w.mu.Unlock()
	script, ok := w.scripts[node]
	if !ok {
		return errors.New("no scripted worker")
	}
	if script.installErrs > 0 {
		script.installErrs--
		return errors.New("injected install failure")
	}
	w.installs = append(w.installs, recordedInstall{node: node, install: install})
	return nil
}

func (w *fakeWorkers) Checkpoint(_ context.Context, node uint16, notice protocol.CheckpointNotice) error {
	w.log.add(fmt.Sprintf("checkpoint:%d", node))
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.scripts[node]; !ok {
		return errors.New("no scripted worker")
	}
	w.checkpoints = append(w.checkpoints, notice)
	return nil
}

func (w *fakeWorkers) installsFor(node uint16, scheduling model.SchedulingState) []protocol.AssignmentSetInstall {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result []protocol.AssignmentSetInstall
	for _, recorded := range w.installs {
		if recorded.node == node && recorded.install.SchedulingState == scheduling {
			result = append(result, recorded.install)
		}
	}
	return result
}

func (w *fakeWorkers) lastAck(node uint16) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acks[node]
}

func (w *fakeWorkers) grantList() []protocol.RepairGrant {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]protocol.RepairGrant(nil), w.grants...)
}

// scriptedNonces yields a deterministic distinct nonce per call.
type scriptedNonces struct {
	mu      sync.Mutex
	counter byte
}

func (n *scriptedNonces) Nonce() ([16]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	return [16]byte{0xAB, n.counter}, nil
}

// harness wires one Actor to scripted fakes and a real Machine and Gate.
type harness struct {
	t           *testing.T
	log         *opLog
	machine     *state.Machine
	raft        *fakeRaft
	members     *fakeMembership
	workers     *fakeWorkers
	gate        *admission.Gate
	clk         *clock.Manual
	actor       *Actor
	workerReady chan struct{}
	cancel      context.CancelFunc
	runDone     chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	log := &opLog{}
	machine := state.NewMachine()
	h := &harness{
		t: t, log: log, machine: machine,
		raft:        newFakeRaft(machine, log),
		members:     newFakeMembership(),
		workers:     newFakeWorkers(log),
		gate:        admission.NewGate(),
		clk:         clock.NewManual(time.Unix(1000, 0)),
		workerReady: make(chan struct{}),
		runDone:     make(chan error, 1),
	}
	actor, err := NewActor(ActorOptions{
		NodeID: 1, Raft: h.raft, Machine: machine, WorkerReady: h.workerReady,
		Membership: h.members, Workers: h.workers, Clock: h.clk,
		Nonces: &scriptedNonces{}, Gate: h.gate,
		FailureGracePeriod: 100 * time.Millisecond, RescanInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewActor: %v", err)
	}
	h.actor = actor
	return h
}

func (h *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.runDone <- h.actor.Run(ctx) }()
	h.t.Cleanup(h.stop)
}

func (h *harness) stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.runDone:
		if err != nil {
			h.t.Errorf("actor Run returned error: %v", err)
		}
	case <-time.After(testWaitBudget):
		h.t.Fatalf("actor Run did not stop")
	}
}

func (h *harness) markReady() {
	close(h.raft.ready)
	close(h.workerReady)
}

func (h *harness) lead(term uint64) {
	h.raft.setTerm(term)
	h.raft.leadership.emit(raft.LeadershipEvent{Term: term, Role: raft.RoleLeader, LeaderID: 1, LocalID: 1})
}

func (h *harness) follow(term uint64) {
	h.raft.leadership.emit(raft.LeadershipEvent{Term: term, Role: raft.RoleFollower, LeaderID: 2, LocalID: 1})
}

func (h *harness) gateOpen() bool {
	release, err := h.gate.Enter()
	if err != nil {
		return false
	}
	release()
	return true
}

func (h *harness) waitFor(condition func() bool, message string) {
	h.t.Helper()
	deadline := time.Now().Add(testWaitBudget)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s; log=%v", message, h.log.snapshot())
}

func (h *harness) waitGateOpen() {
	h.t.Helper()
	h.waitFor(h.gateOpen, "gate open")
}

func (h *harness) waitGateClosed() {
	h.t.Helper()
	h.waitFor(func() bool { return !h.gateOpen() }, "gate closed")
}

// rescan advances the injected clock through one pending rescan timer.
func (h *harness) rescan() {
	h.t.Helper()
	h.waitFor(func() bool { return h.clk.PendingTimers() > 0 }, "pending rescan timer")
	h.clk.Advance(time.Second)
}

func (h *harness) view() state.View { return h.machine.View() }

func (h *harness) workerRecord(node uint16) (state.WorkerRecord, bool) {
	for _, worker := range h.view().Workers {
		if worker.NodeID == node {
			return worker, true
		}
	}
	return state.WorkerRecord{}, false
}

func (h *harness) job(id model.JobID) (state.JobRecord, bool) {
	for _, job := range h.view().Jobs {
		if job.JobID == id {
			return job, true
		}
	}
	return state.JobRecord{}, false
}

// addWorkerMember registers a scripted active member and its handshake identity.
func (h *harness) addWorkerMember(node uint16, epoch model.WorkerEpoch, slots uint16) {
	h.members.setMember(swim.Member{NodeID: node, Host: "127.0.0.1", BasePort: 9000, Incarnation: 1, Status: swim.Alive})
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.identity = WorkerIdentity{
		NodeID: node, WorkerEpoch: epoch, Slots: slots,
		ConsensusFingerprint: model.ConsensusFingerprint(),
		RegistryFingerprint:  model.RegistryFingerprint(),
	}
	h.workers.mu.Unlock()
}

func (h *harness) setMemberStatus(node uint16, status swim.Status) {
	h.members.mu.Lock()
	member := h.members.members[node]
	h.members.mu.Unlock()
	member.Status = status
	h.members.setMember(member)
}

// seedEpoch commits a term-one coordinator epoch owned by a prior coordinator.
func (h *harness) seedEpoch() model.CoordinatorEpoch {
	h.t.Helper()
	view := h.machine.View()
	nonce := [16]byte{0xEE, byte(view.CoordinatorRevision + 1)}
	begin, err := state.NewBeginCoordinatorEpoch(testCommandID("seed-begin", nonce[:]), view.CoordinatorRevision, 7, nonce)
	if err != nil {
		h.t.Fatalf("seed begin: %v", err)
	}
	return h.raft.applySeed(h.t, begin).Epoch
}

// seedWorker commits one eligible worker record under the current fence.
func (h *harness) seedWorker(node uint16, epoch model.WorkerEpoch, slots uint16) {
	h.t.Helper()
	view := h.machine.View()
	revision := uint64(0)
	for _, worker := range view.Workers {
		if worker.NodeID == node {
			revision = worker.Revision
		}
	}
	record := state.WorkerRecord{
		NodeID: node, Epoch: epoch, State: state.WorkerEligible, Revision: revision + 1, Slots: slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	register, err := state.NewRegisterWorker(testCommandID("seed-worker", []byte{byte(node), byte(revision)}), revision, record, view.CoordinatorEpoch)
	if err != nil {
		h.t.Fatalf("seed worker: %v", err)
	}
	h.raft.applySeed(h.t, register)
}

func testTopologySpec(sourceParallelism uint16) model.TopologySpec {
	return model.TopologySpec{
		SchemaVersion: 1, Name: "task-18", RegistryFingerprint: model.RegistryFingerprint(),
		Stages: []model.StageSpec{
			{StageID: 1, Name: "numbers", Role: model.Source, Parallelism: sourceParallelism, Operator: model.OperatorSpec{
				Name: "range", Version: 1,
				Settings: []model.Setting{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "0"}},
			}},
			{StageID: 2, Name: "sink", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
		},
		Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle}},
	}
}

// seedRunningJob commits one running job placed across the seeded workers.
func (h *harness) seedRunningJob(sequence uint64) (model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	h.t.Helper()
	topology, err := model.ValidateTopology(testTopologySpec(1))
	if err != nil {
		h.t.Fatalf("validate topology: %v", err)
	}
	view := h.machine.View()
	submit, err := state.NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x51}, Sequence: sequence}, topology.Spec(), view.CoordinatorEpoch)
	if err != nil {
		h.t.Fatalf("seed submit: %v", err)
	}
	h.raft.applySeed(h.t, submit)
	job := submit.JobID()
	source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	eof, err := model.SourceEOF(topology, source)
	if err != nil {
		h.t.Fatalf("source EOF: %v", err)
	}
	recordEOF, err := state.NewRecordSourceEOF(testCommandID("seed-eof", job[:]), 0, source, eof, view.CoordinatorEpoch)
	if err != nil {
		h.t.Fatalf("seed EOF: %v", err)
	}
	h.raft.applySeed(h.t, recordEOF)
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, eligiblePlacements(h.machine.View(), job))
	if err != nil {
		h.t.Fatalf("seed assignment: %v", err)
	}
	install, err := state.NewInstallAssignments(testCommandID("seed-install", job[:]), 1, assignment, view.CoordinatorEpoch)
	if err != nil {
		h.t.Fatalf("seed install: %v", err)
	}
	h.raft.applySeed(h.t, install)
	transition, err := state.NewTransitionJob(testCommandID("seed-run", job[:]), 2, job, state.JobDeploying, state.JobRunning, view.CoordinatorEpoch)
	if err != nil {
		h.t.Fatalf("seed transition: %v", err)
	}
	h.raft.applySeed(h.t, transition)
	return job, topology, assignment
}

func testCommandID(domain string, parts ...[]byte) state.InternalCommandID {
	var id state.InternalCommandID
	copy(id[:], domain)
	position := len(domain)
	for _, part := range parts {
		for _, value := range part {
			if position >= len(id) {
				break
			}
			id[position] ^= value
			position++
		}
	}
	if id == (state.InternalCommandID{}) {
		id[0] = 1
	}
	return id
}

func TestActorName(t *testing.T) {
	h := newHarness(t)
	if got := h.actor.Name(); got != "crane-coordinator" {
		t.Fatalf("Name() = %q", got)
	}
}

func TestActorReadyAfterSubscriptionWithoutLeadership(t *testing.T) {
	h := newHarness(t)
	h.start()
	select {
	case <-h.actor.Ready():
		t.Fatal("Ready closed before dependencies were ready")
	case <-time.After(20 * time.Millisecond):
	}
	close(h.raft.ready)
	select {
	case <-h.actor.Ready():
		t.Fatal("Ready closed before worker readiness")
	case <-time.After(20 * time.Millisecond):
	}
	close(h.workerReady)
	select {
	case <-h.raft.subscribed:
	case <-time.After(testWaitBudget):
		t.Fatal("actor never subscribed to leadership")
	}
	select {
	case <-h.actor.Ready():
	case <-time.After(testWaitBudget):
		t.Fatal("Ready did not close after subscription")
	}
	if h.log.contains("barrier") {
		t.Fatalf("follower performed leader work: %v", h.log.snapshot())
	}
	if h.gateOpen() {
		t.Fatal("gate opened without leadership")
	}
}

func TestActorOpensGateAfterNoJobReconciliationAndClosesOnLoss(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	view := h.view()
	if view.CoordinatorEpoch.Term != 2 || view.CoordinatorEpoch.Coordinator != 1 {
		t.Fatalf("coordinator epoch = %#v", view.CoordinatorEpoch)
	}
	assertSubsequence(t, h.log.snapshot(), "barrier", "propose:begin")
	h.follow(2)
	h.waitGateClosed()
}

func TestActorShutdownJoinsEpochOwnedOperations(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	blocked := make(chan struct{})
	script := h.workers.script(2)
	h.workers.mu.Lock()
	script.blockFence = blocked
	h.workers.mu.Unlock()
	h.start()
	h.markReady()
	h.lead(2)
	select {
	case <-blocked:
	case <-time.After(testWaitBudget):
		t.Fatal("fence was never attempted")
	}
	h.stop()
	if h.gateOpen() {
		t.Fatal("gate open after shutdown")
	}
}

func TestActorLeadershipFlapRestartsEpochWithFreshNonce(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	first := h.view().CoordinatorEpoch
	h.follow(2)
	h.waitGateClosed()
	h.lead(3)
	h.waitFor(func() bool { return h.view().CoordinatorEpoch.Term == 3 }, "term three epoch")
	h.waitGateOpen()
	second := h.view().CoordinatorEpoch
	if second.Term != 3 || second == first || second.Nonce == first.Nonce {
		t.Fatalf("flap epochs first=%#v second=%#v", first, second)
	}
}

func TestActorRegistersWorkersFromFullScanInNodeIDOrder(t *testing.T) {
	h := newHarness(t)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 5)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	h.waitFor(func() bool {
		_, twoOK := h.workerRecord(2)
		_, threeOK := h.workerRecord(3)
		return twoOK && threeOK
	}, "both workers registered")
	assertSubsequence(t, h.log.snapshot(), "propose:begin", "handshake:2", "handshake:3")
	two, _ := h.workerRecord(2)
	three, _ := h.workerRecord(3)
	if two.State != state.WorkerEligible || two.Slots != 4 || two.Epoch != (model.WorkerEpoch{2}) || two.Revision != 1 {
		t.Fatalf("worker 2 = %#v", two)
	}
	if three.State != state.WorkerEligible || three.Slots != 5 {
		t.Fatalf("worker 3 = %#v", three)
	}
}

func TestActorFailedHandshakeNeverCreatesRecord(t *testing.T) {
	h := newHarness(t)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	script := h.workers.script(3)
	h.workers.mu.Lock()
	script.handshakeErr = errors.New("unreachable")
	h.workers.mu.Unlock()
	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { _, ok := h.workerRecord(2); return ok }, "worker 2 registered")
	if _, ok := h.workerRecord(3); ok {
		t.Fatal("failed handshake created a worker record")
	}
}

func TestActorConsensusFingerprintMismatchNeverRegisters(t *testing.T) {
	h := newHarness(t)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	script := h.workers.script(3)
	h.workers.mu.Lock()
	script.identity.ConsensusFingerprint = [32]byte{0x99}
	h.workers.mu.Unlock()
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	if _, ok := h.workerRecord(3); ok {
		t.Fatal("incompatible nonvoter registered")
	}
	if h.log.contains("fence:3") {
		t.Fatal("incompatible worker was fenced")
	}
	for _, install := range h.workers.installsFor(3, model.Closed) {
		t.Fatalf("incompatible worker received assignment %#v", install)
	}
}

func TestActorReplacesWorkerEpochAtomically(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2, 0xA}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2, 0xB}, 4)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		record, ok := h.workerRecord(2)
		return ok && record.Epoch == (model.WorkerEpoch{2, 0xB})
	}, "worker epoch replaced")
	record, _ := h.workerRecord(2)
	if record.Revision != 2 || record.State != state.WorkerEligible {
		t.Fatalf("replaced record = %#v", record)
	}
	if !h.log.contains("propose:replace-epoch") {
		t.Fatalf("no replace-epoch proposal: %v", h.log.snapshot())
	}
}

func TestActorResyncRequiresFullRescanBeforeActing(t *testing.T) {
	h := newHarness(t)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	h.members.emit(membership.Event{Revision: 9, Cause: membership.ResyncRequired})
	h.waitFor(func() bool { _, ok := h.workerRecord(4); return ok }, "worker 4 registered after resync")
	if h.members.snapshotCount() == 0 {
		t.Fatal("resync did not fetch a complete snapshot")
	}
}

func TestActorPeriodicRescanSchedulesPendingJobWithoutWake(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	topology, err := model.ValidateTopology(testTopologySpec(1))
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	view := h.view()
	submit, err := state.NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x77}, Sequence: 1}, topology.Spec(), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	h.raft.applySeed(t, submit)
	job := submit.JobID()

	// No Wake: only the injected-clock periodic rescan may find the job.
	h.rescan()
	h.waitFor(func() bool {
		record, ok := h.job(job)
		if !ok || record.Lifecycle != state.JobRunning {
			return false
		}
		return len(h.workers.installsFor(2, model.Running)) > 0 && len(h.workers.installsFor(3, model.Running)) > 0
	}, "job running and activated on both workers")
	record, _ := h.job(job)
	if record.Assignment == nil || record.Assignment.Revision != 1 {
		t.Fatalf("job assignment = %#v", record.Assignment)
	}
	entries := h.log.snapshot()
	assertSubsequence(t, entries,
		"propose:eof", "propose:install-assignments",
		"install:2:closed", "install:3:closed",
		"propose:transition",
		"install:2:running", "install:3:running",
	)
	for node := uint16(2); node <= 3; node++ {
		running := h.workers.installsFor(node, model.Running)
		if len(running) == 0 {
			t.Fatalf("worker %d never received running install", node)
		}
		last := running[len(running)-1]
		if last.JobControlRevision != record.JobControlRevision || last.Assignment.Digest != record.Assignment.Digest {
			t.Fatalf("running install fence mismatch: %#v vs job %#v", last, record)
		}
	}
}

func TestActorRetriesAmbiguousWorkerInstallIdempotently(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	h.seedRunningJob(1)
	script := h.workers.script(2)
	h.workers.mu.Lock()
	script.installErrs = 1
	h.workers.mu.Unlock()
	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool { return h.log.count("install:2:closed") >= 1 }, "first install attempt")
	if h.gateOpen() {
		t.Fatal("gate opened despite failed install")
	}
	h.actor.Wake()
	h.waitGateOpen()
	if len(h.workers.installsFor(2, model.Running)) == 0 {
		t.Fatal("worker 2 never accepted the running install")
	}
}
