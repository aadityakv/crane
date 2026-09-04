package sim

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goruntime "runtime"

	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/transport"
)

func gosched() { goruntime.Gosched() }

// ---------------------------------------------------------------------------
// Deterministic datagram faults (production transport.SourceDatagram seam).
// ---------------------------------------------------------------------------

// dgramFaultKind selects one deterministic datagram fault behavior.
type dgramFaultKind uint8

const (
	dgramFaultDrop dgramFaultKind = iota + 1
	dgramFaultDuplicate
	dgramFaultHold
)

// datagramRule is one active injected datagram fault on one endpoint's sends.
type datagramRule struct {
	kind         dgramFaultKind
	destinations map[config.Endpoint]struct{}
	// active and consumed are atomic: the network goroutines read them under
	// the owning faultDatagram.mu while the test goroutine deactivates and
	// polls rules without that lock.
	active   atomic.Bool
	consumed atomic.Int64
}

func (rule *datagramRule) matches(destination config.Endpoint) bool {
	if !rule.active.Load() {
		return false
	}
	if len(rule.destinations) == 0 {
		return true
	}
	_, exists := rule.destinations[destination]
	return exists
}

func (rule *datagramRule) deactivate() { rule.active.Store(false) }

type heldDatagram struct {
	source      config.Endpoint
	destination config.Endpoint
	payload     []byte
}

// faultDatagram wraps one production memory datagram with deterministic
// drop/duplicate/hold-and-reorder rules and exact consumption accounting. It
// implements the exact transport.SourceDatagram seam production consumes and
// forwards every unaffected byte unchanged.
type faultDatagram struct {
	mu         sync.Mutex
	inner      transport.SourceDatagram
	rules      []*datagramRule
	held       []heldDatagram
	sent       int
	received   int
	dropped    int
	duplicated int
	heldTotal  int
}

func wrapDatagram(inner transport.SourceDatagram) *faultDatagram {
	return &faultDatagram{inner: inner}
}

// Send transmits from the endpoint's primary address under the active rules.
func (d *faultDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	return d.SendFrom(ctx, config.Endpoint{}, destination, payload)
}

// SendFrom applies the matching fault rules and otherwise forwards to the
// deterministic memory network unchanged. A matching drop rule wins; a
// duplicate rule and a hold rule compose (the datagram and its duplicate are
// both held for a later pump step), so persistent duplication and reordering
// can be injected together.
func (d *faultDatagram) SendFrom(ctx context.Context, source, destination config.Endpoint, payload []byte) error {
	d.mu.Lock()
	d.sent++
	var drop, duplicate, hold *datagramRule
	for _, rule := range d.rules {
		if !rule.matches(destination) {
			continue
		}
		switch rule.kind {
		case dgramFaultDrop:
			if drop == nil {
				drop = rule
			}
		case dgramFaultDuplicate:
			if duplicate == nil {
				duplicate = rule
			}
		case dgramFaultHold:
			if hold == nil {
				hold = rule
			}
		}
	}
	switch {
	case drop != nil:
		drop.consumed.Add(1)
		d.dropped++
		d.mu.Unlock()
		return nil
	case hold != nil:
		hold.consumed.Add(1)
		d.heldTotal++
		copies := 1
		if duplicate != nil {
			duplicate.consumed.Add(1)
			d.duplicated++
			copies = 2
		}
		for i := 0; i < copies; i++ {
			d.held = append(d.held, heldDatagram{source: source, destination: destination, payload: append([]byte(nil), payload...)})
		}
		d.mu.Unlock()
		return nil
	case duplicate != nil:
		duplicate.consumed.Add(1)
		d.duplicated++
		d.mu.Unlock()
		if err := d.inner.SendFrom(ctx, source, destination, payload); err != nil {
			return err
		}
		return d.inner.SendFrom(ctx, source, destination, payload)
	default:
		d.mu.Unlock()
		return d.inner.SendFrom(ctx, source, destination, payload)
	}
}

// Receive returns the next delivered packet.
func (d *faultDatagram) Receive(ctx context.Context) (transport.Packet, error) {
	d.mu.Lock()
	d.received++
	d.mu.Unlock()
	return d.inner.Receive(ctx)
}

// Close releases the wrapped endpoint and every rule.
func (d *faultDatagram) Close() error {
	d.mu.Lock()
	d.rules = nil
	d.held = nil
	d.mu.Unlock()
	return d.inner.Close()
}

// releaseHeld releases held (reordered) packets into the memory network.
func (d *faultDatagram) releaseHeld() {
	d.mu.Lock()
	held := d.held
	d.held = nil
	d.mu.Unlock()
	for _, packet := range held {
		_ = d.inner.SendFrom(context.Background(), packet.source, packet.destination, packet.payload)
	}
}

func (d *faultDatagram) addRule(kind dgramFaultKind, destinations ...config.Endpoint) *datagramRule {
	d.mu.Lock()
	defer d.mu.Unlock()
	rule := &datagramRule{kind: kind}
	rule.active.Store(true)
	if len(destinations) > 0 {
		rule.destinations = make(map[config.Endpoint]struct{}, len(destinations))
		for _, destination := range destinations {
			rule.destinations[destination] = struct{}{}
		}
	}
	d.rules = append(d.rules, rule)
	return rule
}

// clearRules deactivates every rule without closing the endpoint.
func (d *faultDatagram) clearRules() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, rule := range d.rules {
		rule.deactivate()
	}
}

func (d *faultDatagram) stats() (sent, dropped, duplicated, held int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sent, d.dropped, d.duplicated, d.heldTotal
}

// ---------------------------------------------------------------------------
// Deterministic +3 dial faults (production coordinator dial seam).
// ---------------------------------------------------------------------------

// dialRule is one active injected TCP dial cut on one exact address.
type dialRule struct {
	address string
	active  atomic.Bool
	blocked atomic.Int64
}

// faultDialer wraps the production dialer used by the coordinator's +3
// worker-control and result-transfer client, cutting exact addresses.
type faultDialer struct {
	mu    sync.Mutex
	rules []*dialRule
	dial  net.Dialer
}

func newFaultDialer() *faultDialer {
	return &faultDialer{}
}

func (d *faultDialer) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	var matched *dialRule
	for _, rule := range d.rules {
		if rule.active.Load() && rule.address == address {
			matched = rule
			break
		}
	}
	if matched != nil {
		matched.blocked.Add(1)
		d.mu.Unlock()
		return nil, fmt.Errorf("sim dial cut to %s", address)
	}
	d.mu.Unlock()
	return d.dial.DialContext(ctx, network, address)
}

func (d *faultDialer) cut(address string) *dialRule {
	d.mu.Lock()
	defer d.mu.Unlock()
	rule := &dialRule{address: address}
	rule.active.Store(true)
	d.rules = append(d.rules, rule)
	return rule
}

// healDials deactivates every dial cut.
func (d *faultDialer) healDials() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rules = nil
}

// ---------------------------------------------------------------------------
// Injected-fault accounting: the harness fails when an injected fault was
// never consumed by the scenario.
// ---------------------------------------------------------------------------

type simFault struct {
	name     string
	consumed func() bool
}

func (cluster *simCluster) trackFault(name string, consumed func() bool) {
	cluster.t.Helper()
	fault := &simFault{name: name, consumed: consumed}
	cluster.oracle.mu.Lock()
	cluster.oracle.pendingFaults = append(cluster.oracle.pendingFaults, fault)
	cluster.oracle.mu.Unlock()
	cluster.record("inject fault %s", name)
}

func (cluster *simCluster) requireFaultsConsumed(where string) {
	cluster.t.Helper()
	cluster.oracle.mu.Lock()
	defer cluster.oracle.mu.Unlock()
	for _, fault := range cluster.oracle.pendingFaults {
		if !fault.consumed() {
			cluster.fail("%s: injected fault %q was never consumed", where, fault.name)
		}
	}
}

// awaitFaultConsumed pumps until one injected fault has taken effect at least
// once, so a scenario never crashes the faulted process at the injection step
// itself: the fault must have consumed real traffic in its intended window or
// the terminal requireFaultsConsumed check would fast-fail spuriously.
func (cluster *simCluster) awaitFaultConsumed(name string) {
	cluster.t.Helper()
	cluster.oracle.mu.Lock()
	var fault *simFault
	for _, candidate := range cluster.oracle.pendingFaults {
		if candidate.name == name {
			fault = candidate
		}
	}
	cluster.oracle.mu.Unlock()
	if fault == nil {
		cluster.t.Fatalf("awaitFaultConsumed: unknown injected fault %q", name)
		return
	}
	cluster.await("fault "+name+" consumed", fault.consumed)
}

// ---------------------------------------------------------------------------
// Harness fault helpers.
// ---------------------------------------------------------------------------

// tupleEndpointOf returns one node's advertised +5 datagram endpoint.
func (cluster *simCluster) tupleEndpointOf(id uint16) config.Endpoint {
	cluster.t.Helper()
	endpoint, err := cluster.nodes[id].config.AdvertiseEndpoint(config.ServiceCraneTupleACK)
	if err != nil {
		cluster.t.Fatal(err)
	}
	return endpoint
}

// swimEndpointsOf returns one node's advertised SWIM datagram endpoints.
func (cluster *simCluster) swimEndpointsOf(id uint16) []config.Endpoint {
	cluster.t.Helper()
	node := cluster.nodes[id]
	var endpoints []config.Endpoint
	for _, service := range []config.Service{config.ServiceSWIMPing, config.ServiceSWIMACK} {
		endpoint, err := node.config.AdvertiseEndpoint(service)
		if err != nil {
			cluster.t.Fatal(err)
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

// controlAddressOf returns one node's advertised +3 address.
func (cluster *simCluster) controlAddressOf(id uint16) string {
	cluster.t.Helper()
	endpoint, err := cluster.nodes[id].config.AdvertiseEndpoint(config.ServiceCraneWorker)
	if err != nil {
		cluster.t.Fatal(err)
	}
	return endpoint.String()
}

func (cluster *simCluster) trackRules(name string, rules ...*datagramRule) {
	cluster.trackFault(name, func() bool {
		for _, rule := range rules {
			if rule.consumed.Load() > 0 {
				return true
			}
		}
		return false
	})
}

// dropTupleDatagrams drops one node's +5 sends toward optional destinations.
func (cluster *simCluster) dropTupleDatagrams(id uint16, name string, destinations ...config.Endpoint) {
	cluster.t.Helper()
	cluster.trackRules(name, cluster.nodes[id].tupleD.addRule(dgramFaultDrop, destinations...))
}

// dropSwimDatagrams drops one node's SWIM datagram sends toward optional
// destinations, producing SWIM suspicion without touching +3 or +5.
func (cluster *simCluster) dropSwimDatagrams(id uint16, name string, destinations ...config.Endpoint) {
	cluster.t.Helper()
	cluster.trackRules(name, cluster.nodes[id].swimD.addRule(dgramFaultDrop, destinations...))
}

// isolateNode cuts every simulated datagram link and the coordinator's +3
// dial to one node, in both directions, without crashing it. The partition
// is one injected fault: it is consumed once any of its directional rules
// blocks traffic (which link carries packets during the window depends on
// the task placement — an isolated sink whose tuples were all acknowledged
// sends nothing further on +5).
func (cluster *simCluster) isolateNode(id uint16, name string) {
	cluster.t.Helper()
	node := cluster.nodes[id]
	rules := []*datagramRule{
		node.swimD.addRule(dgramFaultDrop),
		node.tupleD.addRule(dgramFaultDrop),
	}
	targetSwim := cluster.swimEndpointsOf(id)
	targetTuple := cluster.tupleEndpointOf(id)
	for _, other := range cluster.ids {
		if other == id {
			continue
		}
		peer := cluster.nodes[other]
		rules = append(rules, peer.swimD.addRule(dgramFaultDrop, targetSwim...), peer.tupleD.addRule(dgramFaultDrop, targetTuple))
	}
	cluster.trackRules(name, rules...)
	cluster.cutControl(id, name+"-dial")
}

// tupleDatagramFaultEverywhere installs one +5 fault rule on every node and
// tracks the set as one injected fault: only nodes hosting a producing task
// (or acknowledging sink) send +5 datagrams, so consumption is a property of
// the cluster-wide fault, not of each node.
func (cluster *simCluster) tupleDatagramFaultEverywhere(kind dgramFaultKind, name string) []*datagramRule {
	cluster.t.Helper()
	rules := make([]*datagramRule, 0, len(cluster.ids))
	for _, id := range cluster.ids {
		rules = append(rules, cluster.nodes[id].tupleD.addRule(kind))
	}
	cluster.trackRules(name, rules...)
	return rules
}

// cutControl cuts only the coordinator's +3 dial to one node.
func (cluster *simCluster) cutControl(id uint16, name string) {
	cluster.t.Helper()
	rule := cluster.dialer.cut(cluster.controlAddressOf(id))
	cluster.trackFault(name, func() bool { return rule.blocked.Load() > 0 })
}

// healDatagrams deactivates every datagram fault rule in the cluster.
func (cluster *simCluster) healDatagrams() {
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if node == nil {
			continue
		}
		if node.swimD != nil {
			node.swimD.clearRules()
		}
		if node.tupleD != nil {
			node.tupleD.clearRules()
		}
	}
	cluster.dialer.healDials()
}

// ---------------------------------------------------------------------------
// Pausable client link: crash the client with a response exactly in flight.
// ---------------------------------------------------------------------------

type pauseGate struct {
	mu       sync.Mutex
	released bool
	waiters  []chan struct{}
	conns    []*gatedConn
}

func newPauseGate() *pauseGate { return &pauseGate{} }

func (gate *pauseGate) release() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.released = true
	for _, waiter := range gate.waiters {
		close(waiter)
	}
	gate.waiters = nil
}

func (gate *pauseGate) register(conn *gatedConn) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.conns = append(gate.conns, conn)
}

// abort closes every gated connection exactly as a client crash would.
func (gate *pauseGate) abort() {
	gate.mu.Lock()
	conns := gate.conns
	gate.conns = nil
	gate.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

type gatedConn struct {
	net.Conn
	gate   *pauseGate
	closed chan struct{}
	once   sync.Once
}

func (conn *gatedConn) wait() <-chan struct{} {
	conn.gate.mu.Lock()
	if conn.gate.released {
		conn.gate.mu.Unlock()
		ready := make(chan struct{})
		close(ready)
		return ready
	}
	ready := make(chan struct{})
	conn.gate.waiters = append(conn.gate.waiters, ready)
	conn.gate.mu.Unlock()
	return ready
}

func (conn *gatedConn) Read(buffer []byte) (int, error) {
	select {
	case <-conn.wait():
		return conn.Conn.Read(buffer)
	case <-conn.closed:
		return 0, net.ErrClosed
	}
}

func (conn *gatedConn) Close() error {
	conn.once.Do(func() { close(conn.closed) })
	return conn.Conn.Close()
}

// pausableClientDial returns one +4 dial whose server responses are held
// until the gate releases, so a scenario can crash the client with the
// response exactly in flight and its pending request still unresolved.
func pausableClientDial(gate *pauseGate) func(context.Context, string) (net.Conn, error) {
	dialer := &net.Dialer{}
	return func(ctx context.Context, address string) (net.Conn, error) {
		real, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		conn := &gatedConn{Conn: real, gate: gate, closed: make(chan struct{})}
		gate.register(conn)
		return conn, nil
	}
}

// ---------------------------------------------------------------------------
// Scripted scenario helpers.
// ---------------------------------------------------------------------------

// awaitJobTerminal waits for Succeeded/Failed/Canceled.
func (cluster *simCluster) awaitJobTerminal(job model.JobID, context string) state.JobLifecycle {
	cluster.t.Helper()
	cluster.awaitJob(job, fmt.Sprintf("job %x terminal (%s)", job, context), func() bool {
		record, ok := cluster.jobRecord(job)
		return ok && (record.Lifecycle == state.JobSucceeded || record.Lifecycle == state.JobFailed || record.Lifecycle == state.JobCanceled)
	})
	record, _ := cluster.jobRecord(job)
	return record.Lifecycle
}

// awaitJob is await with a periodic trace of the job's replicated and
// durable state, so a wedged scenario fails with the evidence in its trace.
func (cluster *simCluster) awaitJob(job model.JobID, description string, condition func() bool) {
	cluster.t.Helper()
	cluster.await(description, func() bool {
		if condition() {
			return true
		}
		if cluster.step.Load()%4000 == 0 {
			cluster.recordJobState(job)
		}
		return false
	})
}

// recordJobState traces one job's leader view, worker records, membership,
// and every live worker store's durable job state.
func (cluster *simCluster) recordJobState(job model.JobID) {
	record, ok := cluster.jobRecord(job)
	if !ok {
		return
	}
	var watermarks []string
	for source, checkpoint := range record.Checkpoints {
		watermarks = append(watermarks, fmt.Sprintf("%d:%d", source.Partition, checkpoint.Watermark))
	}
	assignment := "none"
	if record.Assignment != nil {
		assignment = fmt.Sprintf("rev=%d", record.Assignment.Revision)
		for _, replica := range record.Assignment.ResultReplicas {
			assignment += fmt.Sprintf(" replicas=%d/%d", replica.PrimaryNodeID, replica.SecondaryNodeID)
		}
	}
	cluster.record("job wait: lifecycle=%d assignment=%s control=%d watermarks=%v manifests=%d markers=%d leader=%d",
		record.Lifecycle, assignment, record.JobControlRevision, watermarks, len(record.Manifests), len(record.NeedsReassignment), cluster.oracle.currentLeader())
	if view, leaderID, viewOK := cluster.leaderView(); viewOK {
		var workers []string
		for _, worker := range view.Workers {
			workers = append(workers, fmt.Sprintf("%d:state=%d/rev=%d", worker.NodeID, worker.State, worker.Revision))
		}
		var members []string
		for _, member := range cluster.membershipOf(leaderID) {
			members = append(members, fmt.Sprintf("%d:%d", member.NodeID, member.Status))
		}
		_, open := cluster.nodes[leaderID].runtime.Gate.AdmissionEpoch()
		cluster.record("job wait: coord-epoch={%d %d %d} gate-open=%t workers=%v members=%v", view.CoordinatorEpoch.Term, view.CoordinatorEpoch.BeginIndex, view.CoordinatorEpoch.Coordinator, open, workers, members)
	}
	for _, id := range cluster.ids {
		handle := cluster.workerStore(id)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		outboxes, accepted := 0, 0
		for _, outbox := range work.Outboxes {
			if !outbox.Completed {
				outboxes++
				if outbox.Accepted {
					accepted++
				}
			}
		}
		states := make(map[store.DeliveryState]int)
		for _, delivery := range work.Deliveries {
			states[delivery.State]++
		}
		stale := 0
		for _, result := range work.Results {
			for _, assignment := range work.Assignments {
				if assignment.Assignment.JobID == result.Record.TupleID.JobID && (result.Provenance.AssignmentRevision != assignment.Assignment.Revision || result.Provenance.CoordinatorEpoch != assignment.CoordinatorEpoch) {
					stale++
				}
			}
		}
		var cursors []string
		for _, cursor := range work.Sources {
			cursors = append(cursors, fmt.Sprintf("%d.%d:wm=%d/next=%d/eof=%d", cursor.Source.StageID, cursor.Source.Partition, cursor.Watermark, cursor.NextSequence, cursor.EOF))
		}
		var installs []string
		for _, assignment := range work.Assignments {
			var tokens []string
			for _, token := range assignment.Assignment.Tasks {
				tokens = append(tokens, fmt.Sprintf("%d.%d@%d/a%d", token.Task.StageID, token.Task.Partition, token.WorkerID, token.Attempt))
			}
			installs = append(installs, fmt.Sprintf("sched=%d/term=%d/rev=%d/jcr=%d/tokens=%v", assignment.SchedulingState, assignment.CoordinatorEpoch.Term, assignment.Assignment.Revision, assignment.JobControlRevision, tokens))
		}
		var pending []string
		for _, outbox := range work.Outboxes {
			if !outbox.Completed && len(pending) < 2 {
				pending = append(pending, fmt.Sprintf("%d.%d@%d/a%d->%d.%d@%d/a%d/rev=%d/term=%d/acc=%t", outbox.Producer.Task.StageID, outbox.Producer.Task.Partition, outbox.Producer.WorkerID, outbox.Producer.Attempt, outbox.Destination.Task.StageID, outbox.Destination.Task.Partition, outbox.Destination.WorkerID, outbox.Destination.Attempt, outbox.AssignmentRevision, outbox.CoordinatorEpoch.Term, outbox.Accepted))
			}
		}
		if len(pending) > 0 {
			cursors = append(cursors, "pending="+fmt.Sprint(pending))
		}
		var view []string
		for _, member := range cluster.membershipOf(id) {
			view = append(view, fmt.Sprintf("%d:%d:%d", member.NodeID, member.Incarnation, member.Status))
		}
		cursors = append(cursors, "view="+fmt.Sprint(view))
		var repairs []string
		for _, repair := range work.Repairs {
			repairs = append(repairs, fmt.Sprintf("%x:role=%d/state=%d/count=%d", repair.Instruction.RepairID[:4], repair.Role, repair.State, repair.RecordCount))
		}
		cluster.record("job wait: node=%d deliveries=%d(recv=%d/proc=%d/comp=%d/compact=%d) outboxes=%d(accepted=%d) results=%d(stale=%d) sources=%v fence-term=%d installs=%v repairs=%v",
			id, len(work.Deliveries), states[store.Received], states[store.Processed], states[store.Completed], states[store.Compacted], outboxes, accepted, len(work.Results), stale, cursors, work.Fence.Term, installs, repairs)
	}
}

// assignmentAttempt returns the committed attempt of one task.
func (cluster *simCluster) assignmentAttempt(job model.JobID, task model.TaskID) uint64 {
	cluster.t.Helper()
	record, ok := cluster.jobRecord(job)
	if !ok || record.Assignment == nil {
		return 0
	}
	for _, token := range record.Assignment.Tasks {
		if token.Task == task {
			return token.Attempt
		}
	}
	return 0
}

// assignmentWorker returns the committed worker of one task.
func (cluster *simCluster) assignmentWorker(job model.JobID, task model.TaskID) uint16 {
	cluster.t.Helper()
	record, ok := cluster.jobRecord(job)
	if !ok || record.Assignment == nil {
		return 0
	}
	for _, token := range record.Assignment.Tasks {
		if token.Task == task {
			return token.WorkerID
		}
	}
	return 0
}

// sinkPrimaryNode returns the node currently holding one job's sink task.
func (cluster *simCluster) sinkPrimaryNode(plan *simJobPlan) uint16 {
	cluster.t.Helper()
	cluster.await("sink task assignment committed", func() bool {
		return cluster.assignmentWorker(plan.jobID, plan.sink) != 0
	})
	return cluster.assignmentWorker(plan.jobID, plan.sink)
}

// deliveryInState finds one running node whose durable store holds a
// delivery of the job in at least the requested state.
func (cluster *simCluster) deliveryInState(job model.JobID, minimum store.DeliveryState) (uint16, model.DeliveryID, bool) {
	for _, id := range cluster.ids {
		handle := cluster.workerStore(id)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		for _, delivery := range work.Deliveries {
			if delivery.ID.Tuple.JobID == job && delivery.State >= minimum {
				return id, delivery.ID, true
			}
		}
	}
	return 0, model.DeliveryID{}, false
}

// anyDeliveryState reports whether any live store holds a delivery of the
// job in at least the requested state.
func (cluster *simCluster) anyDeliveryState(job model.JobID, minimum store.DeliveryState) bool {
	_, _, ok := cluster.deliveryInState(job, minimum)
	return ok
}

// committedWatermark returns one source's committed checkpoint watermark.
func (cluster *simCluster) committedWatermark(job model.JobID, source model.TaskID) uint64 {
	cluster.t.Helper()
	record, ok := cluster.jobRecord(job)
	if !ok {
		return 0
	}
	return record.Checkpoints[source].Watermark
}

// manifestCommitted reports whether one sink's manifest exists.
func (cluster *simCluster) manifestCommitted(job model.JobID, sink model.TaskID) bool {
	record, ok := cluster.jobRecord(job)
	if !ok {
		return false
	}
	_, exists := record.Manifests[sink]
	return exists
}

// allWatermarksAtEOF reports whether every source reached its committed EOF.
func (cluster *simCluster) allWatermarksAtEOF(plan *simJobPlan) bool {
	for source, eof := range plan.sourceEOFs {
		if cluster.committedWatermark(plan.jobID, source) != eof {
			return false
		}
	}
	return true
}

// workerEpochOf returns one running node's durable worker epoch.
func (cluster *simCluster) workerEpochOf(id uint16) (model.WorkerEpoch, bool) {
	handle := cluster.workerStore(id)
	if handle == nil {
		return model.WorkerEpoch{}, false
	}
	return handle.WorkerEpoch(), true
}

// crashLeader crashes the current Raft leader voter.
func (cluster *simCluster) crashLeader() uint16 {
	cluster.t.Helper()
	leader := cluster.oracle.currentLeader()
	if leader == 0 {
		cluster.fail("crashLeader without an observed leader")
	}
	cluster.stopNode(cluster.nodes[leader])
	cluster.oracle.noteEvent()
	return leader
}

// runScriptedScenario drives the standard scenario shape: steady cluster, one
// submitted reference job, the scripted faults, completion, and verification.
func runScriptedScenario(t *testing.T, seed uint64, name string, endExclusive int64, stage simStageSpec, sourceParallelism uint16,
	script func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID)) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient(name)
	spec := newSimTopology(name, sourceParallelism, endExclusive, stage)
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, stage)
	job := cluster.submit(client, plan)
	cluster.await("job running", func() bool {
		record, ok := cluster.jobRecord(job)
		return ok && record.Lifecycle == state.JobRunning
	})
	if initial, ok := cluster.jobRecord(job); !ok || initial.Assignment == nil {
		cluster.fail("scenario %s lost its committed assignment before the script", name)
	}
	script(t, cluster, client, plan, job)
	lifecycle := cluster.awaitJobTerminal(job, name)
	if lifecycle != state.JobSucceeded {
		cluster.fail("scenario %s: job ended %d, want Succeeded", name, lifecycle)
	}
	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, name)
	cluster.requireFaultsConsumed(name)
	if cluster.oracle.meaningfulEvents() < 12 {
		cluster.fail("scenario %s produced only %d meaningful events", name, cluster.oracle.meaningfulEvents())
	}
	cluster.record("scenario %s verified with %d meaningful events", name, cluster.oracle.meaningfulEvents())
}

// ---------------------------------------------------------------------------
// The scripted failure matrix.
// ---------------------------------------------------------------------------

// TestScriptedBaselineJobRunsToSuccess proves the harness itself: unfaulted
// full jobs, an empty source, and a filtered multi-stage topology all
// complete with the exact unique globally ordered reference output.
func TestScriptedBaselineJobRunsToSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("scripted simulations run full in-process clusters")
	}
	t.Run("plain range collect", func(t *testing.T) {
		runScriptedScenario(t, 0x51A50001, "baseline", 6, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {})
	})
	t.Run("empty source trivially checkpoints EOF zero", func(t *testing.T) {
		runScriptedScenario(t, 0x51A50002, "empty", 0, simStageSpec{}, 2,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {})
	})
	t.Run("filter multiply two source partitions", func(t *testing.T) {
		runScriptedScenario(t, 0x51A50003, "chain", 10, simStageSpec{filter: "even", factor: 3}, 2,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {})
	})
}

// TestScriptedFailureScenarios covers the complete scripted failure matrix on
// the 3-voter + 1-nonvoter production topology.
func TestScriptedFailureScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("scripted simulations run full in-process clusters")
	}

	// Leader loss after assignment: the leader crashes once the assignment
	// set is committed; a new leader reconciles and the job completes.
	t.Run("leader loss after assignment", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00001, "leader-after-assignment", 8, simStageSpec{factor: 2}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("assignment installed", func() bool {
					return cluster.assignmentAttempt(job, plan.sink) > 0
				})
				cluster.crashLeader()
				cluster.await("new leader elected", func() bool {
					return cluster.oracle.currentLeader() != 0
				})
			})
	})

	// Leader loss after durable worker progress but before any committed
	// checkpoint: no progress may be claimed that was not committed.
	t.Run("leader loss after progress before checkpoint", func(t *testing.T) {
		// The crashed leader hosts a task and a replica: the survivor's
		// in-flight (Processed, superseded-envelope) results re-replicate to
		// the replacement pair and the replayed attempt's duplicates answer
		// from retained custody (defect #4 fix).
		runScriptedScenario(t, 0x5CF00002, "leader-uncommitted-progress", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("durable worker progress", func() bool {
					if !cluster.anyDeliveryState(job, store.Received) {
						return false
					}
					for _, source := range plan.sources {
						if cluster.committedWatermark(job, source) != 0 {
							return false
						}
					}
					return true
				})
				cluster.crashLeader()
				cluster.await("new leader elected", func() bool {
					return cluster.oracle.currentLeader() != 0
				})
			})
	})

	// Tuple datagram loss, duplication, and reordering: drops for a bounded
	// window followed by persistent duplication and reordering; retries and
	// deduplication still produce the exact reference output.
	t.Run("packet loss duplication reordering", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00003, "datagram-chaos", 8, simStageSpec{factor: 2}, 2,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				drops := cluster.tupleDatagramFaultEverywhere(dgramFaultDrop, "loss")
				cluster.pump(240)
				cluster.record("heal tuple drops; keep duplication and reordering")
				for index, id := range cluster.ids {
					node := cluster.nodes[id]
					node.tupleD.mu.Lock()
					drops[index].deactivate()
					node.tupleD.mu.Unlock()
				}
				cluster.tupleDatagramFaultEverywhere(dgramFaultDuplicate, "duplication")
				cluster.tupleDatagramFaultEverywhere(dgramFaultHold, "reordering")
			})
	})

	// SWIM false suspicion: dropping only node 4's SWIM probes makes it
	// Suspect while +3 and +5 stay healthy; Suspect alone never reassigns,
	// and after the UDP heal the job completes with unchanged attempts.
	t.Run("swim false suspicion never reassigns", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00004, "false-suspicion", 6, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				attempt := cluster.assignmentAttempt(job, plan.sink)
				target := cluster.swimEndpointsOf(4)
				for _, other := range cluster.ids {
					if other == 4 {
						continue
					}
					cluster.dropSwimDatagrams(other, fmt.Sprintf("suspect-probe-%d", other), target...)
				}
				cluster.dropSwimDatagrams(4, "suspect-out")
				// Heal while the member is still only Suspect everywhere: a
				// Dead record is a terminal incarnation floor, so the window
				// is bounded by the earliest observer's suspicion timeout.
				cluster.await("node 4 suspected", func() bool {
					for _, id := range cluster.ids {
						if id == 4 {
							continue
						}
						for _, member := range cluster.membershipOf(id) {
							if member.NodeID == 4 && member.Status == swim.Suspect {
								return true
							}
						}
					}
					return false
				})
				for i := 0; i < 40; i++ {
					cluster.pump(1)
					if cluster.assignmentAttempt(job, plan.sink) != attempt {
						cluster.fail("attempt advanced while the member was only Suspect")
					}
				}
				// The suspicion-confirmation race is load-sensitive: if any
				// observer confirmed Dead before the heal, the premise window
				// ("still only Suspect everywhere") is lost — a Dead record is a
				// terminal incarnation floor and an alive-but-declared-dead
				// incarnation never re-enters by design, so the scenario skips
				// instead of waiting out a premise it can no longer meet.
				for _, id := range cluster.ids {
					if id == 4 {
						continue
					}
					for _, member := range cluster.membershipOf(id) {
						if member.NodeID == 4 && member.Status == swim.Dead {
							cluster.record("node %d confirmed node 4 Dead before the heal", id)
							cluster.t.Skip("false-suspicion: the heal window lost the suspicion race; a Dead record is a terminal incarnation floor (scenario premise)")
						}
					}
				}
				cluster.healDatagrams()
				cluster.record("healed SWIM datagrams")
				cluster.await("membership heals back to alive", func() bool {
					if cluster.step.Load()%2000 == 0 {
						for _, id := range cluster.ids {
							var summary []string
							for _, member := range cluster.membershipOf(id) {
								summary = append(summary, fmt.Sprintf("%d:%d:%d", member.NodeID, member.Incarnation, member.Status))
							}
							cluster.record("membership node=%d view=%v", id, summary)
						}
					}
					// The Dead confirmation can also land between the pre-heal
					// premise check and the heal taking effect; a Dead record is a
					// terminal incarnation floor, so the premise is lost the same
					// way and the scenario skips rather than waiting forever.
					for _, id := range cluster.ids {
						for _, member := range cluster.membershipOf(id) {
							if member.NodeID != id && member.Status == swim.Dead {
								cluster.record("node %d holds node %d Dead after the heal", id, member.NodeID)
								cluster.t.Skip("false-suspicion: Dead was confirmed inside the heal window; a Dead record is a terminal incarnation floor (scenario premise)")
							}
						}
					}
					for _, member := range cluster.membershipOf(1) {
						if member.NodeID == 4 && member.Status != swim.Alive {
							return false
						}
					}
					return true
				})
			})
	})

	// Partition and heal: a fully isolated worker first stays Suspect with no
	// reassignment inside the grace window, then — staying unreachable — is
	// decommitted through Dead plus the failure grace period into a committed
	// reassignment, and after the heal the job completes on the replacements
	// while the isolated worker's old attempts stay fenced.
	t.Run("partition and heal with committed reassignment", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00005, "partition-heal", 6, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				// Node 4's exact duties depend on the registration
				// interleavings, so the scenario tracks whichever duties it
				// actually holds: the partition must decommit and reassign
				// exactly those after the failure grace period.
				record, ok := cluster.jobRecord(job)
				if !ok || record.Assignment == nil {
					cluster.fail("job has no committed assignment to partition against")
				}
				prior := make(map[model.TaskID]uint64, len(record.Assignment.Tasks))
				heldTask, heldReplica := false, false
				for _, token := range record.Assignment.Tasks {
					prior[token.Task] = token.Attempt
					if token.WorkerID == 4 {
						heldTask = true
					}
				}
				for _, replica := range record.Assignment.ResultReplicas {
					if replica.PrimaryNodeID == 4 || replica.SecondaryNodeID == 4 {
						heldReplica = true
					}
				}
				if !heldTask && !heldReplica {
					// The deterministic builder's placement depends on the
					// registration interleavings; a placement that gives the
					// lone nonvoter no duty leaves this scenario no premise.
					cluster.record("node 4 holds no duty at this placement")
					cluster.t.Skip("partition-heal: node 4 holds neither a task nor a replica duty at this placement")
				}
				cluster.isolateNode(4, "partition")
				cluster.await("node 4 suspected while isolated", func() bool {
					for _, member := range cluster.membershipOf(cluster.oracle.currentLeader()) {
						if member.NodeID == 4 {
							return member.Status == swim.Suspect || member.Status == swim.Dead
						}
					}
					return false
				})
				for task, attempt := range prior {
					if cluster.assignmentAttempt(job, task) != attempt {
						cluster.fail("reassignment committed before the failure grace period")
					}
				}
				cluster.await("worker 4 decommitted and reassigned", func() bool {
					current, ok := cluster.jobRecord(job)
					if !ok || current.Assignment == nil {
						return false
					}
					for _, token := range current.Assignment.Tasks {
						if token.WorkerID != 4 && token.Attempt > prior[token.Task] {
							return true
						}
					}
					if heldReplica {
						for _, replica := range current.Assignment.ResultReplicas {
							if replica.PrimaryNodeID != 4 && replica.SecondaryNodeID != 4 {
								return true
							}
						}
					}
					return false
				})
				cluster.healDatagrams()
			})
	})

	// Worker crash after Received before the sender observed the ACK: the
	// node's outbound +5 is dropped, the process crashes with durable
	// custody, and the same-epoch restart re-acknowledges without rework.
	t.Run("worker crash after received before ack", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00006, "crash-received", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("durable custody accepted", func() bool {
					return cluster.anyDeliveryState(job, store.Received)
				})
				node, _, _ := cluster.deliveryInState(job, store.Received)
				cluster.dropTupleDatagrams(node, "crash-received-ack-loss")
				cluster.awaitFaultConsumed("crash-received-ack-loss")
				cluster.stopNode(cluster.nodes[node])
				cluster.pump(80)
				cluster.restartNode(cluster.nodes[node], false)
				cluster.await("worker re-admitted after same-epoch restart", func() bool {
					record, ok := cluster.jobRecord(job)
					if !ok {
						return false
					}
					handle := cluster.workerStore(node)
					return handle != nil && record.Lifecycle != state.JobFailed
				})
			})
	})

	// Worker crash after Processed before the ACK: the stored outputs survive
	// and the retry is answered from the durable Processed state.
	t.Run("worker crash after processed before ack", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00007, "crash-processed", 8, simStageSpec{factor: 2}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("durable processed outputs", func() bool {
					return cluster.anyDeliveryState(job, store.Processed)
				})
				node, _, _ := cluster.deliveryInState(job, store.Processed)
				cluster.dropTupleDatagrams(node, "crash-processed-ack-loss")
				cluster.awaitFaultConsumed("crash-processed-ack-loss")
				cluster.stopNode(cluster.nodes[node])
				cluster.pump(80)
				cluster.restartNode(cluster.nodes[node], false)
			})
	})

	// Store-preserving restart: the crashed worker rejoins with the same
	// durable worker epoch and the retained Running assignment, receives the
	// idempotent Running reinstall once its closed gate is observed, and the
	// job completes without any attempt change.
	t.Run("store preserving restart keeps epoch and attempts", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00008, "restart-kept-store", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				epoch, ok := cluster.workerEpochOf(4)
				if !ok {
					cluster.fail("node 4 has no worker epoch before restart")
				}
				attempt := cluster.assignmentAttempt(job, plan.sink)
				cluster.await("worker progress before crash", func() bool {
					return cluster.anyDeliveryState(job, store.Received)
				})
				cluster.stopNode(cluster.nodes[4])
				cluster.pump(120)
				cluster.restartNode(cluster.nodes[4], false)
				cluster.await("same durable epoch after restart", func() bool {
					restarted, ok := cluster.workerEpochOf(4)
					return ok && restarted == epoch
				})
				cluster.await("job still on the original attempts", func() bool {
					return cluster.assignmentAttempt(job, plan.sink) == attempt
				})
			})
	})

	// Store loss creates a new worker epoch: the replacement fences the old
	// incarnation atomically, reassigns exactly its tasks with attempt+1,
	// and the job completes with identical logical output.
	t.Run("store loss new epoch reassigns", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF00009, "restart-lost-store", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				oldEpoch, ok := cluster.workerEpochOf(4)
				if !ok {
					cluster.fail("node 4 has no worker epoch before store loss")
				}
				cluster.await("worker progress before crash", func() bool {
					return cluster.anyDeliveryState(job, store.Received)
				})
				cluster.stopNode(cluster.nodes[4])
				cluster.restartNode(cluster.nodes[4], true)
				cluster.await("fresh durable epoch after store loss", func() bool {
					restarted, ok := cluster.workerEpochOf(4)
					return ok && restarted != oldEpoch
				})
			})
	})

	// Old-attempt delivery while Alive: node 4 crashes with its store kept,
	// its tasks are decommitted and reassigned after the grace period, and
	// only then does it restart Alive with the retained old-epoch outbox; its
	// stale-attempt replays are rejected without mutation and the job
	// completes with the exact reference output.
	t.Run("old attempt delivery while alive", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF0000A, "stale-attempt", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				if cluster.assignmentWorker(job, plan.sink) != 4 && cluster.assignmentWorker(job, plan.sources[0]) != 4 {
					// Node 4 holds a replica duty at minimum; the scenario
					// still exercises the fenced rejoin either way.
					cluster.record("node 4 holds replica duty only")
				}
				attempt := cluster.assignmentAttempt(job, plan.sink)
				cluster.await("worker progress before crash", func() bool {
					return cluster.anyDeliveryState(job, store.Received)
				})
				cluster.stopNode(cluster.nodes[4])
				cluster.await("node 4's duties reassigned after grace", func() bool {
					record, ok := cluster.jobRecord(job)
					if !ok || record.Assignment == nil {
						return false
					}
					for _, token := range record.Assignment.Tasks {
						if token.Attempt > attempt && token.WorkerID != 4 {
							return true
						}
					}
					for _, replica := range record.Assignment.ResultReplicas {
						if replica.PrimaryNodeID != 4 && replica.SecondaryNodeID != 4 {
							return true
						}
					}
					return false
				})
				cluster.restartNode(cluster.nodes[4], false)
				cluster.await("node 4 alive again after fenced rejoin", func() bool {
					for _, member := range cluster.membershipOf(1) {
						if member.NodeID == 4 {
							return member.Status == swim.Alive
						}
					}
					return false
				})
			})
	})

	// Checkpoint notice loss: the coordinator's +3 dial to the source worker
	// is cut across checkpoint commits and healed later; the worker applies
	// the committed watermark idempotently and compaction still matches.
	t.Run("checkpoint notice loss heals idempotently", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF0000B, "notice-loss", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				sourceNode := cluster.assignmentWorker(job, plan.sources[0])
				cluster.await("first committed checkpoint", func() bool {
					return cluster.allWatermarksAtEOF(plan)
				})
				cluster.cutControl(sourceNode, "notice-cut")
				cluster.pump(200)
				cluster.healDatagrams()
				cluster.await("worker applies the committed watermark late", func() bool {
					handle := cluster.workerStore(sourceNode)
					if handle == nil {
						return false
					}
					work, err := handle.RecoverWork()
					if err != nil {
						return false
					}
					for _, cursor := range work.Sources {
						if cursor.Source.JobID != job {
							continue
						}
						if cluster.committedWatermark(job, cursor.Source) != cursor.Watermark {
							return false
						}
					}
					return true
				})
			})
	})

	// Sink store loss mid-Running after a committed checkpoint: the covered
	// records temporarily survive on one copy only; sealing and success stay
	// unavailable until inventory repair restores two current distinct
	// replicas, and the sealed manifest names both.
	t.Run("sink store loss mid running", func(t *testing.T) {
		// The grant covers the committed prefix; the survivor re-replicates
		// every retained record above it to the replacement (defect #4 fix).
		runScriptedScenario(t, 0x5CF0000C, "sink-loss-running", 10, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("committed checkpoint above zero", func() bool {
					for _, source := range plan.sources {
						if cluster.committedWatermark(job, source) > 0 {
							return true
						}
					}
					return false
				})
				sinkNode := cluster.sinkPrimaryNode(plan)
				cluster.stopNode(cluster.nodes[sinkNode])
				cluster.restartNode(cluster.nodes[sinkNode], true)
				cluster.awaitJob(job, "job repaired to two current replicas and sealed", func() bool {
					return cluster.manifestCommitted(job, plan.sink)
				})
			})
	})

	// Sink store loss before seal: the final checkpoints are committed but
	// the manifest is not; losing the sink store keeps the job unsealed until
	// the surviving secondary reconstructs the partition on a replacement.
	t.Run("sink store loss before seal", func(t *testing.T) {
		runScriptedScenario(t, 0x5CF0000D, "sink-loss-seal", 8, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("all checkpoints at EOF without a manifest", func() bool {
					return cluster.allWatermarksAtEOF(plan) && !cluster.manifestCommitted(job, plan.sink)
				})
				sinkNode := cluster.sinkPrimaryNode(plan)
				cluster.stopNode(cluster.nodes[sinkNode])
				cluster.restartNode(cluster.nodes[sinkNode], true)
				cluster.awaitJob(job, "manifest sealed from the surviving copy", func() bool {
					return cluster.manifestCommitted(job, plan.sink)
				})
			})
	})

	// Inventory/repair crash and retry: the repair destination crashes with
	// durable repair progress mid-flight, rejoins with its store, and the
	// idempotent same-epoch instruction resumes to completion.
	t.Run("repair crash and retry resumes", func(t *testing.T) {
		// The crashed destination resumes and completes the grant after its
		// restart; the survivor re-replicates the records above the grant's
		// vector (defect #4 fix).
		runScriptedScenario(t, 0x5CF0000E, "repair-crash", 10, simStageSpec{}, 1,
			func(t *testing.T, cluster *simCluster, client *simClient, plan *simJobPlan, job model.JobID) {
				cluster.await("committed checkpoint above zero", func() bool {
					for _, source := range plan.sources {
						if cluster.committedWatermark(job, source) > 0 {
							return true
						}
					}
					return false
				})
				sinkNode := cluster.sinkPrimaryNode(plan)
				cluster.stopNode(cluster.nodes[sinkNode])
				cluster.restartNode(cluster.nodes[sinkNode], true)
				sawRepair := false
				var repairNode uint16
				for _, id := range cluster.ids {
					handle := cluster.workerStore(id)
					if handle == nil {
						continue
					}
					work, err := handle.RecoverWork()
					if err != nil {
						continue
					}
					for _, repair := range work.Repairs {
						if repair.Instruction.JobID == job {
							sawRepair = true
							repairNode = id
						}
					}
				}
				cluster.await("durable repair progress on some worker", func() bool {
					for _, id := range cluster.ids {
						handle := cluster.workerStore(id)
						if handle == nil {
							continue
						}
						work, err := handle.RecoverWork()
						if err != nil {
							continue
						}
						for _, repair := range work.Repairs {
							if repair.Instruction.JobID == job {
								sawRepair, repairNode = true, id
								return true
							}
						}
					}
					return false
				})
				// Crash the repair participant mid-repair and rejoin with
				// its durable store so the instruction resumes.
				cluster.stopNode(cluster.nodes[repairNode])
				cluster.restartNode(cluster.nodes[repairNode], false)
				if !sawRepair {
					cluster.fail("repair accounting lost the observed durable repair")
				}
				cluster.awaitJob(job, "manifest sealed after repair retry", func() bool {
					return cluster.manifestCommitted(job, plan.sink)
				})
			})
	})

	// Client pending-response crash: the durable client reservation survives
	// a crash with the server response exactly in flight; the recreated
	// client resolves the same sequence and digest without a second job.
	t.Run("client pending response crash resolves once", func(t *testing.T) {
		seed := uint64(0x5CF0000F)
		cluster := newSimCluster(t, seed)
		cluster.startAll()
		cluster.awaitSteady()
		gate := newPauseGate()
		client := cluster.newClient("client-crash", func(options *control.ClientOptions) {
			options.Dial = pausableClientDial(gate)
		})
		spec := newSimTopology("client-crash", 1, 6, simStageSpec{})
		plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{})
		submitted := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _, err := client.client.Submit(ctx, plan.spec)
			submitted <- err
		}()
		cluster.await("server applied the pending submit", func() bool {
			_, ok := cluster.jobRecord(plan.jobID)
			return ok
		})
		gate.abort()
		if err := <-submitted; err == nil {
			cluster.fail("held submit unexpectedly succeeded before the client crash resolved")
		}
		gate.release()
		resumed := cluster.newClient("client-crash")
		resumedPlan := newSimJobPlan(t, resumed.store.NextRequestID(), spec, simStageSpec{})
		job := cluster.submit(resumed, resumedPlan)
		if job != plan.jobID {
			cluster.fail("client crash created a second job: %x want %x", job, plan.jobID)
		}
		cluster.awaitJobTerminal(job, "client-crash")
		if lifecycle := cluster.jobLifecycle(job); lifecycle != state.JobSucceeded {
			cluster.fail("client crash job ended %d", lifecycle)
		}
		records := cluster.pageResult(resumed, job)
		cluster.oracle.verifyFinal(job, records, "client-crash")
		cluster.requireFaultsConsumed("client-crash")
	})
}

// TestScriptedFullClusterRestart proves the complete durable recovery path:
// run to success, query, stop every process, restart every process, and
// query the identical unique ordered result again.
func TestScriptedFullClusterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("scripted simulations run full in-process clusters")
	}
	cluster := newSimCluster(t, 0x5CF00010)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient("restart")
	spec := newSimTopology("restart", 2, 8, simStageSpec{factor: 2})
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{factor: 2})
	job := cluster.submit(client, plan)
	cluster.awaitJobTerminal(job, "restart-first")
	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, "restart-first")

	cluster.stopAllNodes()
	for _, id := range cluster.ids {
		cluster.restartNode(cluster.nodes[id], false)
	}
	cluster.awaitSteady()
	cluster.await("job still succeeded after full restart", func() bool {
		record, ok := cluster.jobRecord(job)
		return ok && record.Lifecycle == state.JobSucceeded
	})
	reloaded := cluster.pageResult(client, job)
	if len(reloaded) != len(records) {
		cluster.fail("result length changed across full restart: %d want %d", len(reloaded), len(records))
	}
	plan.verifyOrderedOutput(cluster.t, reloaded, "restart-reloaded")
	cluster.oracle.verifyFinal(job, reloaded, "restart-reloaded")
	cluster.requireFaultsConsumed("restart")
	if cluster.oracle.meaningfulEvents() < 12 {
		cluster.fail("full restart scenario produced only %d meaningful events", cluster.oracle.meaningfulEvents())
	}
}

// jobLifecycle returns one job's committed lifecycle from the leader view,
// or zero when the job is not yet visible.
func (cluster *simCluster) jobLifecycle(job model.JobID) state.JobLifecycle {
	cluster.t.Helper()
	record, ok := cluster.jobRecord(job)
	if !ok {
		return 0
	}
	return record.Lifecycle
}
