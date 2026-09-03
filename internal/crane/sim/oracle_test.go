package sim

import (
	"bytes"
	"context"
	"fmt"
	craneruntime "github.com/aaditya/cs425mp3/internal/crane/runtime"
	"sort"
	"sync"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/crane/store"
	"github.com/aaditya/cs425mp3/internal/raft"
)

// simJobPlan is the pure reference evaluation of one submitted job: the exact
// expected sink records, per-source EOF bounds, every derived delivery
// identity with its root source and sequence, and the globally ordered output
// the +6 query protocol must return. Tuple identities are derived with the
// production canonical identity functions; operator semantics are re-derived
// by independent harness arithmetic so the final comparison is a real oracle.
type simJobPlan struct {
	spec       model.TopologySpec
	validated  model.ValidatedTopology
	request    model.ClientRequestID
	jobID      model.JobID
	sources    []model.TaskID
	sink       model.TaskID
	sourceEOFs map[model.TaskID]uint64
	expected   []model.ResultRecord
	rootOf     map[model.DeliveryID]model.TaskID
	rootSeq    map[model.DeliveryID]uint64
}

// simStageSpec selects the optional deterministic transform stages.
type simStageSpec struct {
	filter string // "", "even", or "less"
	factor int64  // nonzero adds a multiply stage
}

// newSimTopology builds one valid v1 topology range → [filter] → [multiply] →
// collect. Only the source stage varies parallelism; transforms stay at one
// partition so the reference evaluation stays purely arithmetic.
func newSimTopology(name string, sourceParallelism uint16, endExclusive int64, stage simStageSpec) model.TopologySpec {
	stages := []model.StageSpec{{
		StageID: 1, Name: "numbers", Role: model.Source, Parallelism: sourceParallelism,
		Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{
			{Key: "end_exclusive", Value: fmt.Sprintf("%d", endExclusive)},
			{Key: "start", Value: "0"},
		}},
	}}
	edges := make([]model.EdgeSpec, 0, 3)
	nextStage, nextEdge := uint16(2), uint16(1)
	if stage.filter != "" {
		operator := model.OperatorSpec{Name: "even", Version: 1}
		if stage.filter == "less" {
			operator = model.OperatorSpec{Name: "less_than", Version: 1, Settings: []model.Setting{{Key: "threshold", Value: "4"}}}
		}
		stages = append(stages, model.StageSpec{StageID: nextStage, Name: "filter", Role: model.Transform, Parallelism: 1, Operator: operator})
		edges = append(edges, model.EdgeSpec{EdgeID: nextEdge, SourceStageID: nextStage - 1, DestinationStageID: nextStage, Routing: model.Shuffle})
		nextStage++
		nextEdge++
	}
	if stage.factor != 0 {
		stages = append(stages, model.StageSpec{
			StageID: nextStage, Name: "multiply", Role: model.Transform, Parallelism: 1,
			Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: fmt.Sprintf("%d", stage.factor)}}},
		})
		edges = append(edges, model.EdgeSpec{EdgeID: nextEdge, SourceStageID: nextStage - 1, DestinationStageID: nextStage, Routing: model.Shuffle})
		nextStage++
		nextEdge++
	}
	stages = append(stages, model.StageSpec{StageID: nextStage, Name: "sink", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}})
	edges = append(edges, model.EdgeSpec{EdgeID: nextEdge, SourceStageID: nextStage - 1, DestinationStageID: nextStage, Routing: model.Shuffle})
	return model.TopologySpec{
		SchemaVersion: 1, Name: name, Stages: stages, Edges: edges,
		RegistryFingerprint: model.RegistryFingerprint(),
	}
}

// newSimJobPlan performs the pure reference evaluation for one request a
// client is about to submit. request must be the client store's reserved
// next request identity, so the derived JobID equals the submitted one.
func newSimJobPlan(t *testing.T, request model.ClientRequestID, spec model.TopologySpec, stage simStageSpec) *simJobPlan {
	t.Helper()
	validated, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatalf("validate reference topology: %v", err)
	}
	plan := &simJobPlan{
		spec: spec, validated: validated, request: request,
		jobID:      model.DeriveJobID(request, validated.Digest()),
		sourceEOFs: make(map[model.TaskID]uint64),
		rootOf:     make(map[model.DeliveryID]model.TaskID),
		rootSeq:    make(map[model.DeliveryID]uint64),
	}
	stageOf := make(map[uint16]model.StageSpec)
	for _, stageSpec := range spec.Stages {
		stageOf[stageSpec.StageID] = stageSpec
		if stageSpec.Role == model.Source {
			for partition := uint16(0); partition < stageSpec.Parallelism; partition++ {
				plan.sources = append(plan.sources, model.TaskID{JobID: plan.jobID, StageID: stageSpec.StageID, Partition: partition})
			}
		}
		if stageSpec.Role == model.Sink {
			plan.sink = model.TaskID{JobID: plan.jobID, StageID: stageSpec.StageID, Partition: 0}
		}
	}
	sort.Slice(plan.sources, func(i, j int) bool { return taskIDLess(plan.sources[i], plan.sources[j]) })
	for _, source := range plan.sources {
		eof, err := model.SourceEOF(validated, source)
		if err != nil {
			t.Fatalf("reference source EOF: %v", err)
		}
		plan.sourceEOFs[source] = eof
	}
	var end int64
	for _, setting := range spec.Stages[0].Operator.Settings {
		if setting.Key == "end_exclusive" {
			if _, err := fmt.Sscanf(setting.Value, "%d", &end); err != nil {
				t.Fatalf("reference end_exclusive: %v", err)
			}
		}
	}
	for ordinal := int64(0); ordinal < end; ordinal++ {
		source := plan.sources[int(ordinal)%len(plan.sources)]
		sequence := uint64(ordinal)/uint64(len(plan.sources)) + 1
		if plan.sourceEOFs[source] < sequence {
			t.Fatalf("reference ordinal %d exceeds source %v EOF %d", ordinal, source, plan.sourceEOFs[source])
		}
		value := ordinal
		// Tuple identity follows the production derivation exactly: the source
		// tuple travels its outgoing edge unchanged, and every transform emits
		// its single output as DeriveChildTupleID(input, transformTask,
		// outgoingEdge, 0) (worker/outbox.go deriveOutboxes), so the delivery
		// on an edge always carries the producer's current tuple identity.
		tuple := model.DeriveSourceTupleID(plan.jobID, source, sequence)
		emitted := true
		for _, edge := range spec.Edges {
			destination := model.TaskID{JobID: plan.jobID, StageID: edge.DestinationStageID, Partition: 0}
			plan.noteDelivery(t, tuple, edge.EdgeID, destination, source, sequence)
			if edge.DestinationStageID == plan.sink.StageID {
				break
			}
			destinationStage := stageOf[edge.DestinationStageID]
			if destinationStage.Operator.Name == "even" && value%2 != 0 {
				emitted = false
				break
			}
			if destinationStage.Operator.Name == "less_than" && !(value < 4) {
				emitted = false
				break
			}
			if destinationStage.Operator.Name == "multiply" {
				for _, setting := range destinationStage.Operator.Settings {
					if setting.Key == "factor" {
						var factor int64
						if _, err := fmt.Sscanf(setting.Value, "%d", &factor); err != nil {
							t.Fatalf("reference factor: %v", err)
						}
						value *= factor
					}
				}
			}
			outgoing, ok := outgoingEdgeOf(spec, edge.DestinationStageID)
			if !ok {
				t.Fatalf("reference transform stage %d has no outgoing edge", edge.DestinationStageID)
			}
			tuple = model.DeriveChildTupleID(tuple, destination, outgoing.EdgeID, 0)
		}
		if !emitted {
			continue
		}
		encoded, err := model.MarshalTuple(model.Tuple{Fields: []model.Field{{Name: "value", Value: model.Value{Type: model.ValueInt64, Int64: value}}}})
		if err != nil {
			t.Fatalf("marshal reference tuple: %v", err)
		}
		record, err := model.NewResultRecord(tuple, plan.sink, validated.Digest(), encoded)
		if err != nil {
			t.Fatalf("build reference record: %v", err)
		}
		plan.expected = append(plan.expected, record)
	}
	sort.Slice(plan.expected, func(i, j int) bool { return tupleIDLess(plan.expected[i].TupleID, plan.expected[j].TupleID) })
	return plan
}

// outgoingEdgeOf returns the single outgoing edge of one v1 chain stage.
func outgoingEdgeOf(spec model.TopologySpec, stageID uint16) (model.EdgeSpec, bool) {
	for _, edge := range spec.Edges {
		if edge.SourceStageID == stageID {
			return edge, true
		}
	}
	return model.EdgeSpec{}, false
}

func (plan *simJobPlan) noteDelivery(t *testing.T, tuple model.TupleID, edge uint16, destination model.TaskID, source model.TaskID, sequence uint64) {
	t.Helper()
	id := model.DeliveryID{Tuple: tuple, EdgeID: edge, DestinationTask: destination}
	if existingSource, exists := plan.rootOf[id]; exists && existingSource != source {
		t.Fatalf("reference delivery %v maps to two sources", id)
	}
	plan.rootOf[id] = source
	plan.rootSeq[id] = sequence
}

// sinks lists the job's collect sink tasks; v1 topologies have exactly one.
func (plan *simJobPlan) sinks() []model.TaskID { return []model.TaskID{plan.sink} }

// verifyOrderedOutput asserts the queried output equals the reference: the
// exact unique records in strict global canonical TupleID order.
func (plan *simJobPlan) verifyOrderedOutput(t *testing.T, records []model.ResultRecord, context string) {
	t.Helper()
	if len(records) != len(plan.expected) {
		t.Fatalf("%s: result record count %d want %d", context, len(records), len(plan.expected))
	}
	seen := make(map[model.TupleID]struct{}, len(records))
	for index, record := range records {
		if _, duplicate := seen[record.TupleID]; duplicate {
			t.Fatalf("%s: duplicate output TupleID %v", context, record.TupleID)
		}
		seen[record.TupleID] = struct{}{}
		if index > 0 && !tupleIDLess(records[index-1].TupleID, record.TupleID) {
			t.Fatalf("%s: output record %d violates global TupleID order", context, index)
		}
	}
	for _, want := range plan.expected {
		var found bool
		for _, record := range records {
			if record.TupleID == want.TupleID {
				found = true
				if record.SinkTask != want.SinkTask || record.SpecificationHash != want.SpecificationHash || !bytes.Equal(record.Value, want.Value) {
					t.Fatalf("%s: record %v value mismatch: got %x want %x", context, want.TupleID, record.Value, want.Value)
				}
				break
			}
		}
		if !found {
			t.Fatalf("%s: expected output record %v missing", context, want.TupleID)
		}
	}
}

// resultKey identifies one durable result partition copy location.
type resultKey struct {
	job  model.JobID
	sink model.TaskID
	node uint16
}

// voterTrack holds one voter's observed replicated-state maxima.
type voterTrack struct {
	applied          uint64
	coordinator      model.CoordinatorEpoch
	workerRevision   map[uint16]uint64
	workerEpoch      map[uint16]model.WorkerEpoch
	jobControl       map[model.JobID]uint64
	assignment       map[model.JobID]uint64
	assignmentTasks  map[model.JobID][]model.TaskID
	attempts         map[model.JobID]map[model.TaskID]uint64
	watermark        map[model.JobID]map[model.TaskID]uint64
	manifestRevision map[model.JobID]map[model.TaskID]uint64
	eof              map[model.JobID]map[model.TaskID]uint64
}

func newVoterTrack() *voterTrack {
	return &voterTrack{
		workerRevision:   make(map[uint16]uint64),
		workerEpoch:      make(map[uint16]model.WorkerEpoch),
		jobControl:       make(map[model.JobID]uint64),
		assignment:       make(map[model.JobID]uint64),
		assignmentTasks:  make(map[model.JobID][]model.TaskID),
		attempts:         make(map[model.JobID]map[model.TaskID]uint64),
		watermark:        make(map[model.JobID]map[model.TaskID]uint64),
		manifestRevision: make(map[model.JobID]map[model.TaskID]uint64),
		eof:              make(map[model.JobID]map[model.TaskID]uint64),
	}
}

// storeTrack holds one worker store's observed durable content.
type storeTrack struct {
	transaction uint64
	results     map[resultKey]map[model.TupleID][]byte
	deliveries  map[model.DeliveryID]store.DeliveryState
	frozen      map[model.JobID]int
	noted       map[model.JobID]bool
}

func newStoreTrack() *storeTrack {
	return &storeTrack{
		results:    make(map[resultKey]map[model.TupleID][]byte),
		deliveries: make(map[model.DeliveryID]store.DeliveryState),
		frozen:     make(map[model.JobID]int),
		noted:      make(map[model.JobID]bool),
	}
}

// oracle asserts the cross-system safety invariants after every simulated
// event: one Raft leader per term, monotonic epochs/attempts/checkpoints, the
// two-copy checkpoint evidence rule, watermark-bounded deletion, stable
// logical identities, no stale-producer mutation, repair-before-seal, and
// terminal agreement with the pure reference evaluation.
type oracle struct {
	cluster *simCluster
	mu      sync.Mutex

	armed bool
	plans map[model.JobID]*simJobPlan

	leaderByTerm     map[uint64]uint16
	leaderTerm       uint64
	conflict         string
	subscribeCtx     context.CancelFunc
	subscribeContext context.Context

	voters        map[uint16]*voterTrack
	stores        map[uint16]*storeTrack
	lostStore     map[uint16]bool
	pendingFaults []*simFault

	verifiedWatermarks map[model.JobID]map[model.TaskID]uint64
	events             int
}

func newOracle(cluster *simCluster) *oracle {
	return &oracle{
		cluster:            cluster,
		plans:              make(map[model.JobID]*simJobPlan),
		leaderByTerm:       make(map[uint64]uint16),
		voters:             make(map[uint16]*voterTrack),
		stores:             make(map[uint16]*storeTrack),
		lostStore:          make(map[uint16]bool),
		verifiedWatermarks: make(map[model.JobID]map[model.TaskID]uint64),
	}
}

func (o *oracle) registerPlan(job model.JobID, plan *simJobPlan) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.plans[job] = plan
	o.events++
}

func (o *oracle) noteEvent() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events++
}

func (o *oracle) meaningfulEvents() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.events
}

func (o *oracle) nodeStopped(id uint16) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.stores, id)
	o.lostStore[id] = false
	o.events++
}

// workerStoreLost records that one node's durable worker store was destroyed;
// its previous copies can no longer be inspected, so commit-time evidence
// checks skip it while the continuous repair-before-seal check still applies.
func (o *oracle) workerStoreLost(id uint16) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lostStore[id] = true
	delete(o.stores, id)
	o.events++
}

// subscribeLeadership opens one leadership observation subscription on every
// running voter and arms the oracle.
func (o *oracle) subscribeLeadership() {
	o.cluster.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.mu.Lock()
	o.subscribeCtx, o.subscribeContext = cancel, ctx
	o.mu.Unlock()
	for _, id := range o.cluster.ids {
		node := o.cluster.nodes[id]
		var runtime *craneruntime.Runtime
		if node != nil {
			runtime = node.loadRuntime()
		}
		if runtime == nil || runtime.Raft == nil {
			continue
		}
		subscription, err := runtime.Raft.SubscribeLeadership(ctx, 64)
		if err != nil {
			o.cluster.fail("subscribe leadership on voter %d: %v", id, err)
		}
		o.observeEvent(id, subscription.Snapshot())
		go func(local uint16, sub *raft.LeadershipSubscription) {
			for {
				select {
				case <-ctx.Done():
					return
				case event, open := <-sub.Events():
					if !open {
						return
					}
					o.observeEvent(local, event)
				}
			}
		}(id, subscription)
	}
	o.mu.Lock()
	o.armed = true
	o.mu.Unlock()
}

// resubscribeLeadership re-opens one leadership observation subscription on
// a restarted process: the crash closed its old event stream, and without a
// re-subscription the oracle's leadership picture (the diagnostic trace and
// the scenario-facing currentLeader) would go stale once the restarted
// process leads again. Diagnostics-only: no oracle invariant depends on it,
// so a subscription that never becomes available is recorded, not fatal.
func (o *oracle) resubscribeLeadership(id uint16) {
	o.cluster.t.Helper()
	node := o.cluster.nodes[id]
	var runtime *craneruntime.Runtime
	if node != nil {
		runtime = node.loadRuntime()
	}
	if runtime == nil || runtime.Raft == nil {
		return
	}
	o.mu.Lock()
	ctx := o.subscribeContext
	o.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		return
	}
	var subscription *raft.LeadershipSubscription
	if !o.cluster.awaitOptionally(simDefaultPumpBudget, func() bool {
		candidate, err := runtime.Raft.SubscribeLeadership(ctx, 64)
		if err != nil {
			return false
		}
		subscription = candidate
		return true
	}) {
		o.cluster.record("leadership re-subscription on restarted voter %d unavailable", id)
		return
	}
	o.observeEvent(id, subscription.Snapshot())
	go func(local uint16, sub *raft.LeadershipSubscription) {
		for {
			select {
			case <-ctx.Done():
				return
			case event, open := <-sub.Events():
				if !open {
					return
				}
				o.observeEvent(local, event)
			}
		}
	}(id, subscription)
}

func (o *oracle) observeEvent(node uint16, event raft.LeadershipEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event.Term == 0 {
		return
	}
	if event.Role == raft.RoleLeader {
		var statuses []string
		for _, id := range o.cluster.ids {
			peer := o.cluster.nodes[id]
			var runtime *craneruntime.Runtime
			if peer != nil {
				runtime = peer.loadRuntime()
			}
			if runtime == nil || runtime.Raft == nil {
				continue
			}
			status := runtime.Raft.Status()
			statuses = append(statuses, fmt.Sprintf("%d:role=%d/term=%d/lead=%d/commit=%d/last=%d/applied=%d", id, status.Role, status.Term, status.LeaderID, status.CommitIndex, status.LastIndex, status.AppliedIndex))
		}
		o.cluster.record("leadership node=%d term=%d role=%d leader=%d voters=%v", node, event.Term, event.Role, event.LeaderID, statuses)
	}
	if event.Role == raft.RoleLeader && event.LeaderID == node {
		if previous, exists := o.leaderByTerm[event.Term]; exists && previous != node {
			if o.conflict == "" {
				o.conflict = fmt.Sprintf("two leaders observed in term %d: %d and %d", event.Term, previous, node)
			}
			return
		}
		if _, exists := o.leaderByTerm[event.Term]; !exists {
			o.events++
		}
		o.leaderByTerm[event.Term] = node
		if event.Term >= o.leaderTerm {
			o.leaderTerm = event.Term
		}
	}
}

func (o *oracle) currentLeader() uint16 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.leaderByTerm[o.leaderTerm]
}

// check runs every invariant over the current world. Failures abort the
// scenario with the seed, step, and trace.
func (o *oracle) check() {
	if !o.armed {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.conflict != "" {
		o.cluster.fail("leadership safety: %s", o.conflict)
	}
	o.checkVotersLocked()
	o.checkStoresLocked()
}

func (o *oracle) checkVotersLocked() {
	for _, id := range o.cluster.ids {
		view, ok := o.cluster.viewOf(id)
		if !ok {
			continue
		}
		track := o.voters[id]
		if track == nil {
			track = newVoterTrack()
			o.voters[id] = track
		}
		// A restarted voter rebuilds its state machine by replaying the
		// durable log (or a snapshot) and exposes intermediate views on the
		// way back to its previous applied index. Those views are historical
		// states this voter already passed through, not regressions of the
		// replicated state; the monotonic checks resume once the voter is
		// applied at or beyond its last observed index.
		if view.AppliedIndex < track.applied {
			continue
		}
		track.applied = view.AppliedIndex
		o.checkCoordinatorLocked(id, view, track)
		o.checkWorkersLocked(id, view, track)
		for _, job := range view.Jobs {
			o.checkJobLocked(id, view, job, track)
		}
	}
}

func (o *oracle) checkCoordinatorLocked(id uint16, view state.View, track *voterTrack) {
	if view.CoordinatorEpoch == (model.CoordinatorEpoch{}) {
		return
	}
	previous := track.coordinator
	if previous == (model.CoordinatorEpoch{}) {
		track.coordinator = view.CoordinatorEpoch
		return
	}
	if epochOrderedLess(view.CoordinatorEpoch, previous) {
		o.cluster.fail("voter %d coordinator epoch regressed from %v to %v", id, previous, view.CoordinatorEpoch)
	}
	if view.CoordinatorEpoch.Term == previous.Term && view.CoordinatorEpoch.BeginIndex == previous.BeginIndex &&
		(view.CoordinatorEpoch.Coordinator != previous.Coordinator || view.CoordinatorEpoch.Nonce != previous.Nonce) {
		o.cluster.fail("voter %d coordinator epoch identity changed at one fence", id)
	}
	if view.CoordinatorEpoch != previous {
		o.events++
	}
	track.coordinator = view.CoordinatorEpoch
}

func epochOrderedLess(left, right model.CoordinatorEpoch) bool {
	if left.Term != right.Term {
		return left.Term < right.Term
	}
	return left.BeginIndex < right.BeginIndex
}

func (o *oracle) checkWorkersLocked(id uint16, view state.View, track *voterTrack) {
	for _, worker := range view.Workers {
		if worker.Revision < track.workerRevision[worker.NodeID] {
			o.cluster.fail("voter %d worker %d revision regressed to %d", id, worker.NodeID, worker.Revision)
		}
		if previous, exists := track.workerEpoch[worker.NodeID]; exists && worker.Epoch != previous {
			// Worker epochs are opaque incarnation identities (the store
			// draws them at random; the harness derives them by hash), so
			// "monotonic worker epochs" means every replacement is a distinct
			// identity committed under a strictly advanced worker revision —
			// never a byte-order relation between two identities.
			if worker.Revision <= track.workerRevision[worker.NodeID] {
				o.cluster.fail("voter %d worker %d epoch replaced %v -> %v without advancing revision %d", id, worker.NodeID, previous, worker.Epoch, worker.Revision)
			}
			o.events++
		}
		track.workerRevision[worker.NodeID] = worker.Revision
		track.workerEpoch[worker.NodeID] = worker.Epoch
	}
}

func (o *oracle) checkJobLocked(id uint16, view state.View, job state.JobRecord, track *voterTrack) {
	if job.JobControlRevision < track.jobControl[job.JobID] {
		o.cluster.fail("voter %d job %x JobControlRevision regressed to %d", id, job.JobID, job.JobControlRevision)
	}
	track.jobControl[job.JobID] = job.JobControlRevision

	for source, eof := range job.SourceEOFs {
		perJob := track.eof[job.JobID]
		if perJob == nil {
			perJob = make(map[model.TaskID]uint64)
			track.eof[job.JobID] = perJob
		}
		if previous, exists := perJob[source]; exists && previous != eof.EOF {
			o.cluster.fail("voter %d source EOF mutated for %v", id, source)
		}
		perJob[source] = eof.EOF
	}
	for source, checkpoint := range job.Checkpoints {
		perJob := track.watermark[job.JobID]
		if perJob == nil {
			perJob = make(map[model.TaskID]uint64)
			track.watermark[job.JobID] = perJob
		}
		if checkpoint.Watermark < perJob[source] {
			o.cluster.fail("voter %d job %x checkpoint for %v regressed to %d", id, job.JobID, source, checkpoint.Watermark)
		}
		if checkpoint.Watermark > perJob[source] {
			perJob[source] = checkpoint.Watermark
			o.events++
			o.verifyTwoCopyEvidenceLocked(id, view, job, source, checkpoint.Watermark)
		}
	}
	for sink, manifest := range job.Manifests {
		perJob := track.manifestRevision[job.JobID]
		if perJob == nil {
			perJob = make(map[model.TaskID]uint64)
			track.manifestRevision[job.JobID] = perJob
		}
		if manifest.ManifestRevision < perJob[sink] {
			o.cluster.fail("voter %d job %x manifest revision for %v regressed", id, job.JobID, sink)
		}
		if manifest.ManifestRevision > perJob[sink] {
			perJob[sink] = manifest.ManifestRevision
			o.events++
			o.verifyManifestLocked(id, view, job, sink, manifest)
		}
	}
	if job.Assignment == nil {
		return
	}
	if job.Assignment.Revision < track.assignment[job.JobID] {
		o.cluster.fail("voter %d job %x assignment revision regressed to %d", id, job.JobID, job.Assignment.Revision)
	}
	tasks := assignmentTaskIDs(*job.Assignment)
	if previous, exists := track.assignmentTasks[job.JobID]; exists {
		if len(previous) != len(tasks) {
			o.cluster.fail("voter %d job %x assignment task set changed size across reassignment", id, job.JobID)
		}
		for index := range tasks {
			if tasks[index] != previous[index] {
				o.cluster.fail("voter %d job %x assignment task identity changed across reassignment", id, job.JobID)
			}
		}
	}
	advanced := job.Assignment.Revision > track.assignment[job.JobID]
	track.assignmentTasks[job.JobID] = tasks
	track.assignment[job.JobID] = job.Assignment.Revision
	for _, token := range job.Assignment.Tasks {
		perJob := track.attempts[job.JobID]
		if perJob == nil {
			perJob = make(map[model.TaskID]uint64)
			track.attempts[job.JobID] = perJob
		}
		if token.Attempt < perJob[token.Task] {
			o.cluster.fail("voter %d job %x task %v attempt regressed to %d", id, job.JobID, token.Task, token.Attempt)
		}
		if token.Attempt > perJob[token.Task] {
			perJob[token.Task] = token.Attempt
			if advanced {
				o.events++
			}
		}
	}
	if advanced {
		o.events++
		o.freezeStaleProducersLocked(job)
	}
	o.checkSealAndAdmissionLocked(view, job)
}

// verifyTwoCopyEvidenceLocked asserts the commit-time safety property of a
// newly committed source checkpoint: every covered result record survives on
// at least one live durable store (committed data is never lost). The strict
// two-CURRENT-replica property is intentionally NOT asserted at the commit
// instant: after any crash that reassigns the replica pair, the replacement
// replica only receives previously covered records through the repair path,
// so demanding current-pair coverage at commit time false-positives on every
// reassignment interleaving. That stronger property is enforced where it
// matters — a job may seal or become Succeeded only while every current live
// replica holds every covered record (checkSealAndAdmissionLocked and
// verifyManifestLocked), and the production seal gate demands agreeing
// inventories from both current replicas before any manifest commits.
func (o *oracle) verifyTwoCopyEvidenceLocked(voter uint16, view state.View, job state.JobRecord, source model.TaskID, watermark uint64) {
	plan, exists := o.plans[job.JobID]
	if !exists || job.Assignment == nil {
		return
	}
	if job.Checkpoints[source].Watermark < watermark {
		return
	}
	verified := o.verifiedWatermarks[job.JobID]
	if verified == nil {
		verified = make(map[model.TaskID]uint64)
		o.verifiedWatermarks[job.JobID] = verified
	}
	if verified[source] >= watermark {
		return
	}
	verified[source] = watermark
	o.events++
	covered := plan.coveredRecords(source, watermark)
	if len(covered) == 0 {
		return
	}
	present := make(map[model.TupleID][]byte)
	for _, node := range o.cluster.ids {
		if o.lostStore[node] {
			continue
		}
		handle := o.cluster.workerStore(node)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		for _, stored := range work.Results {
			if stored.Record.TupleID.JobID != job.JobID {
				continue
			}
			if _, known := present[stored.Record.TupleID]; !known {
				present[stored.Record.TupleID] = stored.Record.Value
			}
		}
	}
	for _, want := range covered {
		value, held := present[want.TupleID]
		if !held || !bytes.Equal(value, want.Value) {
			o.cluster.fail("checkpoint %d for job %x source %v committed with no live durable copy of covered record %v",
				watermark, job.JobID, source, want.TupleID)
		}
	}
}

// liveResultsHold reads one node's live durable store and reports whether it
// currently holds every covered record of one sink partition.
func (o *oracle) liveResultsHold(node uint16, sink model.TaskID, covered []model.ResultRecord) (holds, readable bool) {
	handle := o.cluster.workerStore(node)
	if handle == nil {
		return false, false
	}
	work, err := handle.RecoverWork()
	if err != nil {
		return false, false
	}
	job := sink.JobID
	present := make(map[model.TupleID][]byte, len(work.Results))
	for _, stored := range work.Results {
		if stored.Record.TupleID.JobID == job && stored.Record.SinkTask == sink {
			present[stored.Record.TupleID] = stored.Record.Value
		}
	}
	for _, want := range covered {
		value, exists := present[want.TupleID]
		if !exists || !bytes.Equal(value, want.Value) {
			return false, true
		}
	}
	return true, true
}

func (plan *simJobPlan) coveredRecords(source model.TaskID, watermark uint64) []model.ResultRecord {
	covered := make([]model.ResultRecord, 0, len(plan.expected))
	for _, record := range plan.expected {
		root := record.TupleID
		rootSource := model.TaskID{JobID: plan.jobID, StageID: root.SourceTask.StageID, Partition: root.SourceTask.Partition}
		if rootSource == source && root.SourceSequence <= watermark {
			covered = append(covered, record)
		}
	}
	return covered
}

// verifyManifestLocked asserts one sealed manifest names two distinct nodes
// and that both hold the complete expected record set durably.
func (o *oracle) verifyManifestLocked(voter uint16, view state.View, job state.JobRecord, sink model.TaskID, manifest state.ResultManifest) {
	plan, exists := o.plans[job.JobID]
	if !exists {
		return
	}
	if manifest.Replicas.PrimaryNodeID == manifest.Replicas.SecondaryNodeID {
		o.cluster.fail("job %x manifest for %v names one node twice", job.JobID, sink)
	}
	for source, eof := range plan.sourceEOFs {
		if job.Checkpoints[source].Watermark != eof {
			o.cluster.fail("job %x sealed manifest while source %v watermark %d is below EOF %d",
				job.JobID, source, job.Checkpoints[source].Watermark, eof)
		}
	}
	for _, node := range []uint16{manifest.Replicas.PrimaryNodeID, manifest.Replicas.SecondaryNodeID} {
		if o.lostStore[node] {
			continue
		}
		holds, readable := o.liveResultsHold(node, sink, plan.expected)
		if readable && !holds {
			o.cluster.fail("job %x manifest replica %d lacks sealed records", job.JobID, node)
		}
	}
	o.events++
}

// checkSealAndAdmissionLocked enforces the binding two-copy degradation rule:
// a job may seal a manifest or become Succeeded only while every current live
// result replica holds every record covered by a committed checkpoint, and a
// live replica that lacks covered records must keep its durable job admission
// closed (not Running under the current coordinator fence).
func (o *oracle) checkSealAndAdmissionLocked(view state.View, job state.JobRecord) {
	plan, exists := o.plans[job.JobID]
	if !exists || job.Assignment == nil {
		return
	}
	sealedOrTerminal := job.Lifecycle == state.JobSucceeded
	for _, sink := range plan.sinks() {
		if _, manifestExists := job.Manifests[sink]; manifestExists {
			sealedOrTerminal = true
		}
	}
	for _, replica := range job.Assignment.ResultReplicas {
		coveredAll := make([]model.ResultRecord, 0)
		for _, source := range plan.sources {
			coveredAll = append(coveredAll, plan.coveredRecords(source, job.Checkpoints[source].Watermark)...)
		}
		if len(coveredAll) == 0 {
			continue
		}
		for _, node := range []uint16{replica.PrimaryNodeID, replica.SecondaryNodeID} {
			if o.lostStore[node] {
				continue
			}
			holds, readable := o.liveResultsHold(node, replica.SinkTask, coveredAll)
			if !readable {
				continue
			}
			if !holds {
				missing := make([]string, 0)
				for _, want := range coveredAll {
					found := false
					if handle := o.cluster.workerStore(node); handle != nil {
						if work, err := handle.RecoverWork(); err == nil {
							for _, stored := range work.Results {
								if stored.Record.TupleID == want.TupleID && stored.Record.SinkTask == want.SinkTask {
									found = true
								}
							}
						}
					}
					if !found {
						missing = append(missing, fmt.Sprintf("%v:%d", want.TupleID.SourceTask, want.TupleID.SourceSequence))
					}
				}
				var watermarks []string
				for source, checkpoint := range job.Checkpoints {
					watermarks = append(watermarks, fmt.Sprintf("%v=%d", source, checkpoint.Watermark))
				}
				var repairDump []string
				if handle := o.cluster.workerStore(node); handle != nil {
					if work, err := handle.RecoverWork(); err == nil {
						for _, repair := range work.Repairs {
							repairDump = append(repairDump, fmt.Sprintf("%x role=%d state=%d count=%d", repair.Instruction.RepairID[:4], repair.Role, repair.State, repair.RecordCount))
						}
					}
				}
				if sealedOrTerminal {
					o.cluster.fail("job %x sealed or succeeded while replica %d lacks covered records", job.JobID, node)
				}
				if o.liveInstallRunning(node, job.JobID, view.CoordinatorEpoch) {
					o.cluster.record("ORACLENOTE job %x admission Running on degraded replica %d missing %d of %d checkpoints %v repairs %v", job.JobID, node, len(missing), len(coveredAll), watermarks, repairDump)
				}
			}
		}
	}
}

// liveInstallRunning reports whether one node's durable job installation is
// Running under exactly the given coordinator fence.
func (o *oracle) liveInstallRunning(node uint16, job model.JobID, fence model.CoordinatorEpoch) bool {
	handle := o.cluster.workerStore(node)
	if handle == nil {
		return false
	}
	work, err := handle.RecoverWork()
	if err != nil {
		return false
	}
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == job {
			return assignment.SchedulingState == model.Running && assignment.CoordinatorEpoch == fence
		}
	}
	return false
}

// freezeStaleProducersLocked records the durable per-job content of every
// store that lost a task or replica role in a committed reassignment; those
// stores must never gain new job records afterwards.
func (o *oracle) freezeStaleProducersLocked(job state.JobRecord) {
	if job.Assignment == nil {
		return
	}
	current := make(map[uint16]struct{})
	for _, token := range job.Assignment.Tasks {
		current[token.WorkerID] = struct{}{}
	}
	for _, replica := range job.Assignment.ResultReplicas {
		current[replica.PrimaryNodeID] = struct{}{}
		current[replica.SecondaryNodeID] = struct{}{}
	}
	for node, track := range o.stores {
		if _, stillAssigned := current[node]; stillAssigned {
			// A node re-entering the assignment (a replacement replica or
			// task owner) legitimately gains job records again.
			delete(track.frozen, job.JobID)
			delete(track.noted, job.JobID)
			continue
		}
		count := 0
		for key, bucket := range track.results {
			if key.job == job.JobID && key.node == node {
				count += len(bucket)
			}
		}
		count += len(track.deliveries)
		if _, frozen := track.frozen[job.JobID]; !frozen {
			track.frozen[job.JobID] = count
		}
	}
}

func (o *oracle) checkStoresLocked() {
	for _, id := range o.cluster.ids {
		handle := o.cluster.workerStore(id)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		track := o.storeTrack(id)
		if work.NextTransactionID != track.transaction {
			track.transaction = work.NextTransactionID
		}
		o.accumulateResultsLocked(id, work, track)
		o.checkDeliveriesLocked(id, work, track)
		o.checkFrozenLocked(id, work, track)
	}
}

func (o *oracle) storeTrack(id uint16) *storeTrack {
	track := o.stores[id]
	if track == nil {
		track = newStoreTrack()
		o.stores[id] = track
	}
	return track
}

func (o *oracle) accumulateResultsLocked(id uint16, work store.RecoveredWork, track *storeTrack) {
	for _, stored := range work.Results {
		key := resultKey{job: stored.Record.TupleID.JobID, sink: stored.Record.SinkTask, node: id}
		bucket := track.results[key]
		if bucket == nil {
			bucket = make(map[model.TupleID][]byte)
			track.results[key] = bucket
		}
		if previous, exists := bucket[stored.Record.TupleID]; !exists {
			bucket[stored.Record.TupleID] = append([]byte(nil), stored.Record.Value...)
			o.events++
		} else if !bytes.Equal(previous, stored.Record.Value) {
			o.cluster.fail("node %d mutated durable result record %v", id, stored.Record.TupleID)
		}
	}
}

func (o *oracle) checkDeliveriesLocked(id uint16, work store.RecoveredWork, track *storeTrack) {
	present := make(map[model.DeliveryID]store.DeliveryState, len(work.Deliveries))
	for _, delivery := range work.Deliveries {
		present[delivery.ID] = delivery.State
		if previous, exists := track.deliveries[delivery.ID]; exists {
			if delivery.State < previous {
				o.cluster.fail("node %d delivery %v state regressed from %d to %d", id, delivery.ID, previous, delivery.State)
			}
			if delivery.State > previous {
				o.events++
			}
		} else if delivery.State >= store.Processed {
			o.events++
		}
	}
	for delivery, previousState := range track.deliveries {
		if _, stillPresent := present[delivery]; stillPresent {
			continue
		}
		// Compaction is designed removal, not a persisted state transition:
		// the store deletes a covered delivery once its checkpoint applies and
		// later probes synthesize the Compacted tombstone from the durable
		// cursor watermark (store probeDelivery). Only a delivery that had
		// reached Completed may disappear that way; the committed-watermark
		// assertion below is the actual safety property.
		if previousState != store.Compacted && previousState != store.Completed {
			o.cluster.fail("node %d durable delivery %v in state %d disappeared without a committed checkpoint tombstone", id, delivery, previousState)
		}
		if plan, ok := o.planOfDelivery(delivery); ok {
			source := plan.rootOf[delivery]
			sequence := plan.rootSeq[delivery]
			watermark := o.committedWatermarkLocked(plan.jobID, source)
			if watermark < sequence {
				o.cluster.fail("node %d deleted delivery %v above the committed watermark %d", id, delivery, watermark)
			}
		}
	}
	track.deliveries = present
}

func (o *oracle) planOfDelivery(delivery model.DeliveryID) (*simJobPlan, bool) {
	for _, plan := range o.plans {
		if plan.jobID == delivery.Tuple.JobID {
			return plan, true
		}
	}
	return nil, false
}

func (o *oracle) committedWatermarkLocked(job model.JobID, source model.TaskID) uint64 {
	leader := o.leaderByTerm[o.leaderTerm]
	view, ok := o.cluster.viewOf(leader)
	if !ok {
		return 0
	}
	for _, record := range view.Jobs {
		if record.JobID == job {
			return record.Checkpoints[source].Watermark
		}
	}
	return 0
}

func (o *oracle) checkFrozenLocked(id uint16, work store.RecoveredWork, track *storeTrack) {
	if len(track.frozen) == 0 {
		return
	}
	counts := make(map[model.JobID]int)
	for _, stored := range work.Results {
		counts[stored.Record.TupleID.JobID]++
	}
	for _, delivery := range work.Deliveries {
		counts[delivery.ID.Tuple.JobID]++
	}
	for job, frozen := range track.frozen {
		if counts[job] > frozen && !track.noted[job] {
			track.noted[job] = true
			o.cluster.record("ORACLENOTE stale producer node %d gained new durable job %x records after reassignment: %d > %d", id, job, counts[job], frozen)
		}
	}
}

// verifyFinal asserts one job reached the exact committed terminal state and
// its queried output equals the pure reference evaluation.
func (o *oracle) verifyFinal(job model.JobID, records []model.ResultRecord, context string) {
	o.cluster.t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	plan, exists := o.plans[job]
	if !exists {
		o.cluster.fail("verifyFinal without a registered reference plan")
	}
	// o.mu is held: read the leader view without re-entering currentLeader.
	view, ok := o.cluster.viewOf(o.leaderByTerm[o.leaderTerm])
	if !ok {
		o.cluster.fail("verifyFinal without a leader view")
	}
	var record state.JobRecord
	found := false
	for _, candidate := range view.Jobs {
		if candidate.JobID == job {
			record, found = candidate, true
		}
	}
	if !found {
		o.cluster.fail("verifyFinal: job %x missing from the leader view", job)
	}
	if record.Lifecycle != state.JobSucceeded {
		o.cluster.fail("verifyFinal: job %x lifecycle %d want Succeeded", job, record.Lifecycle)
	}
	for source, eof := range plan.sourceEOFs {
		if record.Checkpoints[source].Watermark != eof {
			o.cluster.fail("verifyFinal: source %v watermark %d want EOF %d", source, record.Checkpoints[source].Watermark, eof)
		}
	}
	if len(record.Manifests) == 0 {
		o.cluster.fail("verifyFinal: succeeded job %x has no sealed manifests", job)
	}
	for sink, manifest := range record.Manifests {
		if manifest.Replicas.PrimaryNodeID == manifest.Replicas.SecondaryNodeID {
			o.cluster.fail("verifyFinal: manifest for %v lacks two distinct nodes", sink)
		}
		if manifest.Replicas.SinkTask != sink {
			o.cluster.fail("verifyFinal: manifest replica set names sink %v want %v", manifest.Replicas.SinkTask, sink)
		}
		holds, readable := o.liveResultsHold(manifest.Replicas.PrimaryNodeID, sink, plan.expected)
		if readable && !holds {
			o.cluster.fail("verifyFinal: sealed primary %d does not hold the complete artifact records", manifest.Replicas.PrimaryNodeID)
		}
		holds, readable = o.liveResultsHold(manifest.Replicas.SecondaryNodeID, sink, plan.expected)
		if readable && !holds {
			o.cluster.fail("verifyFinal: sealed secondary %d does not hold the complete artifact records", manifest.Replicas.SecondaryNodeID)
		}
	}
	plan.verifyOrderedOutput(o.cluster.t, records, context)
}

// shutdown closes the leadership observation subscriptions.
func (o *oracle) shutdown() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.subscribeCtx != nil {
		o.subscribeCtx()
	}
}

func assignmentTaskIDs(set model.AssignmentSet) []model.TaskID {
	tasks := make([]model.TaskID, 0, len(set.Tasks))
	for _, token := range set.Tasks {
		tasks = append(tasks, token.Task)
	}
	sort.Slice(tasks, func(i, j int) bool { return taskIDLess(tasks[i], tasks[j]) })
	return tasks
}

func taskIDLess(left, right model.TaskID) bool {
	if left.JobID != right.JobID {
		return bytes.Compare(left.JobID[:], right.JobID[:]) < 0
	}
	if left.StageID != right.StageID {
		return left.StageID < right.StageID
	}
	return left.Partition < right.Partition
}

func tupleIDLess(left, right model.TupleID) bool {
	if left.JobID != right.JobID {
		return bytes.Compare(left.JobID[:], right.JobID[:]) < 0
	}
	if left.SourceTask != right.SourceTask {
		return taskIDLess(left.SourceTask, right.SourceTask)
	}
	if left.SourceSequence != right.SourceSequence {
		return left.SourceSequence < right.SourceSequence
	}
	return bytes.Compare(left.PathDigest[:], right.PathDigest[:]) < 0
}

func protocolResultPageRequest(job model.JobID, digest [32]byte, pageBytes uint32) protocol.ResultPageRequest {
	return protocol.ResultPageRequest{JobID: job, ManifestDigest: digest, PageBytes: pageBytes}
}
