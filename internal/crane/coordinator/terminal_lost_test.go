package coordinator

import (
	"fmt"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/swim"
)

// closedInstallsFor counts one job's recorded terminal Closed installs,
// optionally restricted to one assignment revision.
func closedInstallsFor(h *harness, job model.JobID, revision uint64) int {
	h.workers.mu.Lock()
	defer h.workers.mu.Unlock()
	total := 0
	for _, recorded := range h.workers.installs {
		if recorded.install.Assignment.JobID == job && recorded.install.SchedulingState == model.Closed &&
			(revision == 0 || recorded.install.Assignment.Revision == revision) {
			total++
		}
	}
	return total
}

func assignmentMustExist(t *testing.T, h *harness, job model.JobID) *model.AssignmentSet {
	t.Helper()
	record, ok := h.job(job)
	if !ok || record.Assignment == nil {
		t.Fatalf("job %x has no assignment", job[:4])
	}
	return record.Assignment
}

// deposedHolderHarness drives one Succeeded job whose sealed artifact ends up
// stranded on a deposed-but-live holder: the committed secondary is first
// drained (excluding it from every future placement) and then epoch-replaced
// store-preserving — invalidating its incarnation so the placement moves onto
// fresh workers and re-seals from the surviving copy — while the deposed
// holder's sealed copy survives untouched. The returned reload function
// destroys every artifact on the then-current placement under new worker
// epochs, the precondition for the retained-holder scan.
func deposedHolderHarness(t *testing.T) (*harness, model.JobID, model.TaskID, protocol.ResultArtifact, uint16, func()) {
	t.Helper()
	h, job, topology, assignment := terminalHarness(t, 1)
	h.seedWorker(4, model.WorkerEpoch{4}, 8)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 8)
	h.seedWorker(5, model.WorkerEpoch{5}, 8)
	h.addWorkerMember(5, model.WorkerEpoch{5}, 8)
	// Fresh spare workers carry empty durable result stores, exactly like
	// registered workers that never held this job's records.
	_ = h.nodeResults(4)
	_ = h.nodeResults(5)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	h.start()
	h.markReady()
	h.lead(2)
	succeeded := waitForSucceeded(t, h, job)
	committed := succeeded.Manifests[replica.SinkTask]
	artifact := protocol.ResultArtifact{
		JobID: committed.JobID, SinkTask: committed.SinkTask, SpecificationHash: committed.SpecificationHash,
		RecordCount: committed.RecordCount, TotalLength: committed.TotalBytes, Checksum: committed.Checksum,
	}

	// Drain the committed secondary first: the worker leaves eligibility —
	// no later placement may ever pick it — while staying live.
	deposed := committed.Replicas.SecondaryNodeID
	view := h.machine.View()
	workerRecord, ok := h.workerRecord(deposed)
	if !ok {
		t.Fatalf("deposed worker %d record missing", deposed)
	}
	drain, err := state.NewDrainWorker(testCommandID("seed-drain", []byte{byte(deposed)}), workerRecord.Revision, deposed, workerRecord.Epoch, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed drain: %v", err)
	}
	h.raft.applySeed(t, drain)

	// Then replace its epoch store-preserving: the invalidated incarnation
	// moves the placement onto fresh workers while the sealed copy stays.
	view = h.machine.View()
	workerRecord, _ = h.workerRecord(deposed)
	reincarnation := state.WorkerRecord{
		NodeID: workerRecord.NodeID, Epoch: model.WorkerEpoch{byte(workerRecord.NodeID), 0x11}, State: workerRecord.State, Revision: workerRecord.Revision + 1, Slots: workerRecord.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-depose", []byte{byte(deposed)}), workerRecord.Revision, workerRecord.NodeID, workerRecord.Epoch, reincarnation, affectedForWorker(view, workerRecord.NodeID, workerRecord.Epoch), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed deposing replace: %v", err)
	}
	h.raft.applySeed(t, replace)
	h.addWorkerMember(deposed, reincarnation.Epoch, reincarnation.Slots)
	script := h.workers.script(deposed)
	h.workers.mu.Lock()
	script.identity.WorkerEpoch = reincarnation.Epoch
	h.workers.mu.Unlock()

	// The leader reassigns off the deposed holder and re-seals from the
	// surviving current copy; the stranded copy on the deposed holder stays.
	resealed := false
	for index := 0; index < 60 && !resealed; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok || record.Assignment == nil || len(record.NeedsReassignment) > 0 {
			continue
		}
		manifest := record.Manifests[replica.SinkTask]
		resealed = manifest.ManifestRevision > committed.ManifestRevision && manifest.Replicas == record.Assignment.ResultReplicas[0]
	}
	if !resealed {
		t.Fatalf("deposed holder never left the placement: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	if live := record.Assignment.ResultReplicas[0]; live.SecondaryNodeID == deposed || live.PrimaryNodeID == deposed {
		t.Fatalf("drained holder stayed in the placement: %+v", live)
	}
	if sealedResultStream(t, h, deposed, artifact) == nil {
		t.Fatalf("deposed holder %d holds no stranded sealed copy", deposed)
	}

	reload := func() {
		record, _ := h.job(job)
		live := record.Assignment.ResultReplicas[0]
		_ = live
		for _, node := range []uint16{live.PrimaryNodeID, live.SecondaryNodeID} {
			if node == deposed {
				t.Fatalf("fixture placed the deposed holder back: %+v", live)
			}
			h.clearResultRecords(node)
			view := h.machine.View()
			workerRecord, ok := h.workerRecord(node)
			if !ok {
				t.Fatalf("worker %d record missing", node)
			}
			replacement := state.WorkerRecord{
				NodeID: workerRecord.NodeID, Epoch: model.WorkerEpoch{byte(workerRecord.NodeID), 0x31}, State: workerRecord.State, Revision: workerRecord.Revision + 1, Slots: workerRecord.Slots,
				ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
			}
			replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-holder-loss", []byte{byte(workerRecord.NodeID)}), workerRecord.Revision, workerRecord.NodeID, workerRecord.Epoch, replacement, affectedForWorker(view, workerRecord.NodeID, workerRecord.Epoch), view.CoordinatorEpoch)
			if err != nil {
				t.Fatalf("seed replace %d: %v", node, err)
			}
			h.raft.applySeed(t, replace)
			h.addWorkerMember(workerRecord.NodeID, replacement.Epoch, workerRecord.Slots)
			script := h.workers.script(workerRecord.NodeID)
			h.workers.mu.Lock()
			script.identity.WorkerEpoch = replacement.Epoch
			h.workers.mu.Unlock()
		}
	}
	return h, job, replica.SinkTask, artifact, deposed, reload
}

// TestTotalLossRestoresFromRetainedDeposedHolder pins the retained-holder
// scan: when every current placement endpoint lost its artifact but a
// deposed-but-live holder still proves the committed bytes — count, bytes,
// and checksum all matching — the artifact is restored from there, the
// manifest re-seals onto the live placement, and no lost marker ever fires.
func TestTotalLossRestoresFromRetainedDeposedHolder(t *testing.T) {
	h, job, sink, artifact, deposed, reload := deposedHolderHarness(t)
	before, _ := h.job(job)
	committed := before.Manifests[sink]
	reload()

	restored := false
	for index := 0; index < 80 && !restored; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok || record.Assignment == nil || len(record.NeedsReassignment) > 0 {
			continue
		}
		manifest := record.Manifests[sink]
		restored = manifest.ManifestRevision > committed.ManifestRevision && manifest.Replicas == record.Assignment.ResultReplicas[0] && !manifest.Lost
	}
	if !restored {
		t.Fatalf("stranded artifact never restored from the deposed holder: %v", h.log.snapshot())
	}
	if h.log.count("propose:mark-lost") != 0 {
		t.Fatalf("lost marker fired despite a retained holder: %v", h.log.snapshot())
	}
	if got := h.log.count(fmt.Sprintf("fetch:%d", deposed)); got == 0 {
		t.Fatalf("retained scan never probed the deposed holder: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	live := record.Assignment.ResultReplicas[0]
	if sealedResultStream(t, h, live.PrimaryNodeID, artifact) == nil || sealedResultStream(t, h, live.SecondaryNodeID, artifact) == nil {
		t.Fatalf("current placement holds no restored copy: %+v", live)
	}
}

// TestTotalLossSkipsRetainedHolderWithMismatchedChecksum pins the identity
// guard of the retained scan: a deposed holder whose bytes no longer prove
// the committed artifact — same count and total length, different content
// checksum — is skipped, the scan exhausts, and the terminal lost marker
// fires instead.
func TestTotalLossSkipsRetainedHolderWithMismatchedChecksum(t *testing.T) {
	h, job, sink, _, deposed, reload := deposedHolderHarness(t)

	// Corrupt the deposed holder's retained copy: swap two same-length
	// record values, so an on-demand seal derives the same count and total
	// bytes under a different content checksum.
	script := h.workers.script(deposed)
	script.results.mu.Lock()
	retained := append([]model.ResultRecord(nil), script.results.records...)
	script.results.sealed = make(map[[32]byte]protocol.ResultArtifact)
	script.results.sealedStream = make(map[[32]byte][]byte)
	script.results.records = nil
	script.results.mu.Unlock()
	if len(retained) < 2 {
		t.Fatalf("fixture requires two retained records: %d", len(retained))
	}
	first, second := retained[0], retained[1]
	rebuiltFirst, err := model.NewResultRecord(first.TupleID, first.SinkTask, first.SpecificationHash, second.Value)
	if err != nil {
		t.Fatalf("rebuild first record: %v", err)
	}
	rebuiltSecond, err := model.NewResultRecord(second.TupleID, second.SinkTask, second.SpecificationHash, first.Value)
	if err != nil {
		t.Fatalf("rebuild second record: %v", err)
	}
	script.results.mu.Lock()
	script.results.records = []model.ResultRecord{rebuiltFirst, rebuiltSecond}
	script.results.mu.Unlock()

	reload()

	marked := false
	for index := 0; index < 80 && !marked; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok {
			continue
		}
		marked = record.Manifests[sink].Lost
	}
	if !marked {
		t.Fatalf("mismatched retained holder never exhausted to the lost marker: %v", h.log.snapshot())
	}
	if got := h.log.count(fmt.Sprintf("fetch:%d", deposed)); got == 0 {
		t.Fatalf("scan never probed the mismatched holder: %v", h.log.snapshot())
	}
	if got := h.log.count("propose:mark-lost"); got != 1 {
		t.Fatalf("mark-lost proposals=%d: %v", got, h.log.snapshot())
	}
}

// TestDeactivateWorkerSkipsTerminalPreStepInstall pins the deactivateWorker
// skip through the real failure path: with a Succeeded job and a nonterminal
// job both affected by the dying incarnation, the pre-deactivation Closed
// install is issued for the nonterminal job only — the Succeeded job has no
// live producer to fence and its already-confirmed terminal propagation
// installs stay memoized.
func TestDeactivateWorkerSkipsTerminalPreStepInstall(t *testing.T) {
	h, job, _, assignment := terminalHarness(t, 1)
	h.seedWorker(4, model.WorkerEpoch{4}, 8)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 8)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, assignmentTopology(t, h, job), assignment, 2)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	h.start()
	h.markReady()
	h.lead(2)
	waitForSucceeded(t, h, job)
	// Drive passes until the succeeded job's terminal propagation has fully
	// settled — the install count is stable across a whole pass — so a later
	// install can be attributed unambiguously to the deactivation pre-step
	// rather than an outstanding propagation retry.
	previous := -1
	for index := 0; index < 10; index++ {
		h.rescan()
		current := closedInstallsFor(h, job, 0)
		if current == previous {
			break
		}
		previous = current
	}
	propagated := closedInstallsFor(h, job, 0)

	// One nonterminal job whose placement shares the dying worker.
	running, _, runningAssignment := h.seedRunningJob(1)
	runningNodes := assignmentNodes(runningAssignment)
	for index := 0; index < 10; index++ {
		h.rescan()
		if len(h.workers.installsFor(runningNodes[0], model.Running)) > 0 {
			break
		}
	}
	h.waitFor(func() bool { return len(h.workers.installsFor(runningNodes[0], model.Running)) > 0 }, "running job activated")
	succeededClosed := closedInstallsFor(h, job, 0)
	if succeededClosed != propagated {
		t.Fatalf("terminal propagation kept installing after settling: %d then %d", propagated, succeededClosed)
	}

	proposed := false
	var capturedSucceeded, capturedRunning int
	h.raft.setProposeHook(func(command any) (bool, error) {
		if _, ok := command.(state.DeactivateWorker); ok {
			proposed = true
			capturedSucceeded = closedInstallsFor(h, job, 0)
			capturedRunning = closedInstallsFor(h, running, 0)
		}
		return true, nil
	})
	dying := replica.SecondaryNodeID
	if dying != runningNodes[0] && dying != runningNodes[len(runningNodes)-1] {
		// The fixture requires the dying worker to hold a nonterminal duty.
		t.Fatalf("dying worker %d holds no running-job duty: %v", dying, runningNodes)
	}
	h.setMemberStatus(dying, swim.Dead)
	h.failWorker(dying)
	// A worker that becomes unreachable mid-session is detected through the
	// event-drain poll, whose failures surface as ErrWorkerUnavailable.
	script := h.workers.script(dying)
	h.workers.mu.Lock()
	script.statusErr = fmt.Errorf("%w: worker unreachable", ErrWorkerUnavailable)
	h.workers.mu.Unlock()
	for index := 0; index < 10 && !proposed; index++ {
		h.rescan()
	}
	if !proposed {
		t.Fatalf("deactivation never proposed: %v", h.log.snapshot())
	}

	if capturedSucceeded != succeededClosed {
		t.Fatalf("deactivation pre-step installed Closed for the succeeded job: before=%d at-propose=%d log=%v", succeededClosed, capturedSucceeded, h.log.snapshot())
	}
	if capturedRunning == 0 {
		t.Fatalf("nonterminal job never received its pre-step Closed install: %v", h.log.snapshot())
	}
	if healthy := firstOther(runningNodes, dying); healthy != 0 {
		assertSubsequence(t, h.log.snapshot(), fmt.Sprintf("install:%d:closed", healthy), "propose:deactivate")
	}
}

func firstOther(nodes []uint16, skip uint16) uint16 {
	for _, node := range nodes {
		if node != skip {
			return node
		}
	}
	return 0
}

func assignmentTopology(t *testing.T, h *harness, job model.JobID) model.ValidatedTopology {
	t.Helper()
	record, ok := h.job(job)
	if !ok {
		t.Fatalf("job missing")
	}
	topology, err := model.DecodeTopology(record.TopologyBytes)
	if err != nil {
		t.Fatalf("decode topology: %v", err)
	}
	return topology
}

// TestMaintainTerminalResultsMemoizesClosedInstalls pins the per-node
// terminal Closed-install memoization inside the maintenance loop: while the
// artifact restoration retries on an unconverged pass, the workers that
// already confirmed this fence's Closed install are never re-installed.
func TestMaintainTerminalResultsMemoizesClosedInstalls(t *testing.T) {
	h, job, _, assignment := terminalHarness(t, 1)
	h.seedWorker(4, model.WorkerEpoch{4}, 8)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 8)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, assignmentTopology(t, h, job), assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	h.start()
	h.markReady()
	h.lead(2)
	succeeded := waitForSucceeded(t, h, job)
	if succeeded.Manifests[replica.SinkTask].Replicas != replica {
		t.Fatalf("fixture manifest binds %+v want %+v", succeeded.Manifests[replica.SinkTask].Replicas, replica)
	}

	// The secondary loses its store and returns under a new epoch; every
	// artifact install onto the replacement endpoint fails persistently, so
	// the maintenance stays unconverged and retries every pass.
	h.clearResultRecords(replica.SecondaryNodeID)
	view := h.machine.View()
	workerRecord, ok := h.workerRecord(replica.SecondaryNodeID)
	if !ok {
		t.Fatal("secondary worker record missing")
	}
	replacement := state.WorkerRecord{
		NodeID: workerRecord.NodeID, Epoch: model.WorkerEpoch{byte(workerRecord.NodeID), 0x12}, State: workerRecord.State, Revision: workerRecord.Revision + 1, Slots: workerRecord.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-memo-loss", []byte{byte(workerRecord.NodeID)}), workerRecord.Revision, workerRecord.NodeID, workerRecord.Epoch, replacement, affectedForWorker(view, workerRecord.NodeID, workerRecord.Epoch), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed replace: %v", err)
	}
	h.raft.applySeed(t, replace)
	h.addWorkerMember(workerRecord.NodeID, replacement.Epoch, workerRecord.Slots)
	script := h.workers.script(workerRecord.NodeID)
	h.workers.mu.Lock()
	script.identity.WorkerEpoch = replacement.Epoch
	h.workers.mu.Unlock()
	// Every possible replacement endpoint except the surviving holder
	// rejects artifact installs persistently, so the restoration stays
	// unconverged and retries every pass whichever placement is chosen.
	survivor := replica.PrimaryNodeID
	for _, node := range []uint16{2, 3, 4} {
		if node == survivor {
			continue
		}
		resultState := h.nodeResults(node)
		resultState.mu.Lock()
		resultState.artifactErrs = 1 << 30
		resultState.mu.Unlock()
	}

	replaced := false
	for index := 0; index < 60 && !replaced; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok || record.Assignment == nil {
			continue
		}
		replaced = record.Assignment.Revision > assignment.Revision && len(record.NeedsReassignment) == 0
	}
	if !replaced {
		t.Fatalf("reassignment never settled: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	replacementRevision := record.Assignment.Revision
	live := record.Assignment.ResultReplicas[0]

	// Several unconverged restoration passes retry the artifact install...
	for index := 0; index < 6; index++ {
		h.rescan()
	}
	if got := h.log.count(fmt.Sprintf("artifact:%d", live.SecondaryNodeID)); got < 6 {
		t.Fatalf("restoration did not retry every pass: %v", h.log.snapshot())
	}
	// ...but the confirmed Closed installs for this fence happened exactly
	// once per assignment node.
	nodes := assignmentNodes(*record.Assignment)
	if got := closedInstallsFor(h, job, replacementRevision); got != len(nodes) {
		t.Fatalf("fence %d Closed installs=%d want exactly one per node (%d): %v", replacementRevision, got, len(nodes), h.log.snapshot())
	}

	// Once the endpoints accept artifacts again the restoration completes.
	for _, node := range []uint16{2, 3, 4} {
		resultState := h.nodeResults(node)
		resultState.mu.Lock()
		resultState.artifactErrs = 0
		resultState.mu.Unlock()
	}
	maintained := false
	for index := 0; index < 60 && !maintained; index++ {
		h.rescan()
		current, ok := h.job(job)
		if !ok || current.Assignment == nil {
			continue
		}
		manifest := current.Manifests[replica.SinkTask]
		maintained = manifest.ManifestRevision > succeeded.Manifests[replica.SinkTask].ManifestRevision &&
			manifest.Replicas == current.Assignment.ResultReplicas[0] && !manifest.Lost
	}
	if !maintained {
		t.Fatalf("maintenance never completed after the endpoint recovered: %v", h.log.snapshot())
	}
}
