package coordinator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
)

// runningHarness seeds two workers plus one committed running job.
func runningHarness(t *testing.T) (*harness, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	job, topology, assignment := h.seedRunningJob(1)
	return h, job, topology, assignment
}

func TestReconcileExactOrder(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	replica := assignment.ResultReplicas[0]
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	entries := h.log.snapshot()
	assertSubsequence(t, entries,
		"barrier",
		"propose:begin",
		"handshake:2", "handshake:3",
		"fence:2", "fence:3",
		"status:2", "status:3",
		"install:2:closed", "install:3:closed",
		"checkpoint:2", "checkpoint:3",
		fmt.Sprintf("inventory:%d", replica.PrimaryNodeID),
		fmt.Sprintf("inventory:%d", replica.SecondaryNodeID),
		"install:2:running", "install:3:running",
	)
	// The gate must have stayed closed through the entire recorded pass: every
	// running install carries the reconciled fence, and gate admission begins
	// only after the last one.
	record, ok := h.job(job)
	if !ok || record.Lifecycle != state.JobRunning {
		t.Fatalf("job record = %#v", record)
	}
	for node := uint16(2); node <= 3; node++ {
		installs := h.workers.installsFor(node, model.Running)
		if len(installs) == 0 {
			t.Fatalf("worker %d missing running install", node)
		}
		if installs[0].Assignment.Digest != assignment.Digest || installs[0].CoordinatorEpoch != h.view().CoordinatorEpoch {
			t.Fatalf("running install not fenced: %#v", installs[0])
		}
	}
}

func TestReconcileAmbiguousBeginResolvedByCommandIdentity(t *testing.T) {
	h := newHarness(t)
	injected := false
	h.raft.setProposeHook(func(command any) (bool, error) {
		if _, ok := command.(state.BeginCoordinatorEpoch); ok && !injected {
			injected = true
			return true, errors.New("injected ambiguous transport failure")
		}
		return true, nil
	})
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	if got := h.log.count("propose:begin"); got != 1 {
		t.Fatalf("begin proposed %d times; ambiguity must resolve by identity: %v", got, h.log.snapshot())
	}
	if h.log.count("barrier") < 2 {
		t.Fatalf("ambiguity resolved without a barrier: %v", h.log.snapshot())
	}
	view := h.view()
	if view.CoordinatorRevision != 1 || view.CoordinatorEpoch.Term != 2 {
		t.Fatalf("coordinator state = revision %d epoch %#v", view.CoordinatorRevision, view.CoordinatorEpoch)
	}
}

// completionEvent builds one valid worker completion event for the current fence.
func completionEvent(t *testing.T, h *harness, job model.JobID, token model.AssignmentToken, transaction, expectedRevision, prior, next, eof uint64) model.WorkerEvent {
	t.Helper()
	view := h.view()
	record, ok := h.job(job)
	if !ok {
		t.Fatalf("job missing")
	}
	report := model.CompletionReport{
		JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: record.Assignment.Revision,
		Source: token.Task, Token: token, Epoch: view.CoordinatorEpoch,
		ExpectedCheckpointRevision: expectedRevision, Prior: prior, New: next, EOF: eof,
		WorkerTransactionID: transaction,
	}
	report.Digest = model.CompletionReportDigest(report)
	return model.WorkerEvent{
		WorkerID: token.WorkerID, WorkerEpoch: token.WorkerEpoch, TransactionID: transaction,
		Kind: model.WorkerEventCompletion, Completion: &report,
	}
}

func sourceToken(t *testing.T, assignment model.AssignmentSet) model.AssignmentToken {
	t.Helper()
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			return token
		}
	}
	t.Fatal("no source token")
	return model.AssignmentToken{}
}

func TestReconcileStatusPagingDrainsAndAdvancesCursorAfterCommit(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	token := sourceToken(t, assignment)
	node := token.WorkerID
	script := h.workers.script(node)
	events := []model.WorkerEvent{
		completionEvent(t, h, job, token, 1, 0, 0, 1, 4),
		completionEvent(t, h, job, token, 2, 1, 1, 2, 4),
		completionEvent(t, h, job, token, 3, 2, 2, 3, 4),
	}
	h.workers.mu.Lock()
	script.events = events
	script.pageSize = 1
	h.workers.mu.Unlock()

	h.actor.Wake()
	h.waitFor(func() bool {
		record, ok := h.job(job)
		if !ok {
			return false
		}
		checkpoint, ok := record.Checkpoints[token.Task]
		return ok && checkpoint.Watermark == 3
	}, "all checkpoint events committed")
	if got := h.log.count("propose:advance"); got != 3 {
		t.Fatalf("advance proposals = %d: %v", got, h.log.snapshot())
	}
	// Each page is drained and the leader-local cursor advances only after the
	// corresponding replicated effect resolved, so the following page (and the
	// eventual acknowledgment) carries the resolved cursor.
	assertSubsequence(t, h.log.snapshot(),
		fmt.Sprintf("status:%d", node), "propose:advance",
		"propose:advance",
		"propose:advance",
	)
	h.actor.Wake()
	h.waitFor(func() bool { return h.workers.lastAck(node) == 3 }, "cursor acknowledged")
}

func TestReconcileRedrivesRunningInstallAfterSameEpochWorkerRestart(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	token := sourceToken(t, assignment)
	node := token.WorkerID
	other := uint16(2)
	if node == 2 {
		other = 3
	}
	statusEntry := "status:" + fmt.Sprint(node)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	h.waitFor(func() bool { return len(h.workers.installsFor(node, model.Running)) >= 1 }, "initial running install")
	if epoch := h.workers.admissionEpoch(node); epoch != h.view().CoordinatorEpoch {
		t.Fatalf("modeled gate = %#v want %#v", epoch, h.view().CoordinatorEpoch)
	}
	passes := h.log.count(statusEntry)

	// Steady state: with the gate observed open under the current epoch, the
	// reconciled job stays inert across passes — no install churn.
	h.rescan()
	h.rescan()
	h.waitFor(func() bool { return h.log.count(statusEntry) >= passes+2 }, "two inert passes")
	if got := len(h.workers.installsFor(node, model.Running)); got != 1 {
		t.Fatalf("install churn on an open gate: %d", got)
	}
	if got := len(h.workers.installsFor(other, model.Running)); got != 1 {
		t.Fatalf("install churn on the healthy worker: %d", got)
	}

	// Same-epoch worker restart: the durable store (identity, events,
	// installed Running assignment) survives, but the process admission gate
	// starts closed. The next pass must observe the closed gate under the
	// per-fence reconciled cache and re-drive the idempotent Running install.
	h.workers.restartGate(node)
	h.rescan()
	h.waitFor(func() bool {
		return len(h.workers.installsFor(node, model.Running)) >= 2 && h.workers.admissionEpoch(node) == h.view().CoordinatorEpoch
	}, "running install re-driven after restart")

	// The re-drive is one idempotent whole-set install: both workers gain at
	// most one duplicate Running install and the job returns to inert — no
	// further churn on later passes.
	h.rescan()
	h.waitFor(func() bool { return h.log.count(statusEntry) >= 5 }, "post-restart inert pass")
	if got := len(h.workers.installsFor(node, model.Running)); got != 2 {
		t.Fatalf("running installs after restart = %d", got)
	}
	if got := len(h.workers.installsFor(other, model.Running)); got != 2 {
		t.Fatalf("healthy worker installs after restart = %d", got)
	}
	record, ok := h.job(job)
	if !ok || record.Lifecycle != state.JobRunning || record.JobControlRevision != 3 {
		t.Fatalf("re-drive mutated the job: %#v", record)
	}
}

func TestReconcileRejectsUnorderedStatusPage(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	token := sourceToken(t, assignment)
	node := token.WorkerID
	good := completionEvent(t, h, job, token, 5, 0, 0, 1, 4)
	stale := completionEvent(t, h, job, token, 3, 0, 0, 1, 4)
	script := h.workers.script(node)
	h.workers.mu.Lock()
	script.events = []model.WorkerEvent{good, stale}
	h.workers.mu.Unlock()

	h.actor.Wake()
	h.waitFor(func() bool { return h.log.count("status:"+fmt.Sprint(node)) >= 2 }, "second status scan")
	time.Sleep(20 * time.Millisecond)
	if h.log.count("propose:advance") != 0 {
		t.Fatalf("unordered page consumed: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	if len(record.Checkpoints) != 0 {
		t.Fatalf("unordered page committed progress: %#v", record.Checkpoints)
	}
}

// repairScripts wires stateful inventory and repair behavior for one sink.
type repairScripts struct {
	mu        sync.Mutex
	repaired  map[uint16]bool
	completed map[[16]byte]map[protocol.RepairEndpointRole]int
	summary   func(query protocol.ResultInventoryQuery) protocol.ResultInventorySummary
}

func newRepairScripts() *repairScripts {
	scripts := &repairScripts{
		repaired:  make(map[uint16]bool),
		completed: make(map[[16]byte]map[protocol.RepairEndpointRole]int),
	}
	scripts.summary = func(query protocol.ResultInventoryQuery) protocol.ResultInventorySummary {
		return protocol.ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: 2, TotalBytes: 100, ContentDigest: [32]byte{0xD1}}
	}
	return scripts
}

func (scripts *repairScripts) fullInventory(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
	return scripts.summary(query), nil
}

func (scripts *repairScripts) inventoryUntilRepaired(node uint16) func(protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
	return func(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
		scripts.mu.Lock()
		repaired := scripts.repaired[node]
		scripts.mu.Unlock()
		if repaired {
			return scripts.summary(query), nil
		}
		return emptySummary(query), nil
	}
}

// repairResponder completes a grant on its second poll and marks the
// destination repaired so re-queries observe the transferred content.
func (scripts *repairScripts) repairResponder() func(grant protocol.RepairGrant) (protocol.ResultRepairStatus, error) {
	return func(grant protocol.RepairGrant) (protocol.ResultRepairStatus, error) {
		scripts.mu.Lock()
		defer scripts.mu.Unlock()
		byRole, ok := scripts.completed[grant.Instruction.RepairID]
		if !ok {
			byRole = make(map[protocol.RepairEndpointRole]int)
			scripts.completed[grant.Instruction.RepairID] = byRole
		}
		byRole[grant.Role]++
		status := protocol.ResultRepairStatus{
			Instruction: grant.Instruction, RepairID: grant.Instruction.RepairID,
			InstructionDigest: grant.Instruction.InstructionDigest, Role: grant.Role, State: protocol.RepairPending,
		}
		if byRole[grant.Role] >= 2 {
			status.State = protocol.RepairComplete
			status.RecordCount = grant.Instruction.ExpectedRecordCount
			status.TotalBytes = grant.Instruction.ExpectedTotalBytes
			status.ContentDigest = grant.Instruction.ExpectedContentDigest
			if grant.Role == protocol.RepairDestination {
				scripts.repaired[grant.Instruction.DestinationNodeID] = true
			}
		}
		return status, nil
	}
}

func TestReconcileRepairsDivergentReplica(t *testing.T) {
	h, _, _, assignment := runningHarness(t)
	replica := assignment.ResultReplicas[0]
	primary, secondary := replica.PrimaryNodeID, replica.SecondaryNodeID
	scripts := newRepairScripts()
	primaryScript := h.workers.script(primary)
	secondaryScript := h.workers.script(secondary)
	h.workers.mu.Lock()
	primaryScript.inventory = scripts.fullInventory
	primaryScript.repair = scripts.repairResponder()
	secondaryScript.inventory = scripts.inventoryUntilRepaired(secondary)
	secondaryScript.repair = scripts.repairResponder()
	h.workers.mu.Unlock()

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	assertSubsequence(t, h.log.snapshot(),
		fmt.Sprintf("inventory:%d", primary),
		fmt.Sprintf("inventory:%d", secondary),
		fmt.Sprintf("repair:%d:destination", secondary),
		fmt.Sprintf("repair:%d:source", primary),
		fmt.Sprintf("inventory:%d", primary),
		fmt.Sprintf("inventory:%d", secondary),
		"install:2:running",
	)
	grants := h.workers.grantList()
	if len(grants) < 2 {
		t.Fatalf("grants = %#v", grants)
	}
	first := grants[0]
	if first.Role != protocol.RepairDestination {
		t.Fatalf("destination grant must be installed first: %#v", first)
	}
	instruction := first.Instruction
	if instruction.SourceNodeID != primary || instruction.DestinationNodeID != secondary {
		t.Fatalf("instruction endpoints = %#v", instruction)
	}
	if instruction.ExpectedRecordCount != 2 || instruction.ExpectedTotalBytes != 100 || instruction.ExpectedContentDigest != ([32]byte{0xD1}) {
		t.Fatalf("expected summary not bound: %#v", instruction)
	}
	if instruction.RepairID != protocol.DeriveRepairID(instruction) || instruction.InstructionDigest != protocol.RepairInstructionDigest(instruction) {
		t.Fatalf("repair identity is not deterministic: %#v", instruction)
	}
	for _, grant := range grants {
		if grant.Instruction.RepairID != instruction.RepairID {
			t.Fatalf("grants are not idempotent repeats: %#v", grant)
		}
	}
}

func TestReconcileDisagreeingSurvivorsLeaveAdmissionClosed(t *testing.T) {
	h, _, _, assignment := runningHarness(t)
	replica := assignment.ResultReplicas[0]
	primaryScript := h.workers.script(replica.PrimaryNodeID)
	secondaryScript := h.workers.script(replica.SecondaryNodeID)
	h.workers.mu.Lock()
	primaryScript.inventory = func(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
		return protocol.ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: 2, TotalBytes: 100, ContentDigest: [32]byte{0xD1}}, nil
	}
	secondaryScript.inventory = func(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
		return protocol.ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: 3, TotalBytes: 150, ContentDigest: [32]byte{0xD2}}, nil
	}
	h.workers.mu.Unlock()

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		return h.log.count(fmt.Sprintf("inventory:%d", replica.SecondaryNodeID)) >= 1
	}, "both replicas queried")
	time.Sleep(20 * time.Millisecond)
	if h.gateOpen() {
		t.Fatal("gate opened with disagreeing survivors")
	}
	for _, entry := range h.log.snapshot() {
		if len(entry) >= 6 && entry[:6] == "repair" {
			t.Fatalf("repair issued between disagreeing survivors: %v", h.log.snapshot())
		}
	}
}

func TestReconcileScansRegisteredWorkersForRetainedInventory(t *testing.T) {
	h, job, _, assignment := runningHarness(t)
	// A fourth registered worker retains old-provenance inventory. It joined
	// after placement so it holds no current duties.
	h.seedWorker(4, model.WorkerEpoch{4}, 4)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 4)
	token := sourceToken(t, assignment)
	view := h.view()
	report := model.CompletionReport{
		JobID: job, JobControlRevision: 3, AssignmentRevision: 1,
		Source: token.Task, Token: token, Epoch: view.CoordinatorEpoch,
		ExpectedCheckpointRevision: 0, Prior: 0, New: 4, EOF: 4, WorkerTransactionID: 1,
	}
	report.Digest = model.CompletionReportDigest(report)
	advance, err := state.NewAdvanceCheckpoint(testCommandID("seed-advance", job[:]), 0, report, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed advance: %v", err)
	}
	h.raft.applySeed(t, advance)

	replica := assignment.ResultReplicas[0]
	scripts := newRepairScripts()
	primaryScript := h.workers.script(replica.PrimaryNodeID)
	secondaryScript := h.workers.script(replica.SecondaryNodeID)
	holderScript := h.workers.script(4)
	h.workers.mu.Lock()
	primaryScript.inventory = scripts.inventoryUntilRepaired(replica.PrimaryNodeID)
	primaryScript.repair = scripts.repairResponder()
	secondaryScript.inventory = scripts.inventoryUntilRepaired(replica.SecondaryNodeID)
	secondaryScript.repair = scripts.repairResponder()
	holderScript.inventory = scripts.fullInventory
	holderScript.repair = scripts.repairResponder()
	h.workers.mu.Unlock()

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()

	assertSubsequence(t, h.log.snapshot(),
		fmt.Sprintf("inventory:%d", replica.PrimaryNodeID),
		fmt.Sprintf("inventory:%d", replica.SecondaryNodeID),
		"install:4:closed",
		"inventory:4",
		fmt.Sprintf("repair:%d:destination", replica.PrimaryNodeID),
		"repair:4:source",
		fmt.Sprintf("repair:%d:destination", replica.SecondaryNodeID),
	)
	sawHolderSource := false
	for _, grant := range h.workers.grantList() {
		if grant.Instruction.SourceNodeID == 4 {
			sawHolderSource = true
		}
	}
	if !sawHolderSource {
		t.Fatalf("holder scan never repaired from worker 4: %#v", h.workers.grantList())
	}
}
