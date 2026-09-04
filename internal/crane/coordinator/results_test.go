package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/state"
	"crane/internal/crane/worker"
)

var _ ResultTransferClient = (*fakeWorkers)(nil)

// terminalTopologySpec clones the shared test topology with an adjustable
// collect-sink parallelism.
func terminalTopologySpec(sinkPartitions uint16) model.TopologySpec {
	spec := testTopologySpec(1)
	spec.Stages[1].Parallelism = sinkPartitions
	return spec
}

// terminalHarness seeds one Running job across workers 2 and 3 whose every
// source checkpoint is committed at EOF minus watermarkDelta (zero commits
// exactly final checkpoints).
func terminalHarness(t *testing.T, sinkPartitions uint16) (*harness, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	return terminalHarnessAt(t, sinkPartitions, 0)
}

func terminalHarnessAt(t *testing.T, sinkPartitions uint16, watermarkDelta uint64) (*harness, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	return terminalHarnessSpec(t, terminalTopologySpec(sinkPartitions), watermarkDelta)
}

// terminalHarnessSpec seeds one Running job for one topology whose every
// nonzero-EOF source checkpoint is committed at EOF minus watermarkDelta (zero
// commits exactly final checkpoints). Sources committing EOF zero are skipped:
// no checkpoint record is constructible for an empty source.
func terminalHarnessSpec(t *testing.T, spec model.TopologySpec, watermarkDelta uint64) (*harness, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 8)
	h.seedWorker(3, model.WorkerEpoch{3}, 8)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 8)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 8)
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	view := h.machine.View()
	submit, err := state.NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x31}, Sequence: 1}, topology.Spec(), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed submit: %v", err)
	}
	h.raft.applySeed(t, submit)
	job := submit.JobID()
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, eligiblePlacements(h.machine.View(), job))
	if err != nil {
		t.Fatalf("seed assignment: %v", err)
	}
	sources := make([]model.AssignmentToken, 0)
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			sources = append(sources, token)
		}
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Task.Partition < sources[j].Task.Partition })
	for _, token := range sources {
		eof, err := model.SourceEOF(topology, token.Task)
		if err != nil {
			t.Fatalf("source EOF: %v", err)
		}
		recordEOF, err := state.NewRecordSourceEOF(testCommandID("seed-eof", job[:], []byte{byte(token.Task.Partition)}), 0, token.Task, eof, view.CoordinatorEpoch)
		if err != nil {
			t.Fatalf("seed EOF: %v", err)
		}
		h.raft.applySeed(t, recordEOF)
	}
	install, err := state.NewInstallAssignments(testCommandID("seed-install", job[:]), 1, assignment, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed install: %v", err)
	}
	h.raft.applySeed(t, install)
	transition, err := state.NewTransitionJob(testCommandID("seed-run", job[:]), 2, job, state.JobDeploying, state.JobRunning, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed transition: %v", err)
	}
	h.raft.applySeed(t, transition)
	record, ok := h.job(job)
	if !ok {
		t.Fatal("seeded job missing")
	}
	transaction := uint64(1)
	for _, token := range sources {
		eof := record.SourceEOFs[token.Task].EOF
		if eof == 0 {
			continue
		}
		watermark := eof
		if watermark > watermarkDelta {
			watermark -= watermarkDelta
		}
		report := model.CompletionReport{
			JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision,
			Source: token.Task, Token: token, Epoch: view.CoordinatorEpoch,
			ExpectedCheckpointRevision: 0, Prior: 0, New: watermark, EOF: eof, WorkerTransactionID: transaction,
		}
		report.Digest = model.CompletionReportDigest(report)
		advance, err := state.NewAdvanceCheckpoint(testCommandID("seed-advance", job[:], []byte{byte(token.Task.Partition)}), 0, report, view.CoordinatorEpoch)
		if err != nil {
			t.Fatalf("seed advance: %v", err)
		}
		h.raft.applySeed(t, advance)
		transaction++
	}
	return h, job, topology, assignment
}

// nodeResults creates (once) one fake worker's durable result state and wires
// its inventory answers to the retained records.
func (h *harness) nodeResults(node uint16) *fakeResultState {
	h.t.Helper()
	script := h.workers.script(node)
	h.workers.mu.Lock()
	defer h.workers.mu.Unlock()
	if script.results == nil {
		script.results = newFakeResultState()
		state := script.results
		script.inventory = func(query protocol.ResultInventoryQuery) (protocol.ResultInventorySummary, error) {
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.inventoryBlocked[query.SinkTask] {
				return protocol.ResultInventorySummary{}, errors.New("inventory unavailable")
			}
			filtered := make([]model.ResultRecord, 0)
			for _, record := range state.records {
				if record.TupleID.JobID != query.JobID || record.SinkTask != query.SinkTask || record.SpecificationHash != query.SpecificationHash {
					continue
				}
				within := false
				for _, checkpoint := range query.Checkpoints {
					if checkpoint.Source == record.TupleID.SourceTask && record.TupleID.SourceSequence <= checkpoint.Watermark {
						within = true
						break
					}
				}
				if within {
					filtered = append(filtered, record)
				}
			}
			count, total, digest, err := worker.ResultInventoryAggregate(query.QueryDigest, filtered)
			if err != nil {
				return protocol.ResultInventorySummary{}, err
			}
			return protocol.ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: count, TotalBytes: total, ContentDigest: digest}, nil
		}
	}
	return script.results
}

// seedResultRecords retains covered result records on one worker.
func (h *harness) seedResultRecords(node uint16, records ...model.ResultRecord) {
	h.t.Helper()
	state := h.nodeResults(node)
	state.mu.Lock()
	state.records = append(state.records, records...)
	state.mu.Unlock()
}

// clearResultRecords destroys one worker's retained records and unsealed
// artifacts (a lost sink store).
func (h *harness) clearResultRecords(node uint16) {
	h.t.Helper()
	state := h.nodeResults(node)
	state.mu.Lock()
	state.records = nil
	state.partial = make(map[[32]byte][]byte)
	state.sealed = make(map[[32]byte]protocol.ResultArtifact)
	state.sealedStream = make(map[[32]byte][]byte)
	state.mu.Unlock()
}

// terminalRecords builds the covered canonical records of every sink
// partition from the source sequence range 1..count.
func terminalRecords(t *testing.T, job model.JobID, topology model.ValidatedTopology, assignment model.AssignmentSet, count uint64) map[model.TaskID][]model.ResultRecord {
	t.Helper()
	source := sourceToken(t, assignment).Task
	records := make(map[model.TaskID][]model.ResultRecord)
	for _, replica := range assignment.ResultReplicas {
		list := make([]model.ResultRecord, 0, count)
		for sequence := uint64(1); sequence <= count; sequence++ {
			tuple, exists, err := model.SourceTuple(topology, source, sequence)
			if err != nil || !exists {
				t.Fatalf("source tuple %d: exists=%t err=%v", sequence, exists, err)
			}
			encoded, err := model.MarshalTuple(tuple)
			if err != nil {
				t.Fatal(err)
			}
			record, err := model.NewResultRecord(model.DeriveSourceTupleID(job, source, sequence), replica.SinkTask, topology.Digest(), encoded)
			if err != nil {
				t.Fatal(err)
			}
			list = append(list, record)
		}
		records[replica.SinkTask] = list
	}
	return records
}

// waitForSucceeded blocks until the job reaches terminal success.
func waitForSucceeded(t *testing.T, h *harness, job model.JobID) state.JobRecord {
	t.Helper()
	h.waitFor(func() bool {
		record, ok := h.job(job)
		return ok && record.Lifecycle == state.JobSucceeded
	}, "job succeeded")
	record, _ := h.job(job)
	return record
}

func TestTerminalWorkflowSealsTwoCopiesAndCommitsManifestsThenSucceeds(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)

	h.start()
	h.markReady()
	h.lead(2)
	record := waitForSucceeded(t, h, job)

	entries := h.log.snapshot()
	assertSubsequence(t, entries, "propose:transition", "install:2:draining", "install:3:draining",
		fmt.Sprintf("fetch:%d", replica.PrimaryNodeID),
		fmt.Sprintf("artifact:%d", replica.PrimaryNodeID),
		fmt.Sprintf("artifact:%d", replica.SecondaryNodeID),
		"propose:seal", "propose:transition")
	if got := h.log.count("propose:seal"); got != 1 {
		t.Fatalf("seal proposals=%d: %v", got, entries)
	}
	if got := h.log.count("propose:transition"); got != 2 {
		t.Fatalf("transition proposals=%d: %v", got, entries)
	}

	manifest, ok := record.Manifests[replica.SinkTask]
	if !ok {
		t.Fatalf("no committed manifest: %#v", record.Manifests)
	}
	sealed := sealedTestArtifact(t, job, topology, replica.SinkTask, records[replica.SinkTask])
	if manifest.RecordCount != sealed.RecordCount || manifest.TotalBytes != sealed.TotalLength || manifest.Checksum != sealed.Checksum ||
		manifest.SpecificationHash != topology.Digest() || manifest.ManifestRevision != 1 {
		t.Fatalf("manifest=%+v want artifact=%+v", manifest, sealed)
	}
	if manifest.Replicas != replica {
		t.Fatalf("manifest replicas=%+v want=%+v", manifest.Replicas, replica)
	}
	for _, node := range []uint16{replica.PrimaryNodeID, replica.SecondaryNodeID} {
		sealedStream := sealedResultStream(t, h, node, sealed)
		if sealedStream == nil {
			t.Fatalf("node %d holds no durable sealed copy", node)
		}
	}
	// The exact Draining revision was installed before any seal work, and the
	// terminal Closed state after success.
	draining := h.workers.installsFor(replica.PrimaryNodeID, model.Draining)
	if len(draining) == 0 || draining[0].Assignment.Revision != assignment.Revision {
		t.Fatalf("draining installs=%+v", draining)
	}
	h.waitFor(func() bool { return len(h.workers.installsFor(replica.PrimaryNodeID, model.Closed)) > 0 }, "terminal closed install")

	// No result bytes ever entered Raft: every seal proposal is bounded
	// metadata without record values.
	for _, captured := range h.raft.capturedCommands() {
		seal, ok := captured.(state.SealManifest)
		if !ok {
			continue
		}
		if seal.Manifest.TotalBytes > 0 && len(seal.Manifest.Checksum) != 32 {
			t.Fatalf("manifest malformed: %+v", seal.Manifest)
		}
		encoded, err := state.MarshalCommand(seal)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > 2048 {
			t.Fatalf("seal proposal carries %d bytes", len(encoded))
		}
		if len(seal.Manifest.Replicas.PrimaryEpoch) == 0 {
			t.Fatal("manifest binds no worker epochs")
		}
	}
}

func TestTerminalWorkflowNeedsFinalCheckpointsBeforeDraining(t *testing.T) {
	h, job, topology, assignment := terminalHarnessAt(t, 1, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 2)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	for index := 0; index < 50; index++ {
		h.rescan()
	}
	record, _ := h.job(job)
	if record.Lifecycle != state.JobRunning {
		t.Fatalf("job left Running without final checkpoints: %v", record.Lifecycle)
	}
	if len(record.Manifests) != 0 || h.log.contains("propose:seal") || h.log.contains("fetch:") {
		t.Fatalf("seal work began before final checkpoints: %v", h.log.snapshot())
	}
}

func TestTerminalWorkflowNeverSucceedsWhenReplicaCopiesDisagree(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	// The secondary retained a diverging subset: two disagreeing survivors
	// leave the job Draining without any manifest.
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask][0])

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		record, _ := h.job(job)
		return record.Lifecycle == state.JobDraining
	}, "job drains")
	for index := 0; index < 30; index++ {
		h.rescan()
	}
	record, _ := h.job(job)
	if record.Lifecycle != state.JobDraining || len(record.Manifests) != 0 {
		t.Fatalf("disagreeing copies produced progress: lifecycle=%v manifests=%d", record.Lifecycle, len(record.Manifests))
	}
	if h.log.contains("propose:seal") {
		t.Fatal("manifest committed over disagreeing replicas")
	}
}

func TestSealPartitionRefusesStaleCollidingOrOfflineReplicaEndpoints(t *testing.T) {
	h, _, _, assignment := terminalHarness(t, 1)
	view := h.machine.View()
	replica := assignment.ResultReplicas[0]
	if !currentDistinctReplicaEndpoints(view, replica) {
		t.Fatalf("healthy endpoints refused: %+v", replica)
	}
	stale := replica
	stale.PrimaryEpoch[0]++
	if currentDistinctReplicaEndpoints(view, stale) {
		t.Fatal("two epochs of one NodeID counted as two current copies")
	}
	colliding := replica
	colliding.SecondaryNodeID = replica.PrimaryNodeID
	if currentDistinctReplicaEndpoints(view, colliding) {
		t.Fatal("one NodeID counted twice")
	}
	offline := replica
	offline.PrimaryNodeID = 9
	if currentDistinctReplicaEndpoints(view, offline) {
		t.Fatal("unknown worker counted as a current copy")
	}
}

func TestTerminalWorkflowReassignsAndSealsAfterReplicaEpochReplacement(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 2)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	// The primary store is wiped and restarts under a new incarnation after
	// the final checkpoint: the committed placement is reassigned, and the
	// job seals under the new current replica epochs.
	view := h.machine.View()
	worker, ok := h.workerRecord(replica.PrimaryNodeID)
	if !ok {
		t.Fatal("primary worker record missing")
	}
	replacement := state.WorkerRecord{
		NodeID: worker.NodeID, Epoch: model.WorkerEpoch{2, 0x11}, State: worker.State, Revision: worker.Revision + 1, Slots: worker.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-replace-epoch", []byte{byte(worker.NodeID)}), worker.Revision, worker.NodeID, worker.Epoch, replacement, affectedForWorker(view, worker.NodeID, worker.Epoch), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed replace: %v", err)
	}
	h.raft.applySeed(t, replace)
	h.addWorkerMember(worker.NodeID, replacement.Epoch, worker.Slots)

	h.start()
	h.markReady()
	h.lead(2)
	record := waitForSucceeded(t, h, job)
	if record.Assignment == nil || record.Assignment.Revision <= assignment.Revision {
		t.Fatalf("job completed without reassignment: %#v", record.Assignment)
	}
	manifest, ok := record.Manifests[record.Assignment.ResultReplicas[0].SinkTask]
	if !ok {
		t.Fatalf("no manifest after reassignment: %#v", record.Manifests)
	}
	if manifest.Replicas != record.Assignment.ResultReplicas[0] {
		t.Fatalf("manifest binds the stale placement: %+v want %+v", manifest.Replicas, record.Assignment.ResultReplicas[0])
	}
}

func TestTerminalWorkflowRequiresEveryPartitionSealedBeforeSucceeding(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 2)
	records := terminalRecords(t, job, topology, assignment, 2)
	first, second := assignment.ResultReplicas[0], assignment.ResultReplicas[1]
	h.seedResultRecords(first.PrimaryNodeID, records[first.SinkTask]...)
	h.seedResultRecords(first.SecondaryNodeID, records[first.SinkTask]...)
	// The second partition's replicas cannot answer inventory yet: sealing
	// must stall after the first partition's manifest without success.
	h.blockInventory(second.SinkTask)

	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		record, _ := h.job(job)
		return record.Lifecycle == state.JobDraining && len(record.Manifests) == 1
	}, "first partition sealed")
	for index := 0; index < 30; index++ {
		h.rescan()
	}
	record, _ := h.job(job)
	if record.Lifecycle == state.JobSucceeded {
		t.Fatal("job succeeded before every expected partition was sealed")
	}
	if len(record.Manifests) != 1 {
		t.Fatalf("manifests=%d", len(record.Manifests))
	}

	h.unblockInventory(second.SinkTask)
	h.seedResultRecords(second.PrimaryNodeID, records[second.SinkTask]...)
	h.seedResultRecords(second.SecondaryNodeID, records[second.SinkTask]...)
	record = waitForSucceeded(t, h, job)
	if len(record.Manifests) != 2 {
		t.Fatalf("manifests after completion=%d", len(record.Manifests))
	}
	for _, replica := range assignment.ResultReplicas {
		manifest := record.Manifests[replica.SinkTask]
		if manifest.Replicas != replica || manifest.RecordCount != 2 {
			t.Fatalf("manifest for %+v = %+v", replica.SinkTask, manifest)
		}
	}
}

// blockInventory makes every worker's inventory answer fail for one sink.
func (h *harness) blockInventory(sink model.TaskID) {
	h.t.Helper()
	for node := range h.workers.scripts {
		state := h.nodeResults(node)
		state.mu.Lock()
		state.inventoryBlocked[sink] = true
		state.mu.Unlock()
	}
}

// unblockInventory restores inventory answers for one sink.
func (h *harness) unblockInventory(sink model.TaskID) {
	h.t.Helper()
	for node := range h.workers.scripts {
		state := h.nodeResults(node)
		state.mu.Lock()
		delete(state.inventoryBlocked, sink)
		state.mu.Unlock()
	}
}

func TestManifestBoundRejectsMoreThanTwoFiftySixPartitions(t *testing.T) {
	if manifestPartitionBound(256) {
		t.Fatal("256 partitions refused")
	}
	if !manifestPartitionBound(257) {
		t.Fatal("257 partitions accepted")
	}
}

func TestJobCheckpointsFinalTreatsEmptySourceAsTriviallyFinal(t *testing.T) {
	normal := model.TaskID{JobID: model.JobID{1}, StageID: 1, Partition: 0}
	empty := model.TaskID{JobID: model.JobID{1}, StageID: 1, Partition: 1}
	final := state.JobRecord{
		SourceEOFs:  map[model.TaskID]state.SourceEOFRecord{normal: {EOF: 4, Revision: 1}, empty: {EOF: 0, Revision: 1}},
		Checkpoints: map[model.TaskID]state.CheckpointRecord{normal: {Watermark: 4, Revision: 1}},
	}
	if !jobCheckpointsFinal(final) {
		t.Fatal("EOF-zero source without a checkpoint blocked finality")
	}
	nonzero := state.JobRecord{
		SourceEOFs:  map[model.TaskID]state.SourceEOFRecord{normal: {EOF: 4, Revision: 1}, empty: {EOF: 3, Revision: 1}},
		Checkpoints: map[model.TaskID]state.CheckpointRecord{normal: {Watermark: 4, Revision: 1}},
	}
	if jobCheckpointsFinal(nonzero) {
		t.Fatal("nonzero-EOF source without a checkpoint passed finality")
	}
	incomplete := state.JobRecord{
		SourceEOFs:  map[model.TaskID]state.SourceEOFRecord{normal: {EOF: 4, Revision: 1}},
		Checkpoints: map[model.TaskID]state.CheckpointRecord{normal: {Watermark: 2, Revision: 1}},
	}
	if jobCheckpointsFinal(incomplete) {
		t.Fatal("watermark below EOF passed finality")
	}
}

// emptySourceTerminalHarness seeds one Running job whose second source
// partition commits the legal EOF zero, so only the first source can ever
// materialize a final checkpoint.
func emptySourceTerminalHarness(t *testing.T) (*harness, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	spec := terminalTopologySpec(1)
	spec.Stages[0].Parallelism = 2
	spec.Stages[0].Operator.Settings[0].Value = "1"
	return terminalHarnessSpec(t, spec, 0)
}

func TestTerminalWorkflowSucceedsWithEmptySourceMissingCheckpoint(t *testing.T) {
	h, job, topology, assignment := emptySourceTerminalHarness(t)
	empty := model.TaskID{JobID: job, StageID: 1, Partition: 1}
	record, ok := h.job(job)
	if !ok || record.SourceEOFs[empty].EOF != 0 {
		t.Fatalf("fixture requires an EOF-zero source: %#v", record.SourceEOFs)
	}
	if _, exists := record.Checkpoints[empty]; exists {
		t.Fatalf("fixture must not checkpoint the empty source: %#v", record.Checkpoints)
	}
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 1)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)

	h.start()
	h.markReady()
	h.lead(2)
	record = waitForSucceeded(t, h, job)
	if len(record.Checkpoints) != 1 || record.Checkpoints[model.TaskID{JobID: job, StageID: 1, Partition: 0}].Watermark != 1 {
		t.Fatalf("checkpoints = %#v", record.Checkpoints)
	}
	manifest, ok := record.Manifests[replica.SinkTask]
	if !ok {
		t.Fatalf("empty-source job sealed no manifest: %#v", record.Manifests)
	}
	if manifest.RecordCount != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestTerminalWorkflowReconstructsRecordsFromSurvivingReplica(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	surviving := replica.SecondaryNodeID
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(surviving, records[replica.SinkTask]...)
	// Model the record-level repair machinery: after the destination grant
	// the surviving replica's exact covered records reappear on the lost
	// primary before the next inventory query.
	h.scriptRepairCopy(replica.PrimaryNodeID, surviving, job, replica.SinkTask)

	h.start()
	h.markReady()
	h.lead(2)
	// The primary store is destroyed after the final checkpoint but before
	// any seal work.
	h.waitFor(func() bool {
		record, _ := h.job(job)
		return record.Lifecycle == state.JobRunning
	}, "job running")
	h.clearResultRecords(replica.PrimaryNodeID)

	record := waitForSucceeded(t, h, job)
	manifest := record.Manifests[replica.SinkTask]
	sealed := sealedTestArtifact(t, job, topology, replica.SinkTask, records[replica.SinkTask])
	if manifest.Checksum != sealed.Checksum || manifest.RecordCount != sealed.RecordCount || manifest.TotalBytes != sealed.TotalLength {
		t.Fatalf("reconstructed manifest=%+v want=%+v", manifest, sealed)
	}
	for _, node := range []uint16{replica.PrimaryNodeID, surviving} {
		if sealedResultStream(t, h, node, sealed) == nil {
			t.Fatalf("node %d holds no reconstructed copy", node)
		}
	}
	// The reconstruction repaired destination-first from the named source.
	grants := h.workers.grantList()
	sawDestination, sawSource := false, false
	for _, grant := range grants {
		if grant.Instruction.RepairID == ([16]byte{}) {
			continue
		}
		if grant.Role == protocol.RepairDestination && grant.Instruction.DestinationNodeID == replica.PrimaryNodeID && grant.Instruction.SourceNodeID == surviving {
			sawDestination = true
		}
		if grant.Role == protocol.RepairSource && grant.Instruction.SourceNodeID == surviving && grant.Instruction.DestinationNodeID == replica.PrimaryNodeID {
			sawSource = true
		}
	}
	if !sawDestination || !sawSource {
		t.Fatalf("no destination-first reconstruction grants: %+v", grants)
	}
}

// scriptRepairCopy models the reviewed Task 19 record repair: once both
// endpoints durably accept the grant, the destination's retained records are
// restored exactly from the surviving source.
func (h *harness) scriptRepairCopy(destination, source uint16, job model.JobID, sink model.TaskID) {
	h.t.Helper()
	destinationState := h.nodeResults(destination)
	sourceState := h.nodeResults(source)
	restored := false
	for _, node := range []uint16{destination, source} {
		script := h.workers.script(node)
		h.workers.mu.Lock()
		script.repair = func(grant protocol.RepairGrant) (protocol.ResultRepairStatus, error) {
			if grant.Instruction.JobID != job || grant.Instruction.SinkTask != sink {
				return protocol.ResultRepairStatus{}, errors.New("foreign repair")
			}
			if grant.Instruction.DestinationNodeID != destination || grant.Instruction.SourceNodeID != source {
				return protocol.ResultRepairStatus{}, errors.New("unrelated repair")
			}
			destinationState.mu.Lock()
			sourceState.mu.Lock()
			if !restored {
				destinationState.records = append([]model.ResultRecord(nil), sourceState.records...)
				restored = true
			}
			held := len(destinationState.records)
			destinationState.mu.Unlock()
			sourceState.mu.Unlock()
			return protocol.ResultRepairStatus{
				Instruction: grant.Instruction, RepairID: grant.Instruction.RepairID, InstructionDigest: grant.Instruction.InstructionDigest,
				Role: grant.Role, State: protocol.RepairComplete,
				RecordCount: uint64(held), TotalBytes: grant.Instruction.ExpectedTotalBytes, ContentDigest: grant.Instruction.ExpectedContentDigest,
			}, nil
		}
		h.workers.mu.Unlock()
	}
}

func TestTerminalWorkflowFetchesSurvivingReplicaWhenPrimaryUnavailable(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 2)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	// The primary becomes unresponsive after both copies hold the covered
	// records: the seal fetch falls back to the surviving secondary and the
	// manifest still binds both current replicas.
	h.nodeResults(replica.PrimaryNodeID).mu.Lock()
	h.nodeResults(replica.PrimaryNodeID).fetchErrs = 1 << 30
	h.nodeResults(replica.PrimaryNodeID).mu.Unlock()

	h.start()
	h.markReady()
	h.lead(2)
	record := waitForSucceeded(t, h, job)
	manifest := record.Manifests[replica.SinkTask]
	if manifest.Replicas != replica || manifest.RecordCount != 2 {
		t.Fatalf("manifest=%+v", manifest)
	}
	if !h.log.contains(fmt.Sprintf("fetch:%d", replica.SecondaryNodeID)) {
		t.Fatalf("surviving replica never served the seal fetch: %v", h.log.snapshot())
	}
}

func TestTerminalWorkflowResumesAcrossLeadershipLossAtEveryBoundary(t *testing.T) {
	boundaries := map[string]func(h *harness, job model.JobID, assignment model.AssignmentSet){
		"drain commit dropped": func(h *harness, _ model.JobID, _ model.AssignmentSet) {
			dropNthProposal(h, "transition", 1)
		},
		"drain install dropped": func(h *harness, _ model.JobID, assignment model.AssignmentSet) {
			for _, token := range assignment.Tasks {
				script := h.workers.script(token.WorkerID)
				h.workers.mu.Lock()
				script.installErrs = 1
				h.workers.mu.Unlock()
			}
		},
		"seal fetch dropped": func(h *harness, _ model.JobID, assignment model.AssignmentSet) {
			state := h.nodeResults(assignment.ResultReplicas[0].PrimaryNodeID)
			state.mu.Lock()
			state.fetchErrs = 1
			state.mu.Unlock()
		},
		"primary artifact dropped": func(h *harness, _ model.JobID, assignment model.AssignmentSet) {
			state := h.nodeResults(assignment.ResultReplicas[0].PrimaryNodeID)
			state.mu.Lock()
			state.artifactErrs = 1
			state.mu.Unlock()
		},
		"secondary artifact dropped": func(h *harness, _ model.JobID, assignment model.AssignmentSet) {
			state := h.nodeResults(assignment.ResultReplicas[0].SecondaryNodeID)
			state.mu.Lock()
			state.artifactErrs = 1
			state.mu.Unlock()
		},
		"manifest commit dropped": func(h *harness, _ model.JobID, _ model.AssignmentSet) {
			dropNthProposal(h, "seal", 1)
		},
		"succeed commit dropped": func(h *harness, _ model.JobID, _ model.AssignmentSet) {
			dropNthProposal(h, "transition", 2)
		},
	}
	for name, inject := range boundaries {
		t.Run(name, func(t *testing.T) {
			h, job, topology, assignment := terminalHarness(t, 1)
			replica := assignment.ResultReplicas[0]
			records := terminalRecords(t, job, topology, assignment, 2)
			h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
			h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
			inject(h, job, assignment)

			h.start()
			h.markReady()
			h.lead(2)
			// Lose leadership mid-work; the next leader re-derives every
			// remaining action from the replicated view plus durable worker
			// state and completes without duplicate or conflicting manifests.
			h.waitFor(func() bool {
				record, _ := h.job(job)
				return record.Lifecycle == state.JobDraining || record.Lifecycle == state.JobSucceeded
			}, "job drained")
			h.follow(2)
			h.waitGateClosed()
			h.lead(3)
			record := waitForSucceeded(t, h, job)

			if len(record.Manifests) != 1 {
				t.Fatalf("manifests=%d", len(record.Manifests))
			}
			manifest := record.Manifests[replica.SinkTask]
			sealed := sealedTestArtifact(t, job, topology, replica.SinkTask, records[replica.SinkTask])
			if manifest.Checksum != sealed.Checksum || manifest.RecordCount != sealed.RecordCount || manifest.TotalBytes != sealed.TotalLength ||
				manifest.Replicas != replica || manifest.SpecificationHash != topology.Digest() {
				t.Fatalf("manifest=%+v want=%+v", manifest, sealed)
			}
			if manifest.ManifestRevision != 1 {
				t.Fatalf("conflicting re-sealed manifest revision=%d", manifest.ManifestRevision)
			}
			for _, node := range []uint16{replica.PrimaryNodeID, replica.SecondaryNodeID} {
				if sealedResultStream(t, h, node, sealed) == nil {
					t.Fatalf("node %d holds no durable copy", node)
				}
			}
			// The terminal Closed installation follows success even after
			// the leadership change.
			h.waitFor(func() bool { return len(h.workers.installsFor(replica.PrimaryNodeID, model.Closed)) > 0 }, "closed install")
		})
	}
}

// dropNthProposal makes the Nth proposal of one command kind fail once as an
// ambiguous transport loss.
func dropNthProposal(h *harness, kind string, occurrence int) {
	seen := 0
	h.raft.setProposeHook(func(command any) (bool, error) {
		if commandName(command) == kind {
			seen++
			if seen == occurrence {
				return false, errors.New("injected ambiguous drop")
			}
		}
		return true, nil
	})
}

// sealedTestArtifact derives the expected sealed artifact identity of one
// partition's exact record set.
func sealedTestArtifact(t *testing.T, job model.JobID, topology model.ValidatedTopology, sink model.TaskID, records []model.ResultRecord) protocol.ResultArtifact {
	t.Helper()
	artifact, _, err := worker.SealResultPartition(job, sink, topology.Digest(), records)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

// sealedResultStream returns one worker's durable sealed stream copy or nil.
func sealedResultStream(t *testing.T, h *harness, node uint16, artifact protocol.ResultArtifact) []byte {
	h.t.Helper()
	h.workers.mu.Lock()
	script, ok := h.workers.scripts[node]
	h.workers.mu.Unlock()
	if !ok || script.results == nil {
		return nil
	}
	state := script.results
	state.mu.Lock()
	defer state.mu.Unlock()
	stream, sealed := state.sealedStream[artifact.Checksum]
	if !sealed {
		return nil
	}
	return stream
}
