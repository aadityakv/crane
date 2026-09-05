package coordinator

import (
	"fmt"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
)

// TestSucceededJobRestoresSecondCopyAfterReplicaStoreLoss pins the terminal
// availability invariant: when a Succeeded job's result-replica worker loses
// its store and returns under a new epoch, the leader reassigns the replica,
// repairs the lost copy from the surviving holder, and re-seals the manifest
// bound to the live placement — the exact divergence that otherwise leaves
// post-restart result pages fenced off with stale authority forever after a
// second store loss takes the surviving copy.
func TestSucceededJobRestoresSecondCopyAfterReplicaStoreLoss(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	// A third eligible worker gives the reassignment a live destination.
	h.seedWorker(4, model.WorkerEpoch{4}, 8)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 8)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	h.start()
	h.markReady()
	h.lead(2)
	succeeded := waitForSucceeded(t, h, job)
	if succeeded.Manifests[replica.SinkTask].Replicas != replica {
		t.Fatalf("fixture manifest binds %+v want %+v", succeeded.Manifests[replica.SinkTask].Replicas, replica)
	}

	// The secondary loses its store and returns under a new worker epoch:
	// its committed manifest endpoint is now a dead incarnation.
	h.clearResultRecords(replica.SecondaryNodeID)
	view := h.machine.View()
	workerRecord, ok := h.workerRecord(replica.SecondaryNodeID)
	if !ok {
		t.Fatal("secondary worker record missing")
	}
	replacement := state.WorkerRecord{
		NodeID: workerRecord.NodeID, Epoch: model.WorkerEpoch{byte(workerRecord.NodeID), 0x11}, State: workerRecord.State, Revision: workerRecord.Revision + 1, Slots: workerRecord.Slots,
		ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
	}
	replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-store-loss", []byte{byte(workerRecord.NodeID)}), workerRecord.Revision, workerRecord.NodeID, workerRecord.Epoch, replacement, affectedForWorker(view, workerRecord.NodeID, workerRecord.Epoch), view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("seed replace: %v", err)
	}
	h.raft.applySeed(t, replace)
	h.addWorkerMember(workerRecord.NodeID, replacement.Epoch, workerRecord.Slots)
	script := h.workers.script(workerRecord.NodeID)
	h.workers.mu.Lock()
	script.identity.WorkerEpoch = replacement.Epoch
	h.workers.mu.Unlock()

	// The leader converges the succeeded job: reassignment, repair, re-seal.
	maintained := false
	for index := 0; index < 60 && !maintained; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok || record.Assignment == nil || record.Lifecycle != state.JobSucceeded {
			continue
		}
		manifest, sealed := record.Manifests[replica.SinkTask]
		maintained = sealed && record.Assignment.Revision > assignment.Revision &&
			manifest.ManifestRevision > succeeded.Manifests[replica.SinkTask].ManifestRevision &&
			manifest.Replicas == record.Assignment.ResultReplicas[0]
	}
	if !maintained {
		t.Fatalf("succeeded job never re-sealed onto the live placement: %v", h.log.snapshot())
	}

	record, _ := h.job(job)
	live := record.Assignment.ResultReplicas[0]
	sealed := succeeded.Manifests[replica.SinkTask]
	if live.PrimaryEpoch != replica.PrimaryEpoch {
		t.Fatalf("reassignment moved the surviving primary: %+v", live)
	}
	// The re-established second copy is durable: the replacement endpoint
	// holds the identical sealed stream.
	newHolder := live.SecondaryNodeID
	if stream := sealedResultStream(t, h, newHolder, protocol.ResultArtifact{
		JobID: sealed.JobID, SinkTask: sealed.SinkTask, RecordCount: sealed.RecordCount, TotalLength: sealed.TotalBytes, Checksum: sealed.Checksum,
	}); stream == nil {
		t.Fatalf("node %d holds no durable sealed copy after repair", newHolder)
	}
	if got := h.log.count(fmt.Sprintf("fetch:%d", live.PrimaryNodeID)); got == 0 {
		t.Fatalf("restoration never fetched the surviving copy: %v", h.log.snapshot())
	}
	if got := h.log.count(fmt.Sprintf("artifact:%d", newHolder)); got == 0 {
		t.Fatalf("no artifact reached the replacement endpoint: %v", h.log.snapshot())
	}
}

// TestSucceededJobNeverResealsShrunkenResultSet pins the integrity guard:
// when no current or retained holder can prove the sealed bytes — both
// durable copies lost before repair — the leader must refuse the re-seal
// rather than commit a shrunken (empty) manifest over the committed one.
// The loss is discharged through exactly one terminal MarkManifestLost whose
// committed marker keeps the no-shrink guarantee honest, after which Raft
// snapshot capture succeeds with the manifest↔placement divergence.
func TestSucceededJobNeverResealsShrunkenResultSet(t *testing.T) {
	h, job, topology, assignment := terminalHarness(t, 1)
	h.seedWorker(4, model.WorkerEpoch{4}, 8)
	h.addWorkerMember(4, model.WorkerEpoch{4}, 8)
	replica := assignment.ResultReplicas[0]
	records := terminalRecords(t, job, topology, assignment, 3)
	h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
	h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
	h.start()
	h.markReady()
	h.lead(2)
	succeeded := waitForSucceeded(t, h, job)
	committed := succeeded.Manifests[replica.SinkTask]

	// Every durable copy of the sealed artifact is destroyed and both
	// holders return under new epochs.
	for _, node := range []uint16{replica.PrimaryNodeID, replica.SecondaryNodeID} {
		h.clearResultRecords(node)
		view := h.machine.View()
		workerRecord, ok := h.workerRecord(node)
		if !ok {
			t.Fatalf("worker %d record missing", node)
		}
		replacement := state.WorkerRecord{
			NodeID: workerRecord.NodeID, Epoch: model.WorkerEpoch{byte(workerRecord.NodeID), 0x21}, State: workerRecord.State, Revision: workerRecord.Revision + 1, Slots: workerRecord.Slots,
			ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
		}
		replace, err := state.NewReplaceWorkerEpoch(testCommandID("seed-total-loss", []byte{byte(workerRecord.NodeID)}), workerRecord.Revision, workerRecord.NodeID, workerRecord.Epoch, replacement, affectedForWorker(view, workerRecord.NodeID, workerRecord.Epoch), view.CoordinatorEpoch)
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

	marked := false
	for index := 0; index < 60 && !marked; index++ {
		h.rescan()
		record, ok := h.job(job)
		if !ok {
			continue
		}
		marked = record.Manifests[replica.SinkTask].Lost
	}
	if !marked {
		t.Fatalf("total loss never declared the manifest lost: %v", h.log.snapshot())
	}
	record, _ := h.job(job)
	if record.Lifecycle != state.JobSucceeded {
		t.Fatalf("total loss left the lifecycle: %v", record.Lifecycle)
	}
	current := record.Manifests[replica.SinkTask]
	// (a) The re-seal never happened: the committed artifact identity — the
	// honest record of the destroyed bytes — is exactly what the marker kept.
	if current.ManifestRevision != committed.ManifestRevision+1 ||
		current.RecordCount != committed.RecordCount || current.TotalBytes != committed.TotalBytes || current.Checksum != committed.Checksum ||
		current.Replicas != committed.Replicas || !current.Lost || current.LostRevision != current.ManifestRevision {
		t.Fatalf("lost manifest = %#v want committed identity plus marker: %#v", current, committed)
	}
	// (b) Exactly one terminal MarkManifestLost was proposed.
	if got := h.log.count("propose:mark-lost"); got != 1 {
		t.Fatalf("mark-lost proposals=%d: %v", got, h.log.snapshot())
	}
	// No shrunken re-seal ever committed over the lost identity.
	if got := h.log.count("propose:seal"); got != 1 {
		t.Fatalf("seal proposals=%d: %v", got, h.log.snapshot())
	}
	// (c) Raft snapshot capture succeeds after the marker commits even
	// though the manifest placement diverges from the live assignment.
	view := h.machine.View()
	snapshot, err := h.machine.Capture(view.AppliedIndex, 2)
	if err != nil {
		t.Fatalf("capture after total loss: %v", err)
	}
	if _, err := snapshot.MarshalBinary(); err != nil {
		t.Fatalf("marshal captured snapshot: %v", err)
	}
}
