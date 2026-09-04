package sim

// Acceptance pin for the Task 24 defect #5 fix (retained-custody wedge after
// a coordinator-epoch change). GREEN by design: the crash-received shape
// must complete once custody published under a superseded coordinator fence
// can re-enter the pipeline, and the fast 14s probe here also covers the
// scenarios worker_crash_after_received_before_ack and
// worker_crash_after_processed_before_ack at the full 30000-step budget.
//
// Fixed defect summary (see the session report and task-24-report.md): when
// a Raft leadership change advanced the coordinator epoch while a source
// worker still retained a pending CompletionReport emitted under the
// superseded fence, the checkpoint chain used to deadlock permanently:
//
//  1. state/checkpoints.go applyAdvanceCheckpointLocked rejected the proposal
//     deterministically, because validFence demanded
//     report.Epoch == machine.coordinatorEpoch and the report's epoch is
//     frozen at publication (ResultInvalidTarget, wire code 6).
//  2. coordinator/checkpoint.go handleCompletionEvent re-read the view, the
//     job was live and the report's token was still current, so the rejection
//     was classified "transiently false" and stayed a retryable error; every
//     reconciliation pass re-proposed the same AdvanceCheckpoint and never
//     converged, so the leader's admission gate never reopened.
//  3. worker/checkpoint.go publishContiguousCompletions refused to supersede
//     the pending report (pendingCompletion), so the worker never re-published
//     under the current fence.
//
// The fix (5137db7) readopts retained custody under the new epoch: the state
// machine accepts a report whose epoch is equal to or strictly ordered before
// the committed coordinator epoch when every other fence holds exactly, and
// the worker re-admits recovered Received custody and retained outbox
// emissions after byte-exact re-validation against the current install. The
// on-wedge dump below is retained for future regressions: it names the exact
// contradiction — a pending completion report whose Epoch no longer equals
// the machine's coordinator epoch.

import (
	"fmt"
	"testing"

	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/crane/store"
)

// probeDump records one node's raft role/term, admission gate, and replicated
// coordination epoch so a wedged scenario shows exactly who leads, who
// coordinates, and which gates are open.
func probeDump(cluster *simCluster, why string) {
	cluster.t.Helper()
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		if node == nil || node.runtime == nil {
			cluster.record("%s: node=%d stopped", why, id)
			continue
		}
		status := node.runtime.Raft.Status()
		epoch, open := node.runtime.Gate.AdmissionEpoch()
		machineText := "none"
		if node.runtime.Machine != nil {
			view := node.runtime.Machine.View()
			machineText = fmt.Sprintf("{t=%d i=%d c=%d}", view.CoordinatorEpoch.Term, view.CoordinatorEpoch.BeginIndex, view.CoordinatorEpoch.Coordinator)
			for _, job := range view.Jobs {
				var watermarks []string
				for source, checkpoint := range job.Checkpoints {
					watermarks = append(watermarks, fmt.Sprintf("%d:%d/rev=%d", source.Partition, checkpoint.Watermark, checkpoint.Revision))
				}
				machineText += fmt.Sprintf(" life=%d wm=[%s]", job.Lifecycle, joinProbeStrings(watermarks))
			}
		}
		cluster.record("%s: node=%d raft(role=%d term=%d lead=%d commit=%d last=%d applied=%d) gate(open=%t coord=%d term=%d) machine=%s",
			why, id, status.Role, status.Term, status.LeaderID, status.CommitIndex, status.LastIndex, status.AppliedIndex,
			open, epoch.Coordinator, epoch.Term, machineText)
	}
}

// joinProbeStrings joins for compact trace dumps.
func joinProbeStrings(parts []string) string {
	out := ""
	for index, part := range parts {
		if index > 0 {
			out += " "
		}
		out += part
	}
	return out
}

// runStallProbe drives the crash-received shape with bounded awaits and
// periodic world dumps. dropAck selects whether the crashed node's outbound
// +5 datagrams are dropped (the ACK-loss fault); crashTarget 0 picks
// whichever node first holds a Received delivery (the scenario's own rule).
// The wedge requires that node to be the current Raft leader — the real-TCP
// interleavings decide placement per seed, so the hunt loops a few seeds.
func runStallProbe(t *testing.T, name string, seed uint64, dropAck bool, crashTarget uint16) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient(name)
	spec := newSimTopology(name, 1, 8, simStageSpec{})
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{})
	job := cluster.submit(client, plan)
	cluster.await("job running", func() bool {
		record, ok := cluster.jobRecord(job)
		return ok && record.Lifecycle == state.JobRunning
	})
	var target uint16
	cluster.await("durable custody accepted", func() bool {
		if crashTarget != 0 {
			target = crashTarget
			return cluster.workerStore(target) != nil
		}
		found, _, ok := cluster.deliveryInState(job, store.Received)
		target = found
		return ok
	})
	cluster.record("probe %s: crashing target node=%d dropAck=%t seed=%d", name, target, dropAck, seed)
	if dropAck {
		cluster.dropTupleDatagrams(target, name+"-ack-loss")
	}
	cluster.stopNode(cluster.nodes[target])
	cluster.pump(80)
	cluster.restartNode(cluster.nodes[target], false)
	// Bounded wait for terminal with periodic dumps; the wedge fails fast
	// with the instrumented trace instead of the full 30000-step budget.
	if !cluster.awaitOptionally(9000, func() bool {
		if cluster.step.Load()%500 == 0 {
			probeDump(cluster, "probe")
			cluster.recordJobState(job)
		}
		record, ok := cluster.jobRecord(job)
		return ok && (record.Lifecycle == state.JobSucceeded || record.Lifecycle == state.JobFailed || record.Lifecycle == state.JobCanceled)
	}) {
		probeDump(cluster, "wedge")
		// Name the exact contradiction: every retained pending completion
		// report against the machine's current coordinator epoch.
		for _, id := range cluster.ids {
			handle := cluster.workerStore(id)
			if handle == nil {
				continue
			}
			work, err := handle.RecoverWork()
			if err != nil {
				continue
			}
			for _, event := range work.PendingEvents {
				if event.Completion != nil {
					report := event.Completion
					cluster.record("wedge: node=%d pending completion report tx=%d source=stage%d/p%d prior=%d new=%d eof=%d reportEpoch={t=%d i=%d c=%d}",
						id, event.TransactionID, report.Source.StageID, report.Source.Partition, report.Prior, report.New, report.EOF,
						report.Epoch.Term, report.Epoch.BeginIndex, report.Epoch.Coordinator)
				}
				if event.Failure != nil {
					cluster.record("wedge: node=%d pending failure report tx=%d", id, event.TransactionID)
				}
			}
		}
		if view, _, viewOK := cluster.leaderView(); viewOK {
			cluster.record("wedge: machine coordinator epoch={t=%d i=%d c=%d}", view.CoordinatorEpoch.Term, view.CoordinatorEpoch.BeginIndex, view.CoordinatorEpoch.Coordinator)
		}
		cluster.fail("probe %s: job wedged after crash of node=%d (seed=%d)", name, target, seed)
	}
	cluster.record("probe %s: completed after crash of node=%d", name, target)
}

// TestRetainedCustodyWedgeAfterLeadershipChange is the acceptance pin: it
// repeats the crash-received scenario on neighboring seeds and every repeat
// must complete after the retained custody under the superseded coordinator
// fence re-enters the pipeline (fix 5137db7). A regression reproduces the
// old wedge and fails with the pending-report vs machine-epoch evidence.
func TestRetainedCustodyWedgeAfterLeadershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("stall probes run full in-process clusters")
	}
	for i := uint64(0); i < 4; i++ {
		runStallProbe(t, fmt.Sprintf("probe-%d", i), 0x5CF00006+i, true, 0)
	}
}

// TestStallProbeBystanderCrash is the negative control for the datagram
// fault: node 1 holds no task and no replica duty on this topology, so its
// crash-with-drop advances nothing that retains custody; the job completes.
func TestStallProbeBystanderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("stall probes run full in-process clusters")
	}
	runStallProbe(t, "probe-bystander", 0x5CF00006, true, 1)
}

// TestStallProbeCrashWithoutDrop is the negative control for the ACK-loss
// fault: the same crash target selection without any datagram drop completes
// whenever the crash does not depose the leader with custody in flight.
func TestStallProbeCrashWithoutDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("stall probes run full in-process clusters")
	}
	runStallProbe(t, "probe-no-drop", 0x5CF00006, false, 0)
}
