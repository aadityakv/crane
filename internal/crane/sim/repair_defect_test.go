package sim

import (
	"testing"

	"crane/internal/crane/state"
	"crane/internal/crane/store"
)

// The third cross-system production defect found by the Task 24 harness —
// the bilateral result-repair transfer had no production driver — is fixed
// by the destination-driven RepairDriver (worker/repair_driver.go, ruling
// "repair driver" in progress.md). This file is the inverted acceptance pin
// for that fix: one real four-process cluster, one job, one sink-replica
// crash after a committed checkpoint below EOF, and the job must reach
// Succeeded with the exact reference output.
//
// The pin exposed the FOURTH production defect (task-24-report.md
// "Production defects found", defect #4), fixed under its ruling; the
// signature it produced while open was:
//
//  1. After the replica pair is reassigned the driver repairs exactly the
//     records covered by the checkpoint vector at grant time (observed:
//     1 of 24). The surviving primary answers the replayed attempt's
//     duplicates for records above that watermark with Completed from its
//     old-provenance deliveries, so the watermark advances to EOF while
//     those records exist on ONE live copy only, and normal replication is
//     fail-closed for old-provenance results by design.
//  2. The seal-time re-repair with the final vector cannot happen: the
//     worker refuses a second grant for the same epoch/job/sink/role
//     (control_session.go installRepair, ErrIdentityReuse) and the
//     coordinator's repairSink treats two disagreeing holders (24 vs 1) as
//     "multiple disagreeing survivors" and leaves admission closed.
//  3. Once the Draining install landed and the seal failed, every later
//     activateJob pass first re-installs Closed at the same
//     JobControlRevision, which the store rejects (only Running<->Closed
//     progress at an equal fence per the defect #1 ruling) — the job cannot
//     even be re-driven.
//
// Observed signature (seed 0x5D3F0001): pair {3,1} -> {3,2}; repair
// 6fc9be4f… completes with 1 record on both endpoints; node 3 holds 24
// results and cursor wm=24; node 2 holds 1; installAssignment rev=2 state=1
// (Closed) is rejected forever with "identity reuse with different defining
// bytes" against the durable Draining install.

// runReplicaCrashAfterCommittedCheckpoint drives the acceptance scenario and
// returns the terminal lifecycle it observed.
func runReplicaCrashAfterCommittedCheckpoint(t *testing.T, seed uint64) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient("repair-acceptance")
	spec := newSimTopology("repair-acceptance", 1, 24, simStageSpec{})
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{})
	job := cluster.submit(client, plan)
	source := plan.sources[0]
	cluster.await("committed checkpoint above zero below EOF", func() bool {
		watermark := cluster.committedWatermark(job, source)
		return watermark > 0 && watermark < plan.sourceEOFs[source]
	})
	record, ok := cluster.jobRecord(job)
	if !ok || record.Assignment == nil || len(record.Assignment.ResultReplicas) == 0 {
		cluster.fail("repair acceptance lost the assignment before the replica crash")
	}
	replica := record.Assignment.ResultReplicas[0]
	victim := replica.SecondaryNodeID
	if victim == 0 {
		victim = replica.PrimaryNodeID
	}
	cluster.record("crash replica node=%d (pair %d/%d)", victim, replica.PrimaryNodeID, replica.SecondaryNodeID)
	cluster.stopNode(cluster.nodes[victim])
	cluster.oracle.noteEvent()
	lifecycle := cluster.awaitJobTerminal(job, "repair-acceptance")
	if lifecycle != state.JobSucceeded {
		cluster.fail("repair acceptance: job ended %d, want Succeeded", lifecycle)
	}
	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, "repair-acceptance")
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
			if repair.Instruction.JobID == job && repair.State != store.RepairComplete {
				cluster.fail("node %d repair %x ended in state %d", id, repair.Instruction.RepairID[:4], repair.State)
			}
		}
	}
}

// TestRepairAfterReplicaCrashCompletes is the inverted defect-#3 acceptance
// pin and the end-to-end acceptance pin for the defect-#4 fix (the holder
// re-replicates every retained record above the grant's vector to the
// replacement partner and retained custody answers the replayed attempt).
func TestRepairAfterReplicaCrashCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance simulations run full in-process clusters")
	}
	runReplicaCrashAfterCommittedCheckpoint(t, 0x5D3F0001)
}
