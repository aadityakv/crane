package sim

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/state"
)

// The acceptance pin for the second cross-system production defect found by
// the Task 24 harness, now fixed (96f4421 "fix: adopt committed checkpoint
// watermarks").
//
// The coordinator re-delivers a committed source checkpoint to every current
// worker of a job (coordinator/reconcile.go resendCheckpointNotices during
// every activateJob pass, and coordinator/checkpoint.go handleCompletionEvent
// right after the checkpoint commits) as a +5 210 CheckpointNotice whose
// RaftIndex, CoordinatorEpoch, and JobControlRevision are the CURRENT view
// values at send time — not the values under which that worker originally
// applied the watermark. Before the fix the worker's idempotent-resend
// validation demanded exact equality with the retained durable proof and the
// seal path demanded the proof equal the CURRENT fence, so after any epoch
// advance, JobControlRevision bump, or further Raft commit the job wedged
// non-terminal forever (evidence: seed 0x51A50001, "raftIdx notice=18
// cursor=14, epoch {T3}→{T1}, jcr 4→3"), and a reassigned source owner could
// not adopt the prior committed watermark at all.
//
// Under the Task 24 defect #2 ruling a 210 notice arriving over a valid
// current-fence authenticated +5 session is the current coordinator's
// authoritative statement of the replicated committed watermark: the worker
// CONFIRMS equal- and below-watermark resends without mutation regardless of
// authority age, ADOPTS strictly higher watermarks (including a reassigned
// owner's first watermark) under the current authority proof without any
// pending CompletionReport, and the seal/inventory paths accept the retained
// durable checkpoint proof as committed-watermark evidence. Both scenarios
// below drive the full production stack through exactly the interleavings
// that wedged before the fix and must now reach Succeeded with the exact
// reference output.

// runLeaderChangeAfterCommittedCheckpoint crashes the leader once after the
// job's first committed checkpoint above zero; the new coordinator epoch must
// re-establish the checkpoint evidence on every worker (resend confirmed
// under send-time authority) and the job must complete.
func runLeaderChangeAfterCommittedCheckpoint(t *testing.T, seed uint64) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient("notice-acceptance")
	spec := newSimTopology("notice-acceptance", 1, 6, simStageSpec{})
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{})
	job := cluster.submit(client, plan)
	cluster.await("first committed checkpoint above zero", func() bool {
		for _, source := range plan.sources {
			if cluster.committedWatermark(job, source) > 0 {
				return true
			}
		}
		return false
	})
	if lifecycle := cluster.jobLifecycle(job); lifecycle == state.JobSucceeded || lifecycle == state.JobFailed || lifecycle == state.JobCanceled {
		cluster.fail("acceptance job already terminal %d before the leadership change", lifecycle)
	}
	cluster.crashLeader()
	cluster.await("new leader elected", func() bool {
		return cluster.oracle.currentLeader() != 0
	})
	lifecycle := cluster.awaitJobTerminal(job, "notice-acceptance")
	if lifecycle != state.JobSucceeded {
		cluster.fail("notice-acceptance job ended %d after one leader crash, want Succeeded", lifecycle)
	}
	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, "notice-acceptance")
}

// runSourceOwnerCrashAboveCommittedCheckpoint crashes the source owner once
// the job holds a committed checkpoint above zero but below EOF; the source
// must be reassigned, and the replacement owner must ADOPT the committed
// watermark (it holds no local CompletionReport for it) so emission resumes
// strictly above the committed prefix — no duplicate, no loss.
func runSourceOwnerCrashAboveCommittedCheckpoint(t *testing.T, seed uint64) {
	t.Helper()
	cluster := newSimCluster(t, seed)
	cluster.startAll()
	cluster.awaitSteady()
	client := cluster.newClient("owner-adopt")
	spec := newSimTopology("owner-adopt", 1, 6, simStageSpec{})
	plan := newSimJobPlan(t, client.store.NextRequestID(), spec, simStageSpec{})
	job := cluster.submit(client, plan)
	source := plan.sources[0]
	cluster.await("committed checkpoint above zero but below EOF", func() bool {
		watermark := cluster.committedWatermark(job, source)
		return watermark > 0 && watermark < plan.sourceEOFs[source]
	})
	record, ok := cluster.jobRecord(job)
	if !ok || record.Assignment == nil {
		cluster.fail("owner-adopt lost the job record before the owner crash")
	}
	owner := uint16(0)
	for _, token := range record.Assignment.Tasks {
		if token.Task == source {
			owner = token.WorkerID
			break
		}
	}
	if owner == 0 {
		cluster.fail("owner-adopt found no owner token for the source")
	}
	committed := cluster.committedWatermark(job, source)
	cluster.record("crash source owner=%d above committed watermark %d", owner, committed)
	cluster.stopNode(cluster.nodes[owner])
	cluster.oracle.noteEvent()
	// The replacement owner must durably adopt at least the committed
	// watermark before the job can finish; the acceptance is the terminal
	// state plus the exact reference output either way.
	lifecycle := cluster.awaitJobTerminal(job, "owner-adopt")
	if lifecycle != state.JobSucceeded {
		cluster.fail("owner-adopt job ended %d after one source-owner crash, want Succeeded", lifecycle)
	}
	adopted := false
	for _, id := range cluster.ids {
		if id == owner {
			continue
		}
		handle := cluster.workerStore(id)
		if handle == nil {
			continue
		}
		work, err := handle.RecoverWork()
		if err != nil {
			continue
		}
		for _, cursor := range work.Sources {
			if cursor.Source == source && cursor.Watermark >= committed {
				adopted = true
			}
		}
	}
	if !adopted {
		cluster.fail("owner-adopt no surviving node durably holds the committed watermark %d", committed)
	}
	records := cluster.pageResult(client, job)
	cluster.oracle.verifyFinal(job, records, "owner-adopt")
}

// TestCheckpointNoticeResendAfterAuthorityAdvanceSucceeds is the end-to-end
// acceptance pin for the fixed defect #2: notice resends after any authority
// advance and reassigned-owner adoption of a committed watermark now succeed.
func TestCheckpointNoticeResendAfterAuthorityAdvanceSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance simulations run full in-process clusters")
	}
	// Both crashes remove a replica-bearing node. The defect-#2 mechanics
	// are observed live (the replacement adopts the committed watermark,
	// the resend confirms under the new authority, the repair completes on
	// both endpoints) and the defect-#4 fix then re-establishes the second
	// copy of every retained record above the grant's vector.
	t.Run("leader change after committed checkpoint", func(t *testing.T) {
		runLeaderChangeAfterCommittedCheckpoint(t, 0x5D2E0001)
	})
	t.Run("source owner crash above committed checkpoint", func(t *testing.T) {
		runSourceOwnerCrashAboveCommittedCheckpoint(t, 0x5D2E0002)
	})
}
