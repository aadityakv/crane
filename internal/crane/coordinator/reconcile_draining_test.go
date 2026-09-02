package coordinator

import (
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
)

// storeSchedulingRule models the durable worker store's equal-revision
// install contract (store/records.go applyAssignment, Task 24 defect #1
// ruling): an identical install is idempotent, a strictly newer coordinator
// epoch rebinds and records the incoming scheduling state, exactly
// Closed<->Running progress at an equal fence, and any other scheduling
// change at an equal JobControlRevision is identity reuse.
func storeSchedulingRule() func(node uint16, install protocol.AssignmentSetInstall) error {
	type key struct {
		node uint16
		job  model.JobID
	}
	last := make(map[key]protocol.AssignmentSetInstall)
	newer := func(left, right model.CoordinatorEpoch) bool {
		if left.Term != right.Term {
			return left.Term > right.Term
		}
		return left.BeginIndex > right.BeginIndex
	}
	return func(node uint16, install protocol.AssignmentSetInstall) error {
		id := key{node: node, job: install.Assignment.JobID}
		prior, ok := last[id]
		if ok && prior.Assignment.Revision == install.Assignment.Revision {
			switch {
			case prior.SchedulingState == install.SchedulingState && prior.JobControlRevision == install.JobControlRevision && prior.CoordinatorEpoch == install.CoordinatorEpoch:
				return nil
			case newer(install.CoordinatorEpoch, prior.CoordinatorEpoch):
			case install.CoordinatorEpoch == prior.CoordinatorEpoch && (prior.SchedulingState == model.Running && install.SchedulingState == model.Closed || prior.SchedulingState == model.Closed && install.SchedulingState == model.Running):
			case install.JobControlRevision <= prior.JobControlRevision:
				return model.ErrIdentityReuse
			}
		}
		last[id] = install
		return nil
	}
}

// TestDrainingJobReactivatesWithoutClosedPreInstall pins the Task 24 defect
// #5 ruling: once a job is Draining its workers durably hold the Draining
// install at the bumped JobControlRevision, and the store refuses
// Draining->Closed and Closed->Draining at that equal fence. activateJob must
// therefore never demand the Closed pre-install for a Draining job and must
// carry the Draining install (the current JobControlRevision, or a new
// epoch's rebind) to every worker BEFORE the checkpoint notices validated
// against it — under the same leadership session (after a seal attempt
// failed in the pass that installed Draining) and under a new leadership
// epoch — so notices are re-sent, repair verification runs, the pass
// converges, and the seal completes.
func TestDrainingJobReactivatesWithoutClosedPreInstall(t *testing.T) {
	for _, leaderChange := range []bool{false, true} {
		name := "same leadership session"
		if leaderChange {
			name = "new leadership epoch"
		}
		t.Run(name, func(t *testing.T) {
			h, job, topology, assignment := terminalHarness(t, 1)
			replica := assignment.ResultReplicas[0]
			records := terminalRecords(t, job, topology, assignment, 3)
			h.seedResultRecords(replica.PrimaryNodeID, records[replica.SinkTask]...)
			h.seedResultRecords(replica.SecondaryNodeID, records[replica.SinkTask]...)
			h.workers.installRule = storeSchedulingRule()
			// The first seal attempt (same pass as the Draining install) fails.
			h.blockInventory(replica.SinkTask)

			h.start()
			h.markReady()
			h.lead(2)
			h.waitFor(func() bool {
				record, ok := h.job(job)
				return ok && record.Lifecycle == state.JobDraining && len(h.workers.installsFor(replica.PrimaryNodeID, model.Draining)) > 0
			}, "job drains with the Draining install recorded")
			// Await fault consumption before releasing it: the draining pass must
			// have fired both blocked inventory queries (the activation repair
			// query and the seal-partition query) before the block lifts, or the
			// unblock can race into that very pass, complete its seal, and finish
			// the job without ever re-driving the activation whose checkpoint
			// resend this test pins.
			h.waitFor(func() bool {
				return countPrefix(h.log.snapshot(), "inventory:") >= 4
			}, "draining pass consumed both blocked inventory queries")
			noticesBefore := countPrefix(h.log.snapshot(), "checkpoint:")
			if leaderChange {
				h.follow(3)
				h.waitGateClosed()
				h.lead(4)
			}
			h.unblockInventory(replica.SinkTask)
			succeeded := func() bool {
				record, ok := h.job(job)
				return ok && record.Lifecycle == state.JobSucceeded
			}
			for index := 0; index < 20 && !succeeded(); index++ {
				h.rescan()
			}
			record := waitForSucceeded(t, h, job)
			if len(record.Manifests) != 1 {
				t.Fatalf("manifests=%d", len(record.Manifests))
			}
			if rejected := countPrefix(h.log.snapshot(), "install-rejected:"); rejected != 0 {
				t.Fatalf("coordinator demanded a scheduling change the store refuses on a Draining job (%d rejected installs): %v", rejected, h.log.snapshot())
			}
			if countPrefix(h.log.snapshot(), "checkpoint:") <= noticesBefore {
				t.Fatalf("checkpoint notices were never re-sent to the Draining job's workers: %v", h.log.snapshot())
			}
			h.waitFor(func() bool {
				return len(h.workers.installsFor(replica.PrimaryNodeID, model.Closed)) > 1
			}, "terminal Closed install after success")
		})
	}
}

func countPrefix(entries []string, prefix string) int {
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry, prefix) {
			count++
		}
	}
	return count
}
