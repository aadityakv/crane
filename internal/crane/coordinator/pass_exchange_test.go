package coordinator

import (
	"errors"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/swim"
)

// TestRegisterWorkersSkipsHandshakeForUnchangedMembers pins the session
// handshake memory: the first pass dials every active member, a pass over an
// unchanged membership view performs zero handshake dials, a membership delta
// re-handshakes exactly the changed member, and a view-record epoch mismatch
// re-handshakes and drives the replace path.
func TestRegisterWorkersSkipsHandshakeForUnchangedMembers(t *testing.T) {
	h := newHarness(t)
	h.seedEpoch()
	h.seedWorker(2, model.WorkerEpoch{2}, 4)
	h.seedWorker(3, model.WorkerEpoch{3}, 4)
	h.addWorkerMember(2, model.WorkerEpoch{2}, 4)
	h.addWorkerMember(3, model.WorkerEpoch{3}, 4)
	h.start()
	h.markReady()
	h.lead(2)
	h.waitGateOpen()
	h.waitFor(func() bool {
		return h.log.count("handshake:2") >= 1 && h.log.count("handshake:3") >= 1
	}, "first pass handshakes every member")

	// pass-exchange elimination: an unchanged membership view performs zero
	// handshake dials on later passes.
	h.rescan()
	h.waitFor(func() bool {
		return h.log.count("status:2") >= 2 && h.log.count("status:3") >= 2
	}, "second pass drained both workers")
	unchangedTwo, unchangedThree := h.log.count("handshake:2"), h.log.count("handshake:3")
	if unchangedTwo != 1 || unchangedThree != 1 {
		t.Fatalf("first-pass handshake dials = %d/%d, want 1/1", unchangedTwo, unchangedThree)
	}

	// An incarnation bump re-handshakes exactly the changed member.
	h.members.setMember(swim.Member{NodeID: 3, Host: "127.0.0.1", BasePort: 9000, Incarnation: 2, Status: swim.Alive})
	h.rescan()
	h.waitFor(func() bool { return h.log.count("handshake:3") == unchangedThree+1 }, "incarnation-bumped member re-handshaken")
	if got := h.log.count("handshake:2"); got != unchangedTwo {
		t.Fatalf("unchanged member handshakes = %d, want %d", got, unchangedTwo)
	}

	// A status change re-handshakes exactly the changed member.
	h.setMemberStatus(2, swim.Suspect)
	h.rescan()
	h.waitFor(func() bool { return h.log.count("handshake:2") == unchangedTwo+1 }, "status-changed member re-handshaken")
	h.waitFor(func() bool { return h.clk.PendingTimers() > 0 }, "prior pass quiesced")
	if got := h.log.count("handshake:3"); got != unchangedThree+1 {
		t.Fatalf("unchanged member handshakes = %d, want %d", got, unchangedThree+1)
	}

	// A view-record epoch mismatch re-handshakes and drives the replace
	// path. Member 3's worker restarts with a fresh store (new incarnation
	// and advertised worker epoch) while every ReplaceWorkerEpoch proposal
	// is dropped, leaving the replicated record at the old epoch.
	h.raft.setProposeHook(func(command any) (bool, error) {
		if _, ok := command.(state.ReplaceWorkerEpoch); ok {
			return false, errors.New("injected replace drop")
		}
		return true, nil
	})
	script := h.workers.script(3)
	h.workers.mu.Lock()
	script.identity.WorkerEpoch = model.WorkerEpoch{3, 0xB}
	h.workers.mu.Unlock()
	h.members.setMember(swim.Member{NodeID: 3, Host: "127.0.0.1", BasePort: 9000, Incarnation: 3, Status: swim.Alive})
	h.rescan()
	h.waitFor(func() bool { return h.log.count("handshake:3") == unchangedThree+2 }, "restarted member re-handshaken")
	// The pass quiesces with the dropped replace unresolved and the
	// replicated record still at the old epoch.
	h.waitFor(func() bool { return h.clk.PendingTimers() > 0 }, "dropped-replace pass quiesced")
	if got := h.log.count("propose:replace-epoch"); got != 3 {
		t.Fatalf("dropped replace attempts = %d, want 3", got)
	}
	if record, ok := h.workerRecord(3); !ok || record.Epoch != (model.WorkerEpoch{3}) {
		t.Fatalf("dropped replace still advanced the record: %#v", record)
	}

	// With membership settled again, only the view record's epoch mismatch
	// re-arms the dial, and the replace path is driven to completion.
	h.raft.setProposeHook(nil)
	fencesThree := h.log.count("fence:3")
	h.rescan()
	h.waitFor(func() bool {
		record, ok := h.workerRecord(3)
		return ok && record.Epoch == (model.WorkerEpoch{3, 0xB})
	}, "mismatch re-handshake drove the replace path")
	if got := h.log.count("handshake:3"); got != unchangedThree+3 {
		t.Fatalf("mismatch-triggered handshakes = %d, want %d", got, unchangedThree+3)
	}
	// The replaced incarnation is re-fenced in the same pass: fence memory
	// is scoped to the exact worker incarnation.
	h.waitFor(func() bool { return h.log.count("fence:3") >= fencesThree+1 }, "replaced incarnation re-fenced")

	// Everything is settled again: no further handshake dials.
	statusThree := h.log.count("status:3")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:3") >= statusThree+1 }, "settled pass drained member three")
	if got := h.log.count("handshake:2"); got != unchangedTwo+1 {
		t.Fatalf("member two handshakes = %d, want %d", got, unchangedTwo+1)
	}
	if got := h.log.count("handshake:3"); got != unchangedThree+3 {
		t.Fatalf("member three handshakes = %d, want %d", got, unchangedThree+3)
	}
}

// TestFenceWorkersFencesOnDemandAfterFirstSweep pins the on-demand fencing
// policy: the first cluster pass of a session fences every non-Offline
// worker, later passes fence nobody settled, a worker whose last observed
// admission epoch is older than the session epoch is re-fenced, a previously
// failed fence retries, and a new session's epoch establishment fences
// everyone again.
func TestFenceWorkersFencesOnDemandAfterFirstSweep(t *testing.T) {
	h, _, _, _ := runningHarness(t)
	older := h.view().CoordinatorEpoch
	h.start()
	h.markReady()
	h.lead(2)
	h.waitFor(func() bool {
		return len(h.workers.installsFor(2, model.Running)) > 0 && len(h.workers.installsFor(3, model.Running)) > 0
	}, "first pass installed running")

	// pass-exchange elimination: the first sweep fences each worker exactly
	// once and an unchanged pass fences nobody.
	h.rescan()
	h.waitFor(func() bool {
		return h.log.count("status:2") >= 2 && h.log.count("status:3") >= 2
	}, "second pass drained both workers")
	firstTwo, firstThree := h.log.count("fence:2"), h.log.count("fence:3")
	if firstTwo != 1 || firstThree != 1 {
		t.Fatalf("first sweep fence dials = %d/%d, want 1/1", firstTwo, firstThree)
	}

	// A worker whose last observed admission epoch is older than the
	// session epoch is fenced on the next pass.
	h.workers.admitUnderEpoch(3, older)
	statusThree := h.log.count("status:3")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:3") >= statusThree+1 }, "older admission observed")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("fence:3") == firstThree+1 }, "older-admission worker re-fenced")
	if got := h.log.count("fence:2"); got != firstTwo {
		t.Fatalf("healthy worker fenced = %d, want %d", got, firstTwo)
	}
	statusThree = h.log.count("status:3")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:3") >= statusThree+1 }, "post-refence pass drained member three")
	if got := h.log.count("fence:3"); got != firstThree+1 {
		t.Fatalf("settled worker re-fenced = %d, want %d", got, firstThree+1)
	}

	// A previously failed fence retries on every later pass until it
	// acknowledges. Node 2's admission is first observed under the older
	// epoch so its fence re-arms, then every attempt fails.
	script := h.workers.script(2)
	h.workers.mu.Lock()
	script.fenceErr = errors.New("fence unreachable")
	h.workers.mu.Unlock()
	h.workers.admitUnderEpoch(2, older)
	statusTwo := h.log.count("status:2")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:2") >= statusTwo+1 }, "node two older admission observed")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("fence:2") == firstTwo+2 }, "failed fence retried once within the pass")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("fence:2") == firstTwo+4 }, "failed fence retried on the next pass")
	h.workers.mu.Lock()
	script.fenceErr = nil
	h.workers.mu.Unlock()
	h.rescan()
	h.waitFor(func() bool { return h.log.count("fence:2") == firstTwo+5 }, "recovered fence acknowledged")
	statusTwo = h.log.count("status:2")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:2") >= statusTwo+1 }, "settled pass drained member two")
	if got := h.log.count("fence:2"); got != firstTwo+5 {
		t.Fatalf("settled worker re-fenced = %d, want %d", got, firstTwo+5)
	}

	// A new session (epoch bump) fences every worker again.
	sweepTwo, sweepThree := h.log.count("fence:2"), h.log.count("fence:3")
	// Snapshot the drains now: nothing runs until the new session's first
	// pass, whose drain and its successor's drain must both appear.
	statusTwo = h.log.count("status:2")
	h.follow(2)
	h.waitGateClosed()
	h.lead(3)
	h.waitFor(func() bool {
		return h.view().CoordinatorEpoch.Term == 3 &&
			h.log.count("fence:2") == sweepTwo+1 && h.log.count("fence:3") == sweepThree+1
	}, "new session sweeps every fence")
	h.rescan()
	h.waitFor(func() bool { return h.log.count("status:2") >= statusTwo+2 }, "new session's first and second passes drained")
	if got := h.log.count("fence:2"); got != sweepTwo+1 {
		t.Fatalf("new session re-fenced a settled worker: %d, want %d", got, sweepTwo+1)
	}
	if got := h.log.count("fence:3"); got != sweepThree+1 {
		t.Fatalf("new session re-fenced a settled worker: %d, want %d", got, sweepThree+1)
	}
}
