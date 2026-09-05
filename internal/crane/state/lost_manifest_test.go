package state

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// lostMarkerSuccessor derives the exact legal lost successor of one committed
// manifest under one declaring fence.
func lostMarkerSuccessor(manifest ResultManifest, epoch model.CoordinatorEpoch) ResultManifest {
	next := manifest
	next.ManifestRevision = manifest.ManifestRevision + 1
	next.Lost = true
	next.LostRevision = next.ManifestRevision
	next.LostEpoch = epoch
	return next
}

// totalLossState commits one Succeeded job whose committed manifest is
// terminally diverged from the live placement: the secondary holder was
// deactivated, the assignment replaced onto current workers, and — until the
// marker is applied — no re-seal could ever legally re-bind the manifest.
func totalLossState(t *testing.T) (*Machine, model.JobID, ResultManifest) {
	t.Helper()
	machine, job, topology, assignment, manifest := succeededSealedJob(t)
	secondary := assignment.ResultReplicas[0].SecondaryNodeID
	secondaryEpoch := assignment.ResultReplicas[0].SecondaryEpoch
	workerBefore := machine.workers[secondary]
	jobBefore := machine.jobs[job]
	affected := []AffectedAssignment{{JobID: job, JobControlRevision: jobBefore.JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xa3}, workerBefore.Revision, secondary, secondaryEpoch, affected)
	if got := applyTask10(t, machine, 60, deactivate); got.Code != ResultSuccess {
		t.Fatalf("deactivate = %#v", got)
	}
	marked := machine.jobs[job]

	placements := task10EligiblePlacements(machine)
	var target model.AssignmentSet
	for attempt := 0; ; attempt++ {
		candidate, err := model.BuildAssignmentSet(job, topology.Digest(), assignment.Revision+1, topology, rotateFirstPlacement(placements, attempt))
		if err != nil {
			t.Fatal(err)
		}
		target = candidate
		if replica := candidate.ResultReplicas[0]; replica.SecondaryNodeID != secondary && replica.SecondaryNodeID != replica.PrimaryNodeID {
			break
		}
	}
	replace, _ := NewReplaceAssignments(InternalCommandID{0xa4}, marked.JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), target)
	if got := applyTask10(t, machine, 61, replace); got.Code != ResultSuccess {
		t.Fatalf("replace = %#v", got)
	}
	if machine.jobs[job].Manifests[manifest.SinkTask].Replicas == machine.jobs[job].Assignment.ResultReplicas[0] {
		t.Fatal("fixture must diverge the manifest from the live placement")
	}
	return machine, job, manifest
}

// TestMarkManifestLostConstructorBindsTheDeclaringFence pins the structural
// contract: the constructor accepts exactly the committed identity plus a
// lost marker bound to the fence, and rejects a successor without the marker
// or bound to a different fence.
func TestMarkManifestLostConstructorBindsTheDeclaringFence(t *testing.T) {
	machine, _, manifest := totalLossState(t)
	epoch := machine.coordinatorEpoch

	next := lostMarkerSuccessor(manifest, epoch)
	command, err := NewMarkManifestLost(InternalCommandID{0xb1}, manifest.ManifestRevision, next, epoch)
	if err != nil {
		t.Fatalf("legal lost declaration rejected: %v", err)
	}
	if _, err := MarshalCommand(command); err != nil {
		t.Fatalf("canonical encoding rejected: %v", err)
	}

	retained := manifest
	retained.ManifestRevision = manifest.ManifestRevision + 1
	if _, err := NewMarkManifestLost(InternalCommandID{0xb2}, manifest.ManifestRevision, retained, epoch); err == nil {
		t.Fatal("retained successor accepted as a lost declaration")
	}

	wrongFence := next
	wrongFence.LostEpoch.Term++
	if _, err := NewMarkManifestLost(InternalCommandID{0xb3}, manifest.ManifestRevision, wrongFence, epoch); err == nil {
		t.Fatal("lost marker bound to a foreign fence accepted")
	}
	if next.LostRevision != next.ManifestRevision {
		t.Fatalf("lost revision = %d want %d", next.LostRevision, next.ManifestRevision)
	}
}

// TestMarkManifestLostAppliesOnlyForSucceededJobs pins the lifecycle guard:
// nonterminal, Failed, and Canceled jobs are rejected — they retain no
// availability duty the marker could discharge — and only the exact
// committed identity of a Succeeded job's manifest may be declared lost.
func TestMarkManifestLostAppliesOnlyForSucceededJobs(t *testing.T) {
	for _, lifecycle := range []JobLifecycle{JobPending, JobDeploying, JobRunning, JobDraining, JobFailed, JobCanceled} {
		t.Run(lifecycleName(lifecycle), func(t *testing.T) {
			machine, job, _, _, manifest := succeededSealedJob(t)
			if lifecycle != JobSucceeded {
				record := cloneJobRecord(machine.jobs[job])
				record.Lifecycle = lifecycle
				machine.jobs[job] = record
			}
			next := lostMarkerSuccessor(manifest, machine.coordinatorEpoch)
			command, err := NewMarkManifestLost(InternalCommandID{0xb4}, manifest.ManifestRevision, next, machine.coordinatorEpoch)
			if err != nil {
				t.Fatalf("constructor: %v", err)
			}
			if got := applyTask10(t, machine, 70, command); got.Code != ResultInvalidTarget {
				t.Fatalf("%s job accepted a lost declaration: %#v", lifecycleName(lifecycle), got)
			}
		})
	}
	machine, job, manifest := totalLossState(t)
	next := lostMarkerSuccessor(manifest, machine.coordinatorEpoch)
	command, _ := NewMarkManifestLost(InternalCommandID{0xb5}, manifest.ManifestRevision, next, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 70, command); got.Code != ResultSuccess {
		t.Fatalf("succeeded job rejected the lost declaration: %#v", got)
	}
	current := machine.jobs[job].Manifests[manifest.SinkTask]
	if !current.Lost || current.LostRevision != current.ManifestRevision || current.LostEpoch != machine.coordinatorEpoch ||
		current.RecordCount != manifest.RecordCount || current.TotalBytes != manifest.TotalBytes || current.Checksum != manifest.Checksum || current.Replicas != manifest.Replicas {
		t.Fatalf("lost manifest = %#v want committed identity plus marker", current)
	}
	// The exact identity replays the identical success without a second
	// revision, and every later seal or re-mark over the lost identity is
	// rejected: the marker is terminal.
	replay, _ := NewMarkManifestLost(InternalCommandID{0xb5}, manifest.ManifestRevision, next, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 71, replay); got.Code != ResultSuccess || got.Revision != current.ManifestRevision {
		t.Fatalf("lost declaration replay = %#v", got)
	}
	again := lostMarkerSuccessor(current, machine.coordinatorEpoch)
	second, _ := NewMarkManifestLost(InternalCommandID{0xb6}, current.ManifestRevision, again, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 72, second); got.Code != ResultInvalidTarget {
		t.Fatalf("second lost declaration accepted: %#v", got)
	}
	reseal := current
	reseal.ManifestRevision = current.ManifestRevision + 1
	reseal.Lost, reseal.LostEpoch, reseal.LostRevision = false, model.CoordinatorEpoch{}, 0
	reseal.Replicas = machine.jobs[job].Assignment.ResultReplicas[0]
	seal, err := NewSealManifest(InternalCommandID{0xb7}, current.ManifestRevision, reseal, machine.coordinatorEpoch)
	if err != nil {
		t.Fatalf("re-seal constructor: %v", err)
	}
	if got := applyTask10(t, machine, 73, seal); got.Code != ResultInvalidTarget {
		t.Fatalf("seal over a lost manifest accepted: %#v", got)
	}
}

// TestSnapshotCaptureAcceptsManifestDivergenceOnlyWithLostMarker pins the
// Critical invariant: after both artifact copies are destroyed, the
// manifest↔assignment divergence wedges snapshot capture until the durable
// lost marker commits, after which capture — and a full restore round trip —
// succeed with the divergence as the final legal state.
func TestSnapshotCaptureAcceptsManifestDivergenceOnlyWithLostMarker(t *testing.T) {
	machine, _, manifest := totalLossState(t)

	// Without the marker the divergence is exactly the rejection it was
	// before: every Capture fails and Raft snapshotting stays disabled.
	if _, err := machine.Capture(machine.lastAppliedIndex, 1); err == nil {
		t.Fatal("capture accepted an unmarked manifest placement divergence")
	}

	next := lostMarkerSuccessor(manifest, machine.coordinatorEpoch)
	command, _ := NewMarkManifestLost(InternalCommandID{0xb8}, manifest.ManifestRevision, next, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 70, command); got.Code != ResultSuccess {
		t.Fatalf("lost declaration = %#v", got)
	}

	captured, err := machine.Capture(machine.lastAppliedIndex, 1)
	if err != nil {
		t.Fatalf("capture after the lost marker: %v", err)
	}
	encoded, err := captured.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal captured snapshot: %v", err)
	}
	restored := NewMachine()
	if err := restored.Restore(SnapshotSchemaVersion, encoded); err != nil {
		t.Fatalf("restore captured snapshot: %v", err)
	}
	if !restored.jobs[manifest.JobID].Manifests[manifest.SinkTask].Lost {
		t.Fatal("restored manifest lost marker missing")
	}
	assertCanonicalSnapshotEstimate(t, machine)
}

// TestMarkManifestLostCodecRoundTrip pins the canonical command encoding:
// the concrete kind decodes exactly and noncanonical encodings are rejected.
func TestMarkManifestLostCodecRoundTrip(t *testing.T) {
	machine, _, manifest := totalLossState(t)
	next := lostMarkerSuccessor(manifest, machine.coordinatorEpoch)
	command, err := NewMarkManifestLost(InternalCommandID{0xb9}, manifest.ManifestRevision, next, machine.coordinatorEpoch)
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	encoded, err := MarshalCommand(command)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := UnmarshalCommand(encoded)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	typed, ok := decoded.(MarkManifestLost)
	if !ok || typed.Manifest != command.Manifest || typed.Envelope.Kind != CommandMarkManifestLost ||
		typed.Envelope.Internal == nil || *typed.Envelope.Internal != *command.Envelope.Internal {
		t.Fatalf("round trip = %#v want %#v", decoded, command)
	}
	if _, err := UnmarshalCommand(append(encoded, 0)); err == nil {
		t.Fatal("noncanonical trailing bytes accepted")
	}
}
