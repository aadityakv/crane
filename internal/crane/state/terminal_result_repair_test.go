package state

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

// succeededSealedJob commits one Succeeded job carrying one current sealed
// manifest, the exact durable state whose result availability a replica
// incarnation loss threatens. Four eligible workers guarantee the
// deterministic placement spreads duties so a secondary-only holder exists.
func succeededSealedJob(t *testing.T) (*Machine, model.JobID, model.ValidatedTopology, model.AssignmentSet, ResultManifest) {
	t.Helper()
	machine := NewMachine()
	epochCommand, _ := NewBeginCoordinatorEpoch(InternalCommandID{0xe1}, 0, 1, [16]byte{0xe1})
	applyTask10(t, machine, 1, epochCommand)
	for index := 1; index <= 4; index++ {
		record := WorkerRecord{NodeID: uint16(index), Epoch: model.WorkerEpoch{byte(index), 0x46}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, _ := NewRegisterWorker(InternalCommandID{byte(index), 0x46}, 0, record, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(index), register)
	}
	spec := task10Topology(0)
	spec.Stages[0].Parallelism = 2
	spec.Stages[0].Operator.Settings[0].Value = decimal(1)
	topology, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xe2}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
	applyTask10(t, machine, 4, submit)
	job := submit.JobID()
	for partition := uint16(0); partition < 2; partition++ {
		source := model.TaskID{JobID: job, StageID: 1, Partition: partition}
		eof, _ := model.SourceEOF(topology, source)
		command, _ := NewRecordSourceEOF(InternalCommandID{0xe3, byte(partition)}, 0, source, eof, machine.coordinatorEpoch)
		applyTask10(t, machine, uint64(5+partition), command)
	}
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}
	install, _ := NewInstallAssignments(InternalCommandID{0xe4}, 1, assignment, machine.coordinatorEpoch)
	applyTask10(t, machine, 10, install)
	running, _ := NewTransitionJob(InternalCommandID{0xe5}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	applyTask10(t, machine, 11, running)

	commitFinalCheckpoint(t, machine, job, assignment, 0, 1)
	draining, _ := NewTransitionJob(InternalCommandID{0xe7}, machine.jobs[job].JobControlRevision, job, JobRunning, JobDraining)
	if got := applyTask10(t, machine, 50, draining); got.Code != ResultSuccess {
		t.Fatalf("draining = %#v", got)
	}
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{0xe8}, Replicas: replica}
	seal, _ := NewSealManifest(InternalCommandID{0xe8}, 0, manifest)
	if got := applyTask10(t, machine, 51, seal); got.Code != ResultSuccess {
		t.Fatalf("seal = %#v", got)
	}
	succeed, _ := NewTransitionJob(InternalCommandID{0xe9}, machine.jobs[job].JobControlRevision, job, JobDraining, JobSucceeded)
	if got := applyTask10(t, machine, 52, succeed); got.Code != ResultSuccess || machine.jobs[job].Lifecycle != JobSucceeded {
		t.Fatalf("succeed = %#v lifecycle=%d", got, machine.jobs[job].Lifecycle)
	}
	return machine, job, topology, assignment, manifest
}

// TestSucceededJobGainsInvalidationMarkersForReplicaHolder pins the
// availability invariant: deactivating a worker incarnation that holds a
// Succeeded job's result replica (or its sink task) must mark the job for
// reassignment exactly like a nonterminal job, because the committed manifest
// set stays servable only while a current worker holds the sealed artifact.
// Failed and Canceled jobs stay frozen: they retain no availability duty.
func TestSucceededJobGainsInvalidationMarkersForReplicaHolder(t *testing.T) {
	for _, lifecycle := range []JobLifecycle{JobSucceeded, JobFailed, JobCanceled} {
		t.Run(lifecycleName(lifecycle), func(t *testing.T) {
			machine, job, _, assignment, _ := succeededSealedJob(t)
			record := cloneJobRecord(machine.jobs[job])
			record.Lifecycle = lifecycle
			machine.jobs[job] = record

			secondary := assignment.ResultReplicas[0].SecondaryNodeID
			secondaryEpoch := assignment.ResultReplicas[0].SecondaryEpoch
			for _, token := range assignment.Tasks {
				if token.WorkerID == secondary {
					t.Fatal("deterministic fixture must produce a secondary-only worker duty")
				}
			}
			workerBefore := machine.workers[secondary]
			jobBefore := machine.jobs[job]
			// Succeeded jobs present exactly the affected list nonterminal
			// jobs present; frozen lifecycles present none.
			var affected []AffectedAssignment
			if lifecycle == JobSucceeded {
				affected = []AffectedAssignment{{JobID: job, JobControlRevision: jobBefore.JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
			}
			deactivate, _ := NewDeactivateWorker(InternalCommandID{0xf3}, workerBefore.Revision, secondary, secondaryEpoch, affected)
			got := applyTask10(t, machine, 60, deactivate)
			if got.Code != ResultSuccess {
				t.Fatalf("deactivate = %#v", got)
			}
			marked := machine.jobs[job]
			if lifecycle != JobSucceeded {
				if len(marked.NeedsReassignment) != 0 || marked.JobControlRevision != jobBefore.JobControlRevision {
					t.Fatalf("%s job gained markers: %#v", lifecycleName(lifecycle), marked.NeedsReassignment)
				}
				return
			}
			if len(marked.NeedsReassignment) != 1 || marked.NeedsReassignment[0].Kind != ResultReplicaTarget || marked.NeedsReassignment[0].ReplicaRole != model.SecondaryReplica || marked.NeedsReassignment[0].OldWorkerID != secondary {
				t.Fatalf("succeeded job markers = %#v", marked.NeedsReassignment)
			}
			if marked.JobControlRevision != jobBefore.JobControlRevision+1 {
				t.Fatalf("succeeded job revision = %d want %d", marked.JobControlRevision, jobBefore.JobControlRevision+1)
			}
			if marked.Assignment.Revision != assignment.Revision {
				t.Fatal("invalidation mutated the retained assignment")
			}
		})
	}
}

// TestSucceededJobResealsManifestOnReplacementReplica pins the terminal
// re-seal: after the marked succeeded job's assignment is replaced onto
// current workers, committing the next manifest revision bound to the live
// replica placement must be legal, restoring the manifest-to-assignment
// binding the result-page query serves from.
func TestSucceededJobResealsManifestOnReplacementReplica(t *testing.T) {
	machine, job, topology, assignment, manifest := succeededSealedJob(t)
	secondary := assignment.ResultReplicas[0].SecondaryNodeID
	secondaryEpoch := assignment.ResultReplicas[0].SecondaryEpoch
	workerBefore := machine.workers[secondary]
	jobBefore := machine.jobs[job]
	affected := []AffectedAssignment{{JobID: job, JobControlRevision: jobBefore.JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0xf4}, workerBefore.Revision, secondary, secondaryEpoch, affected)
	if got := applyTask10(t, machine, 60, deactivate); got.Code != ResultSuccess {
		t.Fatalf("deactivate = %#v", got)
	}
	marked := machine.jobs[job]

	// Replace the secondary onto the remaining current workers.
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
	replace, _ := NewReplaceAssignments(InternalCommandID{0xf5}, marked.JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), target)
	if got := applyTask10(t, machine, 61, replace); got.Code != ResultSuccess {
		t.Fatalf("replace succeeded job = %#v", got)
	}
	replaced := machine.jobs[job]
	if replaced.Lifecycle != JobSucceeded || len(replaced.NeedsReassignment) != 0 || replaced.Assignment.Revision != assignment.Revision+1 {
		t.Fatalf("replaced succeeded job = lifecycle=%d markers=%d revision=%d", replaced.Lifecycle, len(replaced.NeedsReassignment), replaced.Assignment.Revision)
	}

	// Re-seal the manifest bound to the live replacement placement.
	next := manifest
	next.ManifestRevision = manifest.ManifestRevision + 1
	next.Replicas = replaced.Assignment.ResultReplicas[0]
	reseal, _ := NewSealManifest(InternalCommandID{0xf6}, replaced.Manifests[next.SinkTask].ManifestRevision, next, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 62, reseal); got.Code != ResultSuccess {
		t.Fatalf("re-seal succeeded job manifest = %#v", got)
	}
	if current := machine.jobs[job].Manifests[next.SinkTask]; current.Replicas != replaced.Assignment.ResultReplicas[0] || current.ManifestRevision != next.ManifestRevision {
		t.Fatalf("re-sealed manifest = %#v", current)
	}
	assertCanonicalSnapshotEstimate(t, machine)
}

// rotateFirstPlacement reorders the eligible placements so the deterministic
// builder spreads different workers, letting tests escape a placement the
// deactivated worker would otherwise repeat.
func rotateFirstPlacement(placements []model.WorkerPlacement, by int) []model.WorkerPlacement {
	if len(placements) == 0 || by%len(placements) == 0 {
		return placements
	}
	rotated := append([]model.WorkerPlacement(nil), placements[by%len(placements):]...)
	return append(rotated, placements[:by%len(placements)]...)
}
