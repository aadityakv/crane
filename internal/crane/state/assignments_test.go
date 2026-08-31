package state

import (
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestAssignmentCommandsCanonicalRoundTripAndRejectPartialOrMixedRevisionSets(t *testing.T) {
	machine, job, topology, assignment := task10AssignedJob(t, 1)
	install, err := NewInstallAssignments(InternalCommandID{0x31}, 1, assignment, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	targetTokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range targetTokens {
		targetTokens[index].AssignmentRevision++
	}
	target, err := model.NewAssignmentSet(job, assignment.Revision+1, targetTokens, assignment.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	replace, err := NewReplaceAssignments(InternalCommandID{0x32}, 2, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(nil), target, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []any{install, replace} {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		decoded, err := UnmarshalCommand(encoded)
		if err != nil || !reflect.DeepEqual(decoded, command) {
			t.Fatalf("round trip %T = %#v,%v want %#v", command, decoded, err, command)
		}
	}

	partial := assignment
	partial.Tasks = append([]model.AssignmentToken(nil), partial.Tasks[:len(partial.Tasks)-1]...)
	partialCommand, err := NewInstallAssignments(InternalCommandID{0x33}, 1, partial, machine.coordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if got := applyTask10(t, machine, 19, partialCommand); got.Code != ResultInvalidTarget {
		t.Fatalf("partial AssignmentSet result = %#v", got)
	}
	mixed := assignment
	mixed.Tasks = append([]model.AssignmentToken(nil), mixed.Tasks...)
	mixed.Tasks[0].AssignmentRevision++
	if _, err := NewInstallAssignments(InternalCommandID{0x34}, 1, mixed, machine.coordinatorEpoch); err == nil {
		t.Fatal("mixed token AssignmentRevision accepted")
	}
}

func TestAssignmentInitialPlacementEOFAndLifecycleFences(t *testing.T) {
	machine, job, _, assignment := task10AssignedJob(t, 1)
	install, _ := NewInstallAssignments(InternalCommandID{0x41}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 20, install); got.Code != ResultSuccess || got.Revision != 2 {
		t.Fatalf("install = %#v", got)
	}
	record := machine.jobs[job]
	if record.Lifecycle != JobDeploying || record.JobControlRevision != 2 || record.Assignment == nil || !reflect.DeepEqual(*record.Assignment, assignment) {
		t.Fatalf("installed job = %#v", record)
	}
	for _, token := range record.Assignment.Tasks {
		if token.AssignmentRevision != record.Assignment.Revision {
			t.Fatalf("token revision %d != set %d", token.AssignmentRevision, record.Assignment.Revision)
		}
	}
	running, _ := NewTransitionJob(InternalCommandID{0x42}, 2, job, JobDeploying, JobRunning, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 21, running); got.Code != ResultSuccess || got.Revision != 3 {
		t.Fatalf("begin running = %#v", got)
	}
}

func TestAssignmentReplacementChangedTargetAdvancesOnlyChangedAttempt(t *testing.T) {
	_, job, topology, old := task10AssignedJob(t, 4)
	tokens := append([]model.AssignmentToken(nil), old.Tasks...)
	tokens[0].WorkerID = old.ResultReplicas[0].SecondaryNodeID
	tokens[0].WorkerEpoch = old.ResultReplicas[0].SecondaryEpoch
	tokens[0].Attempt++
	for index := range tokens {
		tokens[index].AssignmentRevision = old.Revision + 1
	}
	target, err := model.NewAssignmentSet(job, old.Revision+1, tokens, old.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	if target.Tasks[0].Attempt != old.Tasks[0].Attempt+1 || target.Tasks[1].Attempt != old.Tasks[1].Attempt {
		t.Fatalf("attempts = %d/%d, want changed+1 and unchanged stable", target.Tasks[0].Attempt, target.Tasks[1].Attempt)
	}
}

func TestWorkerInvalidationCreatesAtomicSortedMarkersAndSecondaryOnlyReplacement(t *testing.T) {
	machine, job, topology, assignment := task10AssignedJob(t, 4)
	install, _ := NewInstallAssignments(InternalCommandID{0x51}, 1, assignment, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 30, install); got.Code != ResultSuccess {
		t.Fatalf("install = %#v", got)
	}

	secondary := assignment.ResultReplicas[0].SecondaryNodeID
	secondaryEpoch := assignment.ResultReplicas[0].SecondaryEpoch
	for _, token := range assignment.Tasks {
		if token.WorkerID == secondary {
			t.Fatal("deterministic fixture must produce a secondary-only worker duty")
		}
	}
	workerBefore := machine.workers[secondary]
	jobBefore := machine.jobs[job]
	wrong := []AffectedAssignment{{JobID: job, JobControlRevision: jobBefore.JobControlRevision + 1, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	bad, _ := NewDeactivateWorker(InternalCommandID{0x52}, workerBefore.Revision, secondary, secondaryEpoch, wrong)
	if got := applyTask10(t, machine, 31, bad); got.Code != ResultRevisionMismatch {
		t.Fatalf("mismatched affected list = %#v", got)
	}
	if machine.workers[secondary] != workerBefore || machine.jobs[job].JobControlRevision != jobBefore.JobControlRevision {
		t.Fatal("rejected invalidation partially mutated worker or job")
	}

	affected := []AffectedAssignment{{JobID: job, JobControlRevision: jobBefore.JobControlRevision, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest}}
	deactivate, _ := NewDeactivateWorker(InternalCommandID{0x53}, workerBefore.Revision, secondary, secondaryEpoch, affected)
	if got := applyTask10(t, machine, 32, deactivate); got.Code != ResultSuccess {
		t.Fatalf("deactivate = %#v", got)
	}
	marked := machine.jobs[job]
	if marked.JobControlRevision != jobBefore.JobControlRevision+1 || len(marked.NeedsReassignment) != 1 || marked.NeedsReassignment[0].Kind != ResultReplicaTarget || marked.NeedsReassignment[0].ReplicaRole != model.SecondaryReplica {
		t.Fatalf("secondary-only markers = %#v", marked.NeedsReassignment)
	}
	if marked.Assignment.Revision != assignment.Revision || marked.Assignment.Digest != assignment.Digest {
		t.Fatal("worker invalidation mutated the retained complete AssignmentSet")
	}

	workers := task10EligiblePlacements(machine)
	target, err := model.BuildAssignmentSet(job, topology.Digest(), assignment.Revision+1, topology, workers)
	if err != nil {
		t.Fatal(err)
	}
	for index := range target.Tasks {
		if target.Tasks[index].WorkerID != assignment.Tasks[index].WorkerID || target.Tasks[index].Attempt != assignment.Tasks[index].Attempt {
			t.Fatalf("secondary-only replacement changed task %d", index)
		}
	}
	wrongDigest := assignment.Digest
	wrongDigest[0] ^= 0xff
	badSetDigest, _ := NewReplaceAssignments(InternalCommandID{0x54, 1}, marked.JobControlRevision, job, assignment.Revision, wrongDigest, NeedsReassignmentDigest(marked.NeedsReassignment), target)
	if got := applyTask10(t, machine, 33, badSetDigest); got.Code != ResultInvalidTarget || len(machine.jobs[job].NeedsReassignment) != 1 {
		t.Fatalf("wrong retained-set digest = %#v markers=%#v", got, machine.jobs[job].NeedsReassignment)
	}
	badMarkerDigest := NeedsReassignmentDigest(marked.NeedsReassignment)
	badMarkerDigest[0] ^= 0xff
	badMarkers, _ := NewReplaceAssignments(InternalCommandID{0x54, 2}, marked.JobControlRevision, job, assignment.Revision, assignment.Digest, badMarkerDigest, target)
	if got := applyTask10(t, machine, 34, badMarkers); got.Code != ResultInvalidTarget || len(machine.jobs[job].NeedsReassignment) != 1 {
		t.Fatalf("wrong marker digest = %#v markers=%#v", got, machine.jobs[job].NeedsReassignment)
	}
	revisionThreeTokens := append([]model.AssignmentToken(nil), target.Tasks...)
	for index := range revisionThreeTokens {
		revisionThreeTokens[index].AssignmentRevision = 3
	}
	revisionThree, err := model.NewAssignmentSet(job, 3, revisionThreeTokens, target.ResultReplicas, topology)
	if err != nil {
		t.Fatal(err)
	}
	badOldRevision, _ := NewReplaceAssignments(InternalCommandID{0x54, 3}, marked.JobControlRevision, job, 2, assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), revisionThree)
	if got := applyTask10(t, machine, 35, badOldRevision); got.Code != ResultInvalidTarget || len(machine.jobs[job].NeedsReassignment) != 1 {
		t.Fatalf("wrong retained-set revision = %#v markers=%#v", got, machine.jobs[job].NeedsReassignment)
	}
	replace, _ := NewReplaceAssignments(InternalCommandID{0x54}, marked.JobControlRevision, job, assignment.Revision, assignment.Digest, NeedsReassignmentDigest(marked.NeedsReassignment), target)
	if got := applyTask10(t, machine, 36, replace); got.Code != ResultSuccess {
		t.Fatalf("replace assignments = %#v", got)
	}
	replaced := machine.jobs[job]
	if len(replaced.NeedsReassignment) != 0 || replaced.Assignment.Revision != assignment.Revision+1 || replaced.JobControlRevision != marked.JobControlRevision+1 {
		t.Fatalf("replaced job = %#v", replaced)
	}
}

func task10AssignedJob(t *testing.T, workerCount int) (*Machine, model.JobID, model.ValidatedTopology, model.AssignmentSet) {
	t.Helper()
	machine := NewMachine()
	begin, _ := NewBeginCoordinatorEpoch(InternalCommandID{0x70}, 0, 1, [16]byte{0x70})
	applyTask10(t, machine, 1, begin)
	for index := 1; index <= workerCount; index++ {
		record := WorkerRecord{NodeID: uint16(index), Epoch: model.WorkerEpoch{byte(index)}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
		register, err := NewRegisterWorker(InternalCommandID{byte(index), 0x10}, 0, record, machine.coordinatorEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if got := applyTask10(t, machine, uint64(index+1), register); got.Code != ResultSuccess {
			t.Fatalf("register %d = %#v", index, got)
		}
	}
	if workerCount < 2 {
		// Assignment construction requires two distinct result nodes.
		for index := workerCount + 1; index <= 2; index++ {
			record := WorkerRecord{NodeID: uint16(index), Epoch: model.WorkerEpoch{byte(index)}, State: WorkerEligible, Revision: 1, Slots: 16, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}
			register, _ := NewRegisterWorker(InternalCommandID{byte(index), 0x10}, 0, record, machine.coordinatorEpoch)
			applyTask10(t, machine, uint64(index+1), register)
		}
	}
	topology, err := model.ValidateTopology(task10Topology(0))
	if err != nil {
		t.Fatal(err)
	}
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x71}, Sequence: 1}, topology.Spec(), machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 10, submit); got.Code != ResultSuccess {
		t.Fatalf("submit = %#v", got)
	}
	job := submit.JobID()
	source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	eof, _ := model.SourceEOF(topology, source)
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{0x72}, 0, source, eof, machine.coordinatorEpoch)
	if got := applyTask10(t, machine, 11, recordEOF); got.Code != ResultSuccess {
		t.Fatalf("record EOF = %#v", got)
	}
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, task10EligiblePlacements(machine))
	if err != nil {
		t.Fatal(err)
	}
	return machine, job, topology, assignment
}

func task10EligiblePlacements(machine *Machine) []model.WorkerPlacement {
	placements := make([]model.WorkerPlacement, 0, len(machine.workers))
	for node := uint16(1); int(node) <= len(machine.workers); node++ {
		worker, ok := machine.workers[node]
		if ok && worker.State == WorkerEligible {
			placements = append(placements, model.WorkerPlacement{NodeID: node, WorkerEpoch: worker.Epoch, SlotCapacity: worker.Slots})
		}
	}
	return placements
}
