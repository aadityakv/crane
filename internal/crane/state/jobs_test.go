package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestJobCommandsCanonicalRoundTripAndSubmitSizeAgreement(t *testing.T) {
	topology := task10Topology(0)
	request := model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}
	fence := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
	submit, err := NewSubmitJob(request, topology, fence)
	if err != nil {
		t.Fatal(err)
	}
	cancel, err := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 2}, submit.JobID(), 1, fence)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []any{submit, cancel} {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(%T): %v", command, err)
		}
		decoded, err := UnmarshalCommand(encoded)
		if err != nil || !reflect.DeepEqual(decoded, command) {
			t.Fatalf("round trip %T = %#v, %v; want %#v", command, decoded, err, command)
		}
	}
	validated, _ := model.ValidateTopology(topology)
	encoded, _ := MarshalCommand(submit)
	want, err := model.CompleteSubmitJobBytes(uint64(len(validated.CanonicalBytes())))
	if err != nil || uint64(len(encoded)) != want || uint64(len(encoded)) > model.LimitsV1().MaxSubmitJobBytes {
		t.Fatalf("SubmitJob bytes = %d, model=%d,%v", len(encoded), want, err)
	}

	// The command decoder must reject the same declared topology maximum + 1
	// before attempting to allocate the declared body.
	over := make([]byte, int(model.LimitsV1().SubmitJobFixedBytes)+8)
	binary.BigEndian.PutUint16(over[0:2], CommandSchemaVersion)
	fingerprint := model.ConsensusFingerprint()
	copy(over[2:34], fingerprint[:])
	binary.BigEndian.PutUint16(over[34:36], uint16(CommandSubmitJob))
	over[36] = identityClient
	copy(over[37:53], request.ClientID[:])
	binary.BigEndian.PutUint64(over[53:61], request.Sequence)
	topologyOffset := model.LimitsV1().SubmitJobFixedBytes
	binary.BigEndian.PutUint64(over[topologyOffset:topologyOffset+8], model.LimitsV1().MaxTopologyBytes-8+1)
	if _, err := UnmarshalCommand(over); err == nil {
		t.Fatal("SubmitJob decoder accepted declared topology maximum + 1")
	}
}

func TestMaximallyStructuredSubmitJobFitsRaftCommandBudget(t *testing.T) {
	topology := task10MaximumTopology()
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	limits := model.LimitsV1()
	if uint64(len(topology.Stages)) != limits.MaxStages || uint64(len(topology.Edges)) != limits.MaxEdges {
		t.Fatal("maximum fixture misses structural maxima")
	}
	fence := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
	command, err := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xef}, Sequence: 1}, topology, fence)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	want, err := model.CompleteSubmitJobBytes(uint64(len(validated.CanonicalBytes())))
	if err != nil || uint64(len(encoded)) != want || uint64(len(encoded)) > limits.MaxSubmitJobBytes {
		t.Fatalf("maximum structured SubmitJob = %d, model=%d, max=%d, err=%v", len(encoded), want, limits.MaxSubmitJobBytes, err)
	}
}

func TestJobSubmitRetainsDefiningBytesCollisionDefenseAndCancel(t *testing.T) {
	machine := NewMachine()
	topology := task10Topology(0)
	request := model.ClientRequestID{ClientID: model.ClientID{2}, Sequence: 1}
	submit, _ := NewSubmitJob(request, topology)
	if got := applyTask10(t, machine, 1, submit); got.Code != ResultSuccess || got.JobID != submit.JobID() || got.Revision != 1 {
		t.Fatalf("submit = %#v", got)
	}
	record := machine.jobs[submit.JobID()]
	validated, _ := model.ValidateTopology(topology)
	if record.DefiningRequest != request || record.TopologyDigest != validated.Digest() || !bytes.Equal(record.TopologyBytes, validated.CanonicalBytes()) || record.Lifecycle != JobPending || record.JobControlRevision != 1 {
		t.Fatalf("retained job = %#v", record)
	}
	topology.Stages[0].Name = "mutated"
	if bytes.Equal(record.TopologyBytes, mustTopologyBytes(t, topology)) {
		t.Fatal("job retained aliased topology bytes")
	}

	// Seed the otherwise computationally infeasible truncated-hash collision and
	// prove complete defining bytes, not only JobID, guard identity.
	collisionMachine := NewMachine()
	collisionMachine.jobs[submit.JobID()] = JobRecord{JobID: submit.JobID(), DefiningRequest: request, TopologyDigest: validated.Digest(), TopologyBytes: []byte{1}, Lifecycle: JobCanceled, JobControlRevision: 1}
	if got := applyTask10(t, collisionMachine, 1, submit); got.Code != ResultIdentityCollision {
		t.Fatalf("truncated JobID collision = %#v", got)
	}

	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: request.ClientID, Sequence: 2}, submit.JobID(), 1)
	if got := applyTask10(t, machine, 2, cancel); got.Code != ResultSuccess || got.Revision != 2 {
		t.Fatalf("cancel = %#v", got)
	}
	if got := machine.jobs[submit.JobID()]; got.Lifecycle != JobCanceled || got.JobControlRevision != 2 {
		t.Fatalf("canceled job = %#v", got)
	}
	if got := applyTask10(t, machine, 3, cancel); got.Code != ResultSuccess || got.Revision != 2 {
		t.Fatalf("cancel retry = %#v", got)
	}
}

func TestJobActiveAndRetainedCapacitiesAreExact(t *testing.T) {
	active := NewMachine()
	for index := 1; index <= int(model.LimitsV1().MaxActiveJobs); index++ {
		request := model.ClientRequestID{ClientID: model.ClientID{byte(index), 1}, Sequence: 1}
		command, _ := NewSubmitJob(request, task10Topology(int64(index)))
		if got := applyTask10(t, active, uint64(index), command); got.Code != ResultSuccess {
			t.Fatalf("active submit %d = %#v", index, got)
		}
	}
	overActive, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0xff, 1}, Sequence: 1}, task10Topology(999))
	if got := applyTask10(t, active, 100, overActive); got.Code != ResultCapacityExhausted || len(active.jobs) != int(model.LimitsV1().MaxActiveJobs) {
		t.Fatalf("active capacity = %#v jobs=%d", got, len(active.jobs))
	}

	retained := NewMachine()
	client := model.ClientID{0x55}
	sequence := uint64(1)
	for index := 0; index < int(model.LimitsV1().MaxRetainedJobs); index++ {
		submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: client, Sequence: sequence}, task10Topology(int64(index)))
		if got := applyTask10(t, retained, sequence, submit); got.Code != ResultSuccess {
			t.Fatalf("retained submit %d = %#v", index, got)
		}
		sequence++
		cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: client, Sequence: sequence}, submit.JobID(), 1)
		if got := applyTask10(t, retained, sequence, cancel); got.Code != ResultSuccess {
			t.Fatalf("retained cancel %d = %#v", index, got)
		}
		sequence++
	}
	extra, _ := NewSubmitJob(model.ClientRequestID{ClientID: client, Sequence: sequence}, task10Topology(1000))
	if got := applyTask10(t, retained, sequence, extra); got.Code != ResultCapacityExhausted || len(retained.jobs) != int(model.LimitsV1().MaxRetainedJobs) {
		t.Fatalf("retained capacity = %#v jobs=%d", got, len(retained.jobs))
	}
}

func TestJobCapacityRefusalIsUnacceptedAndCanSucceedAfterCapacityChanges(t *testing.T) {
	machine := NewMachine()
	for index := 1; index <= int(model.LimitsV1().MaxActiveJobs); index++ {
		request := model.ClientRequestID{ClientID: model.ClientID{byte(index), 2}, Sequence: 1}
		command, _ := NewSubmitJob(request, task10Topology(int64(index)))
		if got := applyTask10(t, machine, uint64(index), command); got.Code != ResultSuccess {
			t.Fatalf("setup submit %d = %#v", index, got)
		}
	}
	request := model.ClientRequestID{ClientID: model.ClientID{0xff, 2}, Sequence: 1}
	blocked, _ := NewSubmitJob(request, task10Topology(2000))
	if got := applyTask10(t, machine, 100, blocked); got.Code != ResultCapacityExhausted {
		t.Fatalf("capacity refusal = %#v", got)
	}
	if _, retained := machine.clients[request.ClientID]; retained {
		t.Fatal("capacity refusal consumed client sequence")
	}
	var cancelJob model.JobID
	for job := range machine.jobs {
		cancelJob = job
		break
	}
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{0xee}, Sequence: 1}, cancelJob, 1)
	if got := applyTask10(t, machine, 101, cancel); got.Code != ResultSuccess {
		t.Fatalf("release active capacity = %#v", got)
	}
	if got := applyTask10(t, machine, 102, blocked); got.Code != ResultSuccess {
		t.Fatalf("previously unaccepted request after capacity change = %#v", got)
	}
}

func FuzzUnmarshalStateCommand(f *testing.F) {
	register, _ := NewRegisterWorker(InternalCommandID{1}, 0, WorkerRecord{NodeID: 1, Epoch: model.WorkerEpoch{1}, State: WorkerEligible, Revision: 1, Slots: 1, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()})
	submit, _ := NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}, task10Topology(0))
	validated, _ := model.ValidateTopology(task10Topology(0))
	job := submit.JobID()
	placements := []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: model.WorkerEpoch{1}, SlotCapacity: 2}, {NodeID: 2, WorkerEpoch: model.WorkerEpoch{2}, SlotCapacity: 2}}
	assignment, _ := model.BuildAssignmentSet(job, validated.Digest(), 1, validated, placements)
	targetTokens := append([]model.AssignmentToken(nil), assignment.Tasks...)
	for index := range targetTokens {
		targetTokens[index].AssignmentRevision = 2
	}
	target, _ := model.NewAssignmentSet(job, 2, targetTokens, assignment.ResultReplicas, validated)
	source := assignment.Tasks[0]
	eof, _ := model.SourceEOF(validated, source.Task)
	epoch := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
	report := model.CompletionReport{JobID: job, JobControlRevision: 3, AssignmentRevision: 1, Source: source.Task, Token: source, Epoch: epoch, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: 1}
	report.Digest = model.CompletionReportDigest(report)
	replica := assignment.ResultReplicas[0]
	manifest := ResultManifest{JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: validated.Digest(), RecordCount: 1, TotalBytes: model.ResultArtifactMinRecordBytesV1, Checksum: [32]byte{1}, Replicas: replica}
	failure := model.JobFailureReport{JobID: job, JobControlRevision: 3, AssignmentRevision: 1, Task: source, Epoch: epoch, TransactionID: 2, Code: model.FailureOperator, DetailDigest: [32]byte{2}}
	drain, _ := NewDrainWorker(InternalCommandID{2}, 1, 1, model.WorkerEpoch{1})
	deactivate, _ := NewDeactivateWorker(InternalCommandID{3}, 1, 1, model.WorkerEpoch{1}, nil)
	replacement, _ := NewReplaceWorkerEpoch(InternalCommandID{4}, 1, 1, model.WorkerEpoch{1}, WorkerRecord{NodeID: 1, Epoch: model.WorkerEpoch{3}, State: WorkerEligible, Revision: 2, Slots: 1, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()}, nil)
	cancel, _ := NewCancelJob(model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 2}, job, 1)
	recordEOF, _ := NewRecordSourceEOF(InternalCommandID{5}, 0, source.Task, eof)
	install, _ := NewInstallAssignments(InternalCommandID{6}, 1, assignment)
	replaceAssignments, _ := NewReplaceAssignments(InternalCommandID{7}, 3, job, 1, assignment.Digest, NeedsReassignmentDigest(nil), target)
	advance, _ := NewAdvanceCheckpoint(InternalCommandID{8}, 0, report)
	seal, _ := NewSealManifest(InternalCommandID{9}, 0, manifest)
	transition, _ := NewTransitionJob(InternalCommandID{10}, 3, job, JobRunning, JobDraining)
	fail, _ := NewFailJob(InternalCommandID{11}, 3, failure)
	for _, command := range []any{register, drain, deactivate, replacement, submit, cancel, recordEOF, install, replaceAssignments, advance, seal, transition, fail} {
		encoded, _ := MarshalCommand(command)
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > int(model.LimitsV1().MaxSubmitJobBytes)+1 {
			encoded = encoded[:int(model.LimitsV1().MaxSubmitJobBytes)+1]
		}
		_, _ = UnmarshalCommand(encoded)
	})
}

func task10MaximumTopology() model.TopologySpec {
	limits := model.LimitsV1()
	spec := model.TopologySpec{SchemaVersion: 1, Name: strings.Repeat("j", int(limits.MaxIdentifierBytes)), RegistryFingerprint: model.RegistryFingerprint()}
	for id := uint16(1); id <= uint16(limits.MaxStages); id++ {
		role, parallelism := model.Transform, uint16(1)
		operator := model.OperatorSpec{Name: "even", Version: 1}
		switch id {
		case 1:
			role, parallelism = model.Source, 256
			operator = model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "256000000"}, {Key: "start", Value: "0"}}}
		case 2:
			parallelism = 256
		case 3:
			parallelism = 196
		case uint16(limits.MaxStages):
			role, parallelism = model.Sink, 256
			operator = model.OperatorSpec{Name: "collect", Version: 1}
		}
		name := fmt.Sprintf("stage-%03d", id)
		name += strings.Repeat("x", int(limits.MaxIdentifierBytes)-len(name))
		spec.Stages = append(spec.Stages, model.StageSpec{StageID: id, Name: name, Role: role, Parallelism: parallelism, Operator: operator})
	}
	for id := uint16(1); id < uint16(limits.MaxStages); id++ {
		spec.Edges = append(spec.Edges, model.EdgeSpec{EdgeID: id, SourceStageID: id, DestinationStageID: id + 1, Routing: model.Shuffle})
	}
	for uint64(len(spec.Edges)) < limits.MaxEdges {
		id := uint16(len(spec.Edges) + 1)
		spec.Edges = append(spec.Edges, model.EdgeSpec{EdgeID: id, SourceStageID: 1, DestinationStageID: uint16(limits.MaxStages), Routing: model.Shuffle})
	}
	return spec
}

func task10Topology(start int64) model.TopologySpec {
	return model.TopologySpec{
		SchemaVersion: 1, Name: "task-10", RegistryFingerprint: model.RegistryFingerprint(),
		Stages: []model.StageSpec{
			{StageID: 1, Name: "source", Role: model.Source, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: decimal(start + 2)}, {Key: "start", Value: decimal(start)}}}},
			{StageID: 2, Name: "sink", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
		},
		Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle}},
	}
}

func decimal(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var scratch [32]byte
	position := len(scratch)
	for value > 0 {
		position--
		scratch[position] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		position--
		scratch[position] = '-'
	}
	return string(scratch[position:])
}

func mustTopologyBytes(t *testing.T, topology model.TopologySpec) []byte {
	t.Helper()
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	return validated.CanonicalBytes()
}
