package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func testTopology() TopologySpec {
	return TopologySpec{
		SchemaVersion:       1,
		Name:                "numbers",
		RegistryFingerprint: RegistryFingerprint(),
		Stages: []StageSpec{
			{StageID: 1, Name: "source", Role: StageSource, Parallelism: 2, Operator: OperatorSpec{Name: "range", Version: 1, Settings: []Setting{{Key: "end_exclusive", Value: "7"}, {Key: "start", Value: "1"}}}},
			{StageID: 2, Name: "times", Role: StageTransform, Parallelism: 2, Operator: OperatorSpec{Name: "multiply", Version: 1, Settings: []Setting{{Key: "factor", Value: "3"}}}},
			{StageID: 3, Name: "results", Role: StageSink, Parallelism: 2, Operator: OperatorSpec{Name: "collect", Version: 1}},
		},
		Edges: []EdgeSpec{
			{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: RoutingShuffle},
			{EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: RoutingFieldHash, Field: "value"},
		},
	}
}

func requireValidTopology(t *testing.T, spec TopologySpec) ValidatedTopology {
	t.Helper()
	validated, err := ValidateTopology(spec)
	if err != nil {
		t.Fatalf("ValidateTopology: %v", err)
	}
	return validated
}

func TestTopologyCanonicalOwnedRoundTrip(t *testing.T) {
	spec := testTopology()
	validated := requireValidTopology(t, spec)
	encoded := validated.CanonicalBytes()
	if len(encoded) == 0 || uint64(len(encoded)) > LimitsV1().MaxTopologyBytes {
		t.Fatalf("canonical topology length = %d", len(encoded))
	}
	decoded, err := DecodeTopology(encoded)
	if err != nil {
		t.Fatalf("DecodeTopology: %v", err)
	}
	if !bytes.Equal(decoded.CanonicalBytes(), encoded) || decoded.Digest() != validated.Digest() {
		t.Fatal("topology round trip changed canonical identity")
	}

	// Both accessors must return owned data, including nested settings.
	got := validated.Spec()
	got.Name = "mutated"
	got.Stages[0].Operator.Settings[0].Value = "999"
	encoded[0] ^= 0xff
	if validated.Spec().Name != "numbers" || validated.Spec().Stages[0].Operator.Settings[0].Value != "7" {
		t.Fatal("Spec returned aliased state")
	}
	if bytes.Equal(validated.CanonicalBytes(), encoded) {
		t.Fatal("CanonicalBytes returned aliased state")
	}
}

func TestTopologyRejectsNonCanonicalAndInvalidGraphs(t *testing.T) {
	cases := map[string]func(*TopologySpec){
		"schema":               func(s *TopologySpec) { s.SchemaVersion = 2 },
		"registry":             func(s *TopologySpec) { s.RegistryFingerprint[0] ^= 1 },
		"name characters":      func(s *TopologySpec) { s.Name = "bad\nname" },
		"stage order":          func(s *TopologySpec) { s.Stages[0], s.Stages[1] = s.Stages[1], s.Stages[0] },
		"stage ID duplicate":   func(s *TopologySpec) { s.Stages[1].StageID = 1 },
		"stage name duplicate": func(s *TopologySpec) { s.Stages[1].Name = s.Stages[0].Name },
		"zero parallelism":     func(s *TopologySpec) { s.Stages[1].Parallelism = 0 },
		"operator role":        func(s *TopologySpec) { s.Stages[1].Operator.Name = "collect" },
		"settings order": func(s *TopologySpec) {
			s.Stages[0].Operator.Settings[0], s.Stages[0].Operator.Settings[1] = s.Stages[0].Operator.Settings[1], s.Stages[0].Operator.Settings[0]
		},
		"edge order":        func(s *TopologySpec) { s.Edges[0], s.Edges[1] = s.Edges[1], s.Edges[0] },
		"edge ID duplicate": func(s *TopologySpec) { s.Edges[1].EdgeID = 1 },
		"unknown stage":     func(s *TopologySpec) { s.Edges[0].DestinationStageID = 99 },
		"self cycle":        func(s *TopologySpec) { s.Edges[0].DestinationStageID = 1 },
		"wrong field":       func(s *TopologySpec) { s.Edges[1].Field = "missing" },
		"field on shuffle":  func(s *TopologySpec) { s.Edges[0].Field = "value" },
		"broadcast collect": func(s *TopologySpec) { s.Edges[1].Routing = RoutingBroadcast; s.Edges[1].Field = "" },
		"two sources":       func(s *TopologySpec) { s.Stages[1].Role = StageSource; s.Stages[1].Operator = s.Stages[0].Operator },
		"two sinks":         func(s *TopologySpec) { s.Stages[1].Role = StageSink; s.Stages[1].Operator = s.Stages[2].Operator },
		"unreachable":       func(s *TopologySpec) { s.Edges = s.Edges[:1] },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := testTopology()
			mutate(&spec)
			if _, err := ValidateTopology(spec); err == nil {
				t.Fatal("invalid topology accepted")
			}
		})
	}
}

func TestTopologyRejectsEveryBoundAndCheckedExpansion(t *testing.T) {
	limits := LimitsV1()
	cases := map[string]func(*TopologySpec){
		"name bytes":  func(s *TopologySpec) { s.Name = strings.Repeat("n", int(limits.MaxIdentifierBytes+1)) },
		"stages":      func(s *TopologySpec) { s.Stages = make([]StageSpec, limits.MaxStages+1) },
		"edges":       func(s *TopologySpec) { s.Edges = make([]EdgeSpec, limits.MaxEdges+1) },
		"parallelism": func(s *TopologySpec) { s.Stages[0].Parallelism = uint16(limits.MaxTasksPerStage + 1) },
		"settings":    func(s *TopologySpec) { s.Stages[0].Operator.Settings = make([]Setting, limits.MaxSettingsPerStage+1) },
		"setting key": func(s *TopologySpec) {
			s.Stages[1].Operator.Settings[0].Key = strings.Repeat("k", int(limits.MaxSettingKeyBytes+1))
		},
		"setting value": func(s *TopologySpec) {
			s.Stages[1].Operator.Settings[0].Value = strings.Repeat("1", int(limits.MaxSettingValueBytes+1))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := testTopology()
			mutate(&spec)
			if _, err := ValidateTopology(spec); err == nil {
				t.Fatal("over-bound topology accepted")
			}
		})
	}

	// A three-way broadcast at each of eight stages exceeds 4096 downstream deliveries.
	spec := testTopology()
	spec.Stages = spec.Stages[:1]
	spec.Edges = nil
	for id := uint16(2); id <= 9; id++ {
		spec.Stages = append(spec.Stages, StageSpec{StageID: id, Name: "t" + string(rune('a'+id)), Role: StageTransform, Parallelism: 3, Operator: OperatorSpec{Name: "even", Version: 1}})
		spec.Edges = append(spec.Edges, EdgeSpec{EdgeID: id - 1, SourceStageID: id - 1, DestinationStageID: id, Routing: RoutingBroadcast})
	}
	spec.Stages = append(spec.Stages, StageSpec{StageID: 10, Name: "sink", Role: StageSink, Parallelism: 1, Operator: OperatorSpec{Name: "collect", Version: 1}})
	spec.Edges = append(spec.Edges, EdgeSpec{EdgeID: 9, SourceStageID: 9, DestinationStageID: 10, Routing: RoutingShuffle})
	if _, err := ValidateTopology(spec); err == nil {
		t.Fatal("over-limit DAG expansion accepted")
	}
}

func TestTopologyDecodeRejectsOversizeBeforeAllocationAndNonCanonicalBytes(t *testing.T) {
	declared := make([]byte, 8)
	binary.BigEndian.PutUint64(declared, LimitsV1().MaxTopologyBytes+1)
	if _, err := DecodeTopology(declared); err == nil {
		t.Fatal("oversize declared topology length accepted")
	}
	canonical := requireValidTopology(t, testTopology()).CanonicalBytes()
	if _, err := DecodeTopology(append(canonical, 0)); err == nil {
		t.Fatal("trailing topology bytes accepted")
	}
}

func TestTopologyMaximumStructuredEncodingFitsEveryCompleteCommandReservation(t *testing.T) {
	limits := LimitsV1()
	spec := TopologySpec{
		SchemaVersion:       1,
		Name:                strings.Repeat("j", int(limits.MaxIdentifierBytes)),
		RegistryFingerprint: RegistryFingerprint(),
	}
	for id := uint16(1); id <= uint16(limits.MaxStages); id++ {
		role := StageTransform
		operator := OperatorSpec{Name: "even", Version: 1}
		parallelism := uint16(1)
		if id == 1 {
			role = StageSource
			operator = OperatorSpec{Name: "range", Version: 1, Settings: []Setting{{Key: "end_exclusive", Value: "256000000"}, {Key: "start", Value: "0"}}}
			parallelism = 256
		} else if id == 2 {
			parallelism = 256
		} else if id == 3 {
			parallelism = 196
		} else if id == uint16(limits.MaxStages) {
			role = StageSink
			operator = OperatorSpec{Name: "collect", Version: 1}
			parallelism = 256
		}
		name := fmt.Sprintf("stage-%03d", id)
		name += strings.Repeat("x", int(limits.MaxIdentifierBytes)-len(name))
		spec.Stages = append(spec.Stages, StageSpec{StageID: id, Name: name, Role: role, Parallelism: parallelism, Operator: operator})
	}
	for id := uint16(1); id < uint16(limits.MaxStages); id++ {
		spec.Edges = append(spec.Edges, EdgeSpec{EdgeID: id, SourceStageID: id, DestinationStageID: id + 1, Routing: RoutingShuffle})
	}
	for uint64(len(spec.Edges)) < limits.MaxEdges {
		id := uint16(len(spec.Edges) + 1)
		spec.Edges = append(spec.Edges, EdgeSpec{EdgeID: id, SourceStageID: 1, DestinationStageID: uint16(limits.MaxStages), Routing: RoutingShuffle})
	}
	validated := requireValidTopology(t, spec)
	encodedLength := uint64(len(validated.CanonicalBytes()))
	maxSlots := uint16(limits.MaxWorkerSlots)
	workers := []WorkerPlacement{{NodeID: 1, WorkerEpoch: WorkerEpoch{1}, SlotCapacity: maxSlots}, {NodeID: 2, WorkerEpoch: WorkerEpoch{2}, SlotCapacity: maxSlots}, {NodeID: 3, WorkerEpoch: WorkerEpoch{3}, SlotCapacity: maxSlots}, {NodeID: 4, WorkerEpoch: WorkerEpoch{4}, SlotCapacity: maxSlots}}
	assignment, err := BuildAssignmentSet(JobID{1}, validated.Digest(), 1, validated, workers)
	if err != nil {
		t.Fatalf("maximum complete AssignmentSet: %v", err)
	}
	if uint64(len(assignment.Tasks)) != limits.MaxTasksPerJob || uint64(len(assignment.ResultReplicas)) != limits.MaxTasksPerStage {
		t.Fatalf("maximum AssignmentSet counts = %d,%d", len(assignment.Tasks), len(assignment.ResultReplicas))
	}
	if complete, err := CompleteSubmitJobBytes(encodedLength); err != nil || complete > limits.MaxSubmitJobBytes {
		t.Fatalf("SubmitJob complete bytes = %d,%v", complete, err)
	}
	if complete, err := CompleteSubmitRequestBytes(encodedLength); err != nil || complete > limits.MaxControlFrameBytes {
		t.Fatalf("SubmitRequest complete bytes = %d,%v", complete, err)
	}
	if complete, err := CompleteAssignmentSetInstallBytes(encodedLength, uint64(len(assignment.Tasks)), uint64(len(assignment.ResultReplicas))); err != nil || complete > limits.MaxWorkerControlFrameBytes {
		t.Fatalf("AssignmentSetInstall complete bytes = %d,%v", complete, err)
	}
}

func TestTopologyRejectsTotalTasksAndSettingsBeforeClone(t *testing.T) {
	t.Run("tasks", func(t *testing.T) {
		spec := testTopology()
		spec.Stages = []StageSpec{
			{StageID: 1, Name: "source", Role: StageSource, Parallelism: 256, Operator: spec.Stages[0].Operator},
			{StageID: 2, Name: "a", Role: StageTransform, Parallelism: 256, Operator: OperatorSpec{Name: "even", Version: 1}},
			{StageID: 3, Name: "b", Role: StageTransform, Parallelism: 256, Operator: OperatorSpec{Name: "even", Version: 1}},
			{StageID: 4, Name: "c", Role: StageTransform, Parallelism: 256, Operator: OperatorSpec{Name: "even", Version: 1}},
			{StageID: 5, Name: "sink", Role: StageSink, Parallelism: 1, Operator: OperatorSpec{Name: "collect", Version: 1}},
		}
		spec.Edges = []EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: RoutingShuffle}, {EdgeID: 2, SourceStageID: 2, DestinationStageID: 3, Routing: RoutingShuffle}, {EdgeID: 3, SourceStageID: 3, DestinationStageID: 4, Routing: RoutingShuffle}, {EdgeID: 4, SourceStageID: 4, DestinationStageID: 5, Routing: RoutingShuffle}}
		if _, err := ValidateTopology(spec); err == nil {
			t.Fatal("more than 1024 tasks accepted")
		}
	})
	t.Run("settings bytes", func(t *testing.T) {
		spec := testTopology()
		spec.Stages = make([]StageSpec, LimitsV1().MaxStages)
		for i := range spec.Stages {
			settings := make([]Setting, LimitsV1().MaxSettingsPerStage)
			for j := range settings {
				settings[j] = Setting{Key: fmt.Sprintf("k%02d", j), Value: strings.Repeat("v", int(LimitsV1().MaxSettingValueBytes))}
			}
			spec.Stages[i] = StageSpec{StageID: uint16(i + 1), Name: fmt.Sprintf("s%02d", i), Role: StageTransform, Parallelism: 1, Operator: OperatorSpec{Name: "even", Version: 1, Settings: settings}}
		}
		if _, err := ValidateTopology(spec); err == nil {
			t.Fatal("more than 64 KiB total settings accepted")
		}
	})
}

func TestTopologyCustodyReservationIsExactBoundedAndImmutable(t *testing.T) {
	v := requireValidTopology(t, testTopology())
	job := JobID{1}
	first, err := v.WorstCaseCustodyBytes(TaskID{JobID: job, StageID: 2, Partition: 0})
	if err != nil {
		t.Fatal(err)
	}
	second, err := v.WorstCaseCustodyBytes(TaskID{JobID: job, StageID: 2, Partition: 1})
	if err != nil || second != first {
		t.Fatalf("per-task reservation mismatch: %d %d %v", first, second, err)
	}
	if first != 187_567 {
		t.Fatalf("stage-2 custody bytes = %d, want independent formula 187567", first)
	}
	if _, err := v.WorstCaseCustodyBytes(TaskID{JobID: job, StageID: 2, Partition: 2}); err == nil {
		t.Fatal("invalid task partition reservation accepted")
	}
	if first > LimitsV1().MaxCustodyReservationBytes {
		t.Fatal("accepted reservation exceeds consensus maximum")
	}
}

func TestCompleteEncodingCalculatorsExactMaximumAndPlusOne(t *testing.T) {
	limits := LimitsV1()
	cases := []struct {
		name      string
		overhead  uint64
		maximum   uint64
		calculate func(uint64) (uint64, error)
	}{
		{"SubmitRequest", limits.SubmitRequestFixedBytes, limits.MaxControlFrameBytes, CompleteSubmitRequestBytes},
		{"SubmitJob", limits.SubmitJobFixedBytes, limits.MaxSubmitJobBytes, CompleteSubmitJobBytes},
		{"AssignmentSetInstall", limits.AssignmentSetInstallFixedBytes, limits.MaxWorkerControlFrameBytes, func(topology uint64) (uint64, error) {
			return CompleteAssignmentSetInstallBytes(topology, limits.MaxTasksPerJob, limits.MaxTasksPerStage)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			budget := tc.maximum - tc.overhead
			got, err := tc.calculate(budget)
			if err != nil || got != tc.maximum {
				t.Fatalf("exact maximum = %d,%v", got, err)
			}
			if _, err := tc.calculate(budget + 1); err == nil {
				t.Fatal("maximum + 1 accepted")
			}
		})
	}
	if _, err := CompleteAssignmentSetInstallBytes(0, limits.MaxTasksPerJob+1, 0); err == nil {
		t.Fatal("assignment task count maximum + 1 accepted")
	}
	if _, err := CompleteAssignmentSetInstallBytes(0, 0, limits.MaxTasksPerStage+1); err == nil {
		t.Fatal("result replica count maximum + 1 accepted")
	}
}

func TestCustodyReservationExactMaximumAndPlusOne(t *testing.T) {
	limits := LimitsV1()
	got, err := CustodyReservationUpperBoundV1(limits.MaxDerivedDeliveries)
	if err != nil || got != limits.MaxCustodyReservationBytes {
		t.Fatalf("maximum custody reservation = %d,%v", got, err)
	}
	if _, err := CustodyReservationUpperBoundV1(limits.MaxDerivedDeliveries + 1); err == nil {
		t.Fatal("custody delivery maximum + 1 accepted")
	}
}
