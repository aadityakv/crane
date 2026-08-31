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
		if id == 1 {
			role = StageSource
			operator = OperatorSpec{Name: "range", Version: 1, Settings: []Setting{{Key: "end_exclusive", Value: "16000000"}, {Key: "start", Value: "0"}}}
		} else if id == uint16(limits.MaxStages) {
			role = StageSink
			operator = OperatorSpec{Name: "collect", Version: 1}
		}
		name := fmt.Sprintf("stage-%03d", id)
		name += strings.Repeat("x", int(limits.MaxIdentifierBytes)-len(name))
		spec.Stages = append(spec.Stages, StageSpec{StageID: id, Name: name, Role: role, Parallelism: 16, Operator: operator})
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
	for name, overhead := range map[string]uint64{
		"SubmitJob":            limits.SubmitJobOverheadBytes,
		"SubmitRequest":        limits.SubmitRequestOverheadBytes,
		"AssignmentSetInstall": limits.AssignmentSetOverheadBytes,
	} {
		if encodedLength > limits.MaxSubmitJobBytes-overhead {
			t.Fatalf("%s complete encoding exceeds 1 MiB: topology=%d overhead=%d", name, encodedLength, overhead)
		}
	}
}
