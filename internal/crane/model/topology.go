package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// StageRole identifies a stage's semantic topology role.
type StageRole uint8

const (
	StageSource StageRole = iota + 1
	StageTransform
	StageSink
)

// Canonical v1 role names.
const (
	Source    = StageSource
	Transform = StageTransform
	Sink      = StageSink
)

// RoutingMode identifies deterministic destination selection for an edge.
type RoutingMode uint8

const (
	RoutingShuffle RoutingMode = iota + 1
	RoutingFieldHash
	RoutingBroadcast
)

// Canonical v1 routing names.
const (
	Shuffle   = RoutingShuffle
	FieldHash = RoutingFieldHash
	Broadcast = RoutingBroadcast
)

// Setting is one canonical string-valued operator setting.
type Setting struct{ Key, Value string }

// OperatorSpec selects one immutable built-in and its canonical settings.
type OperatorSpec struct {
	Name     string
	Version  uint16
	Settings []Setting
}

// StageSpec defines one parallel topology stage.
type StageSpec struct {
	StageID     uint16
	Name        string
	Role        StageRole
	Parallelism uint16
	Operator    OperatorSpec
}

// EdgeSpec defines one directed, deterministically routed topology edge.
type EdgeSpec struct {
	EdgeID, SourceStageID, DestinationStageID uint16
	Routing                                   RoutingMode
	Field                                     string
}

// TopologySpec is the complete immutable v1 job graph.
type TopologySpec struct {
	SchemaVersion       uint16
	Name                string
	Stages              []StageSpec
	Edges               []EdgeSpec
	RegistryFingerprint [32]byte
}

// ValidatedTopology owns a canonical, fully checked topology graph.
type ValidatedTopology struct {
	spec                 TopologySpec
	canonical            []byte
	digest               [32]byte
	byStage              map[uint16]StageSpec
	eofs                 map[uint16][]uint64
	downstreamDeliveries map[uint16]uint64
	edgeReservations     map[uint16]uint64
	collectStageID       uint16
	collectPartitions    []uint16
}

// ValidateTopology validates and takes ownership of a complete topology.
func ValidateTopology(input TopologySpec) (ValidatedTopology, error) {
	spec := cloneTopology(input)
	limits := LimitsV1()
	if spec.SchemaVersion != 1 {
		return ValidatedTopology{}, errors.New("unsupported topology schema version")
	}
	if err := validateASCIIName(spec.Name, limits.MaxIdentifierBytes); err != nil {
		return ValidatedTopology{}, fmt.Errorf("topology name: %w", err)
	}
	if spec.RegistryFingerprint != RegistryFingerprint() {
		return ValidatedTopology{}, errors.New("operator registry fingerprint mismatch")
	}
	if len(spec.Stages) == 0 || uint64(len(spec.Stages)) > limits.MaxStages {
		return ValidatedTopology{}, errors.New("topology stage count is outside v1 bounds")
	}
	if len(spec.Edges) == 0 || uint64(len(spec.Edges)) > limits.MaxEdges {
		return ValidatedTopology{}, errors.New("topology edge count is outside v1 bounds")
	}

	byStage := make(map[uint16]StageSpec, len(spec.Stages))
	names := make(map[string]struct{}, len(spec.Stages))
	eofs := make(map[uint16][]uint64)
	var sourceID, sinkID uint16
	var taskCount uint64
	for index, stage := range spec.Stages {
		if stage.StageID == 0 || (index > 0 && spec.Stages[index-1].StageID >= stage.StageID) {
			return ValidatedTopology{}, errors.New("stages are not canonically ordered by unique nonzero ID")
		}
		if err := validateASCIIName(stage.Name, limits.MaxIdentifierBytes); err != nil {
			return ValidatedTopology{}, fmt.Errorf("stage %d name: %w", stage.StageID, err)
		}
		if _, exists := names[stage.Name]; exists {
			return ValidatedTopology{}, errors.New("duplicate stage name")
		}
		names[stage.Name] = struct{}{}
		if stage.Parallelism == 0 || uint64(stage.Parallelism) > limits.MaxTasksPerStage {
			return ValidatedTopology{}, errors.New("stage parallelism is outside v1 bounds")
		}
		if taskCount > math.MaxUint64-uint64(stage.Parallelism) {
			return ValidatedTopology{}, errors.New("task count overflow")
		}
		taskCount += uint64(stage.Parallelism)
		if taskCount > limits.MaxTasksPerJob {
			return ValidatedTopology{}, errors.New("topology task count exceeds limit")
		}
		descriptor, err := validateOperatorSpec(stage.Operator)
		if err != nil {
			return ValidatedTopology{}, fmt.Errorf("stage %d operator: %w", stage.StageID, err)
		}
		if StageRole(descriptor.Role) != stage.Role {
			return ValidatedTopology{}, errors.New("operator role does not match stage role")
		}
		switch stage.Role {
		case StageSource:
			if sourceID != 0 {
				return ValidatedTopology{}, errors.New("topology must contain exactly one source")
			}
			sourceID = stage.StageID
			partitionEOFs, err := rangeEOFs(stage.Operator, stage.Parallelism)
			if err != nil {
				return ValidatedTopology{}, fmt.Errorf("source stage: %w", err)
			}
			eofs[stage.StageID] = partitionEOFs
		case StageTransform:
		case StageSink:
			if stage.Operator.Name != "collect" || sinkID != 0 {
				return ValidatedTopology{}, errors.New("topology must contain exactly one collect sink")
			}
			sinkID = stage.StageID
		default:
			return ValidatedTopology{}, errors.New("unknown stage role")
		}
		byStage[stage.StageID] = stage
	}
	if sourceID == 0 || sinkID == 0 {
		return ValidatedTopology{}, errors.New("topology requires one source and one collect sink")
	}

	outgoing := make(map[uint16][]EdgeSpec)
	incoming := make(map[uint16][]EdgeSpec)
	for index, edge := range spec.Edges {
		if edge.EdgeID == 0 || (index > 0 && spec.Edges[index-1].EdgeID >= edge.EdgeID) {
			return ValidatedTopology{}, errors.New("edges are not canonically ordered by unique nonzero ID")
		}
		source, sourceOK := byStage[edge.SourceStageID]
		destination, destinationOK := byStage[edge.DestinationStageID]
		if !sourceOK || !destinationOK || edge.SourceStageID == edge.DestinationStageID {
			return ValidatedTopology{}, errors.New("edge references invalid stages")
		}
		switch edge.Routing {
		case RoutingShuffle, RoutingBroadcast:
			if edge.Field != "" {
				return ValidatedTopology{}, errors.New("non-field route carries a field")
			}
		case RoutingFieldHash:
			if edge.Field != "value" {
				return ValidatedTopology{}, errors.New("field-hash route requires an emitted field")
			}
		default:
			return ValidatedTopology{}, errors.New("unknown routing mode")
		}
		if edge.Routing == RoutingBroadcast && destination.StageID == sinkID {
			return ValidatedTopology{}, errors.New("broadcast directly into collect is forbidden")
		}
		outgoing[source.StageID] = append(outgoing[source.StageID], edge)
		incoming[destination.StageID] = append(incoming[destination.StageID], edge)
	}
	if len(incoming[sourceID]) != 0 || len(outgoing[sinkID]) != 0 {
		return ValidatedTopology{}, errors.New("source has input or sink has output")
	}
	for _, stage := range spec.Stages {
		if stage.StageID != sourceID && len(incoming[stage.StageID]) == 0 {
			return ValidatedTopology{}, errors.New("stage is unreachable from source")
		}
		if stage.StageID != sinkID && len(outgoing[stage.StageID]) == 0 {
			return ValidatedTopology{}, errors.New("stage cannot reach sink")
		}
	}
	order, err := topologicalOrder(spec.Stages, outgoing, incoming)
	if err != nil {
		return ValidatedTopology{}, err
	}
	if !allReachable(sourceID, outgoing, len(spec.Stages)) || !allCanReach(sinkID, incoming, len(spec.Stages)) {
		return ValidatedTopology{}, errors.New("topology contains a stage outside the source-to-sink graph")
	}
	downstream, reservations, err := validateExpansion(order, outgoing, byStage, limits.MaxDerivedDeliveries)
	if err != nil {
		return ValidatedTopology{}, err
	}

	canonical, err := marshalTopology(spec)
	if err != nil {
		return ValidatedTopology{}, err
	}
	if uint64(len(canonical)) > limits.MaxTopologyBytes {
		return ValidatedTopology{}, errors.New("canonical topology exceeds reserved command/frame budget")
	}
	collectPartitions := make([]uint16, byStage[sinkID].Parallelism)
	for index := range collectPartitions {
		collectPartitions[index] = uint16(index)
	}
	return ValidatedTopology{
		spec:                 spec,
		canonical:            canonical,
		digest:               sha256.Sum256(canonical),
		byStage:              byStage,
		eofs:                 eofs,
		downstreamDeliveries: downstream,
		edgeReservations:     reservations,
		collectStageID:       sinkID,
		collectPartitions:    collectPartitions,
	}, nil
}

// Spec returns an owned copy of the validated topology specification.
func (v ValidatedTopology) Spec() TopologySpec { return cloneTopology(v.spec) }

// CanonicalBytes returns owned canonical topology bytes.
func (v ValidatedTopology) CanonicalBytes() []byte { return append([]byte(nil), v.canonical...) }

// Digest returns the SHA-256 digest of the complete canonical topology.
func (v ValidatedTopology) Digest() [32]byte { return v.digest }

// DecodeTopology decodes canonical bytes and revalidates the complete graph.
func DecodeTopology(encoded []byte) (ValidatedTopology, error) {
	if len(encoded) < 8 {
		return ValidatedTopology{}, errors.New("truncated topology length")
	}
	declared := binary.BigEndian.Uint64(encoded[:8])
	if declared > LimitsV1().MaxTopologyBytes {
		return ValidatedTopology{}, errors.New("declared topology length exceeds limit")
	}
	if declared != uint64(len(encoded)-8) {
		return ValidatedTopology{}, errors.New("topology length mismatch")
	}
	r := checkedReader{input: encoded[8:]}
	version, err := r.uint16()
	if err != nil {
		return ValidatedTopology{}, err
	}
	name, err := r.string(LimitsV1().MaxIdentifierBytes)
	if err != nil {
		return ValidatedTopology{}, err
	}
	if r.remaining() < 32 {
		return ValidatedTopology{}, errors.New("truncated registry fingerprint")
	}
	var fingerprint [32]byte
	copy(fingerprint[:], r.input[r.offset:r.offset+32])
	r.offset += 32
	stageCount, err := r.uint16()
	if err != nil {
		return ValidatedTopology{}, err
	}
	if uint64(stageCount) > LimitsV1().MaxStages || int(stageCount) > r.remaining()/10 {
		return ValidatedTopology{}, errors.New("stage count cannot fit canonical bytes")
	}
	spec := TopologySpec{SchemaVersion: version, Name: name, RegistryFingerprint: fingerprint, Stages: make([]StageSpec, stageCount)}
	for index := range spec.Stages {
		stage := &spec.Stages[index]
		if stage.StageID, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		if stage.Name, err = r.string(LimitsV1().MaxIdentifierBytes); err != nil {
			return ValidatedTopology{}, err
		}
		role, e := r.byte()
		if e != nil {
			return ValidatedTopology{}, e
		}
		stage.Role = StageRole(role)
		if stage.Parallelism, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		if stage.Operator.Name, err = r.string(LimitsV1().MaxIdentifierBytes); err != nil {
			return ValidatedTopology{}, err
		}
		if stage.Operator.Version, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		count, e := r.uint16()
		if e != nil {
			return ValidatedTopology{}, e
		}
		if uint64(count) > LimitsV1().MaxSettingsPerStage || int(count) > r.remaining()/4 {
			return ValidatedTopology{}, errors.New("setting count cannot fit canonical bytes")
		}
		stage.Operator.Settings = make([]Setting, count)
		for settingIndex := range stage.Operator.Settings {
			if stage.Operator.Settings[settingIndex].Key, err = r.string(LimitsV1().MaxSettingKeyBytes); err != nil {
				return ValidatedTopology{}, err
			}
			if stage.Operator.Settings[settingIndex].Value, err = r.string(LimitsV1().MaxSettingValueBytes); err != nil {
				return ValidatedTopology{}, err
			}
		}
	}
	edgeCount, err := r.uint16()
	if err != nil {
		return ValidatedTopology{}, err
	}
	if uint64(edgeCount) > LimitsV1().MaxEdges || int(edgeCount) > r.remaining()/9 {
		return ValidatedTopology{}, errors.New("edge count cannot fit canonical bytes")
	}
	spec.Edges = make([]EdgeSpec, edgeCount)
	for index := range spec.Edges {
		edge := &spec.Edges[index]
		if edge.EdgeID, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		if edge.SourceStageID, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		if edge.DestinationStageID, err = r.uint16(); err != nil {
			return ValidatedTopology{}, err
		}
		routing, e := r.byte()
		if e != nil {
			return ValidatedTopology{}, e
		}
		edge.Routing = RoutingMode(routing)
		if edge.Field, err = r.string(LimitsV1().MaxIdentifierBytes); err != nil {
			return ValidatedTopology{}, err
		}
	}
	if !r.done() {
		return ValidatedTopology{}, errors.New("trailing topology bytes")
	}
	validated, err := ValidateTopology(spec)
	if err != nil {
		return ValidatedTopology{}, err
	}
	if !bytes.Equal(validated.canonical, encoded) {
		return ValidatedTopology{}, errors.New("non-canonical topology encoding")
	}
	return validated, nil
}

func marshalTopology(spec TopologySpec) ([]byte, error) {
	w := newCheckedWriter(1024)
	_ = w.uint16(spec.SchemaVersion)
	if err := w.string(spec.Name); err != nil {
		return nil, err
	}
	w.output = append(w.output, spec.RegistryFingerprint[:]...)
	_ = w.uint16(uint16(len(spec.Stages)))
	for _, stage := range spec.Stages {
		_ = w.uint16(stage.StageID)
		if err := w.string(stage.Name); err != nil {
			return nil, err
		}
		_ = w.byte(byte(stage.Role))
		_ = w.uint16(stage.Parallelism)
		if err := w.string(stage.Operator.Name); err != nil {
			return nil, err
		}
		_ = w.uint16(stage.Operator.Version)
		_ = w.uint16(uint16(len(stage.Operator.Settings)))
		for _, setting := range stage.Operator.Settings {
			if err := w.string(setting.Key); err != nil {
				return nil, err
			}
			if err := w.string(setting.Value); err != nil {
				return nil, err
			}
		}
	}
	_ = w.uint16(uint16(len(spec.Edges)))
	for _, edge := range spec.Edges {
		_ = w.uint16(edge.EdgeID)
		_ = w.uint16(edge.SourceStageID)
		_ = w.uint16(edge.DestinationStageID)
		_ = w.byte(byte(edge.Routing))
		if err := w.string(edge.Field); err != nil {
			return nil, err
		}
	}
	body := w.ownedBytes()
	encoded := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint64(encoded, uint64(len(body)))
	encoded = append(encoded, body...)
	return encoded, nil
}

func cloneTopology(spec TopologySpec) TopologySpec {
	clone := spec
	clone.Stages = append([]StageSpec(nil), spec.Stages...)
	clone.Edges = append([]EdgeSpec(nil), spec.Edges...)
	for index := range clone.Stages {
		clone.Stages[index].Operator.Settings = append([]Setting(nil), spec.Stages[index].Operator.Settings...)
	}
	return clone
}

func validateASCIIName(value string, limit uint64) error {
	if value == "" || uint64(len(value)) > limit {
		return errors.New("length outside bounds")
	}
	for _, b := range []byte(value) {
		if b < 0x20 || b > 0x7e {
			return errors.New("must be printable ASCII")
		}
	}
	return nil
}

func validateOperatorSpec(spec OperatorSpec) (OperatorDescriptor, error) {
	if err := validateASCIIName(spec.Name, LimitsV1().MaxIdentifierBytes); err != nil {
		return OperatorDescriptor{}, err
	}
	if uint64(len(spec.Settings)) > LimitsV1().MaxSettingsPerStage {
		return OperatorDescriptor{}, errors.New("too many settings")
	}
	var total uint64
	for index, setting := range spec.Settings {
		if err := validateASCIIName(setting.Key, LimitsV1().MaxSettingKeyBytes); err != nil {
			return OperatorDescriptor{}, err
		}
		if uint64(len(setting.Value)) > LimitsV1().MaxSettingValueBytes {
			return OperatorDescriptor{}, errors.New("setting value exceeds limit")
		}
		if index > 0 && spec.Settings[index-1].Key >= setting.Key {
			return OperatorDescriptor{}, errors.New("settings are not sorted and unique")
		}
		total += uint64(len(setting.Key) + len(setting.Value))
		if total > LimitsV1().MaxTotalSettingsBytes {
			return OperatorDescriptor{}, errors.New("total settings exceed limit")
		}
	}
	for _, descriptor := range RegistryV1().Operators {
		if descriptor.Name != spec.Name || descriptor.Version != spec.Version {
			continue
		}
		if len(descriptor.Settings) != len(spec.Settings) {
			return OperatorDescriptor{}, errors.New("operator settings do not match schema")
		}
		for index, setting := range spec.Settings {
			if descriptor.Settings[index].Name != setting.Key {
				return OperatorDescriptor{}, errors.New("operator setting name mismatch")
			}
			if descriptor.Settings[index].Type == SettingTypeInt64 {
				parsed, err := strconv.ParseInt(setting.Value, 10, 64)
				if err != nil || strconv.FormatInt(parsed, 10) != setting.Value {
					return OperatorDescriptor{}, errors.New("operator setting is not canonical int64")
				}
			}
		}
		return descriptor, nil
	}
	return OperatorDescriptor{}, errors.New("unknown operator name/version")
}

func topologicalOrder(stages []StageSpec, outgoing, incoming map[uint16][]EdgeSpec) ([]uint16, error) {
	degree := make(map[uint16]int, len(stages))
	queue := make([]uint16, 0, len(stages))
	for _, stage := range stages {
		degree[stage.StageID] = len(incoming[stage.StageID])
		if degree[stage.StageID] == 0 {
			queue = append(queue, stage.StageID)
		}
	}
	order := make([]uint16, 0, len(stages))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, edge := range outgoing[id] {
			degree[edge.DestinationStageID]--
			if degree[edge.DestinationStageID] == 0 {
				queue = append(queue, edge.DestinationStageID)
			}
		}
	}
	if len(order) != len(stages) {
		return nil, errors.New("topology contains a cycle")
	}
	return order, nil
}

func allReachable(start uint16, edges map[uint16][]EdgeSpec, want int) bool {
	seen := map[uint16]bool{start: true}
	queue := []uint16{start}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, e := range edges[id] {
			if !seen[e.DestinationStageID] {
				seen[e.DestinationStageID] = true
				queue = append(queue, e.DestinationStageID)
			}
		}
	}
	return len(seen) == want
}
func allCanReach(sink uint16, edges map[uint16][]EdgeSpec, want int) bool {
	seen := map[uint16]bool{sink: true}
	queue := []uint16{sink}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, e := range edges[id] {
			if !seen[e.SourceStageID] {
				seen[e.SourceStageID] = true
				queue = append(queue, e.SourceStageID)
			}
		}
	}
	return len(seen) == want
}

func validateExpansion(order []uint16, outgoing map[uint16][]EdgeSpec, stages map[uint16]StageSpec, limit uint64) (map[uint16]uint64, map[uint16]uint64, error) {
	deliveries := make(map[uint16]uint64, len(order))
	reservations := make(map[uint16]uint64)
	for index := len(order) - 1; index >= 0; index-- {
		id := order[index]
		var total uint64
		for _, edge := range outgoing[id] {
			factor := uint64(1)
			if edge.Routing == RoutingBroadcast {
				factor = uint64(stages[edge.DestinationStageID].Parallelism)
			}
			child := deliveries[edge.DestinationStageID]
			if child == math.MaxUint64 || factor > limit/(child+1) {
				return nil, nil, errors.New("topology delivery expansion overflow")
			}
			contribution := factor * (child + 1)
			if total > limit-contribution {
				return nil, nil, errors.New("topology derived delivery limit exceeded")
			}
			reservations[edge.EdgeID] = contribution
			total += contribution
		}
		deliveries[id] = total
	}
	return deliveries, reservations, nil
}
