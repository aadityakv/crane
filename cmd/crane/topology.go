package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aadityakv/crane/internal/crane/model"
)

// maxTopologyDocumentBytes bounds one topology JSON document before parsing.
const maxTopologyDocumentBytes = 1 << 20

// topologyDocument is the strict JSON schema of one submitted DAG. The
// registry fingerprint is always the compiled contract and never user input.
type topologyDocument struct {
	SchemaVersion uint16          `json:"schema_version"`
	Name          string          `json:"name"`
	Stages        []stageDocument `json:"stages"`
	Edges         []edgeDocument  `json:"edges"`
}

type stageDocument struct {
	StageID     uint16           `json:"stage_id"`
	Name        string           `json:"name"`
	Role        string           `json:"role"`
	Parallelism uint16           `json:"parallelism"`
	Operator    operatorDocument `json:"operator"`
}

type operatorDocument struct {
	Name     string            `json:"name"`
	Version  uint16            `json:"version"`
	Settings []settingDocument `json:"settings,omitempty"`
}

type settingDocument struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type edgeDocument struct {
	EdgeID             uint16 `json:"edge_id"`
	SourceStageID      uint16 `json:"source_stage_id"`
	DestinationStageID uint16 `json:"destination_stage_id"`
	Routing            string `json:"routing"`
	Field              string `json:"field,omitempty"`
}

// parseTopologyDocument strictly decodes and fully validates one DAG document.
func parseTopologyDocument(encoded []byte) (model.TopologySpec, error) {
	if len(encoded) == 0 || len(encoded) > maxTopologyDocumentBytes {
		return model.TopologySpec{}, fmt.Errorf("topology document is %d bytes, maximum is %d", len(encoded), maxTopologyDocumentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var document topologyDocument
	if err := decoder.Decode(&document); err != nil {
		return model.TopologySpec{}, fmt.Errorf("decode topology document: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return model.TopologySpec{}, errors.New("topology document has trailing JSON data")
	}
	topology := model.TopologySpec{
		SchemaVersion:       document.SchemaVersion,
		Name:                document.Name,
		RegistryFingerprint: model.RegistryFingerprint(),
	}
	for _, stage := range document.Stages {
		role, err := parseStageRole(stage.Role)
		if err != nil {
			return model.TopologySpec{}, fmt.Errorf("stage %d: %w", stage.StageID, err)
		}
		settings := make([]model.Setting, 0, len(stage.Operator.Settings))
		for _, setting := range stage.Operator.Settings {
			settings = append(settings, model.Setting{Key: setting.Key, Value: setting.Value})
		}
		topology.Stages = append(topology.Stages, model.StageSpec{
			StageID: stage.StageID, Name: stage.Name, Role: role, Parallelism: stage.Parallelism,
			Operator: model.OperatorSpec{Name: stage.Operator.Name, Version: stage.Operator.Version, Settings: settings},
		})
	}
	for _, edge := range document.Edges {
		routing, err := parseRoutingMode(edge.Routing)
		if err != nil {
			return model.TopologySpec{}, fmt.Errorf("edge %d: %w", edge.EdgeID, err)
		}
		topology.Edges = append(topology.Edges, model.EdgeSpec{
			EdgeID: edge.EdgeID, SourceStageID: edge.SourceStageID, DestinationStageID: edge.DestinationStageID,
			Routing: routing, Field: edge.Field,
		})
	}
	if _, err := model.ValidateTopology(topology); err != nil {
		return model.TopologySpec{}, fmt.Errorf("validate topology document: %w", err)
	}
	return topology, nil
}
