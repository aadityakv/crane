// Package topology contains the portable public data schemas retained for the
// later Crane modernization. It deliberately contains no plugin loading,
// networking, process control, or global port policy.
package topology

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidTopology classifies a malformed topology graph or schema value.
	ErrInvalidTopology = errors.New("invalid topology")
	// ErrUnknownOperator classifies an operator absent from a supplied registry.
	ErrUnknownOperator = errors.New("unknown topology operator")
)

const (
	// FeatureSpout identifies the topology's single source node.
	FeatureSpout = "S"
	// FeatureBolt identifies a processing node with one parent.
	FeatureBolt = "B"
)

// Node is one portable topology node specification.
type Node struct {
	// FeatureType is FeatureSpout or FeatureBolt.
	FeatureType string
	// SubType names the built-in operator required by this node.
	SubType string
	// ID uniquely identifies this node within its Tree.
	ID string
	// ParentID identifies a bolt's upstream node and is empty for a spout.
	ParentID string
	// Tasks is the positive desired parallelism.
	Tasks int
	// Grouping is an optional tuple field used for deterministic grouping.
	Grouping string
}

// Tree is a named, validated topology graph.
type Tree struct {
	// Name is the nonempty topology name.
	Name string
	// Nodes contains one spout and zero or more acyclic bolts.
	Nodes []Node
}

// Tuple is the retained portable tuple payload schema.
type Tuple struct {
	// ID is the legacy per-source tuple sequence.
	ID uint32
	// Fields contains application fields; runtime logs must not emit it by default.
	Fields map[string]string
}

// TupAck is the retained tuple-acknowledgment schema.
type TupAck struct {
	// Tup identifies the acknowledged tuple.
	Tup *Tuple
	// Weight is the number of downstream acknowledgments represented.
	Weight int
}

// Destination describes one downstream topology node.
type Destination struct {
	// ID is the downstream node ID.
	ID string
	// Tasks is the downstream parallelism.
	Tasks int
	// Grouping is the optional deterministic grouping field.
	Grouping string
}

// HyperEdge describes the retained fanout schema from one root node.
type HyperEdge struct {
	// Root is the upstream topology node ID.
	Root string
	// Weight is the total acknowledgment weight.
	Weight int
	// Children lists downstream destinations.
	Children []Destination
}

// Track contains the retained child-tracking schema.
type Track struct {
	// Children lists downstream node IDs.
	Children []string
	// Weight is the aggregate child weight.
	Weight int
}

// WorkerTask is the retained portable task-location schema.
type WorkerTask struct {
	// TaskID is the stable logical task ID.
	TaskID string
	// Hostname is the configured worker host or IP address.
	Hostname string
	// Port is the retained textual endpoint port.
	Port string
}

// NewBolt returns a bolt schema with one parent.
func NewBolt(subtype, id, parentID string, tasks int, grouping string) Node {
	return Node{FeatureType: FeatureBolt, SubType: subtype, ID: id, ParentID: parentID, Tasks: tasks, Grouping: grouping}
}

// NewSpout returns a source schema with no parent.
func NewSpout(subtype, id string, tasks int) Node {
	return Node{FeatureType: FeatureSpout, SubType: subtype, ID: id, Tasks: tasks}
}

// AddSpout appends node only when it is a spout schema.
func (t *Tree) AddSpout(node Node) {
	if t != nil && node.FeatureType == FeatureSpout {
		t.Nodes = append(t.Nodes, node)
	}
}

// AddBolt appends node only when it is a bolt schema.
func (t *Tree) AddBolt(node Node) {
	if t != nil && node.FeatureType == FeatureBolt {
		t.Nodes = append(t.Nodes, node)
	}
}

// Validate checks names, task counts, unique IDs, one spout, valid parents,
// and acyclicity without requiring a concrete operator registry.
func (t Tree) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("%w: name is empty", ErrInvalidTopology)
	}
	if len(t.Nodes) == 0 {
		return fmt.Errorf("%w: graph has no nodes", ErrInvalidTopology)
	}
	nodes := make(map[string]Node, len(t.Nodes))
	spouts := 0
	for _, node := range t.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return fmt.Errorf("%w: node ID is empty", ErrInvalidTopology)
		}
		if _, duplicate := nodes[node.ID]; duplicate {
			return fmt.Errorf("%w: duplicate node ID %q", ErrInvalidTopology, node.ID)
		}
		if strings.TrimSpace(node.SubType) == "" {
			return fmt.Errorf("%w: node %q operator is empty", ErrInvalidTopology, node.ID)
		}
		if node.Tasks <= 0 {
			return fmt.Errorf("%w: node %q tasks must be positive", ErrInvalidTopology, node.ID)
		}
		switch node.FeatureType {
		case FeatureSpout:
			spouts++
			if node.ParentID != "" {
				return fmt.Errorf("%w: spout %q has a parent", ErrInvalidTopology, node.ID)
			}
		case FeatureBolt:
			if strings.TrimSpace(node.ParentID) == "" {
				return fmt.Errorf("%w: bolt %q has no parent", ErrInvalidTopology, node.ID)
			}
		default:
			return fmt.Errorf("%w: node %q has feature %q", ErrInvalidTopology, node.ID, node.FeatureType)
		}
		nodes[node.ID] = node
	}
	if spouts != 1 {
		return fmt.Errorf("%w: graph has %d spouts, want 1", ErrInvalidTopology, spouts)
	}
	for _, node := range t.Nodes {
		if node.FeatureType == FeatureBolt {
			if _, exists := nodes[node.ParentID]; !exists {
				return fmt.Errorf("%w: node %q has unknown parent %q", ErrInvalidTopology, node.ID, node.ParentID)
			}
		}
	}
	visiting := make(map[string]bool, len(nodes))
	validated := make(map[string]bool, len(nodes))
	var reachesSpout func(string) error
	reachesSpout = func(id string) error {
		if validated[id] {
			return nil
		}
		if visiting[id] {
			return fmt.Errorf("%w: cycle through node %q", ErrInvalidTopology, id)
		}
		visiting[id] = true
		node := nodes[id]
		if node.FeatureType == FeatureBolt {
			if err := reachesSpout(node.ParentID); err != nil {
				return err
			}
		}
		delete(visiting, id)
		validated[id] = true
		return nil
	}
	for id := range nodes {
		if err := reachesSpout(id); err != nil {
			return err
		}
	}
	return nil
}

// ValidateOperators performs structural validation and requires every node's
// operator name to exist in known.
func (t Tree) ValidateOperators(known map[string]struct{}) error {
	if err := t.Validate(); err != nil {
		return err
	}
	for _, node := range t.Nodes {
		if _, exists := known[node.SubType]; !exists {
			return fmt.Errorf("%w %q for node %q", ErrUnknownOperator, node.SubType, node.ID)
		}
	}
	return nil
}
