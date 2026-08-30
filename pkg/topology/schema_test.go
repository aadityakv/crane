package topology

import (
	"errors"
	"testing"
)

func TestLegacyTopologySchemasRemainAvailable(t *testing.T) {
	spout := NewSpout("numbers", "source", 1)
	bolt := NewBolt("filter", "even", "source", 2, "number")
	tree := Tree{Name: "numbers", Nodes: []Node{spout, bolt}}
	if err := tree.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := tree.ValidateOperators(map[string]struct{}{"numbers": {}, "filter": {}}); err != nil {
		t.Fatalf("ValidateOperators() error = %v", err)
	}

	_ = TupAck{Tup: &Tuple{ID: 1, Fields: map[string]string{"number": "2"}}, Weight: 1}
	_ = Destination{ID: bolt.ID, Tasks: bolt.Tasks, Grouping: bolt.Grouping}
	_ = HyperEdge{Root: spout.ID, Weight: 1, Children: []Destination{{ID: bolt.ID}}}
	_ = Track{Children: []string{bolt.ID}, Weight: 1}
	_ = WorkerTask{TaskID: "even-0", Hostname: "node-2", Port: "9000"}
}

func TestTreeValidationRejectsUnsafeGraphs(t *testing.T) {
	valid := Tree{Name: "valid", Nodes: []Node{
		NewSpout("source", "source", 1),
		NewBolt("map", "map", "source", 1, ""),
	}}
	tests := []struct {
		name   string
		mutate func(*Tree)
	}{
		{name: "empty name", mutate: func(tree *Tree) { tree.Name = "" }},
		{name: "duplicate ID", mutate: func(tree *Tree) { tree.Nodes[1].ID = tree.Nodes[0].ID }},
		{name: "no spout", mutate: func(tree *Tree) { tree.Nodes[0].FeatureType = FeatureBolt }},
		{name: "two spouts", mutate: func(tree *Tree) { tree.Nodes[1] = NewSpout("other", "other", 1) }},
		{name: "missing parent", mutate: func(tree *Tree) { tree.Nodes[1].ParentID = "missing" }},
		{name: "nonpositive tasks", mutate: func(tree *Tree) { tree.Nodes[1].Tasks = 0 }},
		{name: "empty operator", mutate: func(tree *Tree) { tree.Nodes[1].SubType = "" }},
		{name: "unknown feature", mutate: func(tree *Tree) { tree.Nodes[1].FeatureType = "?" }},
		{name: "cycle", mutate: func(tree *Tree) {
			tree.Nodes = append(tree.Nodes,
				NewBolt("left", "left", "right", 1, ""),
				NewBolt("right", "right", "left", 1, ""),
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := valid
			tree.Nodes = append([]Node(nil), valid.Nodes...)
			test.mutate(&tree)
			if err := tree.Validate(); !errors.Is(err, ErrInvalidTopology) {
				t.Fatalf("Validate() error = %v, want ErrInvalidTopology", err)
			}
		})
	}
}

func TestTreeValidationRejectsUnknownOperator(t *testing.T) {
	tree := Tree{Name: "numbers", Nodes: []Node{
		NewSpout("source", "source", 1),
		NewBolt("missing", "sink", "source", 1, ""),
	}}
	if err := tree.ValidateOperators(map[string]struct{}{"source": {}}); !errors.Is(err, ErrUnknownOperator) {
		t.Fatalf("ValidateOperators() error = %v, want ErrUnknownOperator", err)
	}
}
