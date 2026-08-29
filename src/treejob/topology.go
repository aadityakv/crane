package main

import (
	"topology"
)

func GetTree() topology.Tree {
	t := &topology.Tree{Name: "Treejob"}
	t.AddSpout(topology.NewSpout("numspout", "numspout", 1))
	t.AddBolt(topology.NewBolt("passeven", "passeven", "numspout", 2, ""))
	t.AddBolt(topology.NewBolt("belowten", "belowten", "passeven", 2, "number"))

	return *t
}
