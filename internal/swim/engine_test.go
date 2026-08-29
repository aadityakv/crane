package swim

import (
	"time"
)

func newTestEngineWithSelf(self Member) *Engine {
	engine, err := NewEngine(
		EngineConfig{
			SelfID:               self.NodeID,
			ProbeInterval:        time.Second,
			DirectProbeTimeout:   300 * time.Millisecond,
			IndirectProbeTimeout: 700 * time.Millisecond,
			IndirectChecks:       3,
			SuspicionMultiplier:  5,
		},
		NewTable(),
		NewDisseminator(32, 3),
		&scriptedRandom{uint64s: []uint64{41}},
	)
	if err != nil {
		panic(err)
	}
	if changed, _ := engine.table.Merge(Update{Member: self, ReporterID: self.NodeID}); !changed {
		panic("test self member was not merged")
	}
	return engine
}
