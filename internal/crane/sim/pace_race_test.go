//go:build race

package sim

import "time"

// Under the race detector every goroutine runs several times slower, so each
// virtual step grants the runtimes proportionally more wall time; the virtual
// schedule (clock quantum, budgets) is unchanged.
func init() {
	simPaceSlice = 3000 * time.Microsecond
}
