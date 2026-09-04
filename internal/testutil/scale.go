package testutil

import (
	"os"
	"strconv"
	"time"
)

// TimeScaleEnvironment names the environment variable that multiplies every
// real-time test budget (deadlines, wait windows) so timing-sensitive suites
// can run unchanged on slower shared runners. Unset or 1 leaves budgets as
// written; values are clamped to [1, 10].
const TimeScaleEnvironment = "CRANE_TEST_TIME_SCALE"

// Scale multiplies a real-time test budget by the configured time scale.
func Scale(d time.Duration) time.Duration {
	raw, present := os.LookupEnv(TimeScaleEnvironment)
	if !present {
		return d
	}
	factor, err := strconv.Atoi(raw)
	if err != nil || factor < 1 {
		return d
	}
	if factor > 10 {
		factor = 10
	}
	return d * time.Duration(factor)
}
