package worker

import (
	"fmt"
	"sync"
)

var (
	diagnosticMu    sync.RWMutex
	diagnosticTrace func(string)
)

// SetDiagnosticTrace installs a process-wide diagnostic trace sink for the
// worker's replication and readoption decisions (nil disables it). It is a
// harness seam: production runs never install one, and the traced paths pay
// only a read lock when none is installed.
func SetDiagnosticTrace(sink func(string)) {
	diagnosticMu.Lock()
	defer diagnosticMu.Unlock()
	diagnosticTrace = sink
}

func tracef(format string, args ...any) {
	diagnosticMu.RLock()
	sink := diagnosticTrace
	diagnosticMu.RUnlock()
	if sink != nil {
		sink(fmt.Sprintf(format, args...))
	}
}
