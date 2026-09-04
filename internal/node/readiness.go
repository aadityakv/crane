package node

import (
	"fmt"
	"strconv"
	"strings"
)

const readySignalPrefix = "CRANE_NODE_READY node_id="

// ReadySignal returns the exact machine-readable line emitted after all node services are ready.
func ReadySignal(nodeID uint16) string {
	return fmt.Sprintf("%s%d", readySignalPrefix, nodeID)
}

// ParseReadySignal accepts only the canonical nonzero node readiness line.
func ParseReadySignal(line string) (uint16, bool) {
	encoded, ok := strings.CutPrefix(line, readySignalPrefix)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseUint(encoded, 10, 16)
	if err != nil || parsed == 0 {
		return 0, false
	}
	nodeID := uint16(parsed)
	if ReadySignal(nodeID) != line {
		return 0, false
	}
	return nodeID, true
}
