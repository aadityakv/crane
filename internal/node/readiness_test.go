package node

import "testing"

func TestReadySignalRoundTripIsStrictAndMachineReadable(t *testing.T) {
	line := ReadySignal(42)
	if line != "CRANE_NODE_READY node_id=42" {
		t.Fatalf("ReadySignal(42) = %q", line)
	}
	if nodeID, ok := ParseReadySignal(line); !ok || nodeID != 42 {
		t.Fatalf("ParseReadySignal(%q) = %d, %v", line, nodeID, ok)
	}
	for _, malformed := range []string{
		"CRANE_NODE_READY node_id=0",
		"CRANE_NODE_READY node_id=65536",
		"CRANE_NODE_READY node_id=42 trailing",
		"[node-42] CRANE_NODE_READY node_id=42",
		"CRANE_NODE_READY node_id=042",
		"",
	} {
		if nodeID, ok := ParseReadySignal(malformed); ok {
			t.Errorf("ParseReadySignal(%q) = %d, true", malformed, nodeID)
		}
	}
}
