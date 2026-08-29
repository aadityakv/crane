package node

import "testing"

func TestReadySignalRoundTripIsStrictAndMachineReadable(t *testing.T) {
	line := ReadySignal(42)
	if line != "CS425_NODE_READY node_id=42" {
		t.Fatalf("ReadySignal(42) = %q", line)
	}
	if nodeID, ok := ParseReadySignal(line); !ok || nodeID != 42 {
		t.Fatalf("ParseReadySignal(%q) = %d, %v", line, nodeID, ok)
	}
	for _, malformed := range []string{
		"CS425_NODE_READY node_id=0",
		"CS425_NODE_READY node_id=65536",
		"CS425_NODE_READY node_id=42 trailing",
		"[node-42] CS425_NODE_READY node_id=42",
		"CS425_NODE_READY node_id=042",
		"",
	} {
		if nodeID, ok := ParseReadySignal(malformed); ok {
			t.Errorf("ParseReadySignal(%q) = %d, true", malformed, nodeID)
		}
	}
}
