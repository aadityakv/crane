package raft

type task8StateMachineStub struct{}

func (task8StateMachineStub) Apply(uint64, uint64, []byte) ([]byte, error) { return nil, nil }
func (task8StateMachineStub) Capture(uint64, uint64) (SnapshotCapture, error) {
	return nil, nil
}
func (task8StateMachineStub) Restore(uint32, []byte) error { return nil }

var _ StateMachine = task8StateMachineStub{}
