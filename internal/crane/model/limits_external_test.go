package model_test

import (
	"testing"

	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestConsensusLimitsMatchExistingIndependentTransportBounds(t *testing.T) {
	limits := model.LimitsV1()
	if limits.MaxSubmitJobBytes != config.MaxRaftCommandBytes {
		t.Fatalf("SubmitJob bytes = %d, Raft command bytes = %d", limits.MaxSubmitJobBytes, config.MaxRaftCommandBytes)
	}
	if limits.MaxSnapshotBytes > config.MaxRaftSnapshotBytes {
		t.Fatalf("Crane snapshot bytes = %d, Raft maximum = %d", limits.MaxSnapshotBytes, config.MaxRaftSnapshotBytes)
	}
	if limits.MaxSubmitJobBytes > uint64(wire.DefaultLimits().MaxFrameSize) {
		t.Fatalf("SubmitJob bytes = %d, frame bytes = %d", limits.MaxSubmitJobBytes, wire.DefaultLimits().MaxFrameSize)
	}
}
