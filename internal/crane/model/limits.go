// Package model defines immutable, dependency-leaf Crane consensus contracts.
package model

import "math"

// ConsensusLimits contains every fixed v1 bound that can affect a replicated
// Crane decision or its canonical encoding.
type ConsensusLimits struct {
	MaxRegisteredWorkers        uint64
	MaxActiveJobs               uint64
	MaxRetainedJobs             uint64
	MaxRetainedClientSessions   uint64
	MaxResultManifestsPerJob    uint64
	MaxCachedCommandResultBytes uint64
	MaxResultRecordsBytesPerJob uint64
	MaxSnapshotBytes            uint64
	MaxIdentifierBytes          uint64
	MaxStages                   uint64
	MaxEdges                    uint64
	MaxTasksPerStage            uint64
	MaxTasksPerJob              uint64
	MaxSettingsPerStage         uint64
	MaxSettingKeyBytes          uint64
	MaxSettingValueBytes        uint64
	MaxTotalSettingsBytes       uint64
	MaxSourceSequences          uint64
	MaxOperatorOutputs          uint64
	MaxDerivedDeliveries        uint64
	MaxTupleFieldPayloadBytes   uint64
	MaxSubmitJobBytes           uint64
	SubmitJobOverheadBytes      uint64
	SubmitRequestOverheadBytes  uint64
	AssignmentSetOverheadBytes  uint64
	MaxTopologyBytes            uint64
}

const maxTopologyStructuralBytes uint64 = 118_784

// LimitsV1 returns the fixed Crane v1 protocol limits.
func LimitsV1() ConsensusLimits {
	const maxSubmitJobBytes uint64 = 1 << 20
	const submitJobOverheadBytes uint64 = 128
	const submitRequestOverheadBytes uint64 = 96
	const assignmentSetOverheadBytes uint64 = 384 << 10
	return ConsensusLimits{
		MaxRegisteredWorkers:        1024,
		MaxActiveJobs:               64,
		MaxRetainedJobs:             256,
		MaxRetainedClientSessions:   1024,
		MaxResultManifestsPerJob:    256,
		MaxCachedCommandResultBytes: 64 << 10,
		MaxResultRecordsBytesPerJob: 64 << 20,
		MaxSnapshotBytes:            8 << 20,
		MaxIdentifierBytes:          64,
		MaxStages:                   64,
		MaxEdges:                    256,
		MaxTasksPerStage:            256,
		MaxTasksPerJob:              1024,
		MaxSettingsPerStage:         32,
		MaxSettingKeyBytes:          64,
		MaxSettingValueBytes:        1024,
		MaxTotalSettingsBytes:       64 << 10,
		MaxSourceSequences:          1_000_000,
		MaxOperatorOutputs:          16,
		MaxDerivedDeliveries:        4096,
		MaxTupleFieldPayloadBytes:   512,
		MaxSubmitJobBytes:           maxSubmitJobBytes,
		SubmitJobOverheadBytes:      submitJobOverheadBytes,
		SubmitRequestOverheadBytes:  submitRequestOverheadBytes,
		AssignmentSetOverheadBytes:  assignmentSetOverheadBytes,
		MaxTopologyBytes: minUint64(
			maxTopologyStructuralBytes,
			checkedSubtract(maxSubmitJobBytes, submitJobOverheadBytes),
			checkedSubtract(maxSubmitJobBytes, submitRequestOverheadBytes),
			checkedSubtract(maxSubmitJobBytes, assignmentSetOverheadBytes),
		),
	}
}

func checkedSubtract(total, reserved uint64) uint64 {
	if reserved > total {
		panic("crane consensus limit underflow")
	}
	return total - reserved
}

func minUint64(values ...uint64) uint64 {
	minimum := uint64(math.MaxUint64)
	for _, value := range values {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}
