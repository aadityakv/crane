// Package model defines immutable, dependency-leaf Crane consensus contracts.
package model

import (
	"errors"
	"math"
)

// ConsensusLimits contains every fixed v1 bound that can affect a replicated
// Crane decision or its canonical encoding.
type ConsensusLimits struct {
	MaxRegisteredWorkers           uint64
	MaxActiveJobs                  uint64
	MaxRetainedJobs                uint64
	MaxRetainedClientSessions      uint64
	MaxResultManifestsPerJob       uint64
	MaxCachedCommandResultBytes    uint64
	MaxResultRecordsBytesPerJob    uint64
	MaxSnapshotBytes               uint64
	MaxIdentifierBytes             uint64
	MaxStages                      uint64
	MaxEdges                       uint64
	MaxTasksPerStage               uint64
	MaxTasksPerJob                 uint64
	MaxWorkerSlots                 uint64
	MaxSettingsPerStage            uint64
	MaxSettingKeyBytes             uint64
	MaxSettingValueBytes           uint64
	MaxTotalSettingsBytes          uint64
	MaxSourceSequences             uint64
	MaxOperatorOutputs             uint64
	MaxDerivedDeliveries           uint64
	MaxTupleFields                 uint64
	MaxTuplePayloadBytes           uint64
	CustodyInboxFixedBytes         uint64
	CustodyOutboxFixedBytes        uint64
	ResultCopyFixedBytes           uint64
	MaxCustodyReservationBytes     uint64
	MaxSubmitJobBytes              uint64
	MaxControlFrameBytes           uint64
	MaxWorkerControlFrameBytes     uint64
	AuthenticatedFrameBytes        uint64
	SubmitJobFixedBytes            uint64
	SubmitRequestFixedBytes        uint64
	AssignmentSetInstallFixedBytes uint64
	MaxTopologyBytes               uint64
}

const maxTopologyStructuralBytes uint64 = 118_784

const (
	v1Uint16Bytes           = uint64(2)
	v1Uint64Bytes           = uint64(8)
	v1JobIDBytes            = uint64(16)
	v1TaskIDBytes           = v1JobIDBytes + 2*v1Uint16Bytes
	v1WorkerEpochBytes      = uint64(16)
	v1DigestBytes           = uint64(32)
	v1TupleIDBytes          = v1JobIDBytes + v1TaskIDBytes + v1Uint64Bytes + v1DigestBytes
	v1DeliveryIDBytes       = v1TupleIDBytes + v1Uint16Bytes + v1TaskIDBytes
	v1AssignmentTokenBytes  = v1TaskIDBytes + v1Uint16Bytes + v1WorkerEpochBytes + v1Uint64Bytes + v1DigestBytes + v1Uint64Bytes
	v1CoordinatorEpochBytes = 2*v1Uint64Bytes + v1Uint16Bytes + 16
	v1ResultReplicaSetBytes = v1TaskIDBytes + 2*v1Uint16Bytes + 2*v1WorkerEpochBytes

	// The current authenticated frame is a 55-byte header plus a 32-byte MAC.
	v1AuthenticatedFrameBytes = uint64(87)
	// SubmitRequest: frame + schema + ClientRequestID + topology digest.
	v1SubmitRequestFixedBytes = v1AuthenticatedFrameBytes + v1Uint16Bytes + (16 + v1Uint64Bytes) + v1DigestBytes
	// SubmitJob: schema + consensus fingerprint + kind + envelope selector +
	// ClientRequestID + complete client-command digest.
	v1SubmitJobFixedBytes = v1Uint16Bytes + v1DigestBytes + v1Uint16Bytes + 1 + (16 + v1Uint64Bytes) + v1DigestBytes
	// One AssignmentSetInstall excluding canonical topology bytes: authenticated
	// frame, message schema, complete maximum AssignmentSet, specification digest,
	// JobControlRevision, SchedulingState, and CoordinatorEpoch.
	v1AssignmentSetInstallBaseBytes = v1AuthenticatedFrameBytes + v1Uint16Bytes +
		(v1JobIDBytes + v1Uint64Bytes + v1DigestBytes) + v1Uint16Bytes +
		v1Uint16Bytes + v1DigestBytes + v1Uint64Bytes + 1 + v1CoordinatorEpochBytes

	// Durable custody records carry canonical identities/fences/checksums beside
	// one length-prefixed tuple. These constants exclude the tuple bytes itself.
	v1CustodyInboxFixedBytes = 1 + v1DeliveryIDBytes + 2*v1AssignmentTokenBytes +
		v1Uint64Bytes + v1DigestBytes + v1CoordinatorEpochBytes + v1Uint16Bytes + v1DigestBytes
	v1CustodyOutboxFixedBytes = 1 + v1DeliveryIDBytes + v1AssignmentTokenBytes +
		v1Uint64Bytes + v1DigestBytes + v1CoordinatorEpochBytes + v1Uint16Bytes + v1DigestBytes
	v1ResultCopyFixedBytes = v1TupleIDBytes + v1TaskIDBytes + v1DigestBytes +
		v1Uint16Bytes + v1DigestBytes + v1Uint64Bytes + v1DigestBytes +
		v1ResultReplicaSetBytes + 1 + v1CoordinatorEpochBytes
)

// LimitsV1 returns the fixed Crane v1 protocol limits.
func LimitsV1() ConsensusLimits {
	const maxSubmitJobBytes uint64 = 1 << 20
	const maxControlFrameBytes uint64 = 1 << 20
	const maxWorkerControlFrameBytes uint64 = 1 << 20
	const maxTasksPerJob uint64 = 1024
	const maxTasksPerStage uint64 = 256
	const maxTupleFields uint64 = 64
	const maxIdentifierBytes uint64 = 64
	const maxTuplePayloadBytes uint64 = 512
	const maxDerivedDeliveries uint64 = 4096
	const maxOperatorOutputs uint64 = 16
	assignmentSetInstallFixedBytes := v1AssignmentSetInstallBaseBytes + maxTasksPerJob*v1AssignmentTokenBytes + maxTasksPerStage*v1ResultReplicaSetBytes
	inboxBytes := v1CustodyInboxFixedBytes + maxTuplePayloadBytes
	outboxBytes := v1CustodyOutboxFixedBytes + maxTuplePayloadBytes
	resultCopyBytes := v1ResultCopyFixedBytes + maxTuplePayloadBytes
	maxCustodyReservationBytes := inboxBytes + maxDerivedDeliveries*(outboxBytes+inboxBytes+2*resultCopyBytes)
	return ConsensusLimits{
		MaxRegisteredWorkers:           1024,
		MaxActiveJobs:                  64,
		MaxRetainedJobs:                256,
		MaxRetainedClientSessions:      1024,
		MaxResultManifestsPerJob:       256,
		MaxCachedCommandResultBytes:    64 << 10,
		MaxResultRecordsBytesPerJob:    64 << 20,
		MaxSnapshotBytes:               8 << 20,
		MaxIdentifierBytes:             maxIdentifierBytes,
		MaxStages:                      64,
		MaxEdges:                       256,
		MaxTasksPerStage:               maxTasksPerStage,
		MaxTasksPerJob:                 maxTasksPerJob,
		MaxWorkerSlots:                 maxTasksPerStage,
		MaxSettingsPerStage:            32,
		MaxSettingKeyBytes:             64,
		MaxSettingValueBytes:           1024,
		MaxTotalSettingsBytes:          64 << 10,
		MaxSourceSequences:             1_000_000,
		MaxOperatorOutputs:             maxOperatorOutputs,
		MaxDerivedDeliveries:           maxDerivedDeliveries,
		MaxTupleFields:                 maxTupleFields,
		MaxTuplePayloadBytes:           maxTuplePayloadBytes,
		CustodyInboxFixedBytes:         v1CustodyInboxFixedBytes,
		CustodyOutboxFixedBytes:        v1CustodyOutboxFixedBytes,
		ResultCopyFixedBytes:           v1ResultCopyFixedBytes,
		MaxCustodyReservationBytes:     maxCustodyReservationBytes,
		MaxSubmitJobBytes:              maxSubmitJobBytes,
		MaxControlFrameBytes:           maxControlFrameBytes,
		MaxWorkerControlFrameBytes:     maxWorkerControlFrameBytes,
		AuthenticatedFrameBytes:        v1AuthenticatedFrameBytes,
		SubmitJobFixedBytes:            v1SubmitJobFixedBytes,
		SubmitRequestFixedBytes:        v1SubmitRequestFixedBytes,
		AssignmentSetInstallFixedBytes: assignmentSetInstallFixedBytes,
		MaxTopologyBytes: minUint64(
			maxTopologyStructuralBytes,
			checkedSubtract(maxSubmitJobBytes, v1SubmitJobFixedBytes),
			checkedSubtract(maxControlFrameBytes, v1SubmitRequestFixedBytes),
			checkedSubtract(maxWorkerControlFrameBytes, assignmentSetInstallFixedBytes),
		),
	}
}

// CustodyReservationUpperBoundV1 returns the exact conservative durable-byte
// ceiling for a delivery tree containing the supplied total derived deliveries.
func CustodyReservationUpperBoundV1(deliveries uint64) (uint64, error) {
	limits := LimitsV1()
	if deliveries > limits.MaxDerivedDeliveries {
		return 0, errors.New("derived delivery count exceeds custody bound")
	}
	inbox, ok := checkedAddUint64(limits.CustodyInboxFixedBytes, limits.MaxTuplePayloadBytes)
	if !ok {
		return 0, errors.New("custody inbox bound overflow")
	}
	outbox, ok := checkedAddUint64(limits.CustodyOutboxFixedBytes, limits.MaxTuplePayloadBytes)
	if !ok {
		return 0, errors.New("custody outbox bound overflow")
	}
	result, ok := checkedAddUint64(limits.ResultCopyFixedBytes, limits.MaxTuplePayloadBytes)
	if !ok {
		return 0, errors.New("result copy bound overflow")
	}
	perDelivery, ok := checkedAddUint64(outbox, inbox)
	if !ok {
		return 0, errors.New("custody bound overflow")
	}
	copies, ok := checkedMultiplyUint64(2, result)
	if !ok {
		return 0, errors.New("custody bound overflow")
	}
	perDelivery, ok = checkedAddUint64(perDelivery, copies)
	if !ok {
		return 0, errors.New("custody bound overflow")
	}
	tree, ok := checkedMultiplyUint64(deliveries, perDelivery)
	if !ok {
		return 0, errors.New("custody bound overflow")
	}
	total, ok := checkedAddUint64(inbox, tree)
	if !ok {
		return 0, errors.New("custody bound overflow")
	}
	return total, nil
}

// CompleteSubmitRequestBytes returns the exact authenticated v1 frame size.
func CompleteSubmitRequestBytes(topologyBytes uint64) (uint64, error) {
	limits := LimitsV1()
	return checkedCompleteSize(topologyBytes, limits.SubmitRequestFixedBytes, limits.MaxControlFrameBytes)
}

// CompleteSubmitJobBytes returns the exact v1 Raft command size.
func CompleteSubmitJobBytes(topologyBytes uint64) (uint64, error) {
	limits := LimitsV1()
	return checkedCompleteSize(topologyBytes, limits.SubmitJobFixedBytes, limits.MaxSubmitJobBytes)
}

// CompleteAssignmentSetInstallBytes returns the exact authenticated v1 frame
// size for the supplied complete assignment counts and canonical topology.
func CompleteAssignmentSetInstallBytes(topologyBytes, taskCount, replicaCount uint64) (uint64, error) {
	limits := LimitsV1()
	if taskCount > limits.MaxTasksPerJob || replicaCount > limits.MaxTasksPerStage {
		return 0, errors.New("assignment count exceeds v1 complete-frame bound")
	}
	fixed := v1AssignmentSetInstallBaseBytes
	taskBytes, ok := checkedMultiplyUint64(taskCount, v1AssignmentTokenBytes)
	if !ok {
		return 0, errors.New("assignment token bytes overflow")
	}
	fixed, ok = checkedAddUint64(fixed, taskBytes)
	if !ok {
		return 0, errors.New("assignment frame size overflow")
	}
	replicaBytes, ok := checkedMultiplyUint64(replicaCount, v1ResultReplicaSetBytes)
	if !ok {
		return 0, errors.New("result replica bytes overflow")
	}
	fixed, ok = checkedAddUint64(fixed, replicaBytes)
	if !ok {
		return 0, errors.New("assignment frame size overflow")
	}
	return checkedCompleteSize(topologyBytes, fixed, limits.MaxWorkerControlFrameBytes)
}

func checkedCompleteSize(variableBytes, fixedBytes, maximumBytes uint64) (uint64, error) {
	total, ok := checkedAddUint64(variableBytes, fixedBytes)
	if !ok {
		return 0, errors.New("complete encoding size overflow")
	}
	if total > maximumBytes {
		return 0, errors.New("complete encoding exceeds v1 1 MiB bound")
	}
	return total, nil
}

func checkedAddUint64(left, right uint64) (uint64, bool) {
	if left > math.MaxUint64-right {
		return 0, false
	}
	return left + right, true
}

func checkedMultiplyUint64(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
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
