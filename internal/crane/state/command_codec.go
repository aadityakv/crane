package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	identityClient   byte = 1
	identityInternal byte = 2
)

// MarshalCommand emits the sole canonical encoding of one concrete command.
func MarshalCommand(command any) ([]byte, error) {
	switch value := command.(type) {
	case BeginCoordinatorEpoch:
		return MarshalBeginCoordinatorEpoch(value)
	case RegisterWorker:
		return marshalConcreteCommand(value.Envelope, registerWorkerTarget(value), value.Validate())
	case DrainWorker:
		return marshalConcreteCommand(value.Envelope, drainWorkerTarget(value), value.Validate())
	case DeactivateWorker:
		return marshalConcreteCommand(value.Envelope, deactivateWorkerTarget(value), value.Validate())
	case ReplaceWorkerEpoch:
		return marshalConcreteCommand(value.Envelope, replaceWorkerEpochTarget(value), value.Validate())
	case SubmitJob:
		target, err := submitJobTargetTraced(value, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
		}
		return marshalConcreteCommand(value.Envelope, target, value.Validate())
	case CancelJob:
		return marshalConcreteCommand(value.Envelope, cancelJobTarget(value), value.Validate())
	case RecordSourceEOF:
		return marshalConcreteCommand(value.Envelope, recordSourceEOFTarget(value), value.Validate())
	case InstallAssignments:
		return marshalConcreteCommand(value.Envelope, installAssignmentsTarget(value), value.Validate())
	case ReplaceAssignments:
		return marshalConcreteCommand(value.Envelope, replaceAssignmentsTarget(value), value.Validate())
	case AdvanceCheckpoint:
		return marshalConcreteCommand(value.Envelope, advanceCheckpointTarget(value), value.Validate())
	case SealManifest:
		return marshalConcreteCommand(value.Envelope, sealManifestTarget(value), value.Validate())
	case TransitionJob:
		return marshalConcreteCommand(value.Envelope, transitionJobTarget(value), value.Validate())
	case FailJob:
		return marshalConcreteCommand(value.Envelope, failJobTarget(value), value.Validate())
	default:
		return nil, fmt.Errorf("%w: unsupported concrete command %T", ErrInvalidCommand, command)
	}
}

// UnmarshalCommand decodes one bounded concrete command without opaque payloads.
func UnmarshalCommand(encoded []byte) (any, error) {
	if uint64(len(encoded)) > model.LimitsV1().MaxSubmitJobBytes {
		return nil, fmt.Errorf("%w: command exceeds maximum bytes", ErrMalformedCommand)
	}
	if len(encoded) < int(commandFixedEnvelopeBytes) {
		return nil, fmt.Errorf("%w: truncated envelope", ErrMalformedCommand)
	}
	kind := CommandKind(binary.BigEndian.Uint16(encoded[34:36]))
	if kind == CommandBeginCoordinatorEpoch {
		return UnmarshalBeginCoordinatorEpoch(encoded)
	}
	decoder := commandDecoder{input: encoded}
	envelope, err := decoder.envelope()
	if err != nil {
		return nil, err
	}
	var command any
	switch kind {
	case CommandRegisterWorker:
		worker, err := decoder.workerRecord()
		if err != nil {
			return nil, err
		}
		command = RegisterWorker{Envelope: envelope, Worker: worker}
	case CommandDrainWorker:
		workerID, err := decoder.u16()
		if err != nil {
			return nil, err
		}
		epoch, err := decoder.workerEpoch()
		if err != nil {
			return nil, err
		}
		command = DrainWorker{Envelope: envelope, WorkerID: workerID, WorkerEpoch: epoch}
	case CommandDeactivateWorker:
		workerID, err := decoder.u16()
		if err != nil {
			return nil, err
		}
		epoch, err := decoder.workerEpoch()
		if err != nil {
			return nil, err
		}
		affected, err := decoder.affected()
		if err != nil {
			return nil, err
		}
		command = DeactivateWorker{Envelope: envelope, WorkerID: workerID, WorkerEpoch: epoch, Affected: affected}
	case CommandReplaceWorkerEpoch:
		workerID, err := decoder.u16()
		if err != nil {
			return nil, err
		}
		oldEpoch, err := decoder.workerEpoch()
		if err != nil {
			return nil, err
		}
		target, err := decoder.workerRecord()
		if err != nil {
			return nil, err
		}
		affected, err := decoder.affected()
		if err != nil {
			return nil, err
		}
		command = ReplaceWorkerEpoch{Envelope: envelope, WorkerID: workerID, OldEpoch: oldEpoch, Target: target, Affected: affected}
	case CommandSubmitJob:
		topology, err := decoder.topology()
		if err != nil {
			return nil, err
		}
		command = SubmitJob{Envelope: envelope, Topology: topology.Spec()}
	case CommandCancelJob:
		job, err := decoder.jobID()
		if err != nil {
			return nil, err
		}
		expected, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		command = CancelJob{Envelope: envelope, Job: job, ExpectedRevision: expected}
	case CommandRecordSourceEOF:
		source, err := decoder.taskID()
		if err != nil {
			return nil, err
		}
		eof, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		command = RecordSourceEOF{Envelope: envelope, Source: source, EOF: eof}
	case CommandInstallAssignments:
		assignment, err := decoder.assignment()
		if err != nil {
			return nil, err
		}
		command = InstallAssignments{Envelope: envelope, Assignment: assignment}
	case CommandReplaceAssignments:
		job, err := decoder.jobID()
		if err != nil {
			return nil, err
		}
		revision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.array32()
		if err != nil {
			return nil, err
		}
		markers, err := decoder.array32()
		if err != nil {
			return nil, err
		}
		target, err := decoder.assignment()
		if err != nil {
			return nil, err
		}
		command = ReplaceAssignments{Envelope: envelope, JobID: job, ExpectedAssignmentRevision: revision, ExpectedDigest: digest, ExpectedMarkersDigest: markers, Target: target}
	case CommandAdvanceCheckpoint:
		report, err := decoder.completionReport()
		if err != nil {
			return nil, err
		}
		command = AdvanceCheckpoint{Envelope: envelope, Report: report}
	case CommandSealManifest:
		manifest, err := decoder.manifest()
		if err != nil {
			return nil, err
		}
		command = SealManifest{Envelope: envelope, Manifest: manifest}
	case CommandTransitionJob:
		job, err := decoder.jobID()
		if err != nil {
			return nil, err
		}
		from, err := decoder.byte()
		if err != nil {
			return nil, err
		}
		to, err := decoder.byte()
		if err != nil {
			return nil, err
		}
		command = TransitionJob{Envelope: envelope, JobID: job, From: JobLifecycle(from), To: JobLifecycle(to)}
	case CommandFailJob:
		report, err := decoder.failureReport()
		if err != nil {
			return nil, err
		}
		command = FailJob{Envelope: envelope, Report: report}
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownCommandKind, kind)
	}
	if !decoder.done() {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformedCommand)
	}
	canonical, err := MarshalCommand(command)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("%w: noncanonical command", ErrMalformedCommand)
	}
	return command, nil
}

func marshalConcreteCommand(envelope Envelope, target []byte, validation error) ([]byte, error) {
	if validation != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, validation)
	}
	encoded := marshalEnvelope(envelope)
	if uint64(len(encoded))+uint64(len(target)) > model.LimitsV1().MaxSubmitJobBytes {
		return nil, fmt.Errorf("%w: command exceeds maximum bytes", ErrInvalidCommand)
	}
	encoded = append(encoded, target...)
	return append([]byte(nil), encoded...), nil
}

func marshalEnvelope(envelope Envelope) []byte {
	return marshalEnvelopeTraced(envelope, nil)
}

func marshalEnvelopeTraced(envelope Envelope, trace *[]string) []byte {
	encoded := make([]byte, 0, commandFixedEnvelopeBytes+internalEnvelopeBytes)
	appendCodecField(&encoded, trace, "SchemaVersion:u16", func(out []byte) []byte { return appendU16(out, envelope.SchemaVersion) })
	appendCodecField(&encoded, trace, "ConsensusFingerprint:sha256", func(out []byte) []byte { return append(out, envelope.ConsensusFingerprint[:]...) })
	appendCodecField(&encoded, trace, "Kind:u16", func(out []byte) []byte { return appendU16(out, uint16(envelope.Kind)) })
	appendCodecField(&encoded, trace, "CoordinatorEpoch:CoordinatorEpoch(zero-only-for-begin)", func(out []byte) []byte { return appendEpoch(out, envelope.CoordinatorEpoch) })
	appendCodecField(&encoded, trace, "IdentitySelector:u8", func(out []byte) []byte {
		if envelope.Client != nil {
			return append(out, identityClient)
		}
		return append(out, identityInternal)
	})
	if trace != nil {
		*trace = append(*trace, "Identity:ClientEnvelope|InternalEnvelope", "Target:concrete-command-fields")
	}
	if envelope.Client != nil {
		return appendClientEnvelopeTraced(encoded, *envelope.Client, nil)
	}
	return appendInternalEnvelopeTraced(encoded, *envelope.Internal, nil)
}

func appendClientEnvelopeTraced(encoded []byte, envelope ClientEnvelope, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "ClientID:bytes16(nonzero)", func(out []byte) []byte { return append(out, envelope.Request.ClientID[:]...) })
	appendCodecField(&encoded, trace, "Sequence:u64(nonzero)", func(out []byte) []byte { return appendU64(out, envelope.Request.Sequence) })
	appendCodecField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, envelope.Digest[:]...) })
	return encoded
}

func appendInternalEnvelopeTraced(encoded []byte, envelope InternalEnvelope, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "ID:bytes32(nonzero)", func(out []byte) []byte { return append(out, envelope.ID[:]...) })
	appendCodecField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, envelope.Digest[:]...) })
	appendCodecField(&encoded, trace, "Subject:SubjectKey", func(out []byte) []byte { return appendSubject(out, envelope.Subject) })
	appendCodecField(&encoded, trace, "ExpectedRevision:u64", func(out []byte) []byte { return appendU64(out, envelope.ExpectedRevision) })
	return encoded
}

func appendCodecField(encoded *[]byte, trace *[]string, descriptor string, appendValue func([]byte) []byte) {
	if trace != nil {
		*trace = append(*trace, descriptor)
	}
	*encoded = appendValue(*encoded)
}

func recordCodecDescriptor(trace *[]string, descriptor string) {
	if trace != nil {
		*trace = append(*trace, descriptor)
	}
}

func appendWorkerRecord(encoded []byte, worker WorkerRecord) []byte {
	return appendWorkerRecordTraced(encoded, worker, nil)
}

func appendWorkerRecordTraced(encoded []byte, worker WorkerRecord, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "NodeID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, worker.NodeID) })
	appendCodecField(&encoded, trace, "Epoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, worker.Epoch[:]...) })
	appendCodecField(&encoded, trace, "State:u8", func(out []byte) []byte { return append(out, byte(worker.State)) })
	appendCodecField(&encoded, trace, "Revision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, worker.Revision) })
	appendCodecField(&encoded, trace, "Slots:u16", func(out []byte) []byte { return appendU16(out, worker.Slots) })
	appendCodecField(&encoded, trace, "ConsensusFingerprint:sha256", func(out []byte) []byte { return append(out, worker.ConsensusFingerprint[:]...) })
	appendCodecField(&encoded, trace, "RegistryFingerprint:sha256", func(out []byte) []byte { return append(out, worker.RegistryFingerprint[:]...) })
	return encoded
}

func appendAffected(encoded []byte, affected []AffectedAssignment) []byte {
	encoded = appendU16(encoded, uint16(len(affected)))
	for _, item := range affected {
		encoded = appendAffectedItemTraced(encoded, item, nil)
	}
	return encoded
}

func appendAffectedItemTraced(encoded []byte, item AffectedAssignment, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:bytes16(nonzero)", func(out []byte) []byte { return append(out, item.JobID[:]...) })
	appendCodecField(&encoded, trace, "JobControlRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, item.JobControlRevision) })
	appendCodecField(&encoded, trace, "AssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, item.AssignmentRevision) })
	appendCodecField(&encoded, trace, "AssignmentDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, item.AssignmentDigest[:]...) })
	return encoded
}

func registerWorkerTarget(command RegisterWorker) []byte {
	return registerWorkerTargetTraced(command, nil)
}
func registerWorkerTargetTraced(command RegisterWorker, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-worker)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Worker:WorkerRecord", func(out []byte) []byte { return appendWorkerRecord(out, command.Worker) })
	return encoded
}
func drainWorkerTarget(command DrainWorker) []byte {
	return drainWorkerTargetTraced(command, nil)
}
func drainWorkerTargetTraced(command DrainWorker, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-worker)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, command.WorkerID) })
	appendCodecField(&encoded, trace, "WorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, command.WorkerEpoch[:]...) })
	return encoded
}
func deactivateWorkerTarget(command DeactivateWorker) []byte {
	return deactivateWorkerTargetTraced(command, nil)
}
func deactivateWorkerTargetTraced(command DeactivateWorker, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-worker)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, command.WorkerID) })
	appendCodecField(&encoded, trace, "WorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, command.WorkerEpoch[:]...) })
	appendCodecField(&encoded, trace, "Affected:list(AffectedAssignment)", func(out []byte) []byte { return appendAffected(out, command.Affected) })
	return encoded
}
func replaceWorkerEpochTarget(command ReplaceWorkerEpoch) []byte {
	return replaceWorkerEpochTargetTraced(command, nil)
}
func replaceWorkerEpochTargetTraced(command ReplaceWorkerEpoch, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-worker)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, command.WorkerID) })
	appendCodecField(&encoded, trace, "OldEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, command.OldEpoch[:]...) })
	appendCodecField(&encoded, trace, "Target:WorkerRecord", func(out []byte) []byte { return appendWorkerRecord(out, command.Target) })
	appendCodecField(&encoded, trace, "Affected:list(AffectedAssignment)", func(out []byte) []byte { return appendAffected(out, command.Affected) })
	return encoded
}
func cancelJobTarget(command CancelJob) []byte {
	return cancelJobTargetTraced(command, nil)
}
func cancelJobTargetTraced(command CancelJob, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(client)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "JobID:bytes16(nonzero)", func(out []byte) []byte { return append(out, command.Job[:]...) })
	appendCodecField(&encoded, trace, "ExpectedRevision:u64(nonzero-successor)", func(out []byte) []byte { return appendU64(out, command.ExpectedRevision) })
	return encoded
}

func submitJobTargetTraced(command SubmitJob, trace *[]string) ([]byte, error) {
	recordCodecDescriptor(trace, "Envelope:Envelope(client)")
	recordCodecDescriptor(trace, "Topology:canonical-topology-v1")
	validated, err := model.ValidateTopology(command.Topology)
	if err != nil {
		return nil, err
	}
	return validated.CanonicalBytes(), nil
}

func appendTask(encoded []byte, task model.TaskID) []byte {
	return appendTaskTraced(encoded, task, nil)
}

func appendTaskTraced(encoded []byte, task model.TaskID, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:bytes16(nonzero)", func(out []byte) []byte { return append(out, task.JobID[:]...) })
	appendCodecField(&encoded, trace, "StageID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, task.StageID) })
	appendCodecField(&encoded, trace, "Partition:u16", func(out []byte) []byte { return appendU16(out, task.Partition) })
	return encoded
}

func appendEpoch(encoded []byte, epoch model.CoordinatorEpoch) []byte {
	return appendEpochTraced(encoded, epoch, nil)
}

func appendEpochTraced(encoded []byte, epoch model.CoordinatorEpoch, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "Term:u64(nonzero)", func(out []byte) []byte { return appendU64(out, epoch.Term) })
	appendCodecField(&encoded, trace, "BeginIndex:u64(nonzero)", func(out []byte) []byte { return appendU64(out, epoch.BeginIndex) })
	appendCodecField(&encoded, trace, "Coordinator:u16(nonzero)", func(out []byte) []byte { return appendU16(out, epoch.Coordinator) })
	appendCodecField(&encoded, trace, "Nonce:bytes16(nonzero)", func(out []byte) []byte { return append(out, epoch.Nonce[:]...) })
	return encoded
}

func appendToken(encoded []byte, token model.AssignmentToken) []byte {
	return appendTokenTraced(encoded, token, nil)
}

func appendTokenTraced(encoded []byte, token model.AssignmentToken, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "Task:TaskID", func(out []byte) []byte { return appendTask(out, token.Task) })
	appendCodecField(&encoded, trace, "WorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, token.WorkerID) })
	appendCodecField(&encoded, trace, "WorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, token.WorkerEpoch[:]...) })
	appendCodecField(&encoded, trace, "Attempt:u64(nonzero)", func(out []byte) []byte { return appendU64(out, token.Attempt) })
	appendCodecField(&encoded, trace, "SpecificationHash:sha256(nonzero)", func(out []byte) []byte { return append(out, token.SpecificationHash[:]...) })
	appendCodecField(&encoded, trace, "AssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, token.AssignmentRevision) })
	return encoded
}

func appendReplica(encoded []byte, replica model.ResultReplicaSet) []byte {
	return appendReplicaTraced(encoded, replica, nil)
}

func appendReplicaTraced(encoded []byte, replica model.ResultReplicaSet, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "SinkTask:TaskID", func(out []byte) []byte { return appendTask(out, replica.SinkTask) })
	appendCodecField(&encoded, trace, "PrimaryNodeID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, replica.PrimaryNodeID) })
	appendCodecField(&encoded, trace, "SecondaryNodeID:u16(nonzero-distinct)", func(out []byte) []byte { return appendU16(out, replica.SecondaryNodeID) })
	appendCodecField(&encoded, trace, "PrimaryEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, replica.PrimaryEpoch[:]...) })
	appendCodecField(&encoded, trace, "SecondaryEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, replica.SecondaryEpoch[:]...) })
	return encoded
}

func appendAssignment(encoded []byte, assignment model.AssignmentSet) []byte {
	return appendAssignmentTraced(encoded, assignment, nil)
}

func appendAssignmentTraced(encoded []byte, assignment model.AssignmentSet, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:bytes16(nonzero)", func(out []byte) []byte { return append(out, assignment.JobID[:]...) })
	appendCodecField(&encoded, trace, "Revision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, assignment.Revision) })
	appendCodecField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, assignment.Digest[:]...) })
	appendCodecField(&encoded, trace, "Tasks:u16-count+list(AssignmentToken)", func(out []byte) []byte {
		out = appendU16(out, uint16(len(assignment.Tasks)))
		for _, token := range assignment.Tasks {
			out = appendToken(out, token)
		}
		return out
	})
	appendCodecField(&encoded, trace, "ResultReplicas:u16-count+list(ResultReplicaSet)", func(out []byte) []byte {
		out = appendU16(out, uint16(len(assignment.ResultReplicas)))
		for _, replica := range assignment.ResultReplicas {
			out = appendReplica(out, replica)
		}
		return out
	})
	return encoded
}

func appendMarker(encoded []byte, marker NeedsReassignment) []byte {
	return appendMarkerTraced(encoded, marker, nil)
}

func appendMarkerTraced(encoded []byte, marker NeedsReassignment, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "Kind:ReassignmentTargetKind", func(out []byte) []byte { return append(out, byte(marker.Kind)) })
	appendCodecField(&encoded, trace, "Task:TaskID", func(out []byte) []byte { return appendTask(out, marker.Task) })
	appendCodecField(&encoded, trace, "SinkTask:TaskID", func(out []byte) []byte { return appendTask(out, marker.SinkTask) })
	appendCodecField(&encoded, trace, "ReplicaRole:ResultReplicaRole", func(out []byte) []byte { return append(out, byte(marker.ReplicaRole)) })
	appendCodecField(&encoded, trace, "OldWorkerID:u16(nonzero)", func(out []byte) []byte { return appendU16(out, marker.OldWorkerID) })
	appendCodecField(&encoded, trace, "OldWorkerEpoch:bytes16(nonzero)", func(out []byte) []byte { return append(out, marker.OldWorkerEpoch[:]...) })
	return encoded
}

func completionReportBytes(report model.CompletionReport) []byte {
	return appendCompletionReportTraced(nil, report, nil)
}

func appendCompletionReportTraced(encoded []byte, report model.CompletionReport, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, report.JobID[:]...) })
	appendCodecField(&encoded, trace, "JobControlRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.JobControlRevision) })
	appendCodecField(&encoded, trace, "AssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.AssignmentRevision) })
	appendCodecField(&encoded, trace, "Source:TaskID", func(out []byte) []byte { return appendTask(out, report.Source) })
	appendCodecField(&encoded, trace, "Token:AssignmentToken", func(out []byte) []byte { return appendToken(out, report.Token) })
	appendCodecField(&encoded, trace, "Epoch:CoordinatorEpoch", func(out []byte) []byte { return appendEpoch(out, report.Epoch) })
	appendCodecField(&encoded, trace, "ExpectedCheckpointRevision:u64", func(out []byte) []byte { return appendU64(out, report.ExpectedCheckpointRevision) })
	appendCodecField(&encoded, trace, "Prior:u64", func(out []byte) []byte { return appendU64(out, report.Prior) })
	appendCodecField(&encoded, trace, "New:u64", func(out []byte) []byte { return appendU64(out, report.New) })
	appendCodecField(&encoded, trace, "EOF:u64", func(out []byte) []byte { return appendU64(out, report.EOF) })
	appendCodecField(&encoded, trace, "WorkerTransactionID:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.WorkerTransactionID) })
	appendCodecField(&encoded, trace, "Digest:sha256(nonzero)", func(out []byte) []byte { return append(out, report.Digest[:]...) })
	return encoded
}

func failureReportBytes(report model.JobFailureReport) []byte {
	return appendFailureReportTraced(nil, report, nil)
}

func appendFailureReportTraced(encoded []byte, report model.JobFailureReport, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, report.JobID[:]...) })
	appendCodecField(&encoded, trace, "JobControlRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.JobControlRevision) })
	appendCodecField(&encoded, trace, "AssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.AssignmentRevision) })
	appendCodecField(&encoded, trace, "Task:AssignmentToken", func(out []byte) []byte { return appendToken(out, report.Task) })
	appendCodecField(&encoded, trace, "Epoch:CoordinatorEpoch", func(out []byte) []byte { return appendEpoch(out, report.Epoch) })
	appendCodecField(&encoded, trace, "TransactionID:u64(nonzero)", func(out []byte) []byte { return appendU64(out, report.TransactionID) })
	appendCodecField(&encoded, trace, "Code:FailureCode", func(out []byte) []byte { return appendU16(out, uint16(report.Code)) })
	appendCodecField(&encoded, trace, "DetailDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, report.DetailDigest[:]...) })
	return encoded
}

func appendManifest(encoded []byte, manifest ResultManifest) []byte {
	return appendManifestTraced(encoded, manifest, nil)
}

func appendManifestTraced(encoded []byte, manifest ResultManifest, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, manifest.JobID[:]...) })
	appendCodecField(&encoded, trace, "SinkTask:TaskID", func(out []byte) []byte { return appendTask(out, manifest.SinkTask) })
	appendCodecField(&encoded, trace, "ManifestRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, manifest.ManifestRevision) })
	appendCodecField(&encoded, trace, "SpecificationHash:sha256(nonzero)", func(out []byte) []byte { return append(out, manifest.SpecificationHash[:]...) })
	appendCodecField(&encoded, trace, "RecordCount:u64", func(out []byte) []byte { return appendU64(out, manifest.RecordCount) })
	appendCodecField(&encoded, trace, "TotalBytes:u64(bounded)", func(out []byte) []byte { return appendU64(out, manifest.TotalBytes) })
	appendCodecField(&encoded, trace, "Checksum:sha256(nonzero)", func(out []byte) []byte { return append(out, manifest.Checksum[:]...) })
	appendCodecField(&encoded, trace, "Replicas:ResultReplicaSet", func(out []byte) []byte { return appendReplica(out, manifest.Replicas) })
	return encoded
}

func recordSourceEOFTarget(command RecordSourceEOF) []byte {
	return recordSourceEOFTargetTraced(command, nil)
}
func recordSourceEOFTargetTraced(command RecordSourceEOF, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-source-eof)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Source:TaskID", func(out []byte) []byte { return appendTask(out, command.Source) })
	appendCodecField(&encoded, trace, "EOF:u64", func(out []byte) []byte { return appendU64(out, command.EOF) })
	return encoded
}
func installAssignmentsTarget(command InstallAssignments) []byte {
	return installAssignmentsTargetTraced(command, nil)
}
func installAssignmentsTargetTraced(command InstallAssignments, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-job-control)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Assignment:AssignmentSet", func(out []byte) []byte { return appendAssignment(out, command.Assignment) })
	return encoded
}
func replaceAssignmentsTarget(command ReplaceAssignments) []byte {
	return replaceAssignmentsTargetTraced(command, nil)
}
func replaceAssignmentsTargetTraced(command ReplaceAssignments, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-job-control)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, command.JobID[:]...) })
	appendCodecField(&encoded, trace, "ExpectedAssignmentRevision:u64(nonzero)", func(out []byte) []byte { return appendU64(out, command.ExpectedAssignmentRevision) })
	appendCodecField(&encoded, trace, "ExpectedDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, command.ExpectedDigest[:]...) })
	appendCodecField(&encoded, trace, "ExpectedMarkersDigest:sha256(nonzero)", func(out []byte) []byte { return append(out, command.ExpectedMarkersDigest[:]...) })
	appendCodecField(&encoded, trace, "Target:AssignmentSet(successor)", func(out []byte) []byte { return appendAssignment(out, command.Target) })
	return encoded
}
func advanceCheckpointTarget(command AdvanceCheckpoint) []byte {
	return advanceCheckpointTargetTraced(command, nil)
}
func advanceCheckpointTargetTraced(command AdvanceCheckpoint, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-source-checkpoint)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Report:CompletionReport", func(out []byte) []byte { return appendCompletionReportTraced(out, command.Report, nil) })
	return encoded
}
func sealManifestTarget(command SealManifest) []byte { return sealManifestTargetTraced(command, nil) }
func sealManifestTargetTraced(command SealManifest, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-result-manifest)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Manifest:ResultManifest", func(out []byte) []byte { return appendManifest(out, command.Manifest) })
	return encoded
}
func transitionJobTarget(command TransitionJob) []byte {
	return transitionJobTargetTraced(command, nil)
}
func transitionJobTargetTraced(command TransitionJob, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-job-control)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, command.JobID[:]...) })
	appendCodecField(&encoded, trace, "From:JobLifecycle", func(out []byte) []byte { return append(out, byte(command.From)) })
	appendCodecField(&encoded, trace, "To:JobLifecycle", func(out []byte) []byte { return append(out, byte(command.To)) })
	return encoded
}
func failJobTarget(command FailJob) []byte { return failJobTargetTraced(command, nil) }
func failJobTargetTraced(command FailJob, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal-job-control)")
	encoded := []byte(nil)
	appendCodecField(&encoded, trace, "Report:JobFailureReport", func(out []byte) []byte { return appendFailureReportTraced(out, command.Report, nil) })
	return encoded
}

// MarshalBeginCoordinatorEpoch emits the sole canonical concrete Task 9 command.
func MarshalBeginCoordinatorEpoch(command BeginCoordinatorEpoch) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	encoded := make([]byte, 0, int(commandFixedEnvelopeBytes+internalEnvelopeBytes+beginTargetBytes))
	encoded = appendU16(encoded, command.Envelope.SchemaVersion)
	encoded = append(encoded, command.Envelope.ConsensusFingerprint[:]...)
	encoded = appendU16(encoded, uint16(command.Envelope.Kind))
	encoded = appendEpoch(encoded, command.Envelope.CoordinatorEpoch)
	encoded = append(encoded, identityInternal)
	internal := command.Envelope.Internal
	encoded = append(encoded, internal.ID[:]...)
	encoded = append(encoded, internal.Digest[:]...)
	encoded = appendSubject(encoded, internal.Subject)
	encoded = appendU64(encoded, internal.ExpectedRevision)
	encoded = append(encoded, beginTarget(command)...)
	return append([]byte(nil), encoded...), nil
}

// UnmarshalBeginCoordinatorEpoch decodes only complete canonical Begin commands.
func UnmarshalBeginCoordinatorEpoch(encoded []byte) (BeginCoordinatorEpoch, error) {
	if uint64(len(encoded)) != commandFixedEnvelopeBytes+internalEnvelopeBytes+beginTargetBytes {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: command length %d", ErrMalformedCommand, len(encoded))
	}
	decoder := commandDecoder{input: encoded}
	version, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	fingerprint, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	kind, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	epoch, err := decoder.epoch()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	selector, err := decoder.byte()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	if selector != identityInternal {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: begin identity selector %d", ErrMalformedCommand, selector)
	}
	idBytes, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	digest, err := decoder.array32()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	subject, err := decoder.subject()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	expectedRevision, err := decoder.u64()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	coordinator, err := decoder.u16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	nonce, err := decoder.array16()
	if err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	if !decoder.done() {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: trailing bytes", ErrMalformedCommand)
	}
	command := BeginCoordinatorEpoch{
		Envelope:    Envelope{SchemaVersion: version, ConsensusFingerprint: fingerprint, Kind: CommandKind(kind), CoordinatorEpoch: epoch, Internal: &InternalEnvelope{ID: InternalCommandID(idBytes), Digest: digest, Subject: subject, ExpectedRevision: expectedRevision}},
		Coordinator: coordinator, Nonce: nonce,
	}
	if err := command.Validate(); err != nil {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	canonical, err := MarshalBeginCoordinatorEpoch(command)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return BeginCoordinatorEpoch{}, fmt.Errorf("%w: noncanonical command", ErrMalformedCommand)
	}
	return command, nil
}

// MarshalCommandResult emits the fixed, bounded canonical result schema.
func MarshalCommandResult(result CommandResult) ([]byte, error) {
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedCommandResult, err)
	}
	encoded := make([]byte, 0, int(commandResultBytes))
	encoded = appendU16(encoded, CommandSchemaVersion)
	encoded = appendU16(encoded, uint16(result.Code))
	encoded = append(encoded, byte(result.Subject))
	encoded = appendU64(encoded, result.Revision)
	encoded = append(encoded, result.JobID[:]...)
	encoded = appendU16(encoded, result.WorkerID)
	encoded = appendU64(encoded, result.Epoch.Term)
	encoded = appendU64(encoded, result.Epoch.BeginIndex)
	encoded = appendU16(encoded, result.Epoch.Coordinator)
	encoded = append(encoded, result.Epoch.Nonce[:]...)
	if uint64(len(encoded)) != commandResultBytes {
		return nil, errors.New("impossible command result size")
	}
	return append([]byte(nil), encoded...), nil
}

// UnmarshalCommandResult accepts only the complete canonical fixed result.
func UnmarshalCommandResult(encoded []byte) (CommandResult, error) {
	if uint64(len(encoded)) != commandResultBytes {
		return CommandResult{}, fmt.Errorf("%w: result length %d", ErrMalformedCommandResult, len(encoded))
	}
	decoder := commandDecoder{input: encoded}
	version, _ := decoder.u16()
	if version != CommandSchemaVersion {
		return CommandResult{}, fmt.Errorf("%w: schema %d", ErrMalformedCommandResult, version)
	}
	code, _ := decoder.u16()
	subject, _ := decoder.byte()
	revision, _ := decoder.u64()
	jobBytes, _ := decoder.array16()
	worker, _ := decoder.u16()
	term, _ := decoder.u64()
	index, _ := decoder.u64()
	coordinator, _ := decoder.u16()
	nonce, _ := decoder.array16()
	result := CommandResult{Code: ResultCode(code), Subject: SubjectKind(subject), Revision: revision, JobID: model.JobID(jobBytes), WorkerID: worker, Epoch: model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: coordinator, Nonce: nonce}}
	if err := result.Validate(); err != nil {
		return CommandResult{}, fmt.Errorf("%w: %w", ErrMalformedCommandResult, err)
	}
	return result, nil
}

func internalDigest(envelope Envelope, target []byte) [32]byte {
	encoded := make([]byte, 0, len(internalCommandDigestDomain)+int(commandFixedEnvelopeBytes+internalEnvelopeBytes-32))
	encoded = append(encoded, internalCommandDigestDomain...)
	encoded = appendU16(encoded, envelope.SchemaVersion)
	encoded = append(encoded, envelope.ConsensusFingerprint[:]...)
	encoded = appendU16(encoded, uint16(envelope.Kind))
	encoded = appendEpoch(encoded, envelope.CoordinatorEpoch)
	encoded = append(encoded, identityInternal)
	if envelope.Internal == nil {
		return sha256.Sum256(encoded)
	}
	encoded = append(encoded, envelope.Internal.ID[:]...)
	encoded = appendSubject(encoded, envelope.Internal.Subject)
	encoded = appendU64(encoded, envelope.Internal.ExpectedRevision)
	hash := sha256.New()
	_, _ = hash.Write(encoded)
	_, _ = hash.Write(target)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func beginTarget(command BeginCoordinatorEpoch) []byte {
	return beginTargetTraced(command, nil)
}

func beginTargetTraced(command BeginCoordinatorEpoch, trace *[]string) []byte {
	recordCodecDescriptor(trace, "Envelope:Envelope(internal)")
	target := make([]byte, 0, beginTargetBytes)
	appendCodecField(&target, trace, "Coordinator:u16(nonzero)", func(out []byte) []byte { return appendU16(out, command.Coordinator) })
	appendCodecField(&target, trace, "Nonce:bytes16(nonzero)", func(out []byte) []byte { return append(out, command.Nonce[:]...) })
	return target
}

func beginAppliedTarget(term uint64, command BeginCoordinatorEpoch) []byte {
	target := make([]byte, 0, 8+beginTargetBytes)
	target = appendU64(target, term)
	return append(target, beginTarget(command)...)
}

func appendSubject(encoded []byte, subject SubjectKey) []byte {
	return appendSubjectTraced(encoded, subject, nil)
}

func appendSubjectTraced(encoded []byte, subject SubjectKey, trace *[]string) []byte {
	appendCodecField(&encoded, trace, "Kind:u8", func(out []byte) []byte { return append(out, byte(subject.Kind)) })
	appendCodecField(&encoded, trace, "JobID:JobID", func(out []byte) []byte { return append(out, subject.JobID[:]...) })
	appendCodecField(&encoded, trace, "TaskID:TaskID", func(out []byte) []byte { return appendTask(out, subject.TaskID) })
	appendCodecField(&encoded, trace, "WorkerID:u16", func(out []byte) []byte { return appendU16(out, subject.WorkerID) })
	return encoded
}

func appendU16(encoded []byte, value uint16) []byte {
	var fixed [2]byte
	binary.BigEndian.PutUint16(fixed[:], value)
	return append(encoded, fixed[:]...)
}

func appendU64(encoded []byte, value uint64) []byte {
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], value)
	return append(encoded, fixed[:]...)
}

type commandDecoder struct {
	input  []byte
	offset int
}

func (decoder *commandDecoder) done() bool     { return decoder.offset == len(decoder.input) }
func (decoder *commandDecoder) remaining() int { return len(decoder.input) - decoder.offset }

func (decoder *commandDecoder) take(length int) ([]byte, error) {
	if length < 0 || length > len(decoder.input)-decoder.offset {
		return nil, fmt.Errorf("%w: truncated field", ErrMalformedCommand)
	}
	end := decoder.offset + length
	value := decoder.input[decoder.offset:end]
	decoder.offset = end
	return value, nil
}

func (decoder *commandDecoder) byte() (byte, error) {
	value, err := decoder.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (decoder *commandDecoder) u16() (uint16, error) {
	value, err := decoder.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func (decoder *commandDecoder) u64() (uint64, error) {
	value, err := decoder.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}

func (decoder *commandDecoder) array16() ([16]byte, error) {
	value, err := decoder.take(16)
	var result [16]byte
	copy(result[:], value)
	return result, err
}

func (decoder *commandDecoder) array32() ([32]byte, error) {
	value, err := decoder.take(32)
	var result [32]byte
	copy(result[:], value)
	return result, err
}

func (decoder *commandDecoder) subject() (SubjectKey, error) {
	kind, err := decoder.byte()
	if err != nil {
		return SubjectKey{}, err
	}
	job, err := decoder.array16()
	if err != nil {
		return SubjectKey{}, err
	}
	taskJob, err := decoder.array16()
	if err != nil {
		return SubjectKey{}, err
	}
	stage, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	partition, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	worker, err := decoder.u16()
	if err != nil {
		return SubjectKey{}, err
	}
	return SubjectKey{Kind: SubjectKind(kind), JobID: model.JobID(job), TaskID: model.TaskID{JobID: model.JobID(taskJob), StageID: stage, Partition: partition}, WorkerID: worker}, nil
}

func (decoder *commandDecoder) envelope() (Envelope, error) {
	version, err := decoder.u16()
	if err != nil {
		return Envelope{}, err
	}
	fingerprint, err := decoder.array32()
	if err != nil {
		return Envelope{}, err
	}
	kind, err := decoder.u16()
	if err != nil {
		return Envelope{}, err
	}
	epoch, err := decoder.epoch()
	if err != nil {
		return Envelope{}, err
	}
	selector, err := decoder.byte()
	if err != nil {
		return Envelope{}, err
	}
	envelope := Envelope{SchemaVersion: version, ConsensusFingerprint: fingerprint, Kind: CommandKind(kind), CoordinatorEpoch: epoch}
	switch selector {
	case identityClient:
		client, err := decoder.array16()
		if err != nil {
			return Envelope{}, err
		}
		sequence, err := decoder.u64()
		if err != nil {
			return Envelope{}, err
		}
		digest, err := decoder.array32()
		if err != nil {
			return Envelope{}, err
		}
		envelope.Client = &ClientEnvelope{Request: model.ClientRequestID{ClientID: model.ClientID(client), Sequence: sequence}, Digest: digest}
	case identityInternal:
		id, err := decoder.array32()
		if err != nil {
			return Envelope{}, err
		}
		digest, err := decoder.array32()
		if err != nil {
			return Envelope{}, err
		}
		subject, err := decoder.subject()
		if err != nil {
			return Envelope{}, err
		}
		expected, err := decoder.u64()
		if err != nil {
			return Envelope{}, err
		}
		envelope.Internal = &InternalEnvelope{ID: InternalCommandID(id), Digest: digest, Subject: subject, ExpectedRevision: expected}
	default:
		return Envelope{}, fmt.Errorf("%w: identity selector %d", ErrMalformedCommand, selector)
	}
	return envelope, nil
}

func (decoder *commandDecoder) workerEpoch() (model.WorkerEpoch, error) {
	value, err := decoder.array16()
	return model.WorkerEpoch(value), err
}

func (decoder *commandDecoder) jobID() (model.JobID, error) {
	value, err := decoder.array16()
	return model.JobID(value), err
}

func (decoder *commandDecoder) workerRecord() (WorkerRecord, error) {
	nodeID, err := decoder.u16()
	if err != nil {
		return WorkerRecord{}, err
	}
	epoch, err := decoder.workerEpoch()
	if err != nil {
		return WorkerRecord{}, err
	}
	stateValue, err := decoder.byte()
	if err != nil {
		return WorkerRecord{}, err
	}
	revision, err := decoder.u64()
	if err != nil {
		return WorkerRecord{}, err
	}
	slots, err := decoder.u16()
	if err != nil {
		return WorkerRecord{}, err
	}
	consensus, err := decoder.array32()
	if err != nil {
		return WorkerRecord{}, err
	}
	registry, err := decoder.array32()
	if err != nil {
		return WorkerRecord{}, err
	}
	return WorkerRecord{NodeID: nodeID, Epoch: epoch, State: WorkerState(stateValue), Revision: revision, Slots: slots, ConsensusFingerprint: consensus, RegistryFingerprint: registry}, nil
}

func (decoder *commandDecoder) affected() ([]AffectedAssignment, error) {
	count, err := decoder.u16()
	if err != nil {
		return nil, err
	}
	const encodedAffectedBytes = 16 + 8 + 8 + 32
	if uint64(count) > model.LimitsV1().MaxActiveJobs || int(count) > decoder.remaining()/encodedAffectedBytes {
		return nil, fmt.Errorf("%w: affected assignment count", ErrMalformedCommand)
	}
	if count == 0 {
		return nil, nil
	}
	result := make([]AffectedAssignment, int(count))
	for index := range result {
		job, err := decoder.jobID()
		if err != nil {
			return nil, err
		}
		jobRevision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		assignmentRevision, err := decoder.u64()
		if err != nil {
			return nil, err
		}
		digest, err := decoder.array32()
		if err != nil {
			return nil, err
		}
		result[index] = AffectedAssignment{JobID: job, JobControlRevision: jobRevision, AssignmentRevision: assignmentRevision, AssignmentDigest: digest}
	}
	return result, nil
}

func (decoder *commandDecoder) topology() (model.ValidatedTopology, error) {
	if decoder.remaining() < 8 {
		return model.ValidatedTopology{}, fmt.Errorf("%w: truncated topology length", ErrMalformedCommand)
	}
	declared := binary.BigEndian.Uint64(decoder.input[decoder.offset : decoder.offset+8])
	if declared > model.LimitsV1().MaxTopologyBytes-8 {
		return model.ValidatedTopology{}, fmt.Errorf("%w: declared topology exceeds limit", ErrMalformedCommand)
	}
	total := declared + 8
	if total > uint64(decoder.remaining()) {
		return model.ValidatedTopology{}, fmt.Errorf("%w: truncated topology", ErrMalformedCommand)
	}
	encoded, err := decoder.take(int(total))
	if err != nil {
		return model.ValidatedTopology{}, err
	}
	validated, err := model.DecodeTopology(encoded)
	if err != nil {
		return model.ValidatedTopology{}, fmt.Errorf("%w: topology: %v", ErrInvalidCommand, err)
	}
	return validated, nil
}

func (decoder *commandDecoder) taskID() (model.TaskID, error) {
	job, err := decoder.jobID()
	if err != nil {
		return model.TaskID{}, err
	}
	stage, err := decoder.u16()
	if err != nil {
		return model.TaskID{}, err
	}
	partition, err := decoder.u16()
	return model.TaskID{JobID: job, StageID: stage, Partition: partition}, err
}

func (decoder *commandDecoder) epoch() (model.CoordinatorEpoch, error) {
	term, err := decoder.u64()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	index, err := decoder.u64()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	coordinator, err := decoder.u16()
	if err != nil {
		return model.CoordinatorEpoch{}, err
	}
	nonce, err := decoder.array16()
	return model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: coordinator, Nonce: nonce}, err
}

func (decoder *commandDecoder) token() (model.AssignmentToken, error) {
	task, err := decoder.taskID()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	workerID, err := decoder.u16()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	epoch, err := decoder.workerEpoch()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	attempt, err := decoder.u64()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	specification, err := decoder.array32()
	if err != nil {
		return model.AssignmentToken{}, err
	}
	revision, err := decoder.u64()
	return model.AssignmentToken{Task: task, WorkerID: workerID, WorkerEpoch: epoch, Attempt: attempt, SpecificationHash: specification, AssignmentRevision: revision}, err
}

func (decoder *commandDecoder) replica() (model.ResultReplicaSet, error) {
	sink, err := decoder.taskID()
	if err != nil {
		return model.ResultReplicaSet{}, err
	}
	primary, err := decoder.u16()
	if err != nil {
		return model.ResultReplicaSet{}, err
	}
	secondary, err := decoder.u16()
	if err != nil {
		return model.ResultReplicaSet{}, err
	}
	primaryEpoch, err := decoder.workerEpoch()
	if err != nil {
		return model.ResultReplicaSet{}, err
	}
	secondaryEpoch, err := decoder.workerEpoch()
	return model.ResultReplicaSet{SinkTask: sink, PrimaryNodeID: primary, SecondaryNodeID: secondary, PrimaryEpoch: primaryEpoch, SecondaryEpoch: secondaryEpoch}, err
}

func (decoder *commandDecoder) assignment() (model.AssignmentSet, error) {
	job, err := decoder.jobID()
	if err != nil {
		return model.AssignmentSet{}, err
	}
	revision, err := decoder.u64()
	if err != nil {
		return model.AssignmentSet{}, err
	}
	digest, err := decoder.array32()
	if err != nil {
		return model.AssignmentSet{}, err
	}
	count, err := decoder.u16()
	if err != nil {
		return model.AssignmentSet{}, err
	}
	const tokenBytes = 86
	if count == 0 || uint64(count) > model.LimitsV1().MaxTasksPerJob || int(count) > decoder.remaining()/tokenBytes {
		return model.AssignmentSet{}, fmt.Errorf("%w: assignment task count", ErrMalformedCommand)
	}
	tasks := make([]model.AssignmentToken, int(count))
	for index := range tasks {
		tasks[index], err = decoder.token()
		if err != nil {
			return model.AssignmentSet{}, err
		}
	}
	replicaCount, err := decoder.u16()
	if err != nil {
		return model.AssignmentSet{}, err
	}
	const replicaBytes = 56
	if uint64(replicaCount) > model.LimitsV1().MaxResultManifestsPerJob || int(replicaCount) > decoder.remaining()/replicaBytes {
		return model.AssignmentSet{}, fmt.Errorf("%w: assignment replica count", ErrMalformedCommand)
	}
	replicas := make([]model.ResultReplicaSet, int(replicaCount))
	for index := range replicas {
		replicas[index], err = decoder.replica()
		if err != nil {
			return model.AssignmentSet{}, err
		}
	}
	return model.AssignmentSet{JobID: job, Revision: revision, Digest: digest, Tasks: tasks, ResultReplicas: replicas}, nil
}

func (decoder *commandDecoder) completionReport() (model.CompletionReport, error) {
	job, err := decoder.jobID()
	if err != nil {
		return model.CompletionReport{}, err
	}
	jobRevision, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	assignmentRevision, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	source, err := decoder.taskID()
	if err != nil {
		return model.CompletionReport{}, err
	}
	token, err := decoder.token()
	if err != nil {
		return model.CompletionReport{}, err
	}
	epoch, err := decoder.epoch()
	if err != nil {
		return model.CompletionReport{}, err
	}
	expected, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	prior, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	newWatermark, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	eof, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	transaction, err := decoder.u64()
	if err != nil {
		return model.CompletionReport{}, err
	}
	digest, err := decoder.array32()
	return model.CompletionReport{JobID: job, JobControlRevision: jobRevision, AssignmentRevision: assignmentRevision, Source: source, Token: token, Epoch: epoch, ExpectedCheckpointRevision: expected, Prior: prior, New: newWatermark, EOF: eof, WorkerTransactionID: transaction, Digest: digest}, err
}

func (decoder *commandDecoder) failureReport() (model.JobFailureReport, error) {
	job, err := decoder.jobID()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	jobRevision, err := decoder.u64()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	assignmentRevision, err := decoder.u64()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	token, err := decoder.token()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	epoch, err := decoder.epoch()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	transaction, err := decoder.u64()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	code, err := decoder.u16()
	if err != nil {
		return model.JobFailureReport{}, err
	}
	detail, err := decoder.array32()
	return model.JobFailureReport{JobID: job, JobControlRevision: jobRevision, AssignmentRevision: assignmentRevision, Task: token, Epoch: epoch, TransactionID: transaction, Code: model.FailureCode(code), DetailDigest: detail}, err
}

func (decoder *commandDecoder) manifest() (ResultManifest, error) {
	job, err := decoder.jobID()
	if err != nil {
		return ResultManifest{}, err
	}
	sink, err := decoder.taskID()
	if err != nil {
		return ResultManifest{}, err
	}
	revision, err := decoder.u64()
	if err != nil {
		return ResultManifest{}, err
	}
	specification, err := decoder.array32()
	if err != nil {
		return ResultManifest{}, err
	}
	records, err := decoder.u64()
	if err != nil {
		return ResultManifest{}, err
	}
	total, err := decoder.u64()
	if err != nil {
		return ResultManifest{}, err
	}
	checksum, err := decoder.array32()
	if err != nil {
		return ResultManifest{}, err
	}
	replica, err := decoder.replica()
	return ResultManifest{JobID: job, SinkTask: sink, ManifestRevision: revision, SpecificationHash: specification, RecordCount: records, TotalBytes: total, Checksum: checksum, Replicas: replica}, err
}
