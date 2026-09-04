package state

import (
	"fmt"

	"crane/internal/crane/model"
)

func stateLayout(name string, fields ...string) model.StateCommandLayoutDescriptor {
	return model.StateCommandLayoutDescriptor{Name: name, Fields: append([]string(nil), fields...)}
}

func tracedStateLayout(name string, appendFields func(*[]string)) model.StateCommandLayoutDescriptor {
	var fields []string
	appendFields(&fields)
	return stateLayout(name, fields...)
}

// StateCommandEncodingLayouts describes the field order exercised by the
// concrete v1 state encoder and its shared nested append helpers.
func StateCommandEncodingLayouts() []model.StateCommandLayoutDescriptor {
	return []model.StateCommandLayoutDescriptor{
		tracedStateLayout("Envelope", func(trace *[]string) { marshalEnvelopeTraced(Envelope{Internal: &InternalEnvelope{}}, trace) }),
		tracedStateLayout("ClientEnvelope", func(trace *[]string) { appendClientEnvelopeTraced(nil, ClientEnvelope{}, trace) }),
		tracedStateLayout("InternalEnvelope", func(trace *[]string) { appendInternalEnvelopeTraced(nil, InternalEnvelope{}, trace) }),
		tracedStateLayout("SubjectKey", func(trace *[]string) { appendSubjectTraced(nil, SubjectKey{}, trace) }),
		tracedStateLayout("BeginCoordinatorEpoch", func(trace *[]string) { beginTargetTraced(BeginCoordinatorEpoch{}, trace) }),
		tracedStateLayout("RegisterWorker", func(trace *[]string) { registerWorkerTargetTraced(RegisterWorker{}, trace) }),
		tracedStateLayout("DrainWorker", func(trace *[]string) { drainWorkerTargetTraced(DrainWorker{}, trace) }),
		tracedStateLayout("DeactivateWorker", func(trace *[]string) { deactivateWorkerTargetTraced(DeactivateWorker{}, trace) }),
		tracedStateLayout("ReplaceWorkerEpoch", func(trace *[]string) { replaceWorkerEpochTargetTraced(ReplaceWorkerEpoch{}, trace) }),
		tracedStateLayout("SubmitJob", func(trace *[]string) { _, _ = submitJobTargetTraced(SubmitJob{}, trace) }),
		tracedStateLayout("CancelJob", func(trace *[]string) { cancelJobTargetTraced(CancelJob{}, trace) }),
		tracedStateLayout("RecordSourceEOF", func(trace *[]string) { recordSourceEOFTargetTraced(RecordSourceEOF{}, trace) }),
		tracedStateLayout("InstallAssignments", func(trace *[]string) { installAssignmentsTargetTraced(InstallAssignments{}, trace) }),
		tracedStateLayout("ReplaceAssignments", func(trace *[]string) { replaceAssignmentsTargetTraced(ReplaceAssignments{}, trace) }),
		tracedStateLayout("AdvanceCheckpoint", func(trace *[]string) { advanceCheckpointTargetTraced(AdvanceCheckpoint{}, trace) }),
		tracedStateLayout("SealManifest", func(trace *[]string) { sealManifestTargetTraced(SealManifest{}, trace) }),
		tracedStateLayout("TransitionJob", func(trace *[]string) { transitionJobTargetTraced(TransitionJob{}, trace) }),
		tracedStateLayout("FailJob", func(trace *[]string) { failJobTargetTraced(FailJob{}, trace) }),
		tracedStateLayout("WorkerRecord", func(trace *[]string) { appendWorkerRecordTraced(nil, WorkerRecord{}, trace) }),
		tracedStateLayout("AffectedAssignment", func(trace *[]string) { appendAffectedItemTraced(nil, AffectedAssignment{}, trace) }),
		tracedStateLayout("TaskID", func(trace *[]string) { appendTaskTraced(nil, model.TaskID{}, trace) }),
		tracedStateLayout("CoordinatorEpoch", func(trace *[]string) { appendEpochTraced(nil, model.CoordinatorEpoch{}, trace) }),
		tracedStateLayout("AssignmentToken", func(trace *[]string) { appendTokenTraced(nil, model.AssignmentToken{}, trace) }),
		tracedStateLayout("ResultReplicaSet", func(trace *[]string) { appendReplicaTraced(nil, model.ResultReplicaSet{}, trace) }),
		tracedStateLayout("AssignmentSet", func(trace *[]string) { appendAssignmentTraced(nil, model.AssignmentSet{}, trace) }),
		stateLayout("AssignmentSetDigest", model.AssignmentSetDigestLayoutV1()...),
		tracedStateLayout("NeedsReassignment", func(trace *[]string) { appendMarkerTraced(nil, NeedsReassignment{}, trace) }),
		stateLayout("InvalidationProvenance", "Kind:WorkerInvalidationKind(zero-iff-worker-anchor-forgotten)", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "WorkerRevision:u64(zero-iff-worker-anchor-forgotten)", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "AssignmentDigest:sha256(nonzero)", "Markers:u16-count+sorted(NeedsReassignment)", "RepairState:InvalidationRepairState", "RepairJobControlRevision:u64(nonzero-iff-anchored)", "RepairAssignmentRevision:u64(successor-iff-anchored)", "RepairAssignmentDigest:sha256(nonzero-iff-anchored)", "RepairMarkersDigest:sha256(nonzero-iff-anchored)"),
		tracedStateLayout("CompletionReport", func(trace *[]string) { appendCompletionReportTraced(nil, model.CompletionReport{}, trace) }),
		tracedStateLayout("JobFailureReport", func(trace *[]string) { appendFailureReportTraced(nil, model.JobFailureReport{}, trace) }),
		tracedStateLayout("ResultManifest", func(trace *[]string) { appendManifestTraced(nil, ResultManifest{}, trace) }),
		stateLayout("SourceEOFRecord", "EOF:u64", "Revision:u64(exactly-one)"),
		stateLayout("CheckpointRecord", "Watermark:u64", "Revision:u64(nonzero)"),
		stateLayout("WorkerEventCursor", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "TransactionID:u64(nonzero)", "Digest:sha256(nonzero)"),
		stateLayout("JobRecord", "JobID:JobID", "DefiningRequest:ClientRequestID", "TopologyDigest:sha256", "TopologyBytes:owned-canonical-topology", "Lifecycle:JobLifecycle", "JobControlRevision:u64", "Assignment:optional(AssignmentSet)", "NeedsReassignment:sorted-list(NeedsReassignment)", "InvalidationHistory:u16-count+chronological(InvalidationProvenance)", "SourceEOFs:task-keyed(SourceEOFRecord)", "Checkpoints:task-keyed(CheckpointRecord)", "Manifests:task-keyed(ResultManifest)", "Failure:optional(JobFailureReport)"),
		stateLayout("SubjectHistory", "Revision:u64", "ID:bytes32", "Digest:sha256", "Target:u32-bytes(owned)", "Result:u32-bytes(owned)", "Applied:u8", "AppliedRevision:u64", "AppliedTarget:u32-bytes(owned)", "AppliedResult:u32-bytes(owned)"),
		stateLayout("CommandResult", "SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"),
	}
}

// StateCommandEnumDomains returns names paired with every actual accepted v1
// state enum constant.
func StateCommandEnumDomains() []model.StateCommandEnumDescriptor {
	return []model.StateCommandEnumDescriptor{
		{Name: "IdentitySelector", Values: []string{fmt.Sprintf("Client=%d", identityClient), fmt.Sprintf("Internal=%d", identityInternal)}},
		{Name: "CommandKind", Values: []string{fmt.Sprintf("BeginCoordinatorEpoch=%d", CommandBeginCoordinatorEpoch), fmt.Sprintf("RegisterWorker=%d", CommandRegisterWorker), fmt.Sprintf("DrainWorker=%d", CommandDrainWorker), fmt.Sprintf("DeactivateWorker=%d", CommandDeactivateWorker), fmt.Sprintf("ReplaceWorkerEpoch=%d", CommandReplaceWorkerEpoch), fmt.Sprintf("SubmitJob=%d", CommandSubmitJob), fmt.Sprintf("CancelJob=%d", CommandCancelJob), fmt.Sprintf("RecordSourceEOF=%d", CommandRecordSourceEOF), fmt.Sprintf("InstallAssignments=%d", CommandInstallAssignments), fmt.Sprintf("ReplaceAssignments=%d", CommandReplaceAssignments), fmt.Sprintf("AdvanceCheckpoint=%d", CommandAdvanceCheckpoint), fmt.Sprintf("SealManifest=%d", CommandSealManifest), fmt.Sprintf("TransitionJob=%d", CommandTransitionJob), fmt.Sprintf("FailJob=%d", CommandFailJob)}},
		{Name: "WorkerState", Values: []string{fmt.Sprintf("Eligible=%d", WorkerEligible), fmt.Sprintf("Draining=%d", WorkerDraining), fmt.Sprintf("Offline=%d", WorkerOffline)}},
		{Name: "JobLifecycle", Values: []string{fmt.Sprintf("Pending=%d", JobPending), fmt.Sprintf("Deploying=%d", JobDeploying), fmt.Sprintf("Running=%d", JobRunning), fmt.Sprintf("Draining=%d", JobDraining), fmt.Sprintf("Succeeded=%d", JobSucceeded), fmt.Sprintf("Failed=%d", JobFailed), fmt.Sprintf("Canceled=%d", JobCanceled)}},
		{Name: "SubjectKind", Values: []string{fmt.Sprintf("None=%d", SubjectNone), fmt.Sprintf("Coordinator=%d", SubjectCoordinator), fmt.Sprintf("Worker=%d", SubjectWorker), fmt.Sprintf("JobControl=%d", SubjectJobControl), fmt.Sprintf("SourceEOF=%d", SubjectSourceEOF), fmt.Sprintf("SourceCheckpoint=%d", SubjectSourceCheckpoint), fmt.Sprintf("ResultManifest=%d", SubjectResultManifest)}},
		{Name: "ResultCode", Values: []string{fmt.Sprintf("Success=%d", ResultSuccess), fmt.Sprintf("IdentityReuse=%d", ResultIdentityReuse), fmt.Sprintf("StaleRequest=%d", ResultStaleRequest), fmt.Sprintf("SkippedRequest=%d", ResultSkippedRequest), fmt.Sprintf("CapacityExhausted=%d", ResultCapacityExhausted), fmt.Sprintf("RevisionMismatch=%d", ResultRevisionMismatch), fmt.Sprintf("StaleEpoch=%d", ResultStaleEpoch), fmt.Sprintf("ResultTooLarge=%d", ResultResultTooLarge)}},
		{Name: "ReassignmentTargetKind", Values: []string{fmt.Sprintf("Task=%d", TaskTarget), fmt.Sprintf("ResultReplica=%d", ResultReplicaTarget)}},
		{Name: "WorkerInvalidationKind", Values: []string{"Forgotten=0", fmt.Sprintf("Deactivate=%d", workerInvalidationDeactivate), fmt.Sprintf("ReplaceEpoch=%d", workerInvalidationReplaceEpoch)}},
		{Name: "InvalidationRepairState", Values: []string{fmt.Sprintf("Active=%d", invalidationRepairActive), fmt.Sprintf("Anchored=%d", invalidationRepairAnchored), fmt.Sprintf("Forgotten=%d", invalidationRepairForgotten)}},
		{Name: "ResultReplicaRole", Values: []string{fmt.Sprintf("Primary=%d", model.PrimaryReplica), fmt.Sprintf("Secondary=%d", model.SecondaryReplica)}},
		{Name: "FailureCode", Values: []string{fmt.Sprintf("Operator=%d", model.FailureOperator), fmt.Sprintf("TupleInvalid=%d", model.FailureTupleInvalid), fmt.Sprintf("Storage=%d", model.FailureStorage)}},
	}
}
