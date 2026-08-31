package model

const stateCommandContractFingerprintDomain = "cs425/crane/state-command-contract/v1\x00"

const (
	stateSnapshotMagicBytesV2          = 4
	stateSnapshotSchemaBytesV2         = 2
	stateSnapshotFingerprintBytesV2    = 32
	stateSnapshotIndexBytesV2          = 8
	stateSnapshotRevisionBytesV2       = 8
	stateSnapshotEpochBytesV2          = 34
	stateSnapshotRootCollectionCountV2 = 5
	stateSnapshotRootCountBytesV2      = 8
	stateSnapshotBytes32PrefixV2       = 4
	stateSnapshotBytes64PrefixV2       = 8
	stateSnapshotNestedCountBytesV2    = 2
	stateSnapshotOptionalSelectorV2    = 1

	// StateCommandSchemaVersionV1 is the canonical command schema version.
	StateCommandSchemaVersionV1 uint16 = 1
	// StateSnapshotSchemaVersionV2 is the canonical Crane snapshot schema.
	StateSnapshotSchemaVersionV2 uint32 = 2
	// StateCommandMaxClientSessionsV1 bounds retained public dedup sessions.
	StateCommandMaxClientSessionsV1 uint64 = 1024
	// StateCommandMaxSubjectHistoriesV1 bounds retained internal subject histories.
	StateCommandMaxSubjectHistoriesV1 uint64 = 197889
	// StateCommandMaxCachedResultBytesV1 bounds one retained command result.
	StateCommandMaxCachedResultBytesV1 uint64 = 65536
	// StateCommandMaxSnapshotBytesV1 bounds all eventual canonical snapshot bytes.
	StateCommandMaxSnapshotBytesV1 uint64 = 8388608
	// StateCommandFixedEnvelopeBytesV1 counts common envelope bytes including epoch.
	StateCommandFixedEnvelopeBytesV1 uint64 = 71
	// StateCommandClientEnvelopeBytesV1 counts the client identity branch.
	StateCommandClientEnvelopeBytesV1 uint64 = 56
	// StateCommandInternalEnvelopeBytesV1 counts the internal identity branch.
	StateCommandInternalEnvelopeBytesV1 uint64 = 111
	// StateCommandSubjectKeyBytesV1 counts one canonical subject union.
	StateCommandSubjectKeyBytesV1 uint64 = 39
	// StateCommandBeginTargetBytesV1 counts the begin target payload.
	StateCommandBeginTargetBytesV1 uint64 = 18
	// StateCommandCommandResultBytesV1 counts one canonical result.
	StateCommandCommandResultBytesV1 uint64 = 65
	// StateCommandSnapshotBaseBytesV1 counts root selectors and collection counts.
	StateCommandSnapshotBaseBytesV1 uint64 = stateSnapshotMagicBytesV2 + stateSnapshotSchemaBytesV2 + stateSnapshotFingerprintBytesV2 + stateSnapshotIndexBytesV2 + stateSnapshotRevisionBytesV2 + stateSnapshotEpochBytesV2 + stateSnapshotRootCollectionCountV2*stateSnapshotRootCountBytesV2
	// StateCommandClientHistoryFixedV1 counts one client key and fixed history fields.
	StateCommandClientHistoryFixedV1 uint64 = 60
	// StateCommandSubjectHistoryFixedV1 counts one subject key and fixed history fields.
	StateCommandSubjectHistoryFixedV1 uint64 = 136
	// StateCommandWorkerRecordBytesV1 counts a worker map key and record.
	StateCommandWorkerRecordBytesV1 uint64 = 95
	// StateCommandJobRecordFixedBytesV1 counts a job key, fixed fields, selectors, and counts.
	StateCommandJobRecordFixedBytesV1 uint64 = 117
	// StateCommandAssignmentTokenBytesV1 counts one assignment token.
	StateCommandAssignmentTokenBytesV1 uint64 = 86
	// StateCommandResultReplicaBytesV1 counts one result replica set.
	StateCommandResultReplicaBytesV1 uint64 = 56
	// StateCommandReassignmentBytesV1 counts one reassignment marker.
	StateCommandReassignmentBytesV1 uint64 = 60
	// StateCommandInvalidationProvenanceFixedBytesV1 counts one provenance record excluding its marker list.
	StateCommandInvalidationProvenanceFixedBytesV1 uint64 = 158
	// StateCommandMaxInvalidationProvenanceV1 is the hard per-job u16 provenance-count bound.
	StateCommandMaxInvalidationProvenanceV1 uint64 = 1<<16 - 1
	// StateCommandSourceEOFEntryBytesV1 counts a source key and EOF record.
	StateCommandSourceEOFEntryBytesV1 uint64 = 36
	// StateCommandCheckpointEntryBytesV1 counts a source key and checkpoint record.
	StateCommandCheckpointEntryBytesV1 uint64 = 36
	// StateCommandManifestEntryBytesV1 counts a sink key and manifest record.
	StateCommandManifestEntryBytesV1 uint64 = 200
	// StateCommandFailureBytesV1 counts one present failure report.
	StateCommandFailureBytesV1 uint64 = 194
	// StateCommandWorkerEventBytesV1 counts one worker-event key and cursor.
	StateCommandWorkerEventBytesV1 uint64 = 58
	// StateCommandAssignmentFixedBytesV1 counts assignment identity, digest, and both list counts.
	StateCommandAssignmentFixedBytesV1 uint64 = 16 + 8 + 32 + 2*stateSnapshotNestedCountBytesV2
	// StateCommandSnapshotBytes32PrefixV2 counts one u32 variable-length prefix.
	StateCommandSnapshotBytes32PrefixV2 uint64 = stateSnapshotBytes32PrefixV2
	// StateCommandSnapshotBytes64PrefixV2 counts one u64 variable-length prefix.
	StateCommandSnapshotBytes64PrefixV2 uint64 = stateSnapshotBytes64PrefixV2
	// StateCommandSnapshotRootCountBytesV2 counts one root u64 collection count.
	StateCommandSnapshotRootCountBytesV2 uint64 = stateSnapshotRootCountBytesV2
	// StateCommandSnapshotNestedCountBytesV2 counts one nested u16 collection count.
	StateCommandSnapshotNestedCountBytesV2 uint64 = stateSnapshotNestedCountBytesV2
	// StateCommandSnapshotOptionalSelectorBytesV2 counts one optional-value selector.
	StateCommandSnapshotOptionalSelectorBytesV2 uint64 = stateSnapshotOptionalSelectorV2
)

// StateCommandLayoutDescriptor pins one canonical Raft-applied layout.
type StateCommandLayoutDescriptor struct {
	Name   string
	Fields []string
}

// StateCommandConstantDescriptor pins one named snapshot-accounting constant.
type StateCommandConstantDescriptor struct {
	Name  string
	Value uint64
}

// StateCommandEnumDescriptor pins one complete accepted enum domain.
type StateCommandEnumDescriptor struct {
	Name   string
	Values []string
}

// StateCommandRevisionPolicy pins the legal revision shape for one result pair.
type StateCommandRevisionPolicy uint8

const (
	StateCommandRevisionZero    StateCommandRevisionPolicy = 1
	StateCommandRevisionNonZero StateCommandRevisionPolicy = 2
	StateCommandRevisionAny     StateCommandRevisionPolicy = 3
)

// StateCommandIdentityPolicy pins the legal JobID/WorkerID correlation fields.
type StateCommandIdentityPolicy uint8

const (
	StateCommandIdentityUnbound     StateCommandIdentityPolicy = 1
	StateCommandIdentityCoordinator StateCommandIdentityPolicy = 2
	StateCommandIdentityWorker      StateCommandIdentityPolicy = 3
	StateCommandIdentityJob         StateCommandIdentityPolicy = 4
)

// StateCommandEpochPolicy pins whether a result may carry a coordinator epoch.
type StateCommandEpochPolicy uint8

const (
	StateCommandEpochZero                StateCommandEpochPolicy = 1
	StateCommandEpochCoordinatorRevision StateCommandEpochPolicy = 2
	StateCommandEpochCurrentFence        StateCommandEpochPolicy = 3
)

// StateCommandResultRule pins one accepted ResultCode/SubjectKind combination.
type StateCommandResultRule struct {
	Code     uint16
	Subject  uint8
	Revision StateCommandRevisionPolicy
	Identity StateCommandIdentityPolicy
	Epoch    StateCommandEpochPolicy
}

// StateCommandContract is the dependency-leaf consensus contract for command
// envelopes, results, deduplication, and coordinator fencing.
type StateCommandContract struct {
	SchemaVersion         uint16
	SnapshotSchemaVersion uint32

	EnvelopeLayouts            []StateCommandLayoutDescriptor
	SnapshotLayouts            []StateCommandLayoutDescriptor
	SnapshotSortRules          []string
	SnapshotMigrationRules     []string
	SnapshotValidationRules    []string
	SnapshotEstimatorConstants []StateCommandConstantDescriptor
	EnumDomains                []StateCommandEnumDescriptor
	ResultMatrix               []StateCommandResultRule
	DigestDomains              []string

	MaxClientSessions    uint64
	MaxSubjectHistories  uint64
	MaxCachedResultBytes uint64
	MaxSnapshotBytes     uint64
	MaxWorkers           uint64
	MaxActiveJobs        uint64
	MaxRetainedJobs      uint64
	MaxTasksPerJob       uint64
	MaxManifestsPerJob   uint64
	MaxReassignmentMarks uint64
	MaxInvalidations     uint64
	MaxWorkerSlots       uint64
	MaxCommandBytes      uint64
	MaxTopologyBytes     uint64
	MaxResultBytes       uint64
	MinResultRecordBytes uint64
	MaxResultRecordBytes uint64
	MaxManifestRecords   uint64

	FixedEnvelopeBytes       uint64
	ClientEnvelopeBytes      uint64
	InternalEnvelopeBytes    uint64
	SubjectKeyBytes          uint64
	BeginTargetBytes         uint64
	CommandResultBytes       uint64
	SnapshotBaseBytes        uint64
	ClientHistoryFixedBytes  uint64
	SubjectHistoryFixedBytes uint64
	WorkerRecordBytes        uint64
	JobRecordFixedBytes      uint64
	AssignmentTokenBytes     uint64
	ResultReplicaBytes       uint64
	ReassignmentBytes        uint64
	InvalidationBytes        uint64
	SourceEOFEntryBytes      uint64
	CheckpointEntryBytes     uint64
	ManifestEntryBytes       uint64
	FailureBytes             uint64
	WorkerEventBytes         uint64

	Rules []string
}

var stateCommandLayoutsV1 = []StateCommandLayoutDescriptor{
	{Name: "Envelope", Fields: []string{"SchemaVersion:u16", "ConsensusFingerprint:sha256", "Kind:u16", "CoordinatorEpoch:CoordinatorEpoch(zero-only-for-begin)", "IdentitySelector:u8", "Identity:ClientEnvelope|InternalEnvelope", "Target:concrete-command-fields"}},
	{Name: "ClientEnvelope", Fields: []string{"ClientID:bytes16(nonzero)", "Sequence:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "InternalEnvelope", Fields: []string{"ID:bytes32(nonzero)", "Digest:sha256(nonzero)", "Subject:SubjectKey", "ExpectedRevision:u64"}},
	{Name: "SubjectKey", Fields: []string{"Kind:u8", "JobID:JobID", "TaskID:TaskID", "WorkerID:u16"}},
	{Name: "BeginCoordinatorEpoch", Fields: []string{"Envelope:Envelope(internal)", "Coordinator:u16(nonzero)", "Nonce:bytes16(nonzero)"}},
	{Name: "RegisterWorker", Fields: []string{"Envelope:Envelope(internal-worker)", "Worker:WorkerRecord"}},
	{Name: "DrainWorker", Fields: []string{"Envelope:Envelope(internal-worker)", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)"}},
	{Name: "DeactivateWorker", Fields: []string{"Envelope:Envelope(internal-worker)", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "Affected:list(AffectedAssignment)"}},
	{Name: "ReplaceWorkerEpoch", Fields: []string{"Envelope:Envelope(internal-worker)", "WorkerID:u16(nonzero)", "OldEpoch:bytes16(nonzero)", "Target:WorkerRecord", "Affected:list(AffectedAssignment)"}},
	{Name: "SubmitJob", Fields: []string{"Envelope:Envelope(client)", "Topology:canonical-topology-v1"}},
	{Name: "CancelJob", Fields: []string{"Envelope:Envelope(client)", "JobID:bytes16(nonzero)", "ExpectedRevision:u64(nonzero-successor)"}},
	{Name: "RecordSourceEOF", Fields: []string{"Envelope:Envelope(internal-source-eof)", "Source:TaskID", "EOF:u64"}},
	{Name: "InstallAssignments", Fields: []string{"Envelope:Envelope(internal-job-control)", "Assignment:AssignmentSet"}},
	{Name: "ReplaceAssignments", Fields: []string{"Envelope:Envelope(internal-job-control)", "JobID:JobID", "ExpectedAssignmentRevision:u64(nonzero)", "ExpectedDigest:sha256(nonzero)", "ExpectedMarkersDigest:sha256(nonzero)", "Target:AssignmentSet(successor)"}},
	{Name: "AdvanceCheckpoint", Fields: []string{"Envelope:Envelope(internal-source-checkpoint)", "Report:CompletionReport"}},
	{Name: "SealManifest", Fields: []string{"Envelope:Envelope(internal-result-manifest)", "Manifest:ResultManifest"}},
	{Name: "TransitionJob", Fields: []string{"Envelope:Envelope(internal-job-control)", "JobID:JobID", "From:JobLifecycle", "To:JobLifecycle"}},
	{Name: "FailJob", Fields: []string{"Envelope:Envelope(internal-job-control)", "Report:JobFailureReport"}},
	{Name: "WorkerRecord", Fields: []string{"NodeID:u16(nonzero)", "Epoch:bytes16(nonzero)", "State:u8", "Revision:u64(nonzero)", "Slots:u16", "ConsensusFingerprint:sha256", "RegistryFingerprint:sha256"}},
	{Name: "AffectedAssignment", Fields: []string{"JobID:bytes16(nonzero)", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "AssignmentDigest:sha256(nonzero)"}},
	{Name: "TaskID", Fields: []string{"JobID:bytes16(nonzero)", "StageID:u16(nonzero)", "Partition:u16"}},
	{Name: "CoordinatorEpoch", Fields: []string{"Term:u64(nonzero)", "BeginIndex:u64(nonzero)", "Coordinator:u16(nonzero)", "Nonce:bytes16(nonzero)"}},
	{Name: "AssignmentToken", Fields: []string{"Task:TaskID", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "Attempt:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "AssignmentRevision:u64(nonzero)"}},
	{Name: "ResultReplicaSet", Fields: []string{"SinkTask:TaskID", "PrimaryNodeID:u16(nonzero)", "SecondaryNodeID:u16(nonzero-distinct)", "PrimaryEpoch:bytes16(nonzero)", "SecondaryEpoch:bytes16(nonzero)"}},
	{Name: "AssignmentSet", Fields: []string{"JobID:bytes16(nonzero)", "Revision:u64(nonzero)", "Digest:sha256(nonzero)", "Tasks:u16-count+list(AssignmentToken)", "ResultReplicas:u16-count+list(ResultReplicaSet)"}},
	{Name: "AssignmentSetDigest", Fields: AssignmentSetDigestLayoutV1()},
	{Name: "NeedsReassignment", Fields: []string{"Kind:ReassignmentTargetKind", "Task:TaskID", "SinkTask:TaskID", "ReplicaRole:ResultReplicaRole", "OldWorkerID:u16(nonzero)", "OldWorkerEpoch:bytes16(nonzero)"}},
	{Name: "InvalidationProvenance", Fields: []string{"Kind:WorkerInvalidationKind(zero-iff-worker-anchor-forgotten)", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "WorkerRevision:u64(zero-iff-worker-anchor-forgotten)", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "AssignmentDigest:sha256(nonzero)", "Markers:u16-count+sorted(NeedsReassignment)", "RepairState:InvalidationRepairState", "RepairJobControlRevision:u64(nonzero-iff-anchored)", "RepairAssignmentRevision:u64(successor-iff-anchored)", "RepairAssignmentDigest:sha256(nonzero-iff-anchored)", "RepairMarkersDigest:sha256(nonzero-iff-anchored)"}},
	{Name: "CompletionReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Source:TaskID", "Token:AssignmentToken", "Epoch:CoordinatorEpoch", "ExpectedCheckpointRevision:u64", "Prior:u64", "New:u64", "EOF:u64", "WorkerTransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "JobFailureReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Task:AssignmentToken", "Epoch:CoordinatorEpoch", "TransactionID:u64(nonzero)", "Code:FailureCode", "DetailDigest:sha256(nonzero)"}},
	{Name: "ResultManifest", Fields: []string{"JobID:JobID", "SinkTask:TaskID", "ManifestRevision:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "RecordCount:u64", "TotalBytes:u64(bounded)", "Checksum:sha256(nonzero)", "Replicas:ResultReplicaSet"}},
	{Name: "SourceEOFRecord", Fields: []string{"EOF:u64", "Revision:u64(exactly-one)"}},
	{Name: "CheckpointRecord", Fields: []string{"Watermark:u64", "Revision:u64(nonzero)"}},
	{Name: "WorkerEventCursor", Fields: []string{"WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "TransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "JobRecord", Fields: []string{"JobID:JobID", "DefiningRequest:ClientRequestID", "TopologyDigest:sha256", "TopologyBytes:owned-canonical-topology", "Lifecycle:JobLifecycle", "JobControlRevision:u64", "Assignment:optional(AssignmentSet)", "NeedsReassignment:sorted-list(NeedsReassignment)", "InvalidationHistory:u16-count+chronological(InvalidationProvenance)", "SourceEOFs:task-keyed(SourceEOFRecord)", "Checkpoints:task-keyed(CheckpointRecord)", "Manifests:task-keyed(ResultManifest)", "Failure:optional(JobFailureReport)"}},
	{Name: "SubjectHistory", Fields: []string{"Revision:u64", "ID:bytes32", "Digest:sha256", "Target:u32-bytes(owned)", "Result:u32-bytes(owned)", "Applied:u8", "AppliedRevision:u64", "AppliedTarget:u32-bytes(owned)", "AppliedResult:u32-bytes(owned)"}},
	{Name: "CommandResult", Fields: []string{"SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"}},
}

var stateSnapshotLayoutsV2 = []StateCommandLayoutDescriptor{
	{Name: "SnapshotRoot", Fields: []string{"Magic:bytes4(CRSN)", "SchemaVersion:u16(2)", "ConsensusFingerprint:sha256", "LastAppliedCraneIndex:u64", "CoordinatorRevision:u64", "CoordinatorEpoch:CoordinatorEpoch", "Clients:u64-count+sorted(ClientHistory)", "Subjects:u64-count+sorted(SubjectHistory)", "Workers:u64-count+sorted(WorkerEntry)", "Jobs:u64-count+sorted(JobRecord)", "WorkerEvents:u64-count+sorted(WorkerEventEntry)"}},
	{Name: "ClientHistory", Fields: []string{"ClientID:bytes16(nonzero)", "Sequence:u64(nonzero)", "Digest:sha256(nonzero)", "Result:u32-bytes(owned,bounded)"}},
	{Name: "SubjectHistory", Fields: []string{"Subject:SubjectKey", "Revision:u64", "ID:bytes32(nonzero)", "Digest:sha256(nonzero)", "Target:u32-bytes(owned,bounded)", "Result:u32-bytes(owned,bounded)", "Applied:u8(bool)", "AppliedRevision:u64", "AppliedTarget:u32-bytes(owned,bounded)", "AppliedResult:u32-bytes(owned,bounded)"}},
	{Name: "WorkerEntry", Fields: []string{"NodeIDKey:u16(nonzero)", "Worker:WorkerRecord"}},
	{Name: "JobRecord", Fields: []string{"JobIDKey:bytes16(nonzero)", "JobID:bytes16(nonzero)", "DefiningRequest:ClientRequestID", "TopologyDigest:sha256(nonzero)", "TopologyBytes:u64-bytes(canonical,bounded)", "Lifecycle:JobLifecycle", "JobControlRevision:u64(nonzero)", "Assignment:optional(AssignmentSet)", "NeedsReassignment:u16-count+sorted(NeedsReassignment)", "InvalidationHistory:u16-count+chronological(InvalidationProvenance)", "SourceEOFs:u16-count+sorted(SourceEOFEntry)", "Checkpoints:u16-count+sorted(CheckpointEntry)", "Manifests:u16-count+sorted(ManifestEntry)", "Failure:optional(JobFailureReport)"}},
	{Name: "ClientRequestID", Fields: []string{"ClientID:bytes16(nonzero)", "Sequence:u64(nonzero)"}},
	{Name: "SourceEOFEntry", Fields: []string{"Source:TaskID", "EOF:u64", "Revision:u64(nonzero)"}},
	{Name: "CheckpointEntry", Fields: []string{"Source:TaskID", "Watermark:u64", "Revision:u64(nonzero)"}},
	{Name: "ManifestEntry", Fields: []string{"SinkTask:TaskID", "Manifest:ResultManifest"}},
	{Name: "WorkerEventEntry", Fields: []string{"WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "TransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "WorkerRecord", Fields: []string{"NodeID:u16(nonzero)", "Epoch:bytes16(nonzero)", "State:u8", "Revision:u64(nonzero)", "Slots:u16", "ConsensusFingerprint:sha256", "RegistryFingerprint:sha256"}},
	{Name: "SubjectKey", Fields: []string{"Kind:u8", "JobID:JobID", "TaskID:TaskID", "WorkerID:u16"}},
	{Name: "TaskID", Fields: []string{"JobID:bytes16(nonzero)", "StageID:u16(nonzero)", "Partition:u16"}},
	{Name: "CoordinatorEpoch", Fields: []string{"Term:u64(nonzero)", "BeginIndex:u64(nonzero)", "Coordinator:u16(nonzero)", "Nonce:bytes16(nonzero)"}},
	{Name: "AssignmentSet", Fields: []string{"JobID:bytes16(nonzero)", "Revision:u64(nonzero)", "Digest:sha256(nonzero)", "Tasks:u16-count+list(AssignmentToken)", "ResultReplicas:u16-count+list(ResultReplicaSet)"}},
	{Name: "AssignmentToken", Fields: []string{"Task:TaskID", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "Attempt:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "AssignmentRevision:u64(nonzero)"}},
	{Name: "ResultReplicaSet", Fields: []string{"SinkTask:TaskID", "PrimaryNodeID:u16(nonzero)", "SecondaryNodeID:u16(nonzero-distinct)", "PrimaryEpoch:bytes16(nonzero)", "SecondaryEpoch:bytes16(nonzero)"}},
	{Name: "NeedsReassignment", Fields: []string{"Kind:ReassignmentTargetKind", "Task:TaskID", "SinkTask:TaskID", "ReplicaRole:ResultReplicaRole", "OldWorkerID:u16(nonzero)", "OldWorkerEpoch:bytes16(nonzero)"}},
	{Name: "InvalidationProvenance", Fields: []string{"Kind:WorkerInvalidationKind(zero-iff-worker-anchor-forgotten)", "WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "WorkerRevision:u64(zero-iff-worker-anchor-forgotten)", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "AssignmentDigest:sha256(nonzero)", "Markers:u16-count+sorted(NeedsReassignment)", "RepairState:InvalidationRepairState", "RepairJobControlRevision:u64(nonzero-iff-anchored)", "RepairAssignmentRevision:u64(successor-iff-anchored)", "RepairAssignmentDigest:sha256(nonzero-iff-anchored)", "RepairMarkersDigest:sha256(nonzero-iff-anchored)"}},
	{Name: "ResultManifest", Fields: []string{"JobID:JobID", "SinkTask:TaskID", "ManifestRevision:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "RecordCount:u64", "TotalBytes:u64(bounded)", "Checksum:sha256(nonzero)", "Replicas:ResultReplicaSet"}},
	{Name: "JobFailureReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Task:AssignmentToken", "Epoch:CoordinatorEpoch", "TransactionID:u64(nonzero)", "Code:FailureCode", "DetailDigest:sha256(nonzero)"}},
}

var stateSnapshotSortRulesV2 = []string{
	"Clients:ClientID:unsigned-lexicographic-bytes16",
	"Subjects:SubjectKey:unsigned-lexicographic-canonical-bytes39",
	"Workers:NodeID:unsigned-u16",
	"Jobs:JobID:unsigned-lexicographic-bytes16",
	"WorkerEvents:WorkerID-unsigned-u16,WorkerEpoch-unsigned-lexicographic-bytes16",
	"NeedsReassignment:Kind,active-TaskID,ReplicaRole,OldWorkerID,OldWorkerEpoch",
	"InvalidationHistory:JobControlRevision-strictly-increasing",
	"SourceEOFs:TaskID:unsigned-lexicographic-canonical-bytes20",
	"Checkpoints:TaskID:unsigned-lexicographic-canonical-bytes20",
	"Manifests:TaskID:unsigned-lexicographic-canonical-bytes20",
}

var stateSnapshotMigrationRulesV2 = []string{
	"schema-0:empty-payload-only:restore-empty",
	"schema-1:empty-payload-only:restore-empty",
	"schema-2:nonempty-bounded-canonical:decode-current",
	"other-schema:reject-unsupported",
}

var stateSnapshotValidationRulesV2 = []string{
	"capture:incremental-estimate-equals-canonical-estimate-equals-encoded-length",
	"restore:temporary-owned-validate-canonical-reencode-byte-equality-then-atomic-swap",
	"ordering:all-declared-sorted-collections-are-strictly-increasing-and-duplicate-free",
	"errors:every-failure-wraps-ErrInvalidSnapshot-and-preserves-nested-sentinel",
	"cached-results:canonical-command-result-and-subject-identity-revision-epoch-correlated",
	"retained-targets:canonical-complete-semantic-correlation-with-authoritative-state",
	"job-control-lag:exact-active-provenance-count-plus-optional-client-cancellation",
	"worker-invalidations:bounded-active-anchored-forgotten-provenance-with-exact-retained-worker-or-job-control-authority-and-canonical-anchor-forgetting",
	"job-definitions:DefiningRequest-unique-across-all-retained-jobs",
	"assigned-jobs:complete-immutable-source-eofs-including-terminal",
	"reverse-references:coordinator-worker-job-control-eof-checkpoint-manifest",
	"artifacts:checked-per-job-aggregate-at-most-MaxResultBytes",
}

var stateSnapshotEstimatorConstantsV2 = []StateCommandConstantDescriptor{
	{Name: "SnapshotBaseFixed", Value: StateCommandSnapshotBaseBytesV1},
	{Name: "ClientHistoryFixed", Value: StateCommandClientHistoryFixedV1},
	{Name: "SubjectHistoryFixed", Value: StateCommandSubjectHistoryFixedV1},
	{Name: "WorkerEntryFixed", Value: StateCommandWorkerRecordBytesV1},
	{Name: "JobRecordFixed", Value: StateCommandJobRecordFixedBytesV1},
	{Name: "AssignmentFixed", Value: StateCommandAssignmentFixedBytesV1},
	{Name: "AssignmentTokenFixed", Value: StateCommandAssignmentTokenBytesV1},
	{Name: "ResultReplicaFixed", Value: StateCommandResultReplicaBytesV1},
	{Name: "ReassignmentMarkerFixed", Value: StateCommandReassignmentBytesV1},
	{Name: "InvalidationProvenanceFixed", Value: StateCommandInvalidationProvenanceFixedBytesV1},
	{Name: "InvalidationProvenanceMaxCount", Value: StateCommandMaxInvalidationProvenanceV1},
	{Name: "SourceEOFEntryFixed", Value: StateCommandSourceEOFEntryBytesV1},
	{Name: "CheckpointEntryFixed", Value: StateCommandCheckpointEntryBytesV1},
	{Name: "ManifestEntryFixed", Value: StateCommandManifestEntryBytesV1},
	{Name: "FailurePresentFixed", Value: StateCommandFailureBytesV1},
	{Name: "WorkerEventEntryFixed", Value: StateCommandWorkerEventBytesV1},
	{Name: "Bytes32LengthPrefix", Value: StateCommandSnapshotBytes32PrefixV2},
	{Name: "Bytes64LengthPrefix", Value: StateCommandSnapshotBytes64PrefixV2},
	{Name: "RootCollectionCount", Value: StateCommandSnapshotRootCountBytesV2},
	{Name: "NestedCollectionCount", Value: StateCommandSnapshotNestedCountBytesV2},
	{Name: "OptionalSelector", Value: StateCommandSnapshotOptionalSelectorBytesV2},
}

var stateCommandEnumsV1 = []StateCommandEnumDescriptor{
	{Name: "IdentitySelector", Values: []string{"Client=1", "Internal=2"}},
	{Name: "CommandKind", Values: []string{"BeginCoordinatorEpoch=1", "RegisterWorker=2", "DrainWorker=3", "DeactivateWorker=4", "ReplaceWorkerEpoch=5", "SubmitJob=6", "CancelJob=7", "RecordSourceEOF=8", "InstallAssignments=9", "ReplaceAssignments=10", "AdvanceCheckpoint=11", "SealManifest=12", "TransitionJob=13", "FailJob=14"}},
	{Name: "WorkerState", Values: []string{"Eligible=1", "Draining=2", "Offline=3"}},
	{Name: "JobLifecycle", Values: []string{"Pending=1", "Deploying=2", "Running=3", "Draining=4", "Succeeded=5", "Failed=6", "Canceled=7"}},
	{Name: "SubjectKind", Values: []string{"None=0", "Coordinator=1", "Worker=2", "JobControl=3", "SourceEOF=4", "SourceCheckpoint=5", "ResultManifest=6"}},
	{Name: "ResultCode", Values: []string{"Success=1", "IdentityReuse=2", "StaleRequest=3", "SkippedRequest=4", "CapacityExhausted=5", "RevisionMismatch=6", "StaleEpoch=7", "ResultTooLarge=8"}},
	{Name: "ReassignmentTargetKind", Values: []string{"Task=1", "ResultReplica=2"}},
	{Name: "WorkerInvalidationKind", Values: []string{"Forgotten=0", "Deactivate=1", "ReplaceEpoch=2"}},
	{Name: "InvalidationRepairState", Values: []string{"Active=1", "Anchored=2", "Forgotten=3"}},
	{Name: "ResultReplicaRole", Values: []string{"Primary=1", "Secondary=2"}},
	{Name: "FailureCode", Values: []string{"Operator=1", "TupleInvalid=2", "Storage=3"}},
}

var stateCommandResultMatrixV1 = []StateCommandResultRule{
	{Code: 1, Subject: 1, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 1, Subject: 2, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
	{Code: 1, Subject: 3, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 1, Subject: 4, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 1, Subject: 5, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 1, Subject: 6, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 1, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 2, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 2, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 3, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
	{Code: 4, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 1, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 5, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 5, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 6, Subject: 1, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 6, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
	{Code: 6, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 6, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 6, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 6, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 7, Subject: 1, Revision: StateCommandRevisionNonZero, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 7, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochCurrentFence},
	{Code: 7, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochCurrentFence},
	{Code: 7, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochCurrentFence},
	{Code: 7, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochCurrentFence},
	{Code: 7, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochCurrentFence},
	{Code: 8, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
	{Code: 8, Subject: 1, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
	{Code: 8, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
	{Code: 8, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 8, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 8, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
	{Code: 8, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
}

var stateCommandRulesV1 = []string{
	"exactly-one-client-or-internal-identity",
	"begin-coordinator-epoch-is-internal-only-and-coordinator-singleton",
	"subject-key-union-fields-are-canonical-and-zeroed",
	"digest-binds-schema-fingerprint-kind-identity-subject-expected-revision-and-exact-target-excluding-only-digest-slot",
	"client-sequences-start-at-one-and-advance-without-gaps",
	"client-exact-last-digest-replays-owned-byte-identical-result",
	"client-records-never-evict-and-new-identities-reject-at-capacity-before-execution",
	"internal-history-retains-most-recent-id-digest-target-result-per-exact-subject",
	"internal-exact-id-and-bytes-replay-owned-byte-identical-result",
	"internal-same-id-changed-bytes-is-identity-reuse",
	"internal-revisions-are-independent-per-exact-subject-key",
	"exact-already-target-replays-prior-success-without-revision-change",
	"exact-target-replay-requires-the-retained-success-applied-revision-to-equal-the-current-authoritative-subject-revision",
	"begin-coordinator-exact-target-is-apply-term-plus-coordinator-plus-nonce",
	"begin-index-and-term-come-only-from-nonzero-apply-position",
	"coordinator-epochs-are-strictly-ordered-by-term-then-begin-index",
	"every-non-begin-command-carries-the-exact-committed-coordinator-epoch-in-the-common-digest-bound-envelope",
	"exact-cached-client-or-internal-identity-replay-precedes-fence-rejection-but-every-unseen-stale-fence-is-stateless-and-non-mutating",
	"an-unaccepted-stale-client-command-may-be-rewrapped-under-the-current-fence-while-an-internal-retry-requires-a-fresh-id-and-current-digest",
	"normal-business-rejection-is-an-encoded-command-result-with-nil-apply-error",
	"corrupt-encoding-fingerprint-digest-or-impossible-invariant-is-an-apply-error",
	"counts-checked-arithmetic-and-eventual-snapshot-estimate-preflight-before-mutation",
	"capacity-exhausted-is-a-stateless-unaccepted-pre-execution-refusal-that-never-mutates-evicts-or-executes-and-retries-byte-identically-while-state-is-unchanged",
	"every-non-capacity-resolution-is-retained-before-return",
	"all-decoded-and-cached-bytes-are-owned",
	"maps-gob-and-opaque-future-command-payloads-are-forbidden-on-wire",
	"worker-records-are-bounded-fingerprinted-and-revisioned-per-node-id",
	"offline-same-epoch-registration-may-return-eligible-but-never-revives-draining",
	"worker-epoch-replacement-preserves-the-exact-operator-selected-eligible-draining-or-offline-state",
	"jobs-retain-defining-client-request-topology-digest-and-owned-canonical-bytes-for-collision-defense",
	"active-and-retained-job-capacities-preflight-before-client-mutation",
	"source-eof-is-derived-from-immutable-topology-recorded-once-before-assignment-and-has-an-independent-revision",
	"assignment-install-is-one-complete-canonical-set-and-every-token-revision-equals-the-enclosing-set-revision",
	"assignment-replacement-requires-the-exact-old-set-revision-digest-and-reassignment-marker-digest",
	"assignment-replacement-changes-only-marked-targets-advances-only-their-attempts-and-replaces-result-replicas-by-marked-role",
	"assignment-replacement-requires-newly-changed-task-and-replica-endpoints-to-be-current-eligible-while-unchanged-exact-epoch-endpoints-may-be-eligible-or-draining-but-never-offline-missing-or-stale",
	"worker-slots-are-cluster-wide-across-task-tokens-in-all-nonterminal-jobs-replacement-excludes-the-current-job-and-result-replicas-consume-no-slots",
	"assignment-replacement-slot-validation-counts-every-target-task-token-including-unchanged-draining-placements-against-residual-capacity-after-other-nonterminal-jobs",
	"worker-deactivation-and-epoch-replacement-require-the-sorted-complete-affected-job-list-and-atomically-union-sorted-reassignment-markers",
	"worker-invalidations-append-bounded-active-causal-provenance-repeat-marked-incarnations-do-not-advance-jobs-worker-history-overwrites-clear-exact-worker-anchors-repairs-become-job-control-anchored-and-later-job-control-successes-forget-or-prune-consumed-evidence",
	"checkpoint-manifest-and-job-control-subject-revisions-advance-independently",
	"worker-event-transactions-use-one-strictly-increasing-digest-bound-cursor-per-exact-worker-id-and-worker-epoch-across-all-jobs-and-sources",
	"worker-epoch-replacement-discards-the-old-epoch-event-cursor-before-the-new-epoch-starts-at-zero",
	"completion-and-failure-events-bind-the-current-coordinator-epoch-job-control-revision-assignment-revision-and-exact-current-token",
	"checkpoints-never-exceed-immutable-source-eof-and-job-draining-requires-every-source-at-eof",
	"sealed-manifests-bind-the-current-two-node-result-replica-set-and-succeeded-requires-every-current-manifest-and-final-checkpoint",
	"result-manifest-count-is-zero-iff-total-bytes-is-zero-and-nonzero-count-times-canonical-record-minimum-and-maximum-brackets-total-bytes-with-checked-arithmetic",
	"snapshot-accounting-includes-every-root-and-job-collection-count-map-key-entry-and-optional-selector-before-mutation",
	"only-deploying-to-running-running-to-draining-and-draining-to-succeeded-are-normal-internal-lifecycle-transitions",
	"all-declared-collection-counts-and-command-sizes-are-bounded-before-allocation-or-mutation",
	"snapshot-schema-two-encodes-the-complete-state-as-canonical-sorted-bounded-collections-with-an-exact-eight-mibibyte-limit",
	"snapshot-schema-zero-and-one-are-accepted-only-with-an-empty-payload-and-restore-the-empty-crane-state",
	"snapshot-restore-decodes-to-temporary-owned-state-validates-cross-references-and-canonical-reencoding-before-atomic-replacement",
	"snapshot-last-applied-index-is-the-last-successful-crane-command-index-not-the-raft-snapshot-barrier-position",
}

// StateCommandContractV1 returns deep-owned consensus descriptor slices.
func StateCommandContractV1() StateCommandContract {
	return StateCommandContract{
		SchemaVersion:              StateCommandSchemaVersionV1,
		SnapshotSchemaVersion:      StateSnapshotSchemaVersionV2,
		EnvelopeLayouts:            cloneStateCommandLayouts(stateCommandLayoutsV1),
		SnapshotLayouts:            cloneStateCommandLayouts(stateSnapshotLayoutsV2),
		SnapshotSortRules:          append([]string(nil), stateSnapshotSortRulesV2...),
		SnapshotMigrationRules:     append([]string(nil), stateSnapshotMigrationRulesV2...),
		SnapshotValidationRules:    append([]string(nil), stateSnapshotValidationRulesV2...),
		SnapshotEstimatorConstants: append([]StateCommandConstantDescriptor(nil), stateSnapshotEstimatorConstantsV2...),
		EnumDomains:                cloneStateCommandEnums(stateCommandEnumsV1),
		ResultMatrix:               append([]StateCommandResultRule(nil), stateCommandResultMatrixV1...),
		DigestDomains:              []string{"cs425/crane/internal-command/v1", "cs425/crane/needs-reassignment/v1", "cs425/crane/completion-report/v1", "cs425/crane/job-failure-event/v1", AssignmentSetDigestDomainV1},
		MaxClientSessions:          StateCommandMaxClientSessionsV1,
		MaxSubjectHistories:        StateCommandMaxSubjectHistoriesV1,
		MaxCachedResultBytes:       StateCommandMaxCachedResultBytesV1,
		MaxSnapshotBytes:           StateCommandMaxSnapshotBytesV1,
		MaxWorkers:                 LimitsV1().MaxRegisteredWorkers,
		MaxActiveJobs:              LimitsV1().MaxActiveJobs,
		MaxRetainedJobs:            LimitsV1().MaxRetainedJobs,
		MaxTasksPerJob:             LimitsV1().MaxTasksPerJob,
		MaxManifestsPerJob:         LimitsV1().MaxResultManifestsPerJob,
		MaxReassignmentMarks:       LimitsV1().MaxTasksPerJob + 2*LimitsV1().MaxResultManifestsPerJob,
		MaxInvalidations:           StateCommandMaxInvalidationProvenanceV1,
		MaxWorkerSlots:             LimitsV1().MaxWorkerSlots,
		MaxCommandBytes:            LimitsV1().MaxSubmitJobBytes,
		MaxTopologyBytes:           LimitsV1().MaxTopologyBytes,
		MaxResultBytes:             LimitsV1().MaxResultRecordsBytesPerJob,
		MinResultRecordBytes:       ResultArtifactMinRecordBytesV1,
		MaxResultRecordBytes:       ResultArtifactMaxRecordBytesV1,
		MaxManifestRecords:         ResultArtifactMaxRecordCountV1,
		FixedEnvelopeBytes:         StateCommandFixedEnvelopeBytesV1,
		ClientEnvelopeBytes:        StateCommandClientEnvelopeBytesV1,
		InternalEnvelopeBytes:      StateCommandInternalEnvelopeBytesV1,
		SubjectKeyBytes:            StateCommandSubjectKeyBytesV1,
		BeginTargetBytes:           StateCommandBeginTargetBytesV1,
		CommandResultBytes:         StateCommandCommandResultBytesV1,
		SnapshotBaseBytes:          StateCommandSnapshotBaseBytesV1,
		ClientHistoryFixedBytes:    StateCommandClientHistoryFixedV1,
		SubjectHistoryFixedBytes:   StateCommandSubjectHistoryFixedV1,
		WorkerRecordBytes:          StateCommandWorkerRecordBytesV1,
		JobRecordFixedBytes:        StateCommandJobRecordFixedBytesV1,
		AssignmentTokenBytes:       StateCommandAssignmentTokenBytesV1,
		ResultReplicaBytes:         StateCommandResultReplicaBytesV1,
		ReassignmentBytes:          StateCommandReassignmentBytesV1,
		InvalidationBytes:          StateCommandInvalidationProvenanceFixedBytesV1,
		SourceEOFEntryBytes:        StateCommandSourceEOFEntryBytesV1,
		CheckpointEntryBytes:       StateCommandCheckpointEntryBytesV1,
		ManifestEntryBytes:         StateCommandManifestEntryBytesV1,
		FailureBytes:               StateCommandFailureBytesV1,
		WorkerEventBytes:           StateCommandWorkerEventBytesV1,
		Rules:                      append([]string(nil), stateCommandRulesV1...),
	}
}

func canonicalStateCommandContractBytes(contract StateCommandContract) []byte {
	encoded := appendString([]byte(stateCommandContractFingerprintDomain), "crane-state-command")
	encoded = appendUint16(encoded, contract.SchemaVersion)
	encoded = appendUint32(encoded, contract.SnapshotSchemaVersion)
	encoded = appendUint16(encoded, uint16(len(contract.EnvelopeLayouts)))
	for _, layout := range contract.EnvelopeLayouts {
		encoded = appendString(encoded, layout.Name)
		encoded = appendStringList(encoded, layout.Fields)
	}
	encoded = appendUint16(encoded, uint16(len(contract.SnapshotLayouts)))
	for _, layout := range contract.SnapshotLayouts {
		encoded = appendString(encoded, layout.Name)
		encoded = appendStringList(encoded, layout.Fields)
	}
	encoded = appendStringList(encoded, contract.SnapshotSortRules)
	encoded = appendStringList(encoded, contract.SnapshotMigrationRules)
	encoded = appendStringList(encoded, contract.SnapshotValidationRules)
	encoded = appendUint16(encoded, uint16(len(contract.SnapshotEstimatorConstants)))
	for _, constant := range contract.SnapshotEstimatorConstants {
		encoded = appendString(encoded, constant.Name)
		encoded = appendUint64(encoded, constant.Value)
	}
	encoded = appendUint16(encoded, uint16(len(contract.EnumDomains)))
	for _, enum := range contract.EnumDomains {
		encoded = appendString(encoded, enum.Name)
		encoded = appendStringList(encoded, enum.Values)
	}
	encoded = appendUint16(encoded, uint16(len(contract.ResultMatrix)))
	for _, rule := range contract.ResultMatrix {
		encoded = appendUint16(encoded, rule.Code)
		encoded = append(encoded, rule.Subject, byte(rule.Revision), byte(rule.Identity), byte(rule.Epoch))
	}
	encoded = appendStringList(encoded, contract.DigestDomains)
	for _, value := range []uint64{
		contract.MaxClientSessions, contract.MaxSubjectHistories,
		contract.MaxCachedResultBytes, contract.MaxSnapshotBytes,
		contract.MaxWorkers, contract.MaxActiveJobs, contract.MaxRetainedJobs,
		contract.MaxTasksPerJob, contract.MaxManifestsPerJob,
		contract.MaxReassignmentMarks, contract.MaxInvalidations, contract.MaxWorkerSlots,
		contract.MaxCommandBytes, contract.MaxTopologyBytes, contract.MaxResultBytes,
		contract.MinResultRecordBytes, contract.MaxResultRecordBytes, contract.MaxManifestRecords,
		contract.FixedEnvelopeBytes, contract.ClientEnvelopeBytes,
		contract.InternalEnvelopeBytes, contract.SubjectKeyBytes,
		contract.BeginTargetBytes, contract.CommandResultBytes,
		contract.SnapshotBaseBytes, contract.ClientHistoryFixedBytes,
		contract.SubjectHistoryFixedBytes, contract.WorkerRecordBytes,
		contract.JobRecordFixedBytes, contract.AssignmentTokenBytes,
		contract.ResultReplicaBytes, contract.ReassignmentBytes, contract.InvalidationBytes,
		contract.SourceEOFEntryBytes, contract.CheckpointEntryBytes,
		contract.ManifestEntryBytes, contract.FailureBytes,
		contract.WorkerEventBytes,
	} {
		encoded = appendUint64(encoded, value)
	}
	return appendStringList(encoded, contract.Rules)
}

// StateCommandResultRuleV1 returns the exact accepted rule for code and subject.
func StateCommandResultRuleV1(code uint16, subject uint8) (StateCommandResultRule, bool) {
	for _, rule := range stateCommandResultMatrixV1 {
		if rule.Code == code && rule.Subject == subject {
			return rule, true
		}
	}
	return StateCommandResultRule{}, false
}

func cloneStateCommandLayouts(input []StateCommandLayoutDescriptor) []StateCommandLayoutDescriptor {
	output := append([]StateCommandLayoutDescriptor(nil), input...)
	for index := range output {
		output[index].Fields = append([]string(nil), input[index].Fields...)
	}
	return output
}

func cloneStateCommandEnums(input []StateCommandEnumDescriptor) []StateCommandEnumDescriptor {
	output := append([]StateCommandEnumDescriptor(nil), input...)
	for index := range output {
		output[index].Values = append([]string(nil), input[index].Values...)
	}
	return output
}
