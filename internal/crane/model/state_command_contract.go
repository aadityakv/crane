package model

const stateCommandContractFingerprintDomain = "cs425/crane/state-command-contract/v1\x00"

const (
	StateCommandSchemaVersionV1         uint16 = 1
	StateCommandMaxClientSessionsV1     uint64 = 1024
	StateCommandMaxSubjectHistoriesV1   uint64 = 197889
	StateCommandMaxCachedResultBytesV1  uint64 = 65536
	StateCommandMaxSnapshotBytesV1      uint64 = 8388608
	StateCommandFixedEnvelopeBytesV1    uint64 = 37
	StateCommandClientEnvelopeBytesV1   uint64 = 56
	StateCommandInternalEnvelopeBytesV1 uint64 = 111
	StateCommandSubjectKeyBytesV1       uint64 = 39
	StateCommandBeginTargetBytesV1      uint64 = 18
	StateCommandCommandResultBytesV1    uint64 = 65
	StateCommandSnapshotBaseBytesV1     uint64 = 128
	StateCommandClientHistoryFixedV1    uint64 = 60
	StateCommandSubjectHistoryFixedV1   uint64 = 128
)

// StateCommandLayoutDescriptor pins one canonical Raft-applied layout.
type StateCommandLayoutDescriptor struct {
	Name   string
	Fields []string
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
	SchemaVersion uint16

	EnvelopeLayouts []StateCommandLayoutDescriptor
	EnumDomains     []StateCommandEnumDescriptor
	ResultMatrix    []StateCommandResultRule
	DigestDomains   []string

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
	MaxWorkerSlots       uint64
	MaxCommandBytes      uint64
	MaxTopologyBytes     uint64
	MaxResultBytes       uint64

	FixedEnvelopeBytes       uint64
	ClientEnvelopeBytes      uint64
	InternalEnvelopeBytes    uint64
	SubjectKeyBytes          uint64
	BeginTargetBytes         uint64
	CommandResultBytes       uint64
	SnapshotBaseBytes        uint64
	ClientHistoryFixedBytes  uint64
	SubjectHistoryFixedBytes uint64

	Rules []string
}

var stateCommandLayoutsV1 = []StateCommandLayoutDescriptor{
	{Name: "Envelope", Fields: []string{"SchemaVersion:u16", "ConsensusFingerprint:sha256", "Kind:u16", "IdentitySelector:u8", "Identity:ClientEnvelope|InternalEnvelope", "Target:concrete-command-fields"}},
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
	{Name: "NeedsReassignment", Fields: []string{"Kind:ReassignmentTargetKind", "Task:TaskID", "SinkTask:TaskID", "ReplicaRole:ResultReplicaRole", "OldWorkerID:u16(nonzero)", "OldWorkerEpoch:bytes16(nonzero)"}},
	{Name: "CompletionReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Source:TaskID", "Token:AssignmentToken", "Epoch:CoordinatorEpoch", "ExpectedCheckpointRevision:u64", "Prior:u64", "New:u64", "EOF:u64", "WorkerTransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "JobFailureReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Task:AssignmentToken", "Epoch:CoordinatorEpoch", "TransactionID:u64(nonzero)", "Code:FailureCode", "DetailDigest:sha256(nonzero)"}},
	{Name: "ResultManifest", Fields: []string{"JobID:JobID", "SinkTask:TaskID", "ManifestRevision:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "RecordCount:u64", "TotalBytes:u64(bounded)", "Checksum:sha256(nonzero)", "Replicas:ResultReplicaSet"}},
	{Name: "SourceEOFRecord", Fields: []string{"EOF:u64", "Revision:u64(exactly-one)"}},
	{Name: "CheckpointRecord", Fields: []string{"Watermark:u64", "Revision:u64(nonzero)"}},
	{Name: "WorkerEventCursor", Fields: []string{"WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "TransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
	{Name: "JobRecord", Fields: []string{"JobID:JobID", "DefiningRequest:ClientRequestID", "TopologyDigest:sha256", "TopologyBytes:owned-canonical-topology", "Lifecycle:JobLifecycle", "JobControlRevision:u64", "Assignment:optional(AssignmentSet)", "NeedsReassignment:sorted-list(NeedsReassignment)", "SourceEOFs:task-keyed(SourceEOFRecord)", "Checkpoints:task-keyed(CheckpointRecord)", "Manifests:task-keyed(ResultManifest)", "Failure:optional(JobFailureReport)"}},
	{Name: "CommandResult", Fields: []string{"SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"}},
}

var stateCommandEnumsV1 = []StateCommandEnumDescriptor{
	{Name: "IdentitySelector", Values: []string{"Client=1", "Internal=2"}},
	{Name: "CommandKind", Values: []string{"BeginCoordinatorEpoch=1", "RegisterWorker=2", "DrainWorker=3", "DeactivateWorker=4", "ReplaceWorkerEpoch=5", "SubmitJob=6", "CancelJob=7", "RecordSourceEOF=8", "InstallAssignments=9", "ReplaceAssignments=10", "AdvanceCheckpoint=11", "SealManifest=12", "TransitionJob=13", "FailJob=14"}},
	{Name: "WorkerState", Values: []string{"Eligible=1", "Draining=2", "Offline=3"}},
	{Name: "JobLifecycle", Values: []string{"Pending=1", "Deploying=2", "Running=3", "Draining=4", "Succeeded=5", "Failed=6", "Canceled=7"}},
	{Name: "SubjectKind", Values: []string{"None=0", "Coordinator=1", "Worker=2", "JobControl=3", "SourceEOF=4", "SourceCheckpoint=5", "ResultManifest=6"}},
	{Name: "ResultCode", Values: []string{"Success=1", "IdentityReuse=2", "StaleRequest=3", "SkippedRequest=4", "CapacityExhausted=5", "RevisionMismatch=6", "StaleEpoch=7", "ResultTooLarge=8"}},
	{Name: "ReassignmentTargetKind", Values: []string{"Task=1", "ResultReplica=2"}},
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
	"begin-coordinator-exact-target-is-apply-term-plus-coordinator-plus-nonce",
	"begin-index-and-term-come-only-from-nonzero-apply-position",
	"coordinator-epochs-are-strictly-ordered-by-term-then-begin-index",
	"normal-business-rejection-is-an-encoded-command-result-with-nil-apply-error",
	"corrupt-encoding-fingerprint-digest-or-impossible-invariant-is-an-apply-error",
	"counts-checked-arithmetic-and-eventual-snapshot-estimate-preflight-before-mutation",
	"capacity-exhausted-is-a-stateless-unaccepted-pre-execution-refusal-that-never-mutates-evicts-or-executes-and-retries-byte-identically-while-state-is-unchanged",
	"every-non-capacity-resolution-is-retained-before-return",
	"all-decoded-and-cached-bytes-are-owned",
	"maps-gob-and-opaque-future-command-payloads-are-forbidden-on-wire",
	"worker-records-are-bounded-fingerprinted-and-revisioned-per-node-id",
	"offline-same-epoch-registration-may-return-eligible-but-never-revives-draining",
	"jobs-retain-defining-client-request-topology-digest-and-owned-canonical-bytes-for-collision-defense",
	"active-and-retained-job-capacities-preflight-before-client-mutation",
	"source-eof-is-derived-from-immutable-topology-recorded-once-before-assignment-and-has-an-independent-revision",
	"assignment-install-is-one-complete-canonical-set-and-every-token-revision-equals-the-enclosing-set-revision",
	"assignment-replacement-requires-the-exact-old-set-revision-digest-and-reassignment-marker-digest",
	"assignment-replacement-changes-only-marked-targets-advances-only-their-attempts-and-replaces-result-replicas-by-marked-role",
	"worker-deactivation-and-epoch-replacement-require-the-sorted-complete-affected-job-list-and-atomically-union-sorted-reassignment-markers",
	"checkpoint-manifest-and-job-control-subject-revisions-advance-independently",
	"worker-event-transactions-use-one-strictly-increasing-digest-bound-cursor-per-exact-worker-id-and-worker-epoch-across-all-jobs-and-sources",
	"worker-epoch-replacement-discards-the-old-epoch-event-cursor-before-the-new-epoch-starts-at-zero",
	"completion-and-failure-events-bind-the-current-coordinator-epoch-job-control-revision-assignment-revision-and-exact-current-token",
	"checkpoints-never-exceed-immutable-source-eof-and-job-draining-requires-every-source-at-eof",
	"sealed-manifests-bind-the-current-two-node-result-replica-set-and-succeeded-requires-every-current-manifest-and-final-checkpoint",
	"only-deploying-to-running-running-to-draining-and-draining-to-succeeded-are-normal-internal-lifecycle-transitions",
	"all-declared-collection-counts-and-command-sizes-are-bounded-before-allocation-or-mutation",
}

// StateCommandContractV1 returns deep-owned consensus descriptor slices.
func StateCommandContractV1() StateCommandContract {
	return StateCommandContract{
		SchemaVersion:            StateCommandSchemaVersionV1,
		EnvelopeLayouts:          cloneStateCommandLayouts(stateCommandLayoutsV1),
		EnumDomains:              cloneStateCommandEnums(stateCommandEnumsV1),
		ResultMatrix:             append([]StateCommandResultRule(nil), stateCommandResultMatrixV1...),
		DigestDomains:            []string{"cs425/crane/internal-command/v1", "cs425/crane/needs-reassignment/v1", "cs425/crane/completion-report/v1", "cs425/crane/job-failure-event/v1"},
		MaxClientSessions:        StateCommandMaxClientSessionsV1,
		MaxSubjectHistories:      StateCommandMaxSubjectHistoriesV1,
		MaxCachedResultBytes:     StateCommandMaxCachedResultBytesV1,
		MaxSnapshotBytes:         StateCommandMaxSnapshotBytesV1,
		MaxWorkers:               LimitsV1().MaxRegisteredWorkers,
		MaxActiveJobs:            LimitsV1().MaxActiveJobs,
		MaxRetainedJobs:          LimitsV1().MaxRetainedJobs,
		MaxTasksPerJob:           LimitsV1().MaxTasksPerJob,
		MaxManifestsPerJob:       LimitsV1().MaxResultManifestsPerJob,
		MaxReassignmentMarks:     LimitsV1().MaxTasksPerJob + 2*LimitsV1().MaxResultManifestsPerJob,
		MaxWorkerSlots:           LimitsV1().MaxWorkerSlots,
		MaxCommandBytes:          LimitsV1().MaxSubmitJobBytes,
		MaxTopologyBytes:         LimitsV1().MaxTopologyBytes,
		MaxResultBytes:           LimitsV1().MaxResultRecordsBytesPerJob,
		FixedEnvelopeBytes:       StateCommandFixedEnvelopeBytesV1,
		ClientEnvelopeBytes:      StateCommandClientEnvelopeBytesV1,
		InternalEnvelopeBytes:    StateCommandInternalEnvelopeBytesV1,
		SubjectKeyBytes:          StateCommandSubjectKeyBytesV1,
		BeginTargetBytes:         StateCommandBeginTargetBytesV1,
		CommandResultBytes:       StateCommandCommandResultBytesV1,
		SnapshotBaseBytes:        StateCommandSnapshotBaseBytesV1,
		ClientHistoryFixedBytes:  StateCommandClientHistoryFixedV1,
		SubjectHistoryFixedBytes: StateCommandSubjectHistoryFixedV1,
		Rules:                    append([]string(nil), stateCommandRulesV1...),
	}
}

func canonicalStateCommandContractBytes(contract StateCommandContract) []byte {
	encoded := appendString([]byte(stateCommandContractFingerprintDomain), "crane-state-command")
	encoded = appendUint16(encoded, contract.SchemaVersion)
	encoded = appendUint16(encoded, uint16(len(contract.EnvelopeLayouts)))
	for _, layout := range contract.EnvelopeLayouts {
		encoded = appendString(encoded, layout.Name)
		encoded = appendStringList(encoded, layout.Fields)
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
		contract.MaxReassignmentMarks, contract.MaxWorkerSlots,
		contract.MaxCommandBytes, contract.MaxTopologyBytes, contract.MaxResultBytes,
		contract.FixedEnvelopeBytes, contract.ClientEnvelopeBytes,
		contract.InternalEnvelopeBytes, contract.SubjectKeyBytes,
		contract.BeginTargetBytes, contract.CommandResultBytes,
		contract.SnapshotBaseBytes, contract.ClientHistoryFixedBytes,
		contract.SubjectHistoryFixedBytes,
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
