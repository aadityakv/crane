package model

import (
	"reflect"
	"testing"
)

func TestStateCommandContractV1PinsCompleteReplicatedDedupContract(t *testing.T) {
	want := StateCommandContract{
		SchemaVersion:         1,
		SnapshotSchemaVersion: 2,
		EnvelopeLayouts: []StateCommandLayoutDescriptor{
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
			{Name: "CompletionReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Source:TaskID", "Token:AssignmentToken", "Epoch:CoordinatorEpoch", "ExpectedCheckpointRevision:u64", "Prior:u64", "New:u64", "EOF:u64", "WorkerTransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
			{Name: "JobFailureReport", Fields: []string{"JobID:JobID", "JobControlRevision:u64(nonzero)", "AssignmentRevision:u64(nonzero)", "Task:AssignmentToken", "Epoch:CoordinatorEpoch", "TransactionID:u64(nonzero)", "Code:FailureCode", "DetailDigest:sha256(nonzero)"}},
			{Name: "ResultManifest", Fields: []string{"JobID:JobID", "SinkTask:TaskID", "ManifestRevision:u64(nonzero)", "SpecificationHash:sha256(nonzero)", "RecordCount:u64", "TotalBytes:u64(bounded)", "Checksum:sha256(nonzero)", "Replicas:ResultReplicaSet"}},
			{Name: "SourceEOFRecord", Fields: []string{"EOF:u64", "Revision:u64(exactly-one)"}},
			{Name: "CheckpointRecord", Fields: []string{"Watermark:u64", "Revision:u64(nonzero)"}},
			{Name: "WorkerEventCursor", Fields: []string{"WorkerID:u16(nonzero)", "WorkerEpoch:bytes16(nonzero)", "TransactionID:u64(nonzero)", "Digest:sha256(nonzero)"}},
			{Name: "JobRecord", Fields: []string{"JobID:JobID", "DefiningRequest:ClientRequestID", "TopologyDigest:sha256", "TopologyBytes:owned-canonical-topology", "Lifecycle:JobLifecycle", "JobControlRevision:u64", "Assignment:optional(AssignmentSet)", "NeedsReassignment:sorted-list(NeedsReassignment)", "SourceEOFs:task-keyed(SourceEOFRecord)", "Checkpoints:task-keyed(CheckpointRecord)", "Manifests:task-keyed(ResultManifest)", "Failure:optional(JobFailureReport)"}},
			{Name: "SubjectHistory", Fields: []string{"Revision:u64", "ID:bytes32", "Digest:sha256", "Target:u32-bytes(owned)", "Result:u32-bytes(owned)", "Applied:u8", "AppliedRevision:u64", "AppliedTarget:u32-bytes(owned)", "AppliedResult:u32-bytes(owned)"}},
			{Name: "CommandResult", Fields: []string{"SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"}},
		},
		SnapshotLayouts:            cloneStateCommandLayouts(stateSnapshotLayoutsV2),
		SnapshotSortRules:          append([]string(nil), stateSnapshotSortRulesV2...),
		SnapshotMigrationRules:     append([]string(nil), stateSnapshotMigrationRulesV2...),
		SnapshotValidationRules:    append([]string(nil), stateSnapshotValidationRulesV2...),
		SnapshotEstimatorConstants: append([]StateCommandConstantDescriptor(nil), stateSnapshotEstimatorConstantsV2...),
		EnumDomains: []StateCommandEnumDescriptor{
			{Name: "IdentitySelector", Values: []string{"Client=1", "Internal=2"}},
			{Name: "CommandKind", Values: []string{"BeginCoordinatorEpoch=1", "RegisterWorker=2", "DrainWorker=3", "DeactivateWorker=4", "ReplaceWorkerEpoch=5", "SubmitJob=6", "CancelJob=7", "RecordSourceEOF=8", "InstallAssignments=9", "ReplaceAssignments=10", "AdvanceCheckpoint=11", "SealManifest=12", "TransitionJob=13", "FailJob=14"}},
			{Name: "WorkerState", Values: []string{"Eligible=1", "Draining=2", "Offline=3"}},
			{Name: "JobLifecycle", Values: []string{"Pending=1", "Deploying=2", "Running=3", "Draining=4", "Succeeded=5", "Failed=6", "Canceled=7"}},
			{Name: "SubjectKind", Values: []string{"None=0", "Coordinator=1", "Worker=2", "JobControl=3", "SourceEOF=4", "SourceCheckpoint=5", "ResultManifest=6"}},
			{Name: "ResultCode", Values: []string{"Success=1", "IdentityReuse=2", "StaleRequest=3", "SkippedRequest=4", "CapacityExhausted=5", "RevisionMismatch=6", "StaleEpoch=7", "ResultTooLarge=8"}},
			{Name: "ReassignmentTargetKind", Values: []string{"Task=1", "ResultReplica=2"}},
			{Name: "ResultReplicaRole", Values: []string{"Primary=1", "Secondary=2"}},
			{Name: "FailureCode", Values: []string{"Operator=1", "TupleInvalid=2", "Storage=3"}},
		},
		ResultMatrix: []StateCommandResultRule{
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
		},
		DigestDomains:     []string{"cs425/crane/internal-command/v1", "cs425/crane/needs-reassignment/v1", "cs425/crane/completion-report/v1", "cs425/crane/job-failure-event/v1", AssignmentSetDigestDomainV1},
		MaxClientSessions: 1024, MaxSubjectHistories: 197889,
		MaxCachedResultBytes: 65536, MaxSnapshotBytes: 8388608,
		MaxWorkers: 1024, MaxActiveJobs: 64, MaxRetainedJobs: 256,
		MaxTasksPerJob: 1024, MaxManifestsPerJob: 256, MaxReassignmentMarks: 1536,
		MaxWorkerSlots: 256, MaxCommandBytes: 1048576, MaxTopologyBytes: 118784, MaxResultBytes: 67108864,
		MinResultRecordBytes: ResultArtifactMinRecordBytesV1, MaxResultRecordBytes: ResultArtifactMaxRecordBytesV1, MaxManifestRecords: ResultArtifactMaxRecordCountV1,
		FixedEnvelopeBytes: 71, ClientEnvelopeBytes: 56, InternalEnvelopeBytes: 111,
		SubjectKeyBytes: 39, BeginTargetBytes: 18, CommandResultBytes: 65,
		SnapshotBaseBytes: 128, ClientHistoryFixedBytes: 60, SubjectHistoryFixedBytes: 136,
		WorkerRecordBytes: 95, JobRecordFixedBytes: 115, AssignmentTokenBytes: 86,
		ResultReplicaBytes: 56, ReassignmentBytes: 60, SourceEOFEntryBytes: 36,
		CheckpointEntryBytes: 36, ManifestEntryBytes: 200, FailureBytes: 194, WorkerEventBytes: 58,
		Rules: []string{
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
		},
	}
	got := StateCommandContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("StateCommandContractV1() = %#v, want %#v", got, want)
	}
	got.ResultMatrix[0].Code = 99
	if again := StateCommandContractV1(); again.ResultMatrix[0].Code == 99 {
		t.Fatal("StateCommandContractV1 returned shared result-matrix storage")
	}
	got.EnvelopeLayouts[0].Fields[0] = "mutated"
	got.SnapshotLayouts[0].Fields[0] = "mutated"
	got.SnapshotSortRules[0] = "mutated"
	got.SnapshotMigrationRules[0] = "mutated"
	got.SnapshotValidationRules[0] = "mutated"
	got.SnapshotEstimatorConstants[0].Value++
	got.EnumDomains[0].Values[0] = "mutated"
	got.Rules[0] = "mutated"
	if again := StateCommandContractV1(); reflect.DeepEqual(got, again) {
		t.Fatal("StateCommandContractV1 returned shared mutable storage")
	}
}

func TestCanonicalStateCommandContractBytesChangeForEveryDefiningField(t *testing.T) {
	base := StateCommandContractV1()
	want := canonicalStateCommandContractBytes(base)
	mutations := []func(*StateCommandContract){
		func(c *StateCommandContract) { c.SchemaVersion++ },
		func(c *StateCommandContract) { c.SnapshotSchemaVersion++ },
		func(c *StateCommandContract) { c.EnvelopeLayouts[0].Name += "x" },
		func(c *StateCommandContract) { c.EnvelopeLayouts[0].Fields[0] += "x" },
		func(c *StateCommandContract) { c.SnapshotLayouts[0].Name += "x" },
		func(c *StateCommandContract) { c.SnapshotLayouts[0].Fields[0] += "x" },
		func(c *StateCommandContract) { c.SnapshotSortRules[0] += "x" },
		func(c *StateCommandContract) { c.SnapshotMigrationRules[0] += "x" },
		func(c *StateCommandContract) { c.SnapshotValidationRules[0] += "x" },
		func(c *StateCommandContract) { c.SnapshotEstimatorConstants[0].Name += "x" },
		func(c *StateCommandContract) { c.SnapshotEstimatorConstants[0].Value++ },
		func(c *StateCommandContract) { c.EnumDomains[0].Name += "x" },
		func(c *StateCommandContract) { c.EnumDomains[0].Values[0] += "x" },
		func(c *StateCommandContract) { c.ResultMatrix[0].Code++ },
		func(c *StateCommandContract) { c.ResultMatrix[0].Subject++ },
		func(c *StateCommandContract) { c.ResultMatrix[0].Revision++ },
		func(c *StateCommandContract) { c.ResultMatrix[0].Identity++ },
		func(c *StateCommandContract) { c.ResultMatrix[0].Epoch++ },
		func(c *StateCommandContract) { c.DigestDomains[0] += "x" },
		func(c *StateCommandContract) { c.MaxClientSessions++ },
		func(c *StateCommandContract) { c.MaxSubjectHistories++ },
		func(c *StateCommandContract) { c.MaxCachedResultBytes++ },
		func(c *StateCommandContract) { c.MaxSnapshotBytes++ },
		func(c *StateCommandContract) { c.MaxWorkers++ },
		func(c *StateCommandContract) { c.MaxActiveJobs++ },
		func(c *StateCommandContract) { c.MaxRetainedJobs++ },
		func(c *StateCommandContract) { c.MaxTasksPerJob++ },
		func(c *StateCommandContract) { c.MaxManifestsPerJob++ },
		func(c *StateCommandContract) { c.MaxReassignmentMarks++ },
		func(c *StateCommandContract) { c.MaxWorkerSlots++ },
		func(c *StateCommandContract) { c.MaxCommandBytes++ },
		func(c *StateCommandContract) { c.MaxTopologyBytes++ },
		func(c *StateCommandContract) { c.MaxResultBytes++ },
		func(c *StateCommandContract) { c.MinResultRecordBytes++ },
		func(c *StateCommandContract) { c.MaxResultRecordBytes++ },
		func(c *StateCommandContract) { c.MaxManifestRecords++ },
		func(c *StateCommandContract) { c.FixedEnvelopeBytes++ },
		func(c *StateCommandContract) { c.ClientEnvelopeBytes++ },
		func(c *StateCommandContract) { c.InternalEnvelopeBytes++ },
		func(c *StateCommandContract) { c.SubjectKeyBytes++ },
		func(c *StateCommandContract) { c.BeginTargetBytes++ },
		func(c *StateCommandContract) { c.CommandResultBytes++ },
		func(c *StateCommandContract) { c.SnapshotBaseBytes++ },
		func(c *StateCommandContract) { c.ClientHistoryFixedBytes++ },
		func(c *StateCommandContract) { c.SubjectHistoryFixedBytes++ },
		func(c *StateCommandContract) { c.WorkerRecordBytes++ },
		func(c *StateCommandContract) { c.JobRecordFixedBytes++ },
		func(c *StateCommandContract) { c.AssignmentTokenBytes++ },
		func(c *StateCommandContract) { c.ResultReplicaBytes++ },
		func(c *StateCommandContract) { c.ReassignmentBytes++ },
		func(c *StateCommandContract) { c.SourceEOFEntryBytes++ },
		func(c *StateCommandContract) { c.CheckpointEntryBytes++ },
		func(c *StateCommandContract) { c.ManifestEntryBytes++ },
		func(c *StateCommandContract) { c.FailureBytes++ },
		func(c *StateCommandContract) { c.WorkerEventBytes++ },
		func(c *StateCommandContract) { c.Rules[0] += "x" },
	}
	for i, mutate := range mutations {
		candidate := StateCommandContractV1()
		mutate(&candidate)
		if reflect.DeepEqual(canonicalStateCommandContractBytes(candidate), want) {
			t.Fatalf("mutation %d did not change canonical contract bytes", i)
		}
	}
}

func TestSnapshotCompatibilityFingerprintSensitivityByCategory(t *testing.T) {
	baseline := canonicalStateCommandContractBytes(StateCommandContractV1())
	tests := map[string]func(*StateCommandContract){
		"sort comparator": func(contract *StateCommandContract) {
			contract.SnapshotSortRules[0] += ":changed"
		},
		"migration policy": func(contract *StateCommandContract) {
			contract.SnapshotMigrationRules[0] += ":changed"
		},
		"validation and error policy": func(contract *StateCommandContract) {
			contract.SnapshotValidationRules[0] += ":changed"
		},
		"estimator constant name": func(contract *StateCommandContract) {
			contract.SnapshotEstimatorConstants[0].Name += ":changed"
		},
		"estimator constant value": func(contract *StateCommandContract) {
			contract.SnapshotEstimatorConstants[0].Value++
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := StateCommandContractV1()
			mutate(&candidate)
			if reflect.DeepEqual(canonicalStateCommandContractBytes(candidate), baseline) {
				t.Fatal("compatibility category did not affect consensus fingerprint input")
			}
		})
	}
}

func TestStateCommandContractPinsWorkerAndJobCommandCompatibility(t *testing.T) {
	contract := StateCommandContractV1()
	if contract.MaxWorkers != 1024 || contract.MaxActiveJobs != 64 || contract.MaxRetainedJobs != 256 {
		t.Fatalf("worker/job bounds = %d/%d/%d", contract.MaxWorkers, contract.MaxActiveJobs, contract.MaxRetainedJobs)
	}
	wantKinds := []string{
		"BeginCoordinatorEpoch=1", "RegisterWorker=2", "DrainWorker=3", "DeactivateWorker=4",
		"ReplaceWorkerEpoch=5", "SubmitJob=6", "CancelJob=7",
	}
	if !reflect.DeepEqual(contract.EnumDomains[1].Values[:len(wantKinds)], wantKinds) {
		t.Fatalf("worker/job command kinds = %#v, want prefix %#v", contract.EnumDomains[1].Values, wantKinds)
	}
	wantLayouts := []string{"RegisterWorker", "DrainWorker", "DeactivateWorker", "ReplaceWorkerEpoch", "SubmitJob", "CancelJob", "WorkerRecord", "AffectedAssignment"}
	for _, name := range wantLayouts {
		found := false
		for _, layout := range contract.EnvelopeLayouts {
			found = found || layout.Name == name
		}
		if !found {
			t.Fatalf("state command contract omits %s", name)
		}
	}
}

func TestStateCommandContractPinsEveryTask10CommandAndNestedRecord(t *testing.T) {
	contract := StateCommandContractV1()
	wantKinds := []string{
		"BeginCoordinatorEpoch=1", "RegisterWorker=2", "DrainWorker=3", "DeactivateWorker=4",
		"ReplaceWorkerEpoch=5", "SubmitJob=6", "CancelJob=7", "RecordSourceEOF=8",
		"InstallAssignments=9", "ReplaceAssignments=10", "AdvanceCheckpoint=11",
		"SealManifest=12", "TransitionJob=13", "FailJob=14",
	}
	if !reflect.DeepEqual(contract.EnumDomains[1].Values, wantKinds) {
		t.Fatalf("Task 10 command domain = %#v, want %#v", contract.EnumDomains[1].Values, wantKinds)
	}
	wantLayouts := []string{
		"RecordSourceEOF", "InstallAssignments", "ReplaceAssignments", "AdvanceCheckpoint",
		"SealManifest", "TransitionJob", "FailJob", "TaskID", "CoordinatorEpoch",
		"AssignmentToken", "ResultReplicaSet", "AssignmentSet", "AssignmentSetDigest", "NeedsReassignment",
		"CompletionReport", "JobFailureReport", "ResultManifest", "SourceEOFRecord",
		"CheckpointRecord", "WorkerEventCursor", "JobRecord",
	}
	seen := make(map[string]bool)
	for _, layout := range contract.EnvelopeLayouts {
		seen[layout.Name] = true
	}
	for _, name := range wantLayouts {
		if !seen[name] {
			t.Fatalf("state command contract omits %s", name)
		}
	}
	wantDomains := []string{
		"cs425/crane/internal-command/v1", "cs425/crane/needs-reassignment/v1",
		"cs425/crane/completion-report/v1", "cs425/crane/job-failure-event/v1", AssignmentSetDigestDomainV1,
	}
	if !reflect.DeepEqual(contract.DigestDomains, wantDomains) {
		t.Fatalf("Task 10 digest domains = %#v, want %#v", contract.DigestDomains, wantDomains)
	}
}
