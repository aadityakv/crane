package model

import (
	"reflect"
	"testing"
)

func TestStateCommandContractV1PinsCompleteReplicatedDedupContract(t *testing.T) {
	want := StateCommandContract{
		SchemaVersion: 1,
		EnvelopeLayouts: []StateCommandLayoutDescriptor{
			{Name: "Envelope", Fields: []string{"SchemaVersion:u16", "ConsensusFingerprint:sha256", "Kind:u16", "IdentitySelector:u8", "Identity:ClientEnvelope|InternalEnvelope", "Target:concrete-command-fields"}},
			{Name: "ClientEnvelope", Fields: []string{"ClientID:bytes16(nonzero)", "Sequence:u64(nonzero)", "Digest:sha256(nonzero)"}},
			{Name: "InternalEnvelope", Fields: []string{"ID:bytes32(nonzero)", "Digest:sha256(nonzero)", "Subject:SubjectKey", "ExpectedRevision:u64"}},
			{Name: "SubjectKey", Fields: []string{"Kind:u8", "JobID:JobID", "TaskID:TaskID", "WorkerID:u16"}},
			{Name: "BeginCoordinatorEpoch", Fields: []string{"Envelope:Envelope(internal)", "Coordinator:u16(nonzero)", "Nonce:bytes16(nonzero)"}},
			{Name: "CommandResult", Fields: []string{"SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"}},
		},
		EnumDomains: []StateCommandEnumDescriptor{
			{Name: "IdentitySelector", Values: []string{"Client=1", "Internal=2"}},
			{Name: "CommandKind", Values: []string{"BeginCoordinatorEpoch=1"}},
			{Name: "SubjectKind", Values: []string{"None=0", "Coordinator=1", "Worker=2", "JobControl=3", "SourceEOF=4", "SourceCheckpoint=5", "ResultManifest=6"}},
			{Name: "ResultCode", Values: []string{"Success=1", "IdentityReuse=2", "StaleRequest=3", "SkippedRequest=4", "CapacityExhausted=5", "RevisionMismatch=6", "StaleEpoch=7", "ResultTooLarge=8"}},
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
			{Code: 8, Subject: 0, Revision: StateCommandRevisionZero, Identity: StateCommandIdentityUnbound, Epoch: StateCommandEpochZero},
			{Code: 8, Subject: 1, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityCoordinator, Epoch: StateCommandEpochCoordinatorRevision},
			{Code: 8, Subject: 2, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityWorker, Epoch: StateCommandEpochZero},
			{Code: 8, Subject: 3, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
			{Code: 8, Subject: 4, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
			{Code: 8, Subject: 5, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
			{Code: 8, Subject: 6, Revision: StateCommandRevisionAny, Identity: StateCommandIdentityJob, Epoch: StateCommandEpochZero},
		},
		DigestDomains:     []string{"cs425/crane/internal-command/v1"},
		MaxClientSessions: 1024, MaxSubjectHistories: 197889,
		MaxCachedResultBytes: 65536, MaxSnapshotBytes: 8388608,
		FixedEnvelopeBytes: 37, ClientEnvelopeBytes: 56, InternalEnvelopeBytes: 111,
		SubjectKeyBytes: 39, BeginTargetBytes: 18, CommandResultBytes: 65,
		SnapshotBaseBytes: 128, ClientHistoryFixedBytes: 60, SubjectHistoryFixedBytes: 128,
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
		func(c *StateCommandContract) { c.EnvelopeLayouts[0].Name += "x" },
		func(c *StateCommandContract) { c.EnvelopeLayouts[0].Fields[0] += "x" },
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
		func(c *StateCommandContract) { c.FixedEnvelopeBytes++ },
		func(c *StateCommandContract) { c.ClientEnvelopeBytes++ },
		func(c *StateCommandContract) { c.InternalEnvelopeBytes++ },
		func(c *StateCommandContract) { c.SubjectKeyBytes++ },
		func(c *StateCommandContract) { c.BeginTargetBytes++ },
		func(c *StateCommandContract) { c.CommandResultBytes++ },
		func(c *StateCommandContract) { c.SnapshotBaseBytes++ },
		func(c *StateCommandContract) { c.ClientHistoryFixedBytes++ },
		func(c *StateCommandContract) { c.SubjectHistoryFixedBytes++ },
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
