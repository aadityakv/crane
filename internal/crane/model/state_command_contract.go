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

// StateCommandContract is the dependency-leaf consensus contract for command
// envelopes, results, deduplication, and coordinator fencing.
type StateCommandContract struct {
	SchemaVersion uint16

	EnvelopeLayouts []StateCommandLayoutDescriptor
	EnumDomains     []StateCommandEnumDescriptor
	DigestDomains   []string

	MaxClientSessions    uint64
	MaxSubjectHistories  uint64
	MaxCachedResultBytes uint64
	MaxSnapshotBytes     uint64

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
	{Name: "CommandResult", Fields: []string{"SchemaVersion:u16", "Code:u16", "Subject:u8", "Revision:u64", "JobID:JobID", "WorkerID:u16", "Epoch:CoordinatorEpoch"}},
}

var stateCommandEnumsV1 = []StateCommandEnumDescriptor{
	{Name: "IdentitySelector", Values: []string{"Client=1", "Internal=2"}},
	{Name: "CommandKind", Values: []string{"BeginCoordinatorEpoch=1"}},
	{Name: "SubjectKind", Values: []string{"None=0", "Coordinator=1", "Worker=2", "JobControl=3", "SourceEOF=4", "SourceCheckpoint=5", "ResultManifest=6"}},
	{Name: "ResultCode", Values: []string{"Success=1", "IdentityReuse=2", "StaleRequest=3", "SkippedRequest=4", "CapacityExhausted=5", "RevisionMismatch=6", "StaleEpoch=7", "ResultTooLarge=8"}},
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
}

// StateCommandContractV1 returns deep-owned consensus descriptor slices.
func StateCommandContractV1() StateCommandContract {
	return StateCommandContract{
		SchemaVersion:            StateCommandSchemaVersionV1,
		EnvelopeLayouts:          cloneStateCommandLayouts(stateCommandLayoutsV1),
		EnumDomains:              cloneStateCommandEnums(stateCommandEnumsV1),
		DigestDomains:            []string{"cs425/crane/internal-command/v1"},
		MaxClientSessions:        StateCommandMaxClientSessionsV1,
		MaxSubjectHistories:      StateCommandMaxSubjectHistoriesV1,
		MaxCachedResultBytes:     StateCommandMaxCachedResultBytesV1,
		MaxSnapshotBytes:         StateCommandMaxSnapshotBytesV1,
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
	encoded = appendStringList(encoded, contract.DigestDomains)
	for _, value := range []uint64{
		contract.MaxClientSessions, contract.MaxSubjectHistories,
		contract.MaxCachedResultBytes, contract.MaxSnapshotBytes,
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
