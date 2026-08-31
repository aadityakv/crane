package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

func TestCommandBeginCoordinatorEpochCanonicalGoldenAndRoundTrip(t *testing.T) {
	command := validBeginCommand(t, 0, 2, 0x31)
	encoded, err := MarshalBeginCoordinatorEpoch(command)
	if err != nil {
		t.Fatalf("MarshalBeginCoordinatorEpoch: %v", err)
	}
	const wantHex = "000181b9af9cd06949835c593837159e075c35aa017d490ecfe083df264f3c3f54c900010211000000000000000000000000000000000000000000000000000000000000003315919b26dc7d134a630c944562e32fbd5854a92f111479a28269b7b5fb92f00100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000231000000000000000000000000000000"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("golden = %s, want %s", got, wantHex)
	}
	decoded, err := UnmarshalBeginCoordinatorEpoch(encoded)
	if err != nil {
		t.Fatalf("UnmarshalBeginCoordinatorEpoch: %v", err)
	}
	if !reflect.DeepEqual(decoded, command) {
		t.Fatalf("round trip = %#v, want %#v", decoded, command)
	}
}

func TestCommandConstructorBindsCompiledFingerprintAndCompleteDigest(t *testing.T) {
	command, err := NewBeginCoordinatorEpoch(InternalCommandID{1}, 4, 3, [16]byte{2})
	if err != nil {
		t.Fatalf("NewBeginCoordinatorEpoch: %v", err)
	}
	if command.Envelope.ConsensusFingerprint != model.ConsensusFingerprint() || command.Envelope.Internal.Digest != independentInternalDigest(command.Envelope, beginTargetForTest(command)) {
		t.Fatalf("constructor did not bind complete command: %#v", command)
	}
	if _, err := NewBeginCoordinatorEpoch(InternalCommandID{}, 0, 3, [16]byte{2}); err == nil {
		t.Fatal("constructor accepted zero internal identity")
	}
}

func TestCommandDigestBindsEveryCanonicalDefiningByte(t *testing.T) {
	encoded, err := MarshalBeginCoordinatorEpoch(validBeginCommand(t, 7, 2, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	for offset := range encoded {
		mutated := append([]byte(nil), encoded...)
		mutated[offset] ^= 0x80
		if _, err := UnmarshalBeginCoordinatorEpoch(mutated); err == nil {
			t.Fatalf("decoder accepted mutation at byte %d", offset)
		}
	}
}

func TestCommandDecoderRejectsEveryTruncationAndTrailingBytes(t *testing.T) {
	encoded, err := MarshalBeginCoordinatorEpoch(validBeginCommand(t, 0, 2, 0x51))
	if err != nil {
		t.Fatal(err)
	}
	for length := 0; length < len(encoded); length++ {
		if _, err := UnmarshalBeginCoordinatorEpoch(encoded[:length]); err == nil {
			t.Fatalf("decoder accepted truncation at %d", length)
		}
	}
	if _, err := UnmarshalBeginCoordinatorEpoch(append(encoded, 0)); err == nil {
		t.Fatal("decoder accepted trailing byte")
	}
}

func TestCommandRejectsUnknownZeroMismatchedAndNoncanonicalValues(t *testing.T) {
	valid := validBeginCommand(t, 0, 2, 0x61)
	tests := []struct {
		name   string
		mutate func(*BeginCoordinatorEpoch)
	}{
		{name: "schema", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.SchemaVersion++ }},
		{name: "fingerprint", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.ConsensusFingerprint[0] ^= 1 }},
		{name: "kind_zero", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.Kind = 0 }},
		{name: "kind_unknown", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.Kind = 99 }},
		{name: "client_identity", mutate: func(c *BeginCoordinatorEpoch) {
			c.Envelope.Client = &ClientEnvelope{Request: model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}, Digest: [32]byte{1}}
			c.Envelope.Internal = nil
		}},
		{name: "both_identities", mutate: func(c *BeginCoordinatorEpoch) {
			c.Envelope.Client = &ClientEnvelope{Request: model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}, Digest: [32]byte{1}}
		}},
		{name: "neither_identity", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.Internal = nil }},
		{name: "zero_internal_id", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.Internal.ID = InternalCommandID{} }},
		{name: "zero_digest", mutate: func(c *BeginCoordinatorEpoch) { c.Envelope.Internal.Digest = [32]byte{} }},
		{name: "wrong_subject", mutate: func(c *BeginCoordinatorEpoch) {
			c.Envelope.Internal.Subject = SubjectKey{Kind: SubjectWorker, WorkerID: 1}
		}},
		{name: "zero_coordinator", mutate: func(c *BeginCoordinatorEpoch) { c.Coordinator = 0 }},
		{name: "zero_nonce", mutate: func(c *BeginCoordinatorEpoch) { c.Nonce = [16]byte{} }},
		{name: "digest_mismatch", mutate: func(c *BeginCoordinatorEpoch) { c.Coordinator++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneBegin(valid)
			test.mutate(&candidate)
			if _, err := MarshalBeginCoordinatorEpoch(candidate); err == nil {
				t.Fatal("marshal accepted invalid command")
			}
		})
	}
}

func TestCommandValidationPreservesTopLevelAndUnderlyingSentinels(t *testing.T) {
	valid := validBeginCommand(t, 0, 2, 0x62)
	marshalCases := []struct {
		name     string
		sentinel error
		mutate   func(*BeginCoordinatorEpoch)
	}{
		{name: "schema", sentinel: ErrUnsupportedCommandSchema, mutate: func(command *BeginCoordinatorEpoch) { command.Envelope.SchemaVersion++ }},
		{name: "fingerprint", sentinel: ErrConsensusFingerprintMismatch, mutate: func(command *BeginCoordinatorEpoch) { command.Envelope.ConsensusFingerprint[0] ^= 1 }},
		{name: "kind", sentinel: ErrUnknownCommandKind, mutate: func(command *BeginCoordinatorEpoch) { command.Envelope.Kind = 99 }},
		{name: "digest", sentinel: ErrCommandDigestMismatch, mutate: func(command *BeginCoordinatorEpoch) { command.Envelope.Internal.Digest[0] ^= 1 }},
		{name: "subject", sentinel: ErrInvalidCommandSubject, mutate: func(command *BeginCoordinatorEpoch) {
			command.Envelope.Internal.Subject = SubjectKey{Kind: SubjectWorker}
		}},
	}
	for _, test := range marshalCases {
		t.Run("marshal/"+test.name, func(t *testing.T) {
			candidate := cloneBegin(valid)
			test.mutate(&candidate)
			_, err := MarshalBeginCoordinatorEpoch(candidate)
			if !errors.Is(err, ErrInvalidCommand) || !errors.Is(err, test.sentinel) {
				t.Fatalf("Marshal error = %v, want ErrInvalidCommand and %v", err, test.sentinel)
			}
		})
	}

	encoded, err := MarshalBeginCoordinatorEpoch(valid)
	if err != nil {
		t.Fatal(err)
	}
	// These offsets are independently pinned by the command golden: schema 0,
	// fingerprint 2, kind 34, digest 69, and subject kind 101.
	unmarshalCases := []struct {
		name     string
		offset   int
		value    byte
		sentinel error
	}{
		{name: "schema", offset: 1, value: 2, sentinel: ErrUnsupportedCommandSchema},
		{name: "fingerprint", offset: 2, value: encoded[2] ^ 1, sentinel: ErrConsensusFingerprintMismatch},
		{name: "kind", offset: 35, value: 99, sentinel: ErrUnknownCommandKind},
		{name: "digest", offset: 69, value: encoded[69] ^ 1, sentinel: ErrCommandDigestMismatch},
		{name: "subject", offset: 101, value: 0, sentinel: ErrInvalidCommandSubject},
	}
	for _, test := range unmarshalCases {
		t.Run("unmarshal/"+test.name, func(t *testing.T) {
			candidate := append([]byte(nil), encoded...)
			candidate[test.offset] = test.value
			_, err := UnmarshalBeginCoordinatorEpoch(candidate)
			if !errors.Is(err, ErrInvalidCommand) || !errors.Is(err, test.sentinel) {
				t.Fatalf("Unmarshal error = %v, want ErrInvalidCommand and %v", err, test.sentinel)
			}
		})
	}
}

func TestCommandSubjectKeysValidateCanonicalUnionAndIndependence(t *testing.T) {
	jobA := model.JobID{1}
	jobB := model.JobID{2}
	taskA := model.TaskID{JobID: jobA, StageID: 1, Partition: 0}
	taskB := model.TaskID{JobID: jobA, StageID: 1, Partition: 1}
	valid := []SubjectKey{
		{Kind: SubjectCoordinator},
		{Kind: SubjectWorker, WorkerID: 7},
		{Kind: SubjectJobControl, JobID: jobA},
		{Kind: SubjectSourceEOF, JobID: jobA, TaskID: taskA},
		{Kind: SubjectSourceCheckpoint, JobID: jobA, TaskID: taskB},
		{Kind: SubjectResultManifest, JobID: jobB, TaskID: model.TaskID{JobID: jobB, StageID: 9, Partition: 3}},
	}
	seen := map[SubjectKey]struct{}{}
	for _, key := range valid {
		if err := key.Validate(); err != nil {
			t.Fatalf("Validate(%#v): %v", key, err)
		}
		seen[key] = struct{}{}
	}
	if len(seen) != len(valid) {
		t.Fatal("independent authoritative keys collided")
	}
	invalid := []SubjectKey{
		{}, {Kind: 99},
		{Kind: SubjectCoordinator, WorkerID: 1},
		{Kind: SubjectWorker}, {Kind: SubjectWorker, WorkerID: 1, JobID: jobA},
		{Kind: SubjectJobControl}, {Kind: SubjectJobControl, JobID: jobA, TaskID: taskA},
		{Kind: SubjectSourceEOF, JobID: jobA},
		{Kind: SubjectSourceEOF, JobID: jobA, TaskID: model.TaskID{JobID: jobB, StageID: 1}},
		{Kind: SubjectSourceCheckpoint, JobID: jobA, TaskID: taskA, WorkerID: 1},
		{Kind: SubjectResultManifest, JobID: jobA, TaskID: model.TaskID{JobID: jobA}},
	}
	for _, key := range invalid {
		if err := key.Validate(); err == nil {
			t.Fatalf("Validate accepted %#v", key)
		}
	}
}

func TestCommandResultCanonicalGoldenBoundsAndOwnership(t *testing.T) {
	result := CommandResult{Code: ResultSuccess, Subject: SubjectCoordinator, Revision: 3, Epoch: model.CoordinatorEpoch{Term: 2, BeginIndex: 9, Coordinator: 4, Nonce: [16]byte{0xaa}}}
	encoded, err := MarshalCommandResult(result)
	if err != nil {
		t.Fatal(err)
	}
	const wantHex = "00010001010000000000000003000000000000000000000000000000000000000000000000000200000000000000090004aa000000000000000000000000000000"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("golden = %s, want %s", got, wantHex)
	}
	decoded, err := UnmarshalCommandResult(encoded)
	if err != nil || decoded != result {
		t.Fatalf("roundtrip = %#v, %v", decoded, err)
	}
	encoded[0] ^= 1
	again, err := MarshalCommandResult(result)
	if err != nil || bytes.Equal(encoded, again) {
		t.Fatal("result encoder returned aliased storage")
	}
	for length := 0; length < len(again); length++ {
		if _, err := UnmarshalCommandResult(again[:length]); err == nil {
			t.Fatalf("accepted result truncation %d", length)
		}
	}
	if _, err := UnmarshalCommandResult(append(again, 0)); err == nil {
		t.Fatal("accepted trailing result byte")
	}
}

func TestCommandResultRejectsUnknownAndNoncanonicalUnionValues(t *testing.T) {
	tests := []CommandResult{
		{}, {Code: 99},
		{Code: ResultSuccess, Subject: SubjectCoordinator, Revision: 1},
		{Code: ResultSuccess, Subject: SubjectWorker, Revision: 1},
		{Code: ResultIdentityReuse, Subject: SubjectCoordinator, WorkerID: 1},
		{Code: ResultStaleRequest, Subject: SubjectNone, Revision: 1},
		{Code: ResultRevisionMismatch, Subject: SubjectJobControl},
	}
	for _, result := range tests {
		if _, err := MarshalCommandResult(result); err == nil {
			t.Fatalf("accepted %#v", result)
		}
	}
}

func TestCommandResultValidationPreservesMalformedAndSubjectSentinels(t *testing.T) {
	result := validResultForMatrix(ResultSuccess, SubjectWorker, 1)
	result.WorkerID = 0
	if _, err := MarshalCommandResult(result); !errors.Is(err, ErrMalformedCommandResult) || !errors.Is(err, ErrInvalidCommandSubject) {
		t.Fatalf("MarshalCommandResult error = %v, want malformed-result and subject sentinels", err)
	}

	valid := validResultForMatrix(ResultSuccess, SubjectWorker, 1)
	encoded, err := MarshalCommandResult(valid)
	if err != nil {
		t.Fatal(err)
	}
	encoded[30] = 0 // worker ID occupies independently pinned bytes 29..30.
	if _, err := UnmarshalCommandResult(encoded); !errors.Is(err, ErrMalformedCommandResult) || !errors.Is(err, ErrInvalidCommandSubject) {
		t.Fatalf("UnmarshalCommandResult error = %v, want malformed-result and subject sentinels", err)
	}
}

func TestCommandResultExhaustiveLegalMatrixAndCorrelationFields(t *testing.T) {
	legal := map[ResultCode]map[SubjectKind]bool{
		ResultSuccess: {
			SubjectCoordinator: true, SubjectWorker: true, SubjectJobControl: true,
			SubjectSourceEOF: true, SubjectSourceCheckpoint: true, SubjectResultManifest: true,
		},
		ResultIdentityReuse: {
			SubjectNone: true, SubjectCoordinator: true, SubjectWorker: true, SubjectJobControl: true,
			SubjectSourceEOF: true, SubjectSourceCheckpoint: true, SubjectResultManifest: true,
		},
		ResultStaleRequest:   {SubjectNone: true},
		ResultSkippedRequest: {SubjectNone: true},
		ResultCapacityExhausted: {
			SubjectNone: true, SubjectCoordinator: true, SubjectWorker: true, SubjectJobControl: true,
			SubjectSourceEOF: true, SubjectSourceCheckpoint: true, SubjectResultManifest: true,
		},
		ResultRevisionMismatch: {
			SubjectCoordinator: true, SubjectWorker: true, SubjectJobControl: true,
			SubjectSourceEOF: true, SubjectSourceCheckpoint: true, SubjectResultManifest: true,
		},
		ResultStaleEpoch: {SubjectCoordinator: true},
		ResultResultTooLarge: {
			SubjectNone: true, SubjectCoordinator: true, SubjectWorker: true, SubjectJobControl: true,
			SubjectSourceEOF: true, SubjectSourceCheckpoint: true, SubjectResultManifest: true,
		},
	}

	for code := ResultSuccess; code <= ResultResultTooLarge; code++ {
		for subject := SubjectNone; subject <= SubjectResultManifest; subject++ {
			name := fmt.Sprintf("code_%d/subject_%d", code, subject)
			t.Run(name, func(t *testing.T) {
				revision := uint64(0)
				if code == ResultSuccess || code == ResultStaleEpoch {
					revision = 1
				}
				result := validResultForMatrix(code, subject, revision)
				_, err := MarshalCommandResult(result)
				if legal[code][subject] && err != nil {
					t.Fatalf("legal result rejected: %#v: %v", result, err)
				}
				if !legal[code][subject] && err == nil {
					t.Fatalf("illegal code/subject pair accepted: %#v", result)
				}
			})
		}
	}

	for code, subjects := range legal {
		for subject := range subjects {
			if code == ResultSuccess || code == ResultStaleEpoch {
				candidate := validResultForMatrix(code, subject, 1)
				candidate.Revision = 0
				candidate.Epoch = model.CoordinatorEpoch{}
				if _, err := MarshalCommandResult(candidate); err == nil {
					t.Fatalf("code %d subject %d accepted forbidden zero revision: %#v", code, subject, candidate)
				}
			}
			if subject != SubjectNone && code != ResultSuccess && code != ResultStaleEpoch {
				if _, err := MarshalCommandResult(validResultForMatrix(code, subject, 1)); err != nil {
					t.Fatalf("code %d subject %d rejected legal nonzero current revision: %v", code, subject, err)
				}
			}

			valid := validResultForMatrix(code, subject, func() uint64 {
				if code == ResultSuccess || code == ResultStaleEpoch {
					return 1
				}
				return 0
			}())
			mutations := invalidCorrelationMutations(subject, valid.Revision)
			for name, mutate := range mutations {
				candidate := valid
				mutate(&candidate)
				if _, err := MarshalCommandResult(candidate); err == nil {
					t.Fatalf("code %d subject %d accepted invalid %s correlation: %#v", code, subject, name, candidate)
				}
			}
		}
	}
}

func TestCommandContractDescriptorMechanicallyMatchesCodecConstantsAndEnums(t *testing.T) {
	contract := model.StateCommandContractV1()
	if CommandSchemaVersion != contract.SchemaVersion || commandFixedEnvelopeBytes != contract.FixedEnvelopeBytes || clientEnvelopeBytes != contract.ClientEnvelopeBytes || internalEnvelopeBytes != contract.InternalEnvelopeBytes || subjectKeyBytes != contract.SubjectKeyBytes || beginTargetBytes != contract.BeginTargetBytes || commandResultBytes != contract.CommandResultBytes {
		t.Fatalf("state codec constants drifted from fingerprinted contract: %#v", contract)
	}
	if estimatedSnapshotBaseBytes != contract.SnapshotBaseBytes || clientHistoryFixedBytes != contract.ClientHistoryFixedBytes || subjectHistoryFixedBytes != contract.SubjectHistoryFixedBytes {
		t.Fatalf("state preflight constants drifted from fingerprinted contract: %#v", contract)
	}
	if CommandBeginCoordinatorEpoch != 1 || SubjectNone != 0 || SubjectCoordinator != 1 || SubjectResultManifest != 6 || ResultSuccess != 1 || ResultResultTooLarge != 8 {
		t.Fatal("state enums drifted from fingerprinted contract")
	}
	var subjectValues []string
	var resultValues []string
	for _, domain := range contract.EnumDomains {
		switch domain.Name {
		case "SubjectKind":
			subjectValues = domain.Values
		case "ResultCode":
			resultValues = domain.Values
		}
	}
	wantSubjectValues := []string{
		fmt.Sprintf("None=%d", SubjectNone),
		fmt.Sprintf("Coordinator=%d", SubjectCoordinator),
		fmt.Sprintf("Worker=%d", SubjectWorker),
		fmt.Sprintf("JobControl=%d", SubjectJobControl),
		fmt.Sprintf("SourceEOF=%d", SubjectSourceEOF),
		fmt.Sprintf("SourceCheckpoint=%d", SubjectSourceCheckpoint),
		fmt.Sprintf("ResultManifest=%d", SubjectResultManifest),
	}
	if !reflect.DeepEqual(subjectValues, wantSubjectValues) {
		t.Fatalf("fingerprinted SubjectKind domain = %v, want actual complete domain %v", subjectValues, wantSubjectValues)
	}
	wantResultValues := []string{
		fmt.Sprintf("Success=%d", ResultSuccess),
		fmt.Sprintf("IdentityReuse=%d", ResultIdentityReuse),
		fmt.Sprintf("StaleRequest=%d", ResultStaleRequest),
		fmt.Sprintf("SkippedRequest=%d", ResultSkippedRequest),
		fmt.Sprintf("CapacityExhausted=%d", ResultCapacityExhausted),
		fmt.Sprintf("RevisionMismatch=%d", ResultRevisionMismatch),
		fmt.Sprintf("StaleEpoch=%d", ResultStaleEpoch),
		fmt.Sprintf("ResultTooLarge=%d", ResultResultTooLarge),
	}
	if !reflect.DeepEqual(resultValues, wantResultValues) {
		t.Fatalf("fingerprinted ResultCode domain = %v, want actual complete domain %v", resultValues, wantResultValues)
	}
}

func validResultForMatrix(code ResultCode, subject SubjectKind, revision uint64) CommandResult {
	result := CommandResult{Code: code, Subject: subject, Revision: revision}
	switch subject {
	case SubjectCoordinator:
		if revision != 0 {
			result.Epoch = model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
		}
	case SubjectWorker:
		result.WorkerID = 1
	case SubjectJobControl, SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest:
		result.JobID = model.JobID{1}
	}
	return result
}

func invalidCorrelationMutations(subject SubjectKind, revision uint64) map[string]func(*CommandResult) {
	validEpoch := model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}
	switch subject {
	case SubjectNone:
		return map[string]func(*CommandResult){
			"revision": func(result *CommandResult) { result.Revision = 1 },
			"job":      func(result *CommandResult) { result.JobID = model.JobID{1} },
			"worker":   func(result *CommandResult) { result.WorkerID = 1 },
			"epoch":    func(result *CommandResult) { result.Epoch = validEpoch },
		}
	case SubjectCoordinator:
		mutations := map[string]func(*CommandResult){
			"job":    func(result *CommandResult) { result.JobID = model.JobID{1} },
			"worker": func(result *CommandResult) { result.WorkerID = 1 },
		}
		if revision == 0 {
			mutations["epoch_at_zero_revision"] = func(result *CommandResult) { result.Epoch = validEpoch }
		} else {
			mutations["zero_epoch_at_nonzero_revision"] = func(result *CommandResult) { result.Epoch = model.CoordinatorEpoch{} }
		}
		return mutations
	case SubjectWorker:
		return map[string]func(*CommandResult){
			"zero_worker": func(result *CommandResult) { result.WorkerID = 0 },
			"job":         func(result *CommandResult) { result.JobID = model.JobID{1} },
			"epoch":       func(result *CommandResult) { result.Epoch = validEpoch },
		}
	default:
		return map[string]func(*CommandResult){
			"zero_job": func(result *CommandResult) { result.JobID = model.JobID{} },
			"worker":   func(result *CommandResult) { result.WorkerID = 1 },
			"epoch":    func(result *CommandResult) { result.Epoch = validEpoch },
		}
	}
}

func FuzzUnmarshalBeginCoordinatorEpoch(f *testing.F) {
	command := validBeginCommandNoTB(0, 2, 0x71)
	encoded, _ := MarshalBeginCoordinatorEpoch(command)
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = UnmarshalBeginCoordinatorEpoch(data) })
}

func FuzzUnmarshalCommandResult(f *testing.F) {
	encoded, _ := MarshalCommandResult(CommandResult{Code: ResultSuccess, Subject: SubjectCoordinator, Revision: 1, Epoch: model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}})
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = UnmarshalCommandResult(data) })
}

func validBeginCommand(t *testing.T, expected uint64, coordinator uint16, nonceByte byte) BeginCoordinatorEpoch {
	t.Helper()
	command := validBeginCommandNoTB(expected, coordinator, nonceByte)
	if err := command.Validate(); err != nil {
		t.Fatalf("valid command: %v", err)
	}
	return command
}

func validBeginCommandNoTB(expected uint64, coordinator uint16, nonceByte byte) BeginCoordinatorEpoch {
	nonce := [16]byte{nonceByte}
	command := BeginCoordinatorEpoch{
		Envelope:    Envelope{SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(), Kind: CommandBeginCoordinatorEpoch, Internal: &InternalEnvelope{ID: InternalCommandID{0x11}, Subject: SubjectKey{Kind: SubjectCoordinator}, ExpectedRevision: expected}},
		Coordinator: coordinator, Nonce: nonce,
	}
	command.Envelope.Internal.Digest = independentInternalDigest(command.Envelope, beginTargetForTest(command))
	return command
}

func independentInternalDigest(envelope Envelope, target []byte) [32]byte {
	encoded := append([]byte("cs425/crane/internal-command/v1\x00"), byte(envelope.SchemaVersion>>8), byte(envelope.SchemaVersion))
	encoded = append(encoded, envelope.ConsensusFingerprint[:]...)
	var fixed [8]byte
	binary.BigEndian.PutUint16(fixed[:2], uint16(envelope.Kind))
	encoded = append(encoded, fixed[:2]...)
	encoded = append(encoded, 2)
	internal := envelope.Internal
	encoded = append(encoded, internal.ID[:]...)
	encoded = appendSubjectForTest(encoded, internal.Subject)
	binary.BigEndian.PutUint64(fixed[:], internal.ExpectedRevision)
	encoded = append(encoded, fixed[:]...)
	encoded = append(encoded, target...)
	return sha256.Sum256(encoded)
}

func appendSubjectForTest(encoded []byte, subject SubjectKey) []byte {
	encoded = append(encoded, byte(subject.Kind))
	encoded = append(encoded, subject.JobID[:]...)
	encoded = append(encoded, subject.TaskID.JobID[:]...)
	var fixed [2]byte
	binary.BigEndian.PutUint16(fixed[:], subject.TaskID.StageID)
	encoded = append(encoded, fixed[:]...)
	binary.BigEndian.PutUint16(fixed[:], subject.TaskID.Partition)
	encoded = append(encoded, fixed[:]...)
	binary.BigEndian.PutUint16(fixed[:], subject.WorkerID)
	return append(encoded, fixed[:]...)
}

func beginTargetForTest(command BeginCoordinatorEpoch) []byte {
	target := make([]byte, 18)
	binary.BigEndian.PutUint16(target[:2], command.Coordinator)
	copy(target[2:], command.Nonce[:])
	return target
}

func cloneBegin(command BeginCoordinatorEpoch) BeginCoordinatorEpoch {
	copy := command
	if command.Envelope.Client != nil {
		client := *command.Envelope.Client
		copy.Envelope.Client = &client
	}
	if command.Envelope.Internal != nil {
		internal := *command.Envelope.Internal
		copy.Envelope.Internal = &internal
	}
	return copy
}
