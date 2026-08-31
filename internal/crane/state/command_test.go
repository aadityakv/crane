package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	const wantHex = "000154a1513845640fbb39b7c687ed31a68e3652906937d477d56564daca00038cd50001021100000000000000000000000000000000000000000000000000000000000000e4a06a3814b9c809163327ac5929ec01b9d76cd35489ff3f8ed45618f0a5ded50100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000231000000000000000000000000000000"
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
	for _, domain := range contract.EnumDomains {
		if domain.Name == "SubjectKind" {
			subjectValues = domain.Values
			break
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
