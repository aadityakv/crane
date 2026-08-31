package model

import (
	"encoding/hex"
	"reflect"
	"testing"
)

func TestLimitsV1PinsEveryConsensusBound(t *testing.T) {
	want := ConsensusLimits{
		MaxRegisteredWorkers:           1024,
		MaxActiveJobs:                  64,
		MaxRetainedJobs:                256,
		MaxRetainedClientSessions:      1024,
		MaxResultManifestsPerJob:       256,
		MaxCachedCommandResultBytes:    64 << 10,
		MaxResultRecordsBytesPerJob:    64 << 20,
		MaxSnapshotBytes:               8 << 20,
		MaxIdentifierBytes:             64,
		MaxStages:                      64,
		MaxEdges:                       256,
		MaxTasksPerStage:               256,
		MaxTasksPerJob:                 1024,
		MaxWorkerSlots:                 256,
		MaxSettingsPerStage:            32,
		MaxSettingKeyBytes:             64,
		MaxSettingValueBytes:           1024,
		MaxTotalSettingsBytes:          64 << 10,
		MaxSourceSequences:             1_000_000,
		MaxOperatorOutputs:             16,
		MaxDerivedDeliveries:           4096,
		MaxTupleFields:                 64,
		MaxTupleFieldPayloadBytes:      512,
		MaxCanonicalTupleBytes:         37_186,
		CustodyInboxFixedBytes:         379,
		CustodyOutboxFixedBytes:        293,
		ResultCopyFixedBytes:           293,
		MaxCustodyReservationBytes:     614_445_757,
		MaxSubmitJobBytes:              1 << 20,
		MaxControlFrameBytes:           1 << 20,
		MaxWorkerControlFrameBytes:     1 << 20,
		AuthenticatedFrameBytes:        87,
		SubmitJobFixedBytes:            93,
		SubmitRequestFixedBytes:        145,
		AssignmentSetInstallFixedBytes: 102_624,
		MaxTopologyBytes:               118_784,
	}
	if got := LimitsV1(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LimitsV1() = %#v, want %#v", got, want)
	}
}

func TestRegistryV1ReturnsSortedOwnedExactBuiltins(t *testing.T) {
	want := Registry{Operators: []OperatorDescriptor{
		{Name: "collect", Version: 1, Role: OperatorRoleSink, MinInputs: 1, MaxInputs: 1, ImplementationFingerprint: mustFingerprint(t, "a5b0b7f51389a3ff7680d1f01315d0310d6fcd2e94ca7287c71631d21113aefb")},
		{Name: "even", Version: 1, Role: OperatorRoleTransform, MinInputs: 1, MaxInputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "1a50bbd7e4997bd656979b54fe6bdf548867e962d778413671c6c41632ca0b1d")},
		{Name: "less_than", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "threshold", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "71f922b0a90501450c91bb24fc3c774ae0de4ef61b6aa665048cebb5d9c06d8d")},
		{Name: "multiply", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "factor", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "84c086f1eaa2f2bb39659cf49c7fbfaec42def79abd21376917d401884f09fc2")},
		{Name: "range", Version: 1, Role: OperatorRoleSource, Settings: []SettingDescriptor{{Name: "end_exclusive", Type: SettingTypeInt64, Required: true}, {Name: "start", Type: SettingTypeInt64, Required: true}}, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "ec92810e95284f19de73d1002a6035fcc2e48e515d7460dc2ff7654036075a5e")},
	}}
	got := RegistryV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RegistryV1() = %#v, want %#v", got, want)
	}
	got.Operators[0].Name = "mutated"
	if again := RegistryV1(); again.Operators[0].Name != "collect" {
		t.Fatalf("RegistryV1 shared mutable storage: %#v", again)
	}
}

func TestV1FingerprintsMatchIndependentGoldens(t *testing.T) {
	registry := RegistryFingerprint()
	if got := hex.EncodeToString(registry[:]); got != "56b222c3476fa78b244396eb8c12a74b1d6f4cfa1ab0b8cf7655d76cfb81d6d0" {
		t.Fatalf("RegistryFingerprint() = %s", got)
	}
	if got := ConsensusFingerprintHex(); got != "bad6fc963d63ba2a021ca91de6bb3960cfa1de84350bfc399676209c6df32b21" {
		t.Fatalf("ConsensusFingerprintHex() = %s", got)
	}
	consensus := ConsensusFingerprint()
	if got := hex.EncodeToString(consensus[:]); got != "bad6fc963d63ba2a021ca91de6bb3960cfa1de84350bfc399676209c6df32b21" {
		t.Fatalf("ConsensusFingerprint() = %s", got)
	}
}

func mustFingerprint(t *testing.T, encoded string) [32]byte {
	t.Helper()
	bytes, err := hex.DecodeString(encoded)
	if err != nil || len(bytes) != 32 {
		t.Fatalf("decode fingerprint %q: %v", encoded, err)
	}
	var result [32]byte
	copy(result[:], bytes)
	return result
}
