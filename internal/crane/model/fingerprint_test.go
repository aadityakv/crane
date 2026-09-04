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
		MaxTuplePayloadBytes:           512,
		CustodyInboxFixedBytes:         379,
		CustodyOutboxFixedBytes:        293,
		ResultCopyFixedBytes:           293,
		MaxCustodyReservationBytes:     13_542_267,
		MaxSubmitJobBytes:              1 << 20,
		MaxControlFrameBytes:           1 << 20,
		MaxWorkerControlFrameBytes:     1 << 20,
		AuthenticatedFrameBytes:        87,
		SubmitJobFixedBytes:            127,
		SubmitRequestFixedBytes:        147,
		AssignmentSetInstallFixedBytes: 102_626,
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
		{Name: "lines", Version: 1, Role: OperatorRoleSource, Settings: []SettingDescriptor{{Name: "corpus", Type: SettingTypeString, Required: true}}, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "5a04d07b54b4c6ae858baa93e355fa58b762a13bf2c903766de51d3e95d74c74")},
		{Name: "min_length", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "length", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "70973e22c4b53c39593e9fa1d4892952b027ecc342c9806cbe172db8fd2d926f")},
		{Name: "multiply", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "factor", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "84c086f1eaa2f2bb39659cf49c7fbfaec42def79abd21376917d401884f09fc2")},
		{Name: "range", Version: 1, Role: OperatorRoleSource, Settings: []SettingDescriptor{{Name: "end_exclusive", Type: SettingTypeInt64, Required: true}, {Name: "start", Type: SettingTypeInt64, Required: true}}, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: mustFingerprint(t, "ec92810e95284f19de73d1002a6035fcc2e48e515d7460dc2ff7654036075a5e")},
		{Name: "split_words", Version: 1, Role: OperatorRoleTransform, MinInputs: 1, MaxInputs: 1, MaxOutputs: 16, ImplementationFingerprint: mustFingerprint(t, "51da21999c2b6c5af3e0bc9bf39fd8693596568033a5fc79d50da251cb758ab6")},
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
	if got := hex.EncodeToString(registry[:]); got != "4da1d0721840a88d3c487efefb2419b4691943012c3ddc581d6430f05ecbb997" {
		t.Fatalf("RegistryFingerprint() = %s", got)
	}
	if got := ConsensusFingerprintHex(); got != "80586b11b13c86be8ec0578a802c6d0f31b5126083e2fd4bcedbc3b8a8776950" {
		t.Fatalf("ConsensusFingerprintHex() = %s", got)
	}
	consensus := ConsensusFingerprint()
	if got := hex.EncodeToString(consensus[:]); got != "80586b11b13c86be8ec0578a802c6d0f31b5126083e2fd4bcedbc3b8a8776950" {
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
