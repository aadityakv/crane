package model

import "encoding/hex"

// OperatorRole identifies an operator's position in a topology.
type OperatorRole uint8

const (
	OperatorRoleSource OperatorRole = iota + 1
	OperatorRoleTransform
	OperatorRoleSink
)

// SettingType identifies the only setting value type accepted by v1 built-ins.
type SettingType uint8

const (
	SettingTypeInt64 SettingType = iota + 1
)

// SettingDescriptor describes one exact named operator setting.
type SettingDescriptor struct {
	Name     string
	Type     SettingType
	Required bool
}

// OperatorDescriptor is one immutable v1 operator compatibility record.
type OperatorDescriptor struct {
	Name                      string
	Version                   uint16
	Role                      OperatorRole
	Settings                  []SettingDescriptor
	MinInputs, MaxInputs      uint8
	MinOutputs, MaxOutputs    uint8
	ImplementationFingerprint [32]byte
}

// Registry is a sorted immutable-by-copy operator compatibility registry.
type Registry struct {
	Operators []OperatorDescriptor
}

var registryV1 = Registry{Operators: []OperatorDescriptor{
	{Name: "collect", Version: 1, Role: OperatorRoleSink, MinInputs: 1, MaxInputs: 1, ImplementationFingerprint: fingerprintLiteral("a5b0b7f51389a3ff7680d1f01315d0310d6fcd2e94ca7287c71631d21113aefb")},
	{Name: "even", Version: 1, Role: OperatorRoleTransform, MinInputs: 1, MaxInputs: 1, MaxOutputs: 1, ImplementationFingerprint: fingerprintLiteral("1a50bbd7e4997bd656979b54fe6bdf548867e962d778413671c6c41632ca0b1d")},
	{Name: "less_than", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "threshold", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MaxOutputs: 1, ImplementationFingerprint: fingerprintLiteral("71f922b0a90501450c91bb24fc3c774ae0de4ef61b6aa665048cebb5d9c06d8d")},
	{Name: "multiply", Version: 1, Role: OperatorRoleTransform, Settings: []SettingDescriptor{{Name: "factor", Type: SettingTypeInt64, Required: true}}, MinInputs: 1, MaxInputs: 1, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: fingerprintLiteral("84c086f1eaa2f2bb39659cf49c7fbfaec42def79abd21376917d401884f09fc2")},
	{Name: "range", Version: 1, Role: OperatorRoleSource, Settings: []SettingDescriptor{{Name: "end_exclusive", Type: SettingTypeInt64, Required: true}, {Name: "start", Type: SettingTypeInt64, Required: true}}, MinOutputs: 1, MaxOutputs: 1, ImplementationFingerprint: fingerprintLiteral("ec92810e95284f19de73d1002a6035fcc2e48e515d7460dc2ff7654036075a5e")},
}}

// RegistryV1 returns an owned copy of the sorted v1 operator registry.
func RegistryV1() Registry {
	registry := Registry{Operators: make([]OperatorDescriptor, len(registryV1.Operators))}
	copy(registry.Operators, registryV1.Operators)
	for index := range registry.Operators {
		registry.Operators[index].Settings = append([]SettingDescriptor(nil), registry.Operators[index].Settings...)
	}
	return registry
}

func fingerprintLiteral(encoded string) [32]byte {
	bytes, err := hex.DecodeString(encoded)
	if err != nil || len(bytes) != 32 {
		panic("invalid compiled Crane fingerprint")
	}
	var result [32]byte
	copy(result[:], bytes)
	return result
}
