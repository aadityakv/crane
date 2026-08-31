package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

const (
	registryFingerprintDomain     = "cs425/crane/operator-registry/v1\x00"
	consensusFingerprintDomain    = "cs425/crane/consensus/v1\x00"
	wireContractFingerprintDomain = "cs425/crane/wire-contract/v1\x00"
)

var identityDomainsV1 = []string{
	"cs425/crane/job/v1",
	"cs425/crane/source-tuple/v1",
	"cs425/crane/derived-tuple/v1",
	"cs425/crane/route/v1",
	"cs425/crane/rendezvous/v1",
	"cs425/crane/internal-command/v1",
	"cs425/crane/completion-report/v1",
}

// RegistryFingerprint returns the SHA-256 compatibility fingerprint for v1 operators.
func RegistryFingerprint() [32]byte {
	encoded := canonicalRegistryBytes(RegistryV1())
	return sha256.Sum256(append([]byte(registryFingerprintDomain), encoded...))
}

// ConsensusFingerprint returns the SHA-256 fingerprint for all v1 Crane rules.
func ConsensusFingerprint() [32]byte {
	encoded := make([]byte, 0, 512)
	encoded = appendUint16(encoded, 1)
	limits := LimitsV1()
	for _, value := range []uint64{
		limits.MaxRegisteredWorkers, limits.MaxActiveJobs, limits.MaxRetainedJobs,
		limits.MaxRetainedClientSessions, limits.MaxResultManifestsPerJob,
		limits.MaxCachedCommandResultBytes, limits.MaxResultRecordsBytesPerJob,
		limits.MaxSnapshotBytes, limits.MaxIdentifierBytes, limits.MaxStages,
		limits.MaxEdges, limits.MaxTasksPerStage, limits.MaxTasksPerJob,
		limits.MaxWorkerSlots,
		limits.MaxSettingsPerStage, limits.MaxSettingKeyBytes, limits.MaxSettingValueBytes,
		limits.MaxTotalSettingsBytes, limits.MaxSourceSequences, limits.MaxOperatorOutputs,
		limits.MaxDerivedDeliveries, limits.MaxTupleFields, limits.MaxTuplePayloadBytes,
		limits.CustodyInboxFixedBytes,
		limits.CustodyOutboxFixedBytes, limits.ResultCopyFixedBytes,
		limits.MaxCustodyReservationBytes,
		limits.MaxSubmitJobBytes, limits.MaxControlFrameBytes,
		limits.MaxWorkerControlFrameBytes,
		limits.AuthenticatedFrameBytes, limits.SubmitJobFixedBytes,
		limits.SubmitRequestFixedBytes, limits.AssignmentSetInstallFixedBytes,
		limits.MaxTopologyBytes,
	} {
		encoded = appendUint64(encoded, value)
	}
	encoded = appendUint16(encoded, uint16(len(identityDomainsV1)))
	for _, domain := range identityDomainsV1 {
		encoded = appendString(encoded, domain)
	}
	encoded = append(encoded, canonicalRegistryBytes(RegistryV1())...)
	encoded = append(encoded, canonicalWireContractBytes(WireContractV1())...)
	return sha256.Sum256(append([]byte(consensusFingerprintDomain), encoded...))
}

// ConsensusFingerprintHex returns the required lower-case configuration value.
func ConsensusFingerprintHex() string {
	fingerprint := ConsensusFingerprint()
	return hex.EncodeToString(fingerprint[:])
}

func canonicalRegistryBytes(registry Registry) []byte {
	encoded := appendUint16(nil, 1)
	encoded = appendUint16(encoded, uint16(len(registry.Operators)))
	for _, operator := range registry.Operators {
		encoded = appendString(encoded, operator.Name)
		encoded = appendUint16(encoded, operator.Version)
		encoded = append(encoded, byte(operator.Role))
		encoded = appendUint16(encoded, uint16(len(operator.Settings)))
		for _, setting := range operator.Settings {
			encoded = appendString(encoded, setting.Name)
			encoded = append(encoded, byte(setting.Type))
			if setting.Required {
				encoded = append(encoded, 1)
			} else {
				encoded = append(encoded, 0)
			}
		}
		encoded = append(encoded, operator.MinInputs, operator.MaxInputs, operator.MinOutputs, operator.MaxOutputs)
		encoded = append(encoded, operator.ImplementationFingerprint[:]...)
	}
	return encoded
}

func canonicalWireContractBytes(contract WireContractDescriptor) []byte {
	encoded := appendString(nil, wireContractFingerprintDomain)
	encoded = appendUint16(encoded, contract.SchemaVersion)
	encoded = appendUint16(encoded, contract.OwnedMessageTypeMin)
	encoded = appendUint16(encoded, contract.OwnedMessageTypeMax)
	encoded = append(encoded, contract.RequiredCodec)
	encoded = appendBool(encoded, contract.RejectUnlistedOwnedMessages)
	encoded = appendUint64(encoded, contract.MaxCraneDatagramBytes)
	encoded = appendUint16(encoded, uint16(len(contract.Messages)))
	for _, message := range contract.Messages {
		encoded = appendString(encoded, message.Name)
		encoded = appendUint16(encoded, message.MessageType)
		encoded = appendBool(encoded, message.Datagram)
	}
	encoded = appendUint16(encoded, uint16(len(contract.ExplicitReservedMessageTypes)))
	for _, messageType := range contract.ExplicitReservedMessageTypes {
		encoded = appendUint16(encoded, messageType)
	}
	return encoded
}

func appendBool(destination []byte, value bool) []byte {
	if value {
		return append(destination, 1)
	}
	return append(destination, 0)
}

func appendString(destination []byte, value string) []byte {
	destination = appendUint16(destination, uint16(len(value)))
	return append(destination, value...)
}

func appendUint16(destination []byte, value uint16) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}
