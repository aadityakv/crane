package model

// WireMessageDescriptor binds one stable Crane semantic message name to its
// canonical wire ID and complete-frame limit class.
type WireMessageDescriptor struct {
	// Name is the stable lower-case semantic name included in compatibility hashing.
	Name string
	// MessageType is the canonical uint16 value carried in the authenticated header.
	MessageType uint16
	// Datagram selects the compiled Crane tuple complete-frame ceiling.
	Datagram bool
}

// WireContractDescriptor is the immutable-by-copy Crane v1 framing contract.
type WireContractDescriptor struct {
	// SchemaVersion identifies the canonical descriptor encoding.
	SchemaVersion uint16
	// OwnedMessageTypeMin is the first message ID exclusively governed by Crane.
	OwnedMessageTypeMin uint16
	// OwnedMessageTypeMax is the last message ID exclusively governed by Crane.
	OwnedMessageTypeMax uint16
	// RequiredCodec is the numeric canonical payload codec for every active message.
	RequiredCodec uint8
	// RejectUnlistedOwnedMessages makes every unlisted ID in the owned range invalid.
	RejectUnlistedOwnedMessages bool
	// MaxCraneDatagramBytes is the absolute complete-frame ceiling for tuple traffic.
	MaxCraneDatagramBytes uint64
	// Messages is the canonical ascending list of active semantic message bindings.
	Messages []WireMessageDescriptor
	// ExplicitReservedMessageTypes pins named reservations within the owned range.
	ExplicitReservedMessageTypes []uint16
}

var wireContractV1 = WireContractDescriptor{
	SchemaVersion:               1,
	OwnedMessageTypeMin:         200,
	OwnedMessageTypeMax:         299,
	RequiredCodec:               2,
	RejectUnlistedOwnedMessages: true,
	MaxCraneDatagramBytes:       1200,
	Messages: []WireMessageDescriptor{
		{Name: "worker_handshake", MessageType: 200},
		{Name: "worker_handshake_ack", MessageType: 201},
		{Name: "worker_fence_request", MessageType: 202},
		{Name: "worker_fence_response", MessageType: 203},
		{Name: "worker_register_request", MessageType: 204},
		{Name: "worker_register_response", MessageType: 205},
		{Name: "assignment_set_install", MessageType: 206},
		{Name: "assignment_set_install_ack", MessageType: 207},
		{Name: "worker_status_request", MessageType: 208},
		{Name: "worker_status_report", MessageType: 209},
		{Name: "checkpoint_notice", MessageType: 210},
		{Name: "checkpoint_ack", MessageType: 211},
		{Name: "result_record_chunk", MessageType: 212},
		{Name: "result_record_ack", MessageType: 213},
		{Name: "result_artifact_chunk", MessageType: 214},
		{Name: "result_artifact_ack", MessageType: 215},
		{Name: "result_fetch_request", MessageType: 216},
		{Name: "result_fetch_chunk", MessageType: 217},
		{Name: "worker_error", MessageType: 218},
		{Name: "submit_request", MessageType: 240},
		{Name: "submit_response", MessageType: 241},
		{Name: "cancel_request", MessageType: 242},
		{Name: "cancel_response", MessageType: 243},
		{Name: "status_request", MessageType: 244},
		{Name: "status_response", MessageType: 245},
		{Name: "result_page_request", MessageType: 246},
		{Name: "result_page_response", MessageType: 247},
		{Name: "leader_redirect", MessageType: 248},
		{Name: "control_error", MessageType: 249},
		{Name: "job_list_request", MessageType: 250},
		{Name: "job_list_response", MessageType: 251},
		{Name: "tuple_delivery", MessageType: 280, Datagram: true},
		{Name: "tuple_delivery_ack", MessageType: 281, Datagram: true},
		{Name: "tuple_delivery_nack", MessageType: 282, Datagram: true},
	},
	ExplicitReservedMessageTypes: []uint16{219},
}

// WireContractV1 returns an owned copy of the canonical Crane v1 wire contract.
func WireContractV1() WireContractDescriptor {
	contract := wireContractV1
	contract.Messages = append([]WireMessageDescriptor(nil), wireContractV1.Messages...)
	contract.ExplicitReservedMessageTypes = append([]uint16(nil), wireContractV1.ExplicitReservedMessageTypes...)
	return contract
}
