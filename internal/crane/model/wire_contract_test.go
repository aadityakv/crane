package model

import (
	"reflect"
	"testing"
)

func TestWireContractV1PinsImmutableCanonicalCraneRegistry(t *testing.T) {
	want := WireContractDescriptor{
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
			{Name: "tuple_delivery", MessageType: 280, Datagram: true},
			{Name: "tuple_delivery_ack", MessageType: 281, Datagram: true},
			{Name: "tuple_delivery_nack", MessageType: 282, Datagram: true},
		},
		ExplicitReservedMessageTypes: []uint16{219},
	}
	got := WireContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WireContractV1() = %#v, want %#v", got, want)
	}

	got.Messages[0].Name = "mutated"
	got.Messages[0].MessageType = 299
	got.Messages[0].Datagram = true
	got.ExplicitReservedMessageTypes[0] = 200
	if again := WireContractV1(); !reflect.DeepEqual(again, want) {
		t.Fatalf("WireContractV1 shared mutable storage: %#v", again)
	}
}
