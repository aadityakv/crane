package model_test

import (
	"errors"
	"reflect"
	"testing"

	"crane/internal/config"
	"crane/internal/crane/model"
	"crane/internal/wire"
)

func TestConsensusLimitsMatchExistingIndependentTransportBounds(t *testing.T) {
	limits := model.LimitsV1()
	if limits.MaxTuplePayloadBytes != 512 {
		t.Fatalf("tuple payload bytes = %d, want frozen 512-byte canonical tuple limit", limits.MaxTuplePayloadBytes)
	}
	if limits.MaxTuplePayloadBytes > uint64(wire.MaxCraneDatagramBytesV1-wire.FixedHeaderSize-wire.MACSize) {
		t.Fatalf("tuple payload bytes = %d, wire datagram payload budget = %d", limits.MaxTuplePayloadBytes, wire.MaxCraneDatagramBytesV1-wire.FixedHeaderSize-wire.MACSize)
	}
	if limits.MaxSubmitJobBytes != config.MaxRaftCommandBytes {
		t.Fatalf("SubmitJob bytes = %d, Raft command bytes = %d", limits.MaxSubmitJobBytes, config.MaxRaftCommandBytes)
	}
	if limits.MaxControlFrameBytes != 1<<20 || limits.MaxWorkerControlFrameBytes != 1<<20 {
		t.Fatalf("Crane control frame bounds = %d,%d", limits.MaxControlFrameBytes, limits.MaxWorkerControlFrameBytes)
	}
	if limits.MaxSnapshotBytes > config.MaxRaftSnapshotBytes {
		t.Fatalf("Crane snapshot bytes = %d, Raft maximum = %d", limits.MaxSnapshotBytes, config.MaxRaftSnapshotBytes)
	}
	if limits.MaxSubmitJobBytes > uint64(wire.DefaultLimits().MaxFrameSize) {
		t.Fatalf("SubmitJob bytes = %d, frame bytes = %d", limits.MaxSubmitJobBytes, wire.DefaultLimits().MaxFrameSize)
	}
}

func TestCraneWireContractMatchesIndependentWireRegistryAndBehavior(t *testing.T) {
	want := model.WireContractDescriptor{
		SchemaVersion:               1,
		OwnedMessageTypeMin:         200,
		OwnedMessageTypeMax:         299,
		RequiredCodec:               uint8(wire.CodecBinary),
		RejectUnlistedOwnedMessages: true,
		MaxCraneDatagramBytes:       uint64(wire.MaxCraneDatagramBytesV1),
		Messages: []model.WireMessageDescriptor{
			{Name: "worker_handshake", MessageType: uint16(wire.MessageCraneWorkerHandshake)},
			{Name: "worker_handshake_ack", MessageType: uint16(wire.MessageCraneWorkerHandshakeAck)},
			{Name: "worker_fence_request", MessageType: uint16(wire.MessageCraneWorkerFenceRequest)},
			{Name: "worker_fence_response", MessageType: uint16(wire.MessageCraneWorkerFenceResponse)},
			{Name: "worker_register_request", MessageType: uint16(wire.MessageCraneWorkerRegisterRequest)},
			{Name: "worker_register_response", MessageType: uint16(wire.MessageCraneWorkerRegisterResponse)},
			{Name: "assignment_set_install", MessageType: uint16(wire.MessageCraneAssignmentSetInstall)},
			{Name: "assignment_set_install_ack", MessageType: uint16(wire.MessageCraneAssignmentSetInstallAck)},
			{Name: "worker_status_request", MessageType: uint16(wire.MessageCraneWorkerStatusRequest)},
			{Name: "worker_status_report", MessageType: uint16(wire.MessageCraneWorkerStatusReport)},
			{Name: "checkpoint_notice", MessageType: uint16(wire.MessageCraneCheckpointNotice)},
			{Name: "checkpoint_ack", MessageType: uint16(wire.MessageCraneCheckpointAck)},
			{Name: "result_record_chunk", MessageType: uint16(wire.MessageCraneResultRecordChunk)},
			{Name: "result_record_ack", MessageType: uint16(wire.MessageCraneResultRecordAck)},
			{Name: "result_artifact_chunk", MessageType: uint16(wire.MessageCraneResultArtifactChunk)},
			{Name: "result_artifact_ack", MessageType: uint16(wire.MessageCraneResultArtifactAck)},
			{Name: "result_fetch_request", MessageType: uint16(wire.MessageCraneResultFetchRequest)},
			{Name: "result_fetch_chunk", MessageType: uint16(wire.MessageCraneResultFetchChunk)},
			{Name: "worker_error", MessageType: uint16(wire.MessageCraneWorkerError)},
			{Name: "submit_request", MessageType: uint16(wire.MessageCraneSubmitRequest)},
			{Name: "submit_response", MessageType: uint16(wire.MessageCraneSubmitResponse)},
			{Name: "cancel_request", MessageType: uint16(wire.MessageCraneCancelRequest)},
			{Name: "cancel_response", MessageType: uint16(wire.MessageCraneCancelResponse)},
			{Name: "status_request", MessageType: uint16(wire.MessageCraneStatusRequest)},
			{Name: "status_response", MessageType: uint16(wire.MessageCraneStatusResponse)},
			{Name: "result_page_request", MessageType: uint16(wire.MessageCraneResultPageRequest)},
			{Name: "result_page_response", MessageType: uint16(wire.MessageCraneResultPageResponse)},
			{Name: "leader_redirect", MessageType: uint16(wire.MessageCraneLeaderRedirect)},
			{Name: "control_error", MessageType: uint16(wire.MessageCraneControlError)},
			{Name: "job_list_request", MessageType: uint16(wire.MessageCraneJobListRequest)},
			{Name: "job_list_response", MessageType: uint16(wire.MessageCraneJobListResponse)},
			{Name: "tuple_delivery", MessageType: uint16(wire.MessageCraneTupleDelivery), Datagram: true},
			{Name: "tuple_delivery_ack", MessageType: uint16(wire.MessageCraneTupleDeliveryAck), Datagram: true},
			{Name: "tuple_delivery_nack", MessageType: uint16(wire.MessageCraneTupleDeliveryNack), Datagram: true},
		},
		ExplicitReservedMessageTypes: []uint16{uint16(wire.MessageCraneWorkerReserved)},
	}
	got := model.WireContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("model wire contract = %#v, independent wire contract = %#v", got, want)
	}
	if uint64(wire.DefaultLimits().MaxCraneDatagramSize) != got.MaxCraneDatagramBytes {
		t.Fatalf("wire default Crane bytes = %d, model compiled bytes = %d", wire.DefaultLimits().MaxCraneDatagramSize, got.MaxCraneDatagramBytes)
	}

	auth := wire.NewHMACAuthenticator([]byte("0123456789abcdef0123456789abcdef"))
	active := make(map[uint16]model.WireMessageDescriptor, len(got.Messages))
	for _, descriptor := range got.Messages {
		if previous, exists := active[descriptor.MessageType]; exists {
			t.Fatalf("wire message ID %d is shared by %q and %q", descriptor.MessageType, previous.Name, descriptor.Name)
		}
		active[descriptor.MessageType] = descriptor
		header := wire.Header{Version: wire.Version1, Message: wire.MessageType(descriptor.MessageType), Codec: wire.CodecBinary}
		if _, err := wire.Encode(header, nil, auth, wire.DefaultLimits()); err != nil {
			t.Fatalf("wire rejects model active message %q (%d): %v", descriptor.Name, descriptor.MessageType, err)
		}
		header.Codec = wire.CodecGob
		if _, err := wire.Encode(header, nil, auth, wire.DefaultLimits()); !errors.Is(err, wire.ErrUnsupportedCodec) {
			t.Fatalf("wire Gob error for %q (%d) = %v, want ErrUnsupportedCodec", descriptor.Name, descriptor.MessageType, err)
		}
		limit, err := wire.EffectiveLimit(wire.MessageType(descriptor.MessageType), wire.DefaultLimits())
		if err != nil {
			t.Fatalf("EffectiveLimit for %q (%d): %v", descriptor.Name, descriptor.MessageType, err)
		}
		wantLimit := wire.DefaultLimits().MaxFrameSize
		if descriptor.Datagram {
			wantLimit = int(got.MaxCraneDatagramBytes)
		}
		if limit != wantLimit {
			t.Fatalf("EffectiveLimit for %q (%d) = %d, want %d", descriptor.Name, descriptor.MessageType, limit, wantLimit)
		}
	}

	reserved := make(map[uint16]struct{}, len(got.ExplicitReservedMessageTypes))
	for _, messageType := range got.ExplicitReservedMessageTypes {
		if _, exists := active[messageType]; exists {
			t.Fatalf("explicitly reserved message ID %d is active", messageType)
		}
		reserved[messageType] = struct{}{}
	}
	for messageType := got.OwnedMessageTypeMin; messageType <= got.OwnedMessageTypeMax; messageType++ {
		if _, exists := active[messageType]; exists {
			continue
		}
		header := wire.Header{Version: wire.Version1, Message: wire.MessageType(messageType), Codec: wire.CodecBinary}
		if _, err := wire.Encode(header, nil, auth, wire.DefaultLimits()); !errors.Is(err, wire.ErrUnsupportedMessage) {
			t.Fatalf("wire unlisted owned message %d error = %v, want ErrUnsupportedMessage", messageType, err)
		}
	}
	if _, exists := reserved[uint16(wire.MessageCraneWorkerReserved)]; !exists {
		t.Fatalf("wire reservation %d is absent from model descriptor", wire.MessageCraneWorkerReserved)
	}
}
