package wire

import "testing"

func TestCodecIDsAreStable(t *testing.T) {
	tests := []struct {
		name string
		got  Codec
		want Codec
	}{
		{name: "gob", got: CodecGob, want: 1},
		{name: "binary", got: CodecBinary, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("codec ID = %d, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestRaftMessageIDsAreStable(t *testing.T) {
	tests := []struct {
		name string
		got  MessageType
		want MessageType
	}{
		{name: "handshake", got: MessageRaftHandshake, want: 100},
		{name: "handshake_ack", got: MessageRaftHandshakeAck, want: 101},
		{name: "pre_vote_request", got: MessageRaftPreVoteRequest, want: 102},
		{name: "pre_vote_response", got: MessageRaftPreVoteResponse, want: 103},
		{name: "request_vote_request", got: MessageRaftRequestVoteRequest, want: 104},
		{name: "request_vote_response", got: MessageRaftRequestVoteResponse, want: 105},
		{name: "append_entries_request", got: MessageRaftAppendEntriesRequest, want: 106},
		{name: "append_entries_response", got: MessageRaftAppendEntriesResponse, want: 107},
		{name: "install_snapshot_request", got: MessageRaftInstallSnapshotRequest, want: 108},
		{name: "install_snapshot_response", got: MessageRaftInstallSnapshotResponse, want: 109},
		{name: "error", got: MessageRaftError, want: 110},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("message ID = %d, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestMessageRegistryRetainsSWIMAndRaftIDs(t *testing.T) {
	tests := []struct {
		name string
		got  MessageType
		want MessageType
	}{
		{name: "swim_ping", got: MessageSWIMPing, want: 1},
		{name: "swim_ack", got: MessageSWIMAck, want: 2},
		{name: "swim_ping_req", got: MessageSWIMPingReq, want: 3},
		{name: "swim_indirect_ack", got: MessageSWIMIndirectAck, want: 4},
		{name: "swim_gossip", got: MessageSWIMGossip, want: 5},
		{name: "swim_digest", got: MessageSWIMDigest, want: 6},
		{name: "swim_join_request", got: MessageSWIMJoinRequest, want: 7},
		{name: "swim_join_snapshot", got: MessageSWIMJoinSnapshot, want: 8},
		{name: "swim_join_announce", got: MessageSWIMJoinAnnounce, want: 9},
		{name: "swim_join_accepted", got: MessageSWIMJoinAccepted, want: 10},
		{name: "swim_snapshot_request", got: MessageSWIMSnapshotRequest, want: 11},
		{name: "swim_snapshot_response", got: MessageSWIMSnapshotResponse, want: 12},
		{name: "swim_error", got: MessageSWIMError, want: 13},
		{name: "swim_snapshot_applied", got: MessageSWIMSnapshotApplied, want: 14},
		{name: "raft_handshake", got: MessageRaftHandshake, want: 100},
		{name: "raft_handshake_ack", got: MessageRaftHandshakeAck, want: 101},
		{name: "raft_pre_vote_request", got: MessageRaftPreVoteRequest, want: 102},
		{name: "raft_pre_vote_response", got: MessageRaftPreVoteResponse, want: 103},
		{name: "raft_request_vote_request", got: MessageRaftRequestVoteRequest, want: 104},
		{name: "raft_request_vote_response", got: MessageRaftRequestVoteResponse, want: 105},
		{name: "raft_append_entries_request", got: MessageRaftAppendEntriesRequest, want: 106},
		{name: "raft_append_entries_response", got: MessageRaftAppendEntriesResponse, want: 107},
		{name: "raft_install_snapshot_request", got: MessageRaftInstallSnapshotRequest, want: 108},
		{name: "raft_install_snapshot_response", got: MessageRaftInstallSnapshotResponse, want: 109},
		{name: "raft_error", got: MessageRaftError, want: 110},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("message ID = %d, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestCraneMessageRegistryUsesExactUniqueIDs(t *testing.T) {
	tests := []struct {
		name string
		got  MessageType
		want MessageType
	}{
		{name: "worker_handshake", got: MessageCraneWorkerHandshake, want: 200},
		{name: "worker_handshake_ack", got: MessageCraneWorkerHandshakeAck, want: 201},
		{name: "worker_fence_request", got: MessageCraneWorkerFenceRequest, want: 202},
		{name: "worker_fence_response", got: MessageCraneWorkerFenceResponse, want: 203},
		{name: "worker_register_request", got: MessageCraneWorkerRegisterRequest, want: 204},
		{name: "worker_register_response", got: MessageCraneWorkerRegisterResponse, want: 205},
		{name: "assignment_set_install", got: MessageCraneAssignmentSetInstall, want: 206},
		{name: "assignment_set_install_ack", got: MessageCraneAssignmentSetInstallAck, want: 207},
		{name: "worker_status_request", got: MessageCraneWorkerStatusRequest, want: 208},
		{name: "worker_status_report", got: MessageCraneWorkerStatusReport, want: 209},
		{name: "checkpoint_notice", got: MessageCraneCheckpointNotice, want: 210},
		{name: "checkpoint_ack", got: MessageCraneCheckpointAck, want: 211},
		{name: "result_record_chunk", got: MessageCraneResultRecordChunk, want: 212},
		{name: "result_record_ack", got: MessageCraneResultRecordAck, want: 213},
		{name: "result_artifact_chunk", got: MessageCraneResultArtifactChunk, want: 214},
		{name: "result_artifact_ack", got: MessageCraneResultArtifactAck, want: 215},
		{name: "result_fetch_request", got: MessageCraneResultFetchRequest, want: 216},
		{name: "result_fetch_chunk", got: MessageCraneResultFetchChunk, want: 217},
		{name: "worker_error", got: MessageCraneWorkerError, want: 218},
		{name: "worker_reserved", got: MessageCraneWorkerReserved, want: 219},
		{name: "submit_request", got: MessageCraneSubmitRequest, want: 240},
		{name: "submit_response", got: MessageCraneSubmitResponse, want: 241},
		{name: "cancel_request", got: MessageCraneCancelRequest, want: 242},
		{name: "cancel_response", got: MessageCraneCancelResponse, want: 243},
		{name: "status_request", got: MessageCraneStatusRequest, want: 244},
		{name: "status_response", got: MessageCraneStatusResponse, want: 245},
		{name: "result_page_request", got: MessageCraneResultPageRequest, want: 246},
		{name: "result_page_response", got: MessageCraneResultPageResponse, want: 247},
		{name: "leader_redirect", got: MessageCraneLeaderRedirect, want: 248},
		{name: "control_error", got: MessageCraneControlError, want: 249},
		{name: "job_list_request", got: MessageCraneJobListRequest, want: 250},
		{name: "job_list_response", got: MessageCraneJobListResponse, want: 251},
		{name: "tuple_delivery", got: MessageCraneTupleDelivery, want: 280},
		{name: "tuple_delivery_ack", got: MessageCraneTupleDeliveryAck, want: 281},
		{name: "tuple_delivery_nack", got: MessageCraneTupleDeliveryNack, want: 282},
	}
	seen := make(map[MessageType]string, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("message ID = %d, want %d", tt.got, tt.want)
			}
			if previous, exists := seen[tt.got]; exists {
				t.Fatalf("message ID %d is shared by %q and %q", tt.got, previous, tt.name)
			}
			seen[tt.got] = tt.name
		})
	}
}
