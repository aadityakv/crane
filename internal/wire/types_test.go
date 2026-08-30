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
