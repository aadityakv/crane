package raft

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/aaditya/cs425mp3/internal/config"
)

func TestVoterSetCanonicalOrderingMajorityAndFingerprint(t *testing.T) {
	voters, err := NewVoterSet([]config.RaftVoter{
		{NodeID: 3, Endpoint: "[2001:0db8::1]:9003"},
		{NodeID: 1, Endpoint: "Example.COM.:9001"},
		{NodeID: 2, Endpoint: "[::ffff:192.0.2.1]:9002"},
	})
	if err != nil {
		t.Fatalf("NewVoterSet: %v", err)
	}

	got := voters.Voters()
	want := []Voter{
		{ID: 1, Endpoint: config.Endpoint{Host: "example.com", Port: 9001}},
		{ID: 2, Endpoint: config.Endpoint{Host: "192.0.2.1", Port: 9002}},
		{ID: 3, Endpoint: config.Endpoint{Host: "2001:db8::1", Port: 9003}},
	}
	if len(got) != len(want) {
		t.Fatalf("len(Voters()) = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("Voters()[%d] = %+v, want %+v", index, got[index], want[index])
		}
	}
	if voters.Majority() != 2 {
		t.Fatalf("Majority() = %d, want 2", voters.Majority())
	}
	wantFingerprint, err := hex.DecodeString("6f2671a478c32bac4c5b2a24b429150ce9ac0dc4dd3d9980ad2a8d0e63d96e07")
	if err != nil {
		t.Fatal(err)
	}
	if gotFingerprint := voters.Fingerprint(); string(gotFingerprint[:]) != string(wantFingerprint) {
		t.Fatalf("Fingerprint() = %x, want %x", gotFingerprint, wantFingerprint)
	}

	aliases, err := NewVoterSet([]config.RaftVoter{
		{NodeID: 1, Endpoint: "example.com:9001"},
		{NodeID: 2, Endpoint: "192.0.2.1:9002"},
		{NodeID: 3, Endpoint: "[2001:db8::1]:9003"},
	})
	if err != nil {
		t.Fatalf("NewVoterSet aliases: %v", err)
	}
	if aliases.Fingerprint() != voters.Fingerprint() {
		t.Fatalf("canonical aliases changed fingerprint: %x != %x", aliases.Fingerprint(), voters.Fingerprint())
	}
}

func TestVoterSetFiveVoterMajorityAndOwnedAccessors(t *testing.T) {
	set := mustVoterSet(t, 5)
	if set.Majority() != 3 {
		t.Fatalf("Majority() = %d, want 3", set.Majority())
	}

	copyOne := set.Voters()
	copyOne[0].ID = 99
	copyOne[0].Endpoint.Host = "changed.invalid"
	copyTwo := set.Voters()
	if copyTwo[0].ID != 1 || copyTwo[0].Endpoint.Host != "voter-1.example" {
		t.Fatalf("Voters returned aliased state: %+v", copyTwo[0])
	}

	voter, ok := set.Voter(1)
	if !ok {
		t.Fatal("Voter(1) not found")
	}
	voter.Endpoint.Host = "also-changed.invalid"
	voterAgain, _ := set.Voter(1)
	if voterAgain.Endpoint.Host != "voter-1.example" {
		t.Fatalf("Voter returned aliased state: %+v", voterAgain)
	}
}

func TestVoterSetRejectsInvalidConfigurationAndLocalIdentity(t *testing.T) {
	tests := []struct {
		name   string
		voters []config.RaftVoter
	}{
		{name: "wrong_count", voters: []config.RaftVoter{{NodeID: 1, Endpoint: "one.example:9001"}}},
		{name: "zero_id", voters: []config.RaftVoter{{NodeID: 0, Endpoint: "zero.example:9000"}, {NodeID: 2, Endpoint: "two.example:9002"}, {NodeID: 3, Endpoint: "three.example:9003"}}},
		{name: "duplicate_id", voters: []config.RaftVoter{{NodeID: 1, Endpoint: "one.example:9001"}, {NodeID: 1, Endpoint: "two.example:9002"}, {NodeID: 3, Endpoint: "three.example:9003"}}},
		{name: "duplicate_canonical_endpoint", voters: []config.RaftVoter{{NodeID: 1, Endpoint: "EXAMPLE.COM.:9001"}, {NodeID: 2, Endpoint: "example.com:9001"}, {NodeID: 3, Endpoint: "three.example:9003"}}},
		{name: "wildcard_endpoint", voters: []config.RaftVoter{{NodeID: 1, Endpoint: "0.0.0.0:9001"}, {NodeID: 2, Endpoint: "two.example:9002"}, {NodeID: 3, Endpoint: "three.example:9003"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewVoterSet(test.voters); !errors.Is(err, ErrInvalidVoterSet) {
				t.Fatalf("NewVoterSet error = %v, want ErrInvalidVoterSet", err)
			}
		})
	}

	set := mustVoterSet(t, 3)
	if err := set.ValidateLocalID(0); !errors.Is(err, ErrNotVoter) {
		t.Fatalf("ValidateLocalID(0) = %v, want ErrNotVoter", err)
	}
	if err := set.ValidateLocalID(4); !errors.Is(err, ErrNotVoter) {
		t.Fatalf("ValidateLocalID(4) = %v, want ErrNotVoter", err)
	}
	if err := set.ValidateLocalID(2); err != nil {
		t.Fatalf("ValidateLocalID(2): %v", err)
	}
}

func TestVoterStorageIdentityKeepsIdentityDomainsSeparate(t *testing.T) {
	set := mustVoterSet(t, 3)
	clusterA := [16]byte{1, 2, 3}
	clusterB := [16]byte{1, 2, 4}

	identityA, err := NewStorageIdentity(StorageFormatVersion1, clusterA, 1, set)
	if err != nil {
		t.Fatalf("NewStorageIdentity: %v", err)
	}
	identityCluster, err := NewStorageIdentity(StorageFormatVersion1, clusterB, 1, set)
	if err != nil {
		t.Fatalf("NewStorageIdentity cluster: %v", err)
	}
	identityLocal, err := NewStorageIdentity(StorageFormatVersion1, clusterA, 2, set)
	if err != nil {
		t.Fatalf("NewStorageIdentity local: %v", err)
	}

	if identityA.VoterFingerprint != identityCluster.VoterFingerprint || identityA.VoterFingerprint != identityLocal.VoterFingerprint {
		t.Fatal("cluster or local voter identity contaminated voter-set fingerprint")
	}
	if identityA.ClusterID == identityCluster.ClusterID {
		t.Fatal("different clusters produced the same persisted cluster identity")
	}
	if identityA.LocalVoterID == identityLocal.LocalVoterID {
		t.Fatal("different local voters produced the same persisted local identity")
	}
	if _, err := NewStorageIdentity(0, clusterA, 1, set); !errors.Is(err, ErrInvalidStorageIdentity) {
		t.Fatalf("zero storage version error = %v, want ErrInvalidStorageIdentity", err)
	}
	if _, err := NewStorageIdentity(StorageFormatVersion1, [16]byte{}, 1, set); !errors.Is(err, ErrInvalidStorageIdentity) {
		t.Fatalf("zero cluster error = %v, want ErrInvalidStorageIdentity", err)
	}
	if _, err := NewStorageIdentity(StorageFormatVersion1, clusterA, 9, set); !errors.Is(err, ErrInvalidStorageIdentity) {
		t.Fatalf("non-voter local ID error = %v, want ErrInvalidStorageIdentity", err)
	}
}

func TestVoterEntryAndSnapshotBytesAreOwned(t *testing.T) {
	command := []byte("set x 1")
	entry, err := NewEntry(7, 3, EntryCommand, command)
	if err != nil {
		t.Fatalf("NewEntry: %v", err)
	}
	command[0] = 'X'
	if string(entry.CommandBytes()) != "set x 1" {
		t.Fatalf("entry retained caller command: %q", entry.CommandBytes())
	}
	returned := entry.CommandBytes()
	returned[0] = 'Y'
	if string(entry.CommandBytes()) != "set x 1" {
		t.Fatalf("CommandBytes returned aliased state: %q", entry.CommandBytes())
	}
	clone := entry.Clone()
	clone.command[0] = 'Z'
	if string(entry.CommandBytes()) != "set x 1" {
		t.Fatalf("Clone returned aliased state: %q", entry.CommandBytes())
	}

	if _, err := NewEntry(1, 1, EntryNoOp, []byte("unexpected")); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("NoOp command error = %v, want ErrInvalidEntry", err)
	}
}

func mustVoterSet(t *testing.T, count int) VoterSet {
	t.Helper()
	voters := make([]config.RaftVoter, count)
	for index := range voters {
		voters[index] = config.RaftVoter{
			NodeID:   uint16(index + 1),
			Endpoint: "voter-" + string(rune('1'+index)) + ".example:" + string(rune('0'+index)) + "001",
		}
	}
	set, err := NewVoterSet(voters)
	if err != nil {
		t.Fatalf("NewVoterSet: %v", err)
	}
	return set
}
