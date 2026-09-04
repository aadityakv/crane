package raft

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/aadityakv/crane/internal/config"
)

const voterFingerprintDomain = "cs425/raft/voter-set/v1\x00"

// StorageFormatVersion identifies an incompatible durable Raft storage layout.
type StorageFormatVersion uint16

const (
	// StorageFormatVersion1 is the initial identity, WAL, and snapshot storage format.
	StorageFormatVersion1 StorageFormatVersion = 1
)

// VoterFingerprint is the domain-separated hash of a canonical fixed voter set.
type VoterFingerprint [sha256.Size]byte

// Voter is one canonical member of the fixed Raft trust and quorum boundary.
type Voter struct {
	// ID is the nonzero configured node identity.
	ID uint16
	// Endpoint is the canonical statically configured Raft TCP endpoint.
	Endpoint config.Endpoint
}

// VoterSet is an immutable ordered fixed voter configuration.
type VoterSet struct {
	voters      []Voter
	byID        map[uint16]Voter
	fingerprint VoterFingerprint
}

// NewVoterSet validates, canonicalizes, and owns a three- or five-voter set.
func NewVoterSet(configured []config.RaftVoter) (VoterSet, error) {
	if len(configured) != 3 && len(configured) != 5 {
		return VoterSet{}, fmt.Errorf("%w: voter count is %d, want 3 or 5", ErrInvalidVoterSet, len(configured))
	}
	voters := make([]Voter, 0, len(configured))
	byID := make(map[uint16]Voter, len(configured))
	byEndpoint := make(map[config.Endpoint]uint16, len(configured))
	for _, configuredVoter := range configured {
		if configuredVoter.NodeID == 0 {
			return VoterSet{}, fmt.Errorf("%w: voter ID is zero", ErrInvalidVoterSet)
		}
		if _, exists := byID[configuredVoter.NodeID]; exists {
			return VoterSet{}, fmt.Errorf("%w: duplicate voter ID %d", ErrInvalidVoterSet, configuredVoter.NodeID)
		}
		endpoint, err := config.ParseRoutableEndpoint(configuredVoter.Endpoint)
		if err != nil {
			return VoterSet{}, fmt.Errorf("%w: voter %d endpoint: %v", ErrInvalidVoterSet, configuredVoter.NodeID, err)
		}
		if otherID, exists := byEndpoint[endpoint]; exists {
			return VoterSet{}, fmt.Errorf("%w: voters %d and %d share endpoint %s", ErrInvalidVoterSet, otherID, configuredVoter.NodeID, endpoint)
		}
		voter := Voter{ID: configuredVoter.NodeID, Endpoint: endpoint}
		voters = append(voters, voter)
		byID[voter.ID] = voter
		byEndpoint[endpoint] = voter.ID
	}
	sort.Slice(voters, func(left, right int) bool { return voters[left].ID < voters[right].ID })
	return VoterSet{voters: voters, byID: byID, fingerprint: fingerprintVoters(voters)}, nil
}

// Voters returns an owned, ascending-ID copy of the fixed voter set.
func (s VoterSet) Voters() []Voter {
	owned := make([]Voter, len(s.voters))
	copy(owned, s.voters)
	return owned
}

// Voter returns the canonical voter with id by value.
func (s VoterSet) Voter(id uint16) (Voter, bool) {
	voter, ok := s.byID[id]
	return voter, ok
}

// Contains reports whether id is in the fixed voter set.
func (s VoterSet) Contains(id uint16) bool {
	_, ok := s.byID[id]
	return ok
}

// Majority returns the exact number of votes required for quorum.
func (s VoterSet) Majority() int {
	return len(s.voters)/2 + 1
}

// Fingerprint returns the domain-separated canonical voter-set fingerprint.
func (s VoterSet) Fingerprint() VoterFingerprint {
	return s.fingerprint
}

// ValidateLocalID rejects a zero or unconfigured local voter identity.
func (s VoterSet) ValidateLocalID(id uint16) error {
	if id == 0 || !s.Contains(id) {
		return fmt.Errorf("%w: local ID %d", ErrNotVoter, id)
	}
	return nil
}

// StorageIdentity binds one store to independent format, cluster, local voter, and voter-set identities.
type StorageIdentity struct {
	// FormatVersion identifies the durable storage encoding.
	FormatVersion StorageFormatVersion
	// ClusterID separates consensus domains that happen to use the same voters.
	ClusterID [16]byte
	// LocalVoterID prevents copied term and vote state from becoming another voter.
	LocalVoterID uint16
	// VoterFingerprint binds the complete canonical trust and quorum boundary.
	VoterFingerprint VoterFingerprint
}

// NewStorageIdentity validates and constructs independent persisted identity fields.
func NewStorageIdentity(version StorageFormatVersion, clusterID [16]byte, localID uint16, voters VoterSet) (StorageIdentity, error) {
	if version == 0 {
		return StorageIdentity{}, fmt.Errorf("%w: storage format version is zero", ErrInvalidStorageIdentity)
	}
	if clusterID == [16]byte{} {
		return StorageIdentity{}, fmt.Errorf("%w: cluster ID is zero", ErrInvalidStorageIdentity)
	}
	if err := voters.ValidateLocalID(localID); err != nil {
		return StorageIdentity{}, fmt.Errorf("%w: %v", ErrInvalidStorageIdentity, err)
	}
	return StorageIdentity{
		FormatVersion:    version,
		ClusterID:        clusterID,
		LocalVoterID:     localID,
		VoterFingerprint: voters.Fingerprint(),
	}, nil
}

func fingerprintVoters(voters []Voter) VoterFingerprint {
	hash := sha256.New()
	_, _ = hash.Write([]byte(voterFingerprintDomain))
	var fixed [2]byte
	binary.BigEndian.PutUint16(fixed[:], uint16(len(voters)))
	_, _ = hash.Write(fixed[:])
	for _, voter := range voters {
		binary.BigEndian.PutUint16(fixed[:], voter.ID)
		_, _ = hash.Write(fixed[:])
		endpoint := voter.Endpoint.String()
		binary.BigEndian.PutUint16(fixed[:], uint16(len(endpoint)))
		_, _ = hash.Write(fixed[:])
		_, _ = hash.Write([]byte(endpoint))
	}
	var fingerprint VoterFingerprint
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}
