package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/aaditya/cs425mp3/internal/config"
)

const (
	// SnapshotFormatVersion1 is the initial portable canonical snapshot envelope.
	SnapshotFormatVersion1 uint16 = 1

	snapshotEnvelopeClusterOffset       = 6
	snapshotEnvelopeVotersOffset        = 22
	snapshotEnvelopeIndexOffset         = 54
	snapshotEnvelopeTermOffset          = 62
	snapshotEnvelopeSchemaOffset        = 70
	snapshotEnvelopeStateLengthOffset   = 74
	snapshotEnvelopeStateChecksumOffset = 82
	snapshotEnvelopeChecksumOffset      = 114
	snapshotEnvelopeHeaderBytes         = 146
)

var snapshotEnvelopeMagic = [4]byte{'R', 'F', 'S', 'N'}

// Snapshot is one validated portable application snapshot and its canonical
// envelope. It binds a cluster and fixed voter set, never a local voter ID.
type Snapshot struct {
	// Metadata identifies the exact applied log position and application schema.
	Metadata SnapshotMetadata
	// ID is the stable identifier derived from immutable canonical envelope content.
	ID SnapshotID
	// StateChecksum is the SHA-256 checksum of the application state bytes.
	StateChecksum SnapshotChecksum

	state    []byte
	envelope []byte
}

// SnapshotStageResult reports durable byte progress or a completed installed state.
type SnapshotStageResult struct {
	// NextOffset is the next exact byte required after durable staging.
	NextOffset uint64
	// Done is true only after the complete snapshot and WAL transition are durable.
	Done bool
	// State is the installed independently owned durable state when Done is true.
	State RecoveredState
}

// Clone returns an independently owned snapshot.
func (snapshot Snapshot) Clone() Snapshot {
	snapshot.state = cloneBytes(snapshot.state)
	snapshot.envelope = cloneBytes(snapshot.envelope)
	return snapshot
}

// StateBytes returns an owned copy of the application snapshot bytes.
func (snapshot Snapshot) StateBytes() []byte { return cloneBytes(snapshot.state) }

// EnvelopeBytes returns an owned copy of the complete canonical envelope.
func (snapshot Snapshot) EnvelopeBytes() []byte { return cloneBytes(snapshot.envelope) }

// NewSnapshot validates owned state and constructs its canonical v1 envelope.
func NewSnapshot(identity StorageIdentity, metadata SnapshotMetadata, state []byte, maximumStateBytes uint64) (Snapshot, error) {
	if err := validateSnapshotInputs(identity, metadata, uint64(len(state)), maximumStateBytes); err != nil {
		return Snapshot{}, err
	}
	if uint64(len(state)) > uint64(math.MaxInt)-snapshotEnvelopeHeaderBytes {
		return Snapshot{}, fmt.Errorf("%w: snapshot envelope exceeds local integer domain", ErrInvalidSnapshot)
	}

	ownedState := cloneBytes(state)
	stateChecksum := sha256.Sum256(ownedState)
	envelope := make([]byte, snapshotEnvelopeHeaderBytes+len(ownedState))
	copy(envelope[:4], snapshotEnvelopeMagic[:])
	binary.BigEndian.PutUint16(envelope[4:6], SnapshotFormatVersion1)
	copy(envelope[snapshotEnvelopeClusterOffset:snapshotEnvelopeVotersOffset], identity.ClusterID[:])
	copy(envelope[snapshotEnvelopeVotersOffset:snapshotEnvelopeIndexOffset], identity.VoterFingerprint[:])
	binary.BigEndian.PutUint64(envelope[snapshotEnvelopeIndexOffset:snapshotEnvelopeTermOffset], metadata.LastIncludedIndex)
	binary.BigEndian.PutUint64(envelope[snapshotEnvelopeTermOffset:snapshotEnvelopeSchemaOffset], metadata.LastIncludedTerm)
	binary.BigEndian.PutUint32(envelope[snapshotEnvelopeSchemaOffset:snapshotEnvelopeStateLengthOffset], metadata.StateMachineSchemaVersion)
	binary.BigEndian.PutUint64(envelope[snapshotEnvelopeStateLengthOffset:snapshotEnvelopeStateChecksumOffset], uint64(len(ownedState)))
	copy(envelope[snapshotEnvelopeStateChecksumOffset:snapshotEnvelopeChecksumOffset], stateChecksum[:])
	copy(envelope[snapshotEnvelopeHeaderBytes:], ownedState)
	envelopeChecksum := sha256.Sum256(envelope)
	copy(envelope[snapshotEnvelopeChecksumOffset:snapshotEnvelopeHeaderBytes], envelopeChecksum[:])

	var id SnapshotID
	copy(id[:], envelopeChecksum[:len(id)])
	return Snapshot{
		Metadata:      metadata,
		ID:            id,
		StateChecksum: SnapshotChecksum(stateChecksum),
		state:         ownedState,
		envelope:      envelope,
	}, nil
}

// DecodeSnapshotEnvelope validates a complete canonical v1 envelope before
// returning any application state bytes.
func DecodeSnapshotEnvelope(envelope []byte, expected StorageIdentity, maximumStateBytes uint64) (Snapshot, error) {
	if len(envelope) < snapshotEnvelopeHeaderBytes {
		return Snapshot{}, fmt.Errorf("%w: snapshot envelope is truncated", ErrInvalidSnapshot)
	}
	if !bytes.Equal(envelope[:4], snapshotEnvelopeMagic[:]) {
		return Snapshot{}, fmt.Errorf("%w: invalid snapshot magic", ErrInvalidSnapshot)
	}
	if version := binary.BigEndian.Uint16(envelope[4:6]); version != SnapshotFormatVersion1 {
		return Snapshot{}, fmt.Errorf("%w: unsupported snapshot format %d", ErrInvalidSnapshot, version)
	}
	if !bytes.Equal(envelope[snapshotEnvelopeClusterOffset:snapshotEnvelopeVotersOffset], expected.ClusterID[:]) ||
		!bytes.Equal(envelope[snapshotEnvelopeVotersOffset:snapshotEnvelopeIndexOffset], expected.VoterFingerprint[:]) {
		return Snapshot{}, fmt.Errorf("%w: snapshot cluster or voter set mismatch", ErrInvalidSnapshot)
	}
	metadata := SnapshotMetadata{
		LastIncludedIndex:         binary.BigEndian.Uint64(envelope[snapshotEnvelopeIndexOffset:snapshotEnvelopeTermOffset]),
		LastIncludedTerm:          binary.BigEndian.Uint64(envelope[snapshotEnvelopeTermOffset:snapshotEnvelopeSchemaOffset]),
		StateMachineSchemaVersion: binary.BigEndian.Uint32(envelope[snapshotEnvelopeSchemaOffset:snapshotEnvelopeStateLengthOffset]),
	}
	stateLength := binary.BigEndian.Uint64(envelope[snapshotEnvelopeStateLengthOffset:snapshotEnvelopeStateChecksumOffset])
	if err := validateSnapshotInputs(expected, metadata, stateLength, maximumStateBytes); err != nil {
		return Snapshot{}, err
	}
	if stateLength > uint64(math.MaxInt) || stateLength > math.MaxUint64-snapshotEnvelopeHeaderBytes {
		return Snapshot{}, fmt.Errorf("%w: snapshot state length overflows", ErrInvalidSnapshot)
	}
	total := uint64(snapshotEnvelopeHeaderBytes) + stateLength
	if total != uint64(len(envelope)) {
		return Snapshot{}, fmt.Errorf("%w: snapshot length=%d want=%d", ErrInvalidSnapshot, len(envelope), total)
	}

	wantEnvelopeChecksum := append([]byte(nil), envelope[snapshotEnvelopeChecksumOffset:snapshotEnvelopeHeaderBytes]...)
	zeroed := cloneBytes(envelope)
	clear(zeroed[snapshotEnvelopeChecksumOffset:snapshotEnvelopeHeaderBytes])
	gotEnvelopeChecksum := sha256.Sum256(zeroed)
	if !bytes.Equal(gotEnvelopeChecksum[:], wantEnvelopeChecksum) {
		return Snapshot{}, fmt.Errorf("%w: snapshot envelope checksum mismatch", ErrInvalidSnapshot)
	}
	state := cloneBytes(envelope[snapshotEnvelopeHeaderBytes:])
	gotStateChecksum := sha256.Sum256(state)
	if !bytes.Equal(gotStateChecksum[:], envelope[snapshotEnvelopeStateChecksumOffset:snapshotEnvelopeChecksumOffset]) {
		return Snapshot{}, fmt.Errorf("%w: snapshot state checksum mismatch", ErrInvalidSnapshot)
	}

	var id SnapshotID
	copy(id[:], gotEnvelopeChecksum[:len(id)])
	return Snapshot{
		Metadata:      metadata,
		ID:            id,
		StateChecksum: SnapshotChecksum(gotStateChecksum),
		state:         state,
		envelope:      cloneBytes(envelope),
	}, nil
}

func validateSnapshotInputs(identity StorageIdentity, metadata SnapshotMetadata, stateLength, maximumStateBytes uint64) error {
	if identity.ClusterID == ([16]byte{}) || identity.VoterFingerprint == (VoterFingerprint{}) {
		return fmt.Errorf("%w: snapshot identity is incomplete", ErrInvalidSnapshot)
	}
	if metadata.LastIncludedIndex == 0 || metadata.LastIncludedTerm == 0 || metadata.StateMachineSchemaVersion == 0 {
		return fmt.Errorf("%w: snapshot index, term, and schema must be nonzero", ErrInvalidSnapshot)
	}
	if maximumStateBytes == 0 || maximumStateBytes > config.MaxRaftSnapshotBytes {
		return fmt.Errorf("%w: invalid maximum snapshot bytes %d", ErrInvalidSnapshot, maximumStateBytes)
	}
	if stateLength > maximumStateBytes {
		return fmt.Errorf("%w: snapshot state is %d bytes, maximum is %d", ErrInvalidSnapshot, stateLength, maximumStateBytes)
	}
	return nil
}

type snapshotTransferMetadata struct {
	leaderID    uint16
	term        uint64
	transferID  TransferID
	snapshotID  SnapshotID
	metadata    SnapshotMetadata
	totalLength uint64
	checksum    SnapshotChecksum
}

func snapshotTransferMetadataFor(request InstallSnapshotRequest) snapshotTransferMetadata {
	return snapshotTransferMetadata{
		leaderID: request.LeaderID, term: request.Term, transferID: request.TransferID,
		snapshotID:  request.SnapshotID,
		metadata:    SnapshotMetadata{LastIncludedIndex: request.LastIncludedIndex, LastIncludedTerm: request.LastIncludedTerm, StateMachineSchemaVersion: request.StateMachineSchemaVersion},
		totalLength: request.TotalLength, checksum: request.Checksum,
	}
}

func installRecoveredSnapshot(current RecoveredState, snapshot Snapshot, expected StorageIdentity, voters VoterSet) (RecoveredState, error) {
	metadata := snapshot.Metadata
	if metadata.LastIncludedIndex <= current.HardState.CommitIndex {
		return RecoveredState{}, fmt.Errorf("%w: snapshot index %d is not newer than commit %d", ErrSnapshotRejected, metadata.LastIncludedIndex, current.HardState.CommitIndex)
	}
	prospective := current.Clone()
	retained := []Entry(nil)
	lastIndex := current.SnapshotBase.LastIncludedIndex + uint64(len(current.Entries))
	if lastIndex < current.SnapshotBase.LastIncludedIndex {
		return RecoveredState{}, ErrLogOverflow
	}
	if metadata.LastIncludedIndex <= lastIndex {
		term, err := recoveredTermAt(current, metadata.LastIncludedIndex)
		if err != nil {
			return RecoveredState{}, err
		}
		if term == metadata.LastIncludedTerm {
			offset := metadata.LastIncludedIndex - current.SnapshotBase.LastIncludedIndex
			retained = cloneEntries(current.Entries[offset:])
		}
	}
	prospective.SnapshotBase = metadata
	owned := snapshot.Clone()
	prospective.Snapshot = &owned
	prospective.Entries = retained
	prospective.HardState.CommitIndex = metadata.LastIncludedIndex
	if prospective.AppliedIndex < metadata.LastIncludedIndex {
		prospective.AppliedIndex = metadata.LastIncludedIndex
	}
	if err := ValidateRecoveredState(prospective, expected, voters); err != nil {
		return RecoveredState{}, err
	}
	return prospective.Clone(), nil
}
