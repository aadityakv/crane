// Package clientstate persists the durable Crane client identity that makes
// exact-bytes public-control retries safe across process crashes. Every
// ClientRequestID sequence is durably reserved together with its exact request
// bytes before any network send, and only a durably recorded resolution
// advances the sequence, so a crash can never reuse a sequence for different
// bytes or skip one the cluster already consumed.
package clientstate

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"crane/internal/crane/model"
)

const (
	// MaxClientPayloadBytes bounds one retained pending request or resolution.
	MaxClientPayloadBytes = int(model.PublicControlMaxFrameBytesV1)

	clientStateMagic   = "CRCS"
	clientStateVersion = uint16(1)

	// clientStateFixedBytes is the encoded size of every non-payload field:
	// magic, version, cluster, client, sequence, two length prefixes, the
	// pending digest, and the trailing checksum.
	clientStateFixedBytes = 4 + 2 + 16 + 16 + 8 + 4 + 32 + 4 + 32

	// maxClientStateFileBytes bounds allocation before any file read.
	maxClientStateFileBytes = clientStateFixedBytes + 2*MaxClientPayloadBytes
)

var (
	// ErrClientStateUnprotected reports a state file or parent directory whose
	// permissions grant group or other access.
	ErrClientStateUnprotected = errors.New("crane client state must be owner-only")
	// ErrClientStateCorrupt reports a state file whose bytes fail the
	// checksum, layout, or semantic validation and is never silently replaced.
	ErrClientStateCorrupt = errors.New("crane client state is corrupt")
	// ErrClientStateForeignCluster reports a state file bound to another cluster.
	ErrClientStateForeignCluster = errors.New("crane client state belongs to another cluster")
	// ErrClientStatePending reports a reservation attempted while an
	// unresolved request is still pending.
	ErrClientStatePending = errors.New("crane client state has an unresolved pending request")
	// ErrClientStateNoPending reports a resolution without a pending request.
	ErrClientStateNoPending = errors.New("crane client state has no pending request")
	// ErrClientStateBounds reports an empty or over-bound payload.
	ErrClientStateBounds = errors.New("crane client payload is outside bounds")
	// ErrClientStateExhausted reports a client sequence with no valid successor.
	ErrClientStateExhausted = errors.New("crane client sequence is exhausted")
)

// persistBoundaries names the injected crash points of one atomic persist, in
// execution order: writing the owner-only temporary file, syncing it, renaming
// it over the state path, and syncing the parent directory.
var persistBoundaries = []string{"temp-write", "temp-sync", "rename", "dir-sync"}

// ClientState is one complete durable client identity snapshot.
type ClientState struct {
	// ClusterID binds the identity to exactly one cluster.
	ClusterID [16]byte
	// ClientID is the stable random submitter identity.
	ClientID model.ClientID
	// NextSequence is the next unreserved client request sequence.
	NextSequence uint64
	// Pending is the exact unresolved request payload, or empty.
	Pending []byte
	// PendingDigest is the SHA-256 of Pending when a request is pending.
	PendingDigest [32]byte
	// Resolved is the durable resolution payload of the last resolved request.
	Resolved []byte
}

// clone returns an independently owned deep copy.
func (state ClientState) clone() ClientState {
	state.Pending = append([]byte(nil), state.Pending...)
	state.Resolved = append([]byte(nil), state.Resolved...)
	return state
}

// ClientStore owns one durable client identity file with atomic, checksummed,
// fsynced persistence.
type ClientStore struct {
	path  string
	state ClientState

	// crash, when set by fault tests, aborts a persist at one named boundary.
	crash func(boundary string) error
}

// OpenClientState opens or creates the owner-only durable client identity at
// path, bound to exactly clusterID. A missing file creates a fresh nonzero
// random ClientID at sequence 1; an existing file is fully validated and never
// silently replaced.
func OpenClientState(path string, clusterID [16]byte) (*ClientStore, error) {
	return openClientStateAt(path, clusterID, nil)
}

// openClientStateAt is OpenClientState with an injectable crash boundary hook.
func openClientStateAt(path string, clusterID [16]byte, crash func(string) error) (*ClientStore, error) {
	if path == "" {
		return nil, errors.New("open Crane client state: empty path")
	}
	if clusterID == ([16]byte{}) {
		return nil, errors.New("open Crane client state: zero cluster ID")
	}
	if err := requireProtectedDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	// A crash between the temporary write and the rename leaves the durable
	// state intact but orphans <path>.tmp; the next persist would overwrite
	// it anyway, so remove it best-effort rather than let it outlive the run.
	_ = os.Remove(path + ".tmp")
	store := &ClientStore{path: path, crash: crash}
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		created, err := newClientState(clusterID)
		if err != nil {
			return nil, err
		}
		store.state = created
		if err := store.persist(); err != nil {
			return nil, err
		}
		return store, nil
	case err != nil:
		return nil, fmt.Errorf("stat Crane client state %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q is not a regular file", ErrClientStateUnprotected, path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: file %q mode %v grants group or other access", ErrClientStateUnprotected, path, info.Mode().Perm())
	}
	if info.Size() > int64(maxClientStateFileBytes) {
		return nil, fmt.Errorf("%w: file is %d bytes, maximum is %d", ErrClientStateCorrupt, info.Size(), maxClientStateFileBytes)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Crane client state %q: %w", path, err)
	}
	state, err := decodeClientState(encoded)
	if err != nil {
		return nil, err
	}
	if state.ClusterID != clusterID {
		return nil, fmt.Errorf("%w: state binds cluster %x", ErrClientStateForeignCluster, state.ClusterID)
	}
	store.state = state
	return store, nil
}

// State returns an independently owned copy of the durable snapshot.
func (store *ClientStore) State() ClientState { return store.state.clone() }

// NextRequestID returns the request identity the next Begin will reserve.
func (store *ClientStore) NextRequestID() model.ClientRequestID {
	return model.ClientRequestID{ClientID: store.state.ClientID, Sequence: store.state.NextSequence}
}

// Begin durably reserves the next sequence for the exact request bytes before
// any send, returning the reserved identity and the pending digest. It fails
// closed while an earlier request is still unresolved.
func (store *ClientStore) Begin(request []byte) (model.ClientRequestID, [32]byte, error) {
	if len(request) == 0 || len(request) > MaxClientPayloadBytes {
		return model.ClientRequestID{}, [32]byte{}, fmt.Errorf("%w: request is %d bytes", ErrClientStateBounds, len(request))
	}
	if len(store.state.Pending) != 0 {
		return model.ClientRequestID{}, [32]byte{}, ErrClientStatePending
	}
	if store.state.NextSequence == ^uint64(0) {
		return model.ClientRequestID{}, [32]byte{}, ErrClientStateExhausted
	}
	next := store.state
	next.Pending = append([]byte(nil), request...)
	next.PendingDigest = sha256.Sum256(request)
	if err := store.persistState(next); err != nil {
		return model.ClientRequestID{}, [32]byte{}, err
	}
	return model.ClientRequestID{ClientID: store.state.ClientID, Sequence: store.state.NextSequence}, store.state.PendingDigest, nil
}

// Resolve durably records the resolution of the pending request and only then
// advances the sequence, so a crash replays the same identity, never a new one.
func (store *ClientStore) Resolve(resolution []byte) error {
	if len(resolution) == 0 || len(resolution) > MaxClientPayloadBytes {
		return fmt.Errorf("%w: resolution is %d bytes", ErrClientStateBounds, len(resolution))
	}
	if len(store.state.Pending) == 0 {
		return ErrClientStateNoPending
	}
	next := store.state
	next.Pending = nil
	next.PendingDigest = [32]byte{}
	next.Resolved = append([]byte(nil), resolution...)
	next.NextSequence = store.state.NextSequence + 1
	return store.persistState(next)
}

// newClientState builds a fresh identity with a nonzero random ClientID.
func newClientState(clusterID [16]byte) (ClientState, error) {
	var client model.ClientID
	for {
		if _, err := rand.Read(client[:]); err != nil {
			return ClientState{}, fmt.Errorf("generate Crane client ID: %w", err)
		}
		if client.Validate() == nil {
			break
		}
	}
	return ClientState{ClusterID: clusterID, ClientID: client, NextSequence: 1}, nil
}

// persistState atomically replaces the durable snapshot and only then adopts
// it in memory.
func (store *ClientStore) persistState(next ClientState) error {
	previous := store.state
	store.state = next
	if err := store.persist(); err != nil {
		store.state = previous
		return err
	}
	return nil
}

// persist writes the complete checksummed snapshot to an owner-only temporary
// file, fsyncs it, atomically renames it over the state path, and fsyncs the
// parent directory.
func (store *ClientStore) persist() (err error) {
	encoded := encodeClientState(store.state)
	temporary := store.path + ".tmp"
	if err := store.crashAt("temp-write"); err != nil {
		return err
	}
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create Crane client state temporary: %w", err)
	}
	defer func() {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}()
	if _, err := file.Write(encoded); err != nil {
		return fmt.Errorf("write Crane client state temporary: %w", err)
	}
	if err := store.crashAt("temp-sync"); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync Crane client state temporary: %w", err)
	}
	closing := file
	file = nil
	if err := closing.Close(); err != nil {
		return fmt.Errorf("close Crane client state temporary: %w", err)
	}
	if err := store.crashAt("rename"); err != nil {
		return err
	}
	if err := os.Rename(temporary, store.path); err != nil {
		return fmt.Errorf("rename Crane client state: %w", err)
	}
	if err := store.crashAt("dir-sync"); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(store.path))
}

// crashAt invokes the injected fault hook at one named persist boundary.
func (store *ClientStore) crashAt(boundary string) error {
	if store.crash == nil {
		return nil
	}
	if err := store.crash(boundary); err != nil {
		return fmt.Errorf("persist Crane client state: %w", err)
	}
	return nil
}

// syncDirectory makes a completed rename durable against power loss.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open Crane client state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync Crane client state directory: %w", err)
	}
	return nil
}

// requireProtectedDirectory fails closed unless the parent directory exists
// and grants no group or other access.
func requireProtectedDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat Crane client state directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: parent %q is not a directory", ErrClientStateUnprotected, path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: directory %q mode %v grants group or other access", ErrClientStateUnprotected, path, info.Mode().Perm())
	}
	return nil
}

// encodeClientState emits the canonical checksummed snapshot layout.
func encodeClientState(state ClientState) []byte {
	encoded := make([]byte, 0, clientStateFixedBytes+len(state.Pending)+len(state.Resolved))
	encoded = append(encoded, clientStateMagic...)
	encoded = binary.BigEndian.AppendUint16(encoded, clientStateVersion)
	encoded = append(encoded, state.ClusterID[:]...)
	encoded = append(encoded, state.ClientID[:]...)
	encoded = binary.BigEndian.AppendUint64(encoded, state.NextSequence)
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(state.Pending)))
	encoded = append(encoded, state.Pending...)
	encoded = append(encoded, state.PendingDigest[:]...)
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(state.Resolved)))
	encoded = append(encoded, state.Resolved...)
	checksum := sha256.Sum256(encoded)
	return append(encoded, checksum[:]...)
}

// decodeClientState validates bounds, checksum, layout, and semantics before
// returning an owned snapshot; every failure is fail-closed corruption.
func decodeClientState(encoded []byte) (ClientState, error) {
	if len(encoded) < clientStateFixedBytes || len(encoded) > maxClientStateFileBytes {
		return ClientState{}, fmt.Errorf("%w: %d encoded bytes", ErrClientStateCorrupt, len(encoded))
	}
	body, storedChecksum := encoded[:len(encoded)-32], encoded[len(encoded)-32:]
	checksum := sha256.Sum256(body)
	if !bytes.Equal(checksum[:], storedChecksum) {
		return ClientState{}, fmt.Errorf("%w: checksum mismatch", ErrClientStateCorrupt)
	}
	reader := body
	if string(reader[:4]) != clientStateMagic {
		return ClientState{}, fmt.Errorf("%w: bad magic", ErrClientStateCorrupt)
	}
	if binary.BigEndian.Uint16(reader[4:6]) != clientStateVersion {
		return ClientState{}, fmt.Errorf("%w: unsupported version %d", ErrClientStateCorrupt, binary.BigEndian.Uint16(reader[4:6]))
	}
	var state ClientState
	copy(state.ClusterID[:], reader[6:22])
	copy(state.ClientID[:], reader[22:38])
	state.NextSequence = binary.BigEndian.Uint64(reader[38:46])
	reader = reader[46:]

	pending, reader, err := readClientPayload(reader)
	if err != nil {
		return ClientState{}, err
	}
	if len(reader) < 32 {
		return ClientState{}, fmt.Errorf("%w: truncated pending digest", ErrClientStateCorrupt)
	}
	copy(state.PendingDigest[:], reader[:32])
	reader = reader[32:]
	resolved, reader, err := readClientPayload(reader)
	if err != nil {
		return ClientState{}, err
	}
	if len(reader) != 0 {
		return ClientState{}, fmt.Errorf("%w: %d trailing bytes", ErrClientStateCorrupt, len(reader))
	}
	state.Pending, state.Resolved = pending, resolved

	if state.ClusterID == ([16]byte{}) {
		return ClientState{}, fmt.Errorf("%w: zero cluster binding", ErrClientStateCorrupt)
	}
	if state.ClientID.Validate() != nil {
		return ClientState{}, fmt.Errorf("%w: zero client identity", ErrClientStateCorrupt)
	}
	if state.NextSequence == 0 {
		return ClientState{}, fmt.Errorf("%w: zero next sequence", ErrClientStateCorrupt)
	}
	if len(state.Pending) == 0 {
		if state.PendingDigest != ([32]byte{}) {
			return ClientState{}, fmt.Errorf("%w: pending digest without pending bytes", ErrClientStateCorrupt)
		}
	} else if state.PendingDigest != sha256.Sum256(state.Pending) {
		return ClientState{}, fmt.Errorf("%w: pending digest mismatch", ErrClientStateCorrupt)
	}
	return state, nil
}

// readClientPayload reads one bounded length-prefixed payload without
// allocating past the proven remaining bytes.
func readClientPayload(reader []byte) ([]byte, []byte, error) {
	if len(reader) < 4 {
		return nil, nil, fmt.Errorf("%w: truncated payload length", ErrClientStateCorrupt)
	}
	length := binary.BigEndian.Uint32(reader[:4])
	reader = reader[4:]
	if length > uint32(MaxClientPayloadBytes) || uint64(length) > uint64(len(reader)) {
		return nil, nil, fmt.Errorf("%w: payload length %d exceeds remaining bytes", ErrClientStateCorrupt, length)
	}
	if length == 0 {
		return nil, reader, nil
	}
	return append([]byte(nil), reader[:length]...), reader[length:], nil
}
