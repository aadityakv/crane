package clientstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"crane/internal/crane/model"
)

var (
	testClusterA = [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	testClusterB = [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01}
)

// protectedStatePath returns a state path inside a fresh owner-only directory.
func protectedStatePath(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "crane-client")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "state.crane")
}

func openTestStore(t *testing.T, path string, cluster [16]byte) *ClientStore {
	t.Helper()
	store, err := OpenClientState(path, cluster)
	if err != nil {
		t.Fatalf("open client state %q: %v", path, err)
	}
	return store
}

func TestClientStateCreateEstablishesDurableIdentity(t *testing.T) {
	path := protectedStatePath(t)
	store := openTestStore(t, path, testClusterA)
	state := store.State()
	if state.ClusterID != testClusterA {
		t.Fatalf("created cluster binding = %x, want %x", state.ClusterID, testClusterA)
	}
	if state.ClientID.Validate() != nil {
		t.Fatal("created client ID must be nonzero")
	}
	if state.NextSequence != 1 {
		t.Fatalf("created next sequence = %d, want 1", state.NextSequence)
	}
	if len(state.Pending) != 0 || state.PendingDigest != ([32]byte{}) || len(state.Resolved) != 0 {
		t.Fatalf("created state carries payloads: %#v", state)
	}
	next := store.NextRequestID()
	if next.ClientID != state.ClientID || next.Sequence != 1 {
		t.Fatalf("next request ID = %#v", next)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("state file mode = %v, want owner-only regular file", info.Mode())
	}

	reopened := openTestStore(t, path, testClusterA)
	if reopened.State().ClientID != state.ClientID {
		t.Fatalf("reopened client ID = %x, want stable %x", reopened.State().ClientID, state.ClientID)
	}
	if reopened.State().NextSequence != 1 {
		t.Fatalf("reopened next sequence = %d, want 1", reopened.State().NextSequence)
	}
}

func TestClientStateRefusesUnprotectedFileOrParent(t *testing.T) {
	t.Run("group accessible parent directory", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "open-parent")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenClientState(filepath.Join(directory, "state.crane"), testClusterA); !errors.Is(err, ErrClientStateUnprotected) {
			t.Fatalf("open under group-accessible parent = %v, want ErrClientStateUnprotected", err)
		}
	})
	t.Run("group accessible state file", func(t *testing.T) {
		path := protectedStatePath(t)
		openTestStore(t, path, testClusterA)
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenClientState(path, testClusterA); !errors.Is(err, ErrClientStateUnprotected) {
			t.Fatalf("open group-readable state = %v, want ErrClientStateUnprotected", err)
		}
	})
	t.Run("state path is a directory", func(t *testing.T) {
		path := protectedStatePath(t)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenClientState(path, testClusterA); err == nil {
			t.Fatal("open on a directory path must fail")
		}
	})
	t.Run("missing parent directory", func(t *testing.T) {
		if _, err := OpenClientState(filepath.Join(t.TempDir(), "absent", "state.crane"), testClusterA); err == nil {
			t.Fatal("open under a missing parent must fail")
		}
	})
}

func TestClientStateBindsClusterIdentity(t *testing.T) {
	path := protectedStatePath(t)
	openTestStore(t, path, testClusterA)
	if _, err := OpenClientState(path, testClusterB); !errors.Is(err, ErrClientStateForeignCluster) {
		t.Fatalf("open with foreign cluster = %v, want ErrClientStateForeignCluster", err)
	}
	if _, err := OpenClientState(path, [16]byte{}); err == nil {
		t.Fatal("open with a zero cluster ID must fail")
	}
}

func TestClientStateBeginReservesSequenceBeforeSend(t *testing.T) {
	path := protectedStatePath(t)
	store := openTestStore(t, path, testClusterA)
	request := []byte("submit-request-bytes-v1")

	id, digest, err := store.Begin(request)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if id.ClientID != store.State().ClientID || id.Sequence != 1 {
		t.Fatalf("reserved request ID = %#v", id)
	}
	if digest != sha256.Sum256(request) {
		t.Fatalf("reserved digest = %x", digest)
	}

	// The reservation is durable before any network send: a reopened store
	// carries the exact pending bytes, digest, and unadvanced sequence.
	reopened := openTestStore(t, path, testClusterA)
	state := reopened.State()
	if !bytes.Equal(state.Pending, request) {
		t.Fatalf("durable pending = %q, want %q", state.Pending, request)
	}
	if state.PendingDigest != digest {
		t.Fatalf("durable pending digest = %x, want %x", state.PendingDigest, digest)
	}
	if state.NextSequence != 1 {
		t.Fatalf("sequence advanced before resolution: %d", state.NextSequence)
	}

	if _, _, err := reopened.Begin([]byte("second")); !errors.Is(err, ErrClientStatePending) {
		t.Fatalf("second begin with pending = %v, want ErrClientStatePending", err)
	}
}

func TestClientStateResolveAdvancesSequenceDurably(t *testing.T) {
	path := protectedStatePath(t)
	store := openTestStore(t, path, testClusterA)
	if err := store.Resolve([]byte("early")); !errors.Is(err, ErrClientStateNoPending) {
		t.Fatalf("resolve without pending = %v, want ErrClientStateNoPending", err)
	}

	request := []byte("submit-request-bytes-v1")
	resolution := []byte("submit-response-bytes-v1")
	if _, _, err := store.Begin(request); err != nil {
		t.Fatal(err)
	}
	if err := store.Resolve(resolution); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	reopened := openTestStore(t, path, testClusterA)
	state := reopened.State()
	if len(state.Pending) != 0 || state.PendingDigest != ([32]byte{}) {
		t.Fatalf("resolution retained pending: %#v", state)
	}
	if !bytes.Equal(state.Resolved, resolution) {
		t.Fatalf("durable resolution = %q, want %q", state.Resolved, resolution)
	}
	if state.NextSequence != 2 {
		t.Fatalf("resolved next sequence = %d, want 2", state.NextSequence)
	}

	id, _, err := reopened.Begin([]byte("next-command"))
	if err != nil {
		t.Fatal(err)
	}
	if id.Sequence != 2 {
		t.Fatalf("second reservation sequence = %d, want 2", id.Sequence)
	}
}

func TestClientStateBoundsPayloads(t *testing.T) {
	path := protectedStatePath(t)
	store := openTestStore(t, path, testClusterA)
	oversize := make([]byte, MaxClientPayloadBytes+1)
	for _, request := range [][]byte{nil, {}, oversize} {
		if _, _, err := store.Begin(request); !errors.Is(err, ErrClientStateBounds) {
			t.Fatalf("begin with %d bytes = %v, want ErrClientStateBounds", len(request), err)
		}
	}
	if _, _, err := store.Begin([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	for _, resolution := range [][]byte{nil, {}, oversize} {
		if err := store.Resolve(resolution); !errors.Is(err, ErrClientStateBounds) {
			t.Fatalf("resolve with %d bytes = %v, want ErrClientStateBounds", len(resolution), err)
		}
	}
	// The bounded rejections must not have consumed the reservation.
	if err := store.Resolve([]byte("resolution")); err != nil {
		t.Fatalf("resolve after bounded rejections: %v", err)
	}
}

func TestClientStateSequenceExhaustionFailsClosed(t *testing.T) {
	path := protectedStatePath(t)
	store := openTestStore(t, path, testClusterA)
	state := store.State()
	state.NextSequence = ^uint64(0)
	rewriteClientStateFile(t, path, state)

	reopened := openTestStore(t, path, testClusterA)
	if _, _, err := reopened.Begin([]byte("wraps")); !errors.Is(err, ErrClientStateExhausted) {
		t.Fatalf("begin at maximum sequence = %v, want ErrClientStateExhausted", err)
	}
}

func mustBegin(t *testing.T, store *ClientStore, request []byte) (model.ClientRequestID, [32]byte) {
	t.Helper()
	id, digest, err := store.Begin(request)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return id, digest
}
