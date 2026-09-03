package clientstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"
)

// encodeClientStateBytes mirrors the canonical durable layout so tests can
// construct exact valid and semantically invalid files with real checksums.
func encodeClientStateBytes(state ClientState) []byte {
	encoded := make([]byte, 0, 128+len(state.Pending)+len(state.Resolved))
	encoded = append(encoded, "CRCS"...)
	encoded = binary.BigEndian.AppendUint16(encoded, 1)
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

// rewriteClientStateFile atomically replaces the state file with an exact
// encoding of state, including a valid trailing checksum.
func rewriteClientStateFile(t *testing.T, path string, state ClientState) {
	t.Helper()
	if err := os.WriteFile(path, encodeClientStateBytes(state), 0o600); err != nil {
		t.Fatal(err)
	}
}

func validTestState() ClientState {
	pending := []byte("pending-request-bytes")
	return ClientState{
		ClusterID:     testClusterA,
		ClientID:      [16]byte{0xaa, 0xbb, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		NextSequence:  7,
		Pending:       pending,
		PendingDigest: sha256.Sum256(pending),
		Resolved:      []byte("prior-resolution-bytes"),
	}
}

func TestClientStateCorruptionRefusal(t *testing.T) {
	base := encodeClientStateBytes(validTestState())
	corrupt := func(mutate func(ClientState) ClientState) []byte {
		return encodeClientStateBytes(mutate(validTestState()))
	}
	cases := []struct {
		name    string
		encoded []byte
	}{
		{name: "flipped checksum byte", encoded: func() []byte {
			mutated := append([]byte(nil), base...)
			mutated[len(mutated)-1] ^= 0xff
			return mutated
		}()},
		{name: "flipped payload byte", encoded: func() []byte {
			mutated := append([]byte(nil), base...)
			mutated[40] ^= 0x01
			return mutated
		}()},
		{name: "truncated file", encoded: base[:10]},
		{name: "trailing garbage", encoded: append(append([]byte(nil), base...), 0x00)},
		{name: "wrong magic", encoded: func() []byte {
			mutated := append([]byte(nil), base...)
			mutated[0] = 'X'
			return mutated
		}()},
		{name: "unsupported version", encoded: func() []byte {
			mutated := append([]byte(nil), base...)
			mutated[4], mutated[5] = 0xff, 0xfe
			return mutated
		}()},
		{name: "declared pending length beyond file", encoded: func() []byte {
			mutated := append([]byte(nil), base...)
			binary.BigEndian.PutUint32(mutated[46:50], 1<<24)
			return mutated
		}()},
		{name: "zero client identity", encoded: corrupt(func(state ClientState) ClientState {
			state.ClientID = [16]byte{}
			return state
		})},
		{name: "zero next sequence", encoded: corrupt(func(state ClientState) ClientState {
			state.NextSequence = 0
			return state
		})},
		{name: "pending digest mismatch", encoded: corrupt(func(state ClientState) ClientState {
			state.PendingDigest[0] ^= 0xff
			return state
		})},
		{name: "digest without pending bytes", encoded: corrupt(func(state ClientState) ClientState {
			state.Pending = nil
			return state
		})},
		{name: "empty file", encoded: []byte{}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			path := protectedStatePath(t)
			if err := os.WriteFile(path, testCase.encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenClientState(path, testClusterA); !errors.Is(err, ErrClientStateCorrupt) {
				t.Fatalf("open corrupt state = %v, want ErrClientStateCorrupt", err)
			}
		})
	}
}

func TestClientStateOversizeFileRefusal(t *testing.T) {
	path := protectedStatePath(t)
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x5a}, 2*MaxClientPayloadBytes+4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenClientState(path, testClusterA); !errors.Is(err, ErrClientStateCorrupt) {
		t.Fatalf("open oversize state = %v, want ErrClientStateCorrupt", err)
	}
}

// failAtBoundary aborts one persist at the exact named durability boundary,
// simulating a crash whose surviving bytes are whatever preceded the boundary.
func failAtBoundary(target string) func(string) error {
	return func(boundary string) error {
		if boundary == target {
			return fmt.Errorf("injected crash at %s", boundary)
		}
		return nil
	}
}

func TestClientStateReopenAfterCreationCrashBoundaries(t *testing.T) {
	for _, boundary := range persistBoundaries {
		t.Run(boundary, func(t *testing.T) {
			path := protectedStatePath(t)
			if _, err := openClientStateAt(path, testClusterA, failAtBoundary(boundary)); err == nil {
				t.Fatal("creation with an injected crash must fail")
			}
			store := openTestStore(t, path, testClusterA)
			state := store.State()
			if state.ClientID.Validate() != nil || state.NextSequence != 1 || len(state.Pending) != 0 {
				t.Fatalf("recovered creation state = %#v", state)
			}
		})
	}
}

func TestClientStateReopenAfterBeginCrashBoundaries(t *testing.T) {
	request := []byte("crash-boundary-request")
	for _, boundary := range persistBoundaries {
		t.Run(boundary, func(t *testing.T) {
			path := protectedStatePath(t)
			store := openTestStore(t, path, testClusterA)
			identity := store.State().ClientID
			store.crash = failAtBoundary(boundary)
			if _, _, err := store.Begin(request); err == nil {
				t.Fatal("begin with an injected crash must fail")
			}

			reopened := openTestStore(t, path, testClusterA)
			state := reopened.State()
			if state.ClientID != identity {
				t.Fatalf("client identity changed across crash: %x != %x", state.ClientID, identity)
			}
			if state.NextSequence != 1 {
				t.Fatalf("sequence advanced during begin crash: %d", state.NextSequence)
			}
			switch {
			case len(state.Pending) == 0:
				// The reservation did not survive: re-reserving uses the same
				// never-sent sequence.
				id, _ := mustBegin(t, reopened, request)
				if id.Sequence != 1 {
					t.Fatalf("re-reserved sequence = %d, want 1", id.Sequence)
				}
			case bytes.Equal(state.Pending, request):
				// The reservation survives complete: resolution still advances.
				if state.PendingDigest != sha256.Sum256(request) {
					t.Fatalf("surviving pending digest = %x", state.PendingDigest)
				}
			default:
				t.Fatalf("torn pending bytes after crash: %q", state.Pending)
			}
		})
	}
}

func TestClientStateReopenAfterResolveCrashBoundaries(t *testing.T) {
	request := []byte("crash-boundary-request")
	resolution := []byte("crash-boundary-resolution")
	for _, boundary := range persistBoundaries {
		t.Run(boundary, func(t *testing.T) {
			path := protectedStatePath(t)
			store := openTestStore(t, path, testClusterA)
			mustBegin(t, store, request)
			store.crash = failAtBoundary(boundary)
			if err := store.Resolve(resolution); err == nil {
				t.Fatal("resolve with an injected crash must fail")
			}

			reopened := openTestStore(t, path, testClusterA)
			state := reopened.State()
			switch {
			case bytes.Equal(state.Pending, request) && state.NextSequence == 1:
				// Resolution lost: the exact pending request is still retryable
				// and a repeated resolution advances exactly once.
				if err := reopened.Resolve(resolution); err != nil {
					t.Fatalf("repeat resolve: %v", err)
				}
				if reopened.State().NextSequence != 2 {
					t.Fatalf("sequence after repeat resolve = %d", reopened.State().NextSequence)
				}
			case len(state.Pending) == 0 && state.NextSequence == 2:
				// Resolution survived complete: it can never be applied twice.
				if !bytes.Equal(state.Resolved, resolution) {
					t.Fatalf("surviving resolution = %q", state.Resolved)
				}
				if err := reopened.Resolve(resolution); !errors.Is(err, ErrClientStateNoPending) {
					t.Fatalf("double resolve = %v, want ErrClientStateNoPending", err)
				}
			default:
				t.Fatalf("torn resolve state after crash: %#v", state)
			}
		})
	}
}

// TestClientStateOpenRemovesOrphanedTemporary pins that a crash between the
// temporary write and the rename never leaves <path>.tmp behind: opening the
// state removes the orphan whether or not the durable file exists.
func TestClientStateOpenRemovesOrphanedTemporary(t *testing.T) {
	t.Run("ExistingState", func(t *testing.T) {
		path := protectedStatePath(t)
		identity := openTestStore(t, path, testClusterA).State().ClientID
		if err := os.WriteFile(path+".tmp", []byte("torn temporary"), 0o600); err != nil {
			t.Fatal(err)
		}
		reopened := openTestStore(t, path, testClusterA)
		if reopened.State().ClientID != identity {
			t.Fatal("orphaned temporary changed the durable identity")
		}
		if _, err := os.Lstat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphaned temporary survived open: %v", err)
		}
	})
	t.Run("MissingState", func(t *testing.T) {
		path := protectedStatePath(t)
		if err := os.WriteFile(path+".tmp", []byte("torn temporary"), 0o600); err != nil {
			t.Fatal(err)
		}
		store := openTestStore(t, path, testClusterA)
		if store.State().NextSequence != 1 {
			t.Fatalf("fresh state sequence = %d, want 1", store.State().NextSequence)
		}
		if _, err := os.Lstat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphaned temporary survived creation: %v", err)
		}
	})
}
