package wire

import (
	"encoding/hex"
	"testing"
)

func TestHMACMatchesSHA256TestVector(t *testing.T) {
	auth := NewHMACAuthenticator([]byte("Jefe"))
	want, err := hex.DecodeString("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("what do ya want for nothing?")
	mac := auth.Sign(message)
	if !auth.Verify(message, want) {
		t.Fatalf("Verify rejected RFC 4231 MAC; Sign returned %x", mac)
	}
	if hex.EncodeToString(mac) != hex.EncodeToString(want) {
		t.Fatalf("Sign = %x, want %x", mac, want)
	}

	mac[0] ^= 0xff
	if auth.Verify(message, mac) {
		t.Fatal("Verify accepted a modified MAC")
	}
	if auth.Verify(message, mac[:len(mac)-1]) {
		t.Fatal("Verify accepted a truncated MAC")
	}
}

func TestHMACAuthenticatorOwnsKeyMaterial(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	auth := NewHMACAuthenticator(key)
	want := auth.Sign([]byte("message"))

	for i := range key {
		key[i] = 0
	}
	if got := auth.Sign([]byte("message")); hex.EncodeToString(got) != hex.EncodeToString(want) {
		t.Fatalf("Sign changed after caller mutated key: got %x, want %x", got, want)
	}
}
