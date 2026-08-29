package wire

import (
	"crypto/hmac"
	"crypto/sha256"
)

// Authenticator signs canonical frame bytes and verifies their trailing MAC.
type Authenticator interface {
	Sign(message []byte) []byte
	Verify(message, mac []byte) bool
}

// HMACAuthenticator authenticates frames with HMAC-SHA256 and an owned key copy.
type HMACAuthenticator struct {
	key []byte
}

// NewHMACAuthenticator returns an authenticator that retains its own copy of key.
func NewHMACAuthenticator(key []byte) *HMACAuthenticator {
	return &HMACAuthenticator{key: append([]byte(nil), key...)}
}

// Sign returns the HMAC-SHA256 of message in a newly allocated slice.
func (a *HMACAuthenticator) Sign(message []byte) []byte {
	mac := hmac.New(sha256.New, a.key)
	_, _ = mac.Write(message)
	return mac.Sum(nil)
}

// Verify compares mac with the expected HMAC-SHA256 in constant time.
func (a *HMACAuthenticator) Verify(message, macBytes []byte) bool {
	expected := a.Sign(message)
	return hmac.Equal(expected, macBytes)
}
