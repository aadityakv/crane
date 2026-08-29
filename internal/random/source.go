// Package random provides concurrency-safe random sources for protocol code.
package random

import (
	"math/rand"
	"sync"
)

// Source is the randomness seam used by selection and simulation code.
type Source interface {
	Uint64() uint64
	Intn(int) int
	Shuffle(int, func(int, int))
}

// LockedSource wraps a seeded math/rand.Rand for safe concurrent use.
type LockedSource struct {
	mu   sync.Mutex
	rand *rand.Rand
}

// NewLockedSource returns a concurrency-safe source initialized with seed.
func NewLockedSource(seed int64) *LockedSource {
	return &LockedSource{rand: rand.New(rand.NewSource(seed))}
}

// Uint64 returns a uniformly distributed random uint64.
func (s *LockedSource) Uint64() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rand.Uint64()
}

// Intn returns a uniformly distributed value in [0, n), and panics if n <= 0.
func (s *LockedSource) Intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rand.Intn(n)
}

// Shuffle pseudo-randomly permutes the indexes [0, n) using swap.
func (s *LockedSource) Shuffle(n int, swap func(i, j int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rand.Shuffle(n, swap)
}
