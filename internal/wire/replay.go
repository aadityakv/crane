package wire

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

	"crane/internal/clock"
)

var (
	// ErrReplay classifies a sender/request pair already accepted within the replay window.
	ErrReplay = errors.New("replayed wire request")
	// ErrTimestamp classifies a request timestamp outside the permitted replay window and future skew.
	ErrTimestamp = errors.New("wire request timestamp outside permitted window")
	// ErrReplayCacheFull classifies a request that cannot be verified because the bounded cache is full.
	ErrReplayCacheFull = errors.New("wire replay cache full")
	// ErrReplayConfiguration classifies a replay guard that cannot verify requests safely.
	ErrReplayConfiguration = errors.New("wire replay guard misconfigured")
)

// ReplayGuard rejects duplicate request IDs while bounding retained replay state.
type ReplayGuard struct {
	mu            sync.Mutex
	clock         clock.Clock
	window        time.Duration
	maxFutureSkew time.Duration
	maxEntries    int
	seen          map[replayKey]time.Time
	expirations   replayExpiryHeap
	invalid       map[replayKey]time.Time
	invalidExpiry replayExpiryHeap
}

type replayKey struct {
	senderID  uint16
	requestID RequestID
}

type replayExpiry struct {
	key       replayKey
	expiresAt time.Time
}

type replayExpiryHeap []replayExpiry

func (h replayExpiryHeap) Len() int           { return len(h) }
func (h replayExpiryHeap) Less(i, j int) bool { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h replayExpiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *replayExpiryHeap) Push(value any) {
	*h = append(*h, value.(replayExpiry))
}

func (h *replayExpiryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = replayExpiry{}
	*h = old[:last]
	return value
}

// NewReplayGuard returns a replay guard retaining at most maxEntries sender/request pairs.
func NewReplayGuard(source clock.Clock, window, maxFutureSkew time.Duration, maxEntries int) *ReplayGuard {
	return &ReplayGuard{
		clock:         source,
		window:        window,
		maxFutureSkew: maxFutureSkew,
		maxEntries:    maxEntries,
		seen:          make(map[replayKey]time.Time),
		invalid:       make(map[replayKey]time.Time),
	}
}

// Preflight rejects invalid timestamps and request IDs already observed as
// either valid or semantically invalid, without consuming accepted capacity.
func (g *ReplayGuard) Preflight(senderID uint16, requestID RequestID, timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.preflightLocked(senderID, requestID, timestamp, g.clock.Now())
}

// PreflightInvalid reserves no state but verifies that a semantically invalid
// request can be retained without evicting any live invalid replay identity.
// Invalid and accepted entries have separate capacities.
func (g *ReplayGuard) PreflightInvalid(senderID uint16, requestID RequestID, timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.preflightInvalidLocked(senderID, requestID, timestamp, g.clock.Now())
}

func (g *ReplayGuard) preflightLocked(senderID uint16, requestID RequestID, timestamp, now time.Time) error {
	if timestamp.After(now.Add(g.maxFutureSkew)) || !timestamp.Add(g.window).After(now) {
		return fmt.Errorf("%w: message %s, local %s", ErrTimestamp, timestamp, now)
	}

	g.purgeExpired(now)
	g.purgeInvalidExpired(now)
	key := replayKey{senderID: senderID, requestID: requestID}
	if _, exists := g.seen[key]; exists {
		return ErrReplay
	}
	if _, exists := g.invalid[key]; exists {
		return ErrReplay
	}
	if len(g.seen) >= g.maxEntries {
		return ErrReplayCacheFull
	}
	return nil
}

func (g *ReplayGuard) preflightInvalidLocked(senderID uint16, requestID RequestID, timestamp, now time.Time) error {
	if timestamp.After(now.Add(g.maxFutureSkew)) || !timestamp.Add(g.window).After(now) {
		return fmt.Errorf("%w: message %s, local %s", ErrTimestamp, timestamp, now)
	}

	g.purgeExpired(now)
	g.purgeInvalidExpired(now)
	key := replayKey{senderID: senderID, requestID: requestID}
	if _, exists := g.seen[key]; exists {
		return ErrReplay
	}
	if _, exists := g.invalid[key]; exists {
		return ErrReplay
	}
	if len(g.invalid) >= g.maxEntries {
		return ErrReplayCacheFull
	}
	return nil
}

// Commit records a preflighted, semantically valid sender/request pair.
func (g *ReplayGuard) Commit(senderID uint16, requestID RequestID, timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	if err := g.preflightLocked(senderID, requestID, timestamp, now); err != nil {
		return err
	}

	key := replayKey{senderID: senderID, requestID: requestID}
	expiresAt := timestamp.Add(g.window)
	g.seen[key] = expiresAt
	heap.Push(&g.expirations, replayExpiry{key: key, expiresAt: expiresAt})
	return nil
}

// CommitInvalid records a preflighted semantically invalid request without
// evicting another live invalid identity or consuming accepted capacity.
func (g *ReplayGuard) CommitInvalid(senderID uint16, requestID RequestID, timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	if err := g.preflightInvalidLocked(senderID, requestID, timestamp, now); err != nil {
		return err
	}
	key := replayKey{senderID: senderID, requestID: requestID}
	expiresAt := timestamp.Add(g.window)
	g.invalid[key] = expiresAt
	heap.Push(&g.invalidExpiry, replayExpiry{key: key, expiresAt: expiresAt})
	return nil
}

// Accept records a fresh sender/request pair or returns a classified rejection.
// It is the compatibility shorthand for Commit when no semantic validation is
// required between the timestamp/duplicate check and recording.
func (g *ReplayGuard) Accept(senderID uint16, requestID RequestID, timestamp time.Time) error {
	return g.Commit(senderID, requestID, timestamp)
}

// RecordInvalid remembers a preflighted request ID that failed semantic
// validation. Invalid IDs use a separate bounded cache and never consume
// capacity reserved for accepted messages.
func (g *ReplayGuard) RecordInvalid(senderID uint16, requestID RequestID, timestamp time.Time) {
	if g == nil || g.clock == nil || g.maxEntries <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	if timestamp.After(now.Add(g.maxFutureSkew)) || !timestamp.Add(g.window).After(now) {
		return
	}
	g.purgeExpired(now)
	g.purgeInvalidExpired(now)
	key := replayKey{senderID: senderID, requestID: requestID}
	if _, accepted := g.seen[key]; accepted {
		return
	}
	if _, exists := g.invalid[key]; exists {
		return
	}
	for len(g.invalid) >= g.maxEntries && g.invalidExpiry.Len() > 0 {
		evicted := heap.Pop(&g.invalidExpiry).(replayExpiry)
		if current, exists := g.invalid[evicted.key]; exists && current.Equal(evicted.expiresAt) {
			delete(g.invalid, evicted.key)
		}
	}
	if len(g.invalid) >= g.maxEntries {
		return
	}
	expiresAt := timestamp.Add(g.window)
	g.invalid[key] = expiresAt
	heap.Push(&g.invalidExpiry, replayExpiry{key: key, expiresAt: expiresAt})
}

// ValidateTimestamp checks only the replay-window and future-skew boundary.
func (g *ReplayGuard) ValidateTimestamp(timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	if timestamp.After(now.Add(g.maxFutureSkew)) || !timestamp.Add(g.window).After(now) {
		return fmt.Errorf("%w: message %s, local %s", ErrTimestamp, timestamp, now)
	}
	return nil
}

func (g *ReplayGuard) purgeExpired(now time.Time) {
	for g.expirations.Len() > 0 && !g.expirations[0].expiresAt.After(now) {
		expired := heap.Pop(&g.expirations).(replayExpiry)
		if current, exists := g.seen[expired.key]; exists && current.Equal(expired.expiresAt) {
			delete(g.seen, expired.key)
		}
	}
}

func (g *ReplayGuard) purgeInvalidExpired(now time.Time) {
	for g.invalidExpiry.Len() > 0 && !g.invalidExpiry[0].expiresAt.After(now) {
		expired := heap.Pop(&g.invalidExpiry).(replayExpiry)
		if current, exists := g.invalid[expired.key]; exists && current.Equal(expired.expiresAt) {
			delete(g.invalid, expired.key)
		}
	}
}
