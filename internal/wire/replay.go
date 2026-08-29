package wire

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
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
	}
}

// Accept records a fresh sender/request pair or returns a classified rejection.
func (g *ReplayGuard) Accept(senderID uint16, requestID RequestID, timestamp time.Time) error {
	if g == nil || g.clock == nil {
		return ErrReplayConfiguration
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	if timestamp.After(now.Add(g.maxFutureSkew)) || !timestamp.Add(g.window).After(now) {
		return fmt.Errorf("%w: message %s, local %s", ErrTimestamp, timestamp, now)
	}

	g.purgeExpired(now)
	key := replayKey{senderID: senderID, requestID: requestID}
	if _, exists := g.seen[key]; exists {
		return ErrReplay
	}
	if len(g.seen) >= g.maxEntries {
		return ErrReplayCacheFull
	}

	expiresAt := timestamp.Add(g.window)
	g.seen[key] = expiresAt
	heap.Push(&g.expirations, replayExpiry{key: key, expiresAt: expiresAt})
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
