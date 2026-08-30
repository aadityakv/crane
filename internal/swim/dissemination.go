package swim

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
)

var (
	// ErrInvalidByteBudget reports a negative outbound batch byte limit.
	ErrInvalidByteBudget = errors.New("swim: dissemination byte budget must not be negative")
	// ErrNilBatchEncoder reports a missing concrete update batch encoder.
	ErrNilBatchEncoder = errors.New("swim: dissemination batch encoder is nil")
)

// Disseminator retains a bounded set of current membership updates for
// prioritized retransmission. It is owned by the SWIM event-loop goroutine
// and deliberately provides no internal synchronization.
type Disseminator struct {
	// DigestRequired is set when current state cannot be admitted to the
	// bounded queue. The owner clears it only after an authenticated snapshot
	// repair for the current digest generation.
	DigestRequired bool

	maxItems             int
	retransmitMultiplier int
	nextSequence         uint64
	digestGeneration     uint64
	items                map[uint16]*disseminationItem
}

type disseminationItem struct {
	update        Update
	sequence      uint64
	transmissions int
}

type disseminationToken struct {
	nodeID   uint16
	sequence uint64
}

// DisseminationBatch is a non-mutating encoded-size selection. The owner calls
// Commit only after the datagram carrying Updates succeeds to a healthy peer.
type DisseminationBatch struct {
	Updates []Update
	tokens  []disseminationToken
	budget  int
}

// NewDisseminator returns an empty bounded dissemination queue. Non-positive
// bounds disable admission; a later valid Enqueue then sets DigestRequired.
func NewDisseminator(maxItems int, retransmitMultiplier int) *Disseminator {
	return &Disseminator{
		maxItems:             maxItems,
		retransmitMultiplier: retransmitMultiplier,
		nextSequence:         1,
		items:                make(map[uint16]*disseminationItem),
	}
}

// Enqueue coalesces update with the queued state for its node. A higher
// incarnation replaces the complete record; at an equal incarnation only a
// more severe status replaces the pending value. Invalid and stale updates
// are ignored.
func (d *Disseminator) Enqueue(update Update, aliveMembers int) {
	if !validUpdate(update) {
		return
	}

	nodeID := update.Member.NodeID
	if current, exists := d.items[nodeID]; exists {
		merged, changed := mergePendingUpdate(current.update, update)
		if !changed {
			return
		}
		update = merged
	}

	budget := RetransmitBudget(d.retransmitMultiplier, aliveMembers)
	if d.maxItems <= 0 || budget <= 0 {
		delete(d.items, nodeID)
		d.requireDigest()
		return
	}

	if _, exists := d.items[nodeID]; !exists && len(d.items) >= d.maxItems {
		d.requireDigest()
		return
	}

	d.items[nodeID] = &disseminationItem{
		update:   update,
		sequence: d.takeSequence(),
	}
}

func (d *Disseminator) requireDigest() {
	d.DigestRequired = true
	d.digestGeneration++
	if d.digestGeneration == 0 {
		d.digestGeneration = 1
	}
}

func (d *Disseminator) markDigestRepaired(generation uint64) bool {
	if !d.DigestRequired || generation == 0 || generation != d.digestGeneration {
		return false
	}
	d.DigestRequired = false
	return true
}

// Take removes items already exhausted under the current aliveMembers view,
// then returns the highest-priority prefix whose concrete encoding fits
// maxBytes. Only updates in a successfully encoded returned prefix count as
// transmissions. A zero byte limit still performs exhaustion cleanup.
func (d *Disseminator) Take(maxBytes int, aliveMembers int, encode func([]Update) ([]byte, error)) ([]Update, error) {
	batch, err := d.Peek(maxBytes, aliveMembers, encode)
	if err != nil {
		return nil, err
	}
	d.Commit(batch)
	return batch.Updates, nil
}

// Peek removes already exhausted items and returns a size-bounded selection
// without charging any retransmission. The returned batch remains safe to
// commit after newer state replaces one or more selected items.
func (d *Disseminator) Peek(maxBytes int, aliveMembers int, encode func([]Update) ([]byte, error)) (DisseminationBatch, error) {
	if maxBytes < 0 {
		return DisseminationBatch{}, ErrInvalidByteBudget
	}
	if encode == nil {
		return DisseminationBatch{}, ErrNilBatchEncoder
	}
	if len(d.items) == 0 {
		return DisseminationBatch{Updates: []Update{}}, nil
	}

	currentBudget := RetransmitBudget(d.retransmitMultiplier, aliveMembers)
	candidates := make([]*disseminationItem, 0, len(d.items))
	for nodeID, item := range d.items {
		if item.transmissions >= currentBudget {
			delete(d.items, nodeID)
			continue
		}
		candidates = append(candidates, item)
	}
	if maxBytes == 0 || len(candidates) == 0 {
		return DisseminationBatch{Updates: []Update{}, budget: currentBudget}, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.update.Member.Status != right.update.Member.Status {
			return left.update.Member.Status > right.update.Member.Status
		}
		if left.sequence != right.sequence {
			return left.sequence < right.sequence
		}
		return left.update.Member.NodeID < right.update.Member.NodeID
	})

	selected := make([]Update, 0, len(candidates))
	for _, item := range candidates {
		trial := append(selected, item.update)
		encoded, err := encode(append([]Update(nil), trial...))
		if err != nil {
			return DisseminationBatch{}, fmt.Errorf("encode dissemination batch: %w", err)
		}
		if len(encoded) > maxBytes {
			break
		}
		selected = trial
	}

	tokens := make([]disseminationToken, 0, len(selected))
	for _, update := range selected {
		item := d.items[update.Member.NodeID]
		tokens = append(tokens, disseminationToken{nodeID: update.Member.NodeID, sequence: item.sequence})
	}
	return DisseminationBatch{Updates: selected, tokens: tokens, budget: currentBudget}, nil
}

// Commit charges one successful healthy delivery for every still-current item
// selected by batch. Replaced items are ignored by sequence.
func (d *Disseminator) Commit(batch DisseminationBatch) {
	if batch.budget <= 0 {
		return
	}
	for _, token := range batch.tokens {
		item, exists := d.items[token.nodeID]
		if !exists || item.sequence != token.sequence {
			continue
		}
		item.transmissions++
		if item.transmissions >= batch.budget {
			delete(d.items, token.nodeID)
		}
	}
}

// RetransmitBudget returns multiplier*ceil(log2(aliveMembers+1)). Invalid
// inputs return zero, and multiplication overflow saturates at math.MaxInt.
func RetransmitBudget(multiplier int, aliveMembers int) int {
	if multiplier <= 0 || aliveMembers <= 0 {
		return 0
	}
	levels := bits.Len(uint(aliveMembers))
	if multiplier > math.MaxInt/levels {
		return math.MaxInt
	}
	return multiplier * levels
}

func validUpdate(update Update) bool {
	return update.Member.NodeID != 0 && update.Member.Incarnation != 0 && update.ReporterID != 0 && update.Member.Status.valid()
}

func mergePendingUpdate(current Update, incoming Update) (Update, bool) {
	switch {
	case incoming.Member.Incarnation < current.Member.Incarnation:
		return Update{}, false
	case incoming.Member.Incarnation > current.Member.Incarnation:
		return incoming, true
	case incoming.Member.Status <= current.Member.Status:
		return Update{}, false
	default:
		member := current.Member
		member.Status = incoming.Member.Status
		incoming.Member = member
		return incoming, true
	}
}

func (d *Disseminator) takeSequence() uint64 {
	sequence := d.nextSequence
	if d.nextSequence < math.MaxUint64 {
		d.nextSequence++
	}
	return sequence
}
