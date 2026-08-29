package swim

import (
	"errors"
	"math"
	"time"

	"github.com/aaditya/cs425mp3/internal/random"
)

var (
	// ErrInvalidEngineConfig reports an unusable identity or timing policy.
	ErrInvalidEngineConfig = errors.New("swim: invalid engine config")
	// ErrNilEngineTable reports a missing owner-confined membership table.
	ErrNilEngineTable = errors.New("swim: engine table is nil")
	// ErrNilEngineDisseminator reports a missing owner-confined update queue.
	ErrNilEngineDisseminator = errors.New("swim: engine disseminator is nil")
	// ErrNilEngineRandomSource reports a missing deterministic randomness seam.
	ErrNilEngineRandomSource = errors.New("swim: engine random source is nil")
)

// EngineConfig contains identity and deterministic SWIM timing policy.
type EngineConfig struct {
	SelfID               uint16
	ProbeInterval        time.Duration
	DirectProbeTimeout   time.Duration
	IndirectProbeTimeout time.Duration
	IndirectChecks       uint16
	SuspicionMultiplier  uint16
}

// Engine is the single owner of SWIM membership and probe state.
type Engine struct {
	config        EngineConfig
	table         *Table
	dissemination *Disseminator
	source        random.Source
	selector      probeSelector
	nextSequence  uint64
	activeProbes  map[uint64]*activeProbe
	relayProbes   map[relayProbeKey]relayProbe
}

type probePhase uint8

const (
	probeDirect probePhase = iota
	probeIndirect
)

type activeProbe struct {
	target Member
	phase  probePhase
	relays map[uint16]Member
}

type relayProbeKey struct {
	originID uint16
	sequence uint64
}

type relayProbe struct {
	origin Member
	target Member
}

// NewEngine constructs an owner-confined SWIM state machine.
func NewEngine(config EngineConfig, table *Table, dissemination *Disseminator, source random.Source) (*Engine, error) {
	if config.SelfID == 0 || config.ProbeInterval <= 0 || config.DirectProbeTimeout <= 0 || config.IndirectProbeTimeout <= 0 || config.IndirectChecks == 0 || config.SuspicionMultiplier == 0 {
		return nil, ErrInvalidEngineConfig
	}
	if config.DirectProbeTimeout > config.ProbeInterval-config.IndirectProbeTimeout {
		return nil, ErrInvalidEngineConfig
	}
	if table == nil {
		return nil, ErrNilEngineTable
	}
	if dissemination == nil {
		return nil, ErrNilEngineDisseminator
	}
	if source == nil {
		return nil, ErrNilEngineRandomSource
	}
	sequence := source.Uint64() & uint64(math.MaxInt64)
	if sequence == 0 {
		sequence = 1
	}
	return &Engine{
		config:        config,
		table:         table,
		dissemination: dissemination,
		source:        source,
		selector:      probeSelector{source: source},
		nextSequence:  sequence,
		activeProbes:  make(map[uint64]*activeProbe),
		relayProbes:   make(map[relayProbeKey]relayProbe),
	}, nil
}

// BeginProbe selects the next eligible peer and returns a direct ping and its
// generation-specific timeout. An empty membership view produces no effects.
func (e *Engine) BeginProbe(now time.Time) Effects {
	target, ok := e.selector.next(e.table.Snapshot(), e.config.SelfID)
	if !ok {
		return Effects{}
	}
	sequence := e.takeSequence()
	e.activeProbes[sequence] = &activeProbe{target: target, phase: probeDirect}
	return Effects{
		Outbound: []Outbound{{
			To:      target,
			Message: Ping{OriginID: e.config.SelfID, Sequence: sequence},
		}},
		Timers: []TimerRequest{{
			Kind:     TimerDirectProbe,
			Sequence: sequence,
			Deadline: now.Add(e.config.DirectProbeTimeout),
		}},
	}
}

// HandleDirectTimeout advances a still-current direct probe generation into
// its indirect phase. Duplicate, late, and phase-mismatched callbacks are
// harmless.
func (e *Engine) HandleDirectTimeout(sequence uint64, now time.Time) Effects {
	probe, exists := e.activeProbes[sequence]
	if !exists || probe.phase != probeDirect {
		return Effects{}
	}
	currentTarget, exists := e.table.Get(probe.target.NodeID)
	if !exists || currentTarget.Status != Alive || !sameMemberGeneration(currentTarget, probe.target) {
		delete(e.activeProbes, sequence)
		return Effects{}
	}

	relays := e.selectRelays(probe.target)
	probe.phase = probeIndirect
	probe.relays = make(map[uint16]Member, len(relays))
	outbound := make([]Outbound, 0, len(relays))
	for _, relay := range relays {
		probe.relays[relay.NodeID] = relay
		outbound = append(outbound, Outbound{
			To: relay,
			Message: PingReq{
				OriginID: e.config.SelfID,
				Target:   probe.target,
				Sequence: sequence,
			},
		})
	}
	return Effects{
		Outbound: outbound,
		Timers: []TimerRequest{{
			Kind:     TimerIndirectProbe,
			Sequence: sequence,
			Deadline: now.Add(e.config.IndirectProbeTimeout),
		}},
	}
}

// HandlePing acknowledges a valid current peer while preserving the original
// probe identity through a relay hop.
func (e *Engine) HandlePing(from Member, message Ping, _ time.Time) Effects {
	originID := message.OriginID
	if originID == 0 {
		originID = from.NodeID
	}
	if message.Sequence == 0 || !e.currentAlivePeer(from) {
		return Effects{}
	}
	return Effects{Outbound: []Outbound{{
		To:      from,
		Message: Ack{OriginID: originID, Sequence: message.Sequence},
	}}}
}

// HandlePingReq records a validated relay generation, pings the requested
// target, and schedules bounded cleanup if no matching ACK arrives.
func (e *Engine) HandlePingReq(from Member, message PingReq, now time.Time) Effects {
	originID := message.OriginID
	if originID == 0 {
		originID = from.NodeID
	}
	if message.Sequence == 0 || originID != from.NodeID || !e.currentAlivePeer(from) {
		return Effects{}
	}
	if message.Target.NodeID == e.config.SelfID || message.Target.NodeID == from.NodeID {
		return Effects{}
	}
	target, exists := e.table.Get(message.Target.NodeID)
	if !exists || target.Status != Alive || !sameProbeIdentity(target, message.Target) {
		return Effects{}
	}
	key := relayProbeKey{originID: originID, sequence: message.Sequence}
	if _, exists := e.relayProbes[key]; exists {
		return Effects{}
	}
	e.relayProbes[key] = relayProbe{origin: from, target: target}
	return Effects{
		Outbound: []Outbound{{
			To:      target,
			Message: Ping{OriginID: originID, Sequence: message.Sequence},
		}},
		Timers: []TimerRequest{{
			Kind:     TimerRelayProbe,
			OriginID: originID,
			Sequence: message.Sequence,
			Deadline: now.Add(e.config.DirectProbeTimeout),
		}},
	}
}

// HandleAck completes a local direct or late-direct probe, or turns a target
// ACK for a relay generation into an IndirectAck to the preserved origin.
func (e *Engine) HandleAck(from Member, message Ack, _ time.Time) Effects {
	originID := message.OriginID
	if originID == 0 || originID == e.config.SelfID {
		probe, exists := e.activeProbes[message.Sequence]
		if exists && sameProbeIdentity(from, probe.target) {
			delete(e.activeProbes, message.Sequence)
			return Effects{}
		}
	}

	key := relayProbeKey{originID: originID, sequence: message.Sequence}
	probe, exists := e.relayProbes[key]
	if !exists || !sameProbeIdentity(from, probe.target) {
		return Effects{}
	}
	delete(e.relayProbes, key)
	return Effects{Outbound: []Outbound{{
		To: probe.origin,
		Message: IndirectAck{
			OriginID: originID,
			Target:   probe.target,
			Sequence: message.Sequence,
		},
	}}}
}

// HandleIndirectAck completes only an indirect-phase generation from one of
// the relays authorized for that probe and for the exact target identity.
func (e *Engine) HandleIndirectAck(from Member, message IndirectAck, _ time.Time) Effects {
	probe, exists := e.activeProbes[message.Sequence]
	if !exists || probe.phase != probeIndirect || (message.OriginID != 0 && message.OriginID != e.config.SelfID) || !sameProbeIdentity(message.Target, probe.target) {
		return Effects{}
	}
	relay, allowed := probe.relays[from.NodeID]
	if !allowed || !sameProbeIdentity(from, relay) {
		return Effects{}
	}
	delete(e.activeProbes, message.Sequence)
	return Effects{}
}

// HandleRelayTimeout discards only the named origin/sequence generation.
func (e *Engine) HandleRelayTimeout(originID uint16, sequence uint64, _ time.Time) Effects {
	delete(e.relayProbes, relayProbeKey{originID: originID, sequence: sequence})
	return Effects{}
}

// HandleIndirectTimeout consumes only a matching indirect-phase generation.
// Task 10 attaches suspicion effects to this transition.
func (e *Engine) HandleIndirectTimeout(sequence uint64, _ time.Time) Effects {
	probe, exists := e.activeProbes[sequence]
	if !exists || probe.phase != probeIndirect {
		return Effects{}
	}
	delete(e.activeProbes, sequence)
	return Effects{}
}

// Snapshot returns the engine owner's immutable, sorted membership view.
func (e *Engine) Snapshot() []Member {
	return e.table.Snapshot()
}

func (e *Engine) takeSequence() uint64 {
	sequence := e.nextSequence
	e.nextSequence++
	if e.nextSequence == 0 {
		e.nextSequence = 1
	}
	return sequence
}

func (e *Engine) selectRelays(target Member) []Member {
	members := e.table.Snapshot()
	relays := make([]Member, 0, len(members))
	for _, member := range members {
		if member.Status == Alive && member.NodeID != e.config.SelfID && member.NodeID != target.NodeID {
			relays = append(relays, member)
		}
	}
	e.source.Shuffle(len(relays), func(i, j int) {
		relays[i], relays[j] = relays[j], relays[i]
	})
	limit := int(e.config.IndirectChecks)
	if len(relays) > limit {
		relays = relays[:limit]
	}
	return relays
}

func sameMemberGeneration(left, right Member) bool {
	return left.NodeID == right.NodeID && left.Incarnation == right.Incarnation
}

func sameProbeIdentity(left, right Member) bool {
	return sameMemberGeneration(left, right) && left.Host == right.Host && left.BasePort == right.BasePort
}

func (e *Engine) currentAlivePeer(peer Member) bool {
	if peer.NodeID == 0 || peer.NodeID == e.config.SelfID {
		return false
	}
	current, exists := e.table.Get(peer.NodeID)
	return exists && current.Status == Alive && sameProbeIdentity(current, peer)
}
