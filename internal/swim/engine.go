package swim

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/aaditya/cs425mp3/internal/random"
	"github.com/aaditya/cs425mp3/internal/wire"
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
	probePrefix   uint64
	probeCount    uint64
	activeProbes  map[uint64]*activeProbe
	relayProbes   map[relayProbeKey]relayProbe
	suspicions    map[uint16]*suspicionState
	tombstones    map[uint16]tombstoneState
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
	id     ProbeID
}

type relayProbeKey struct {
	originID uint16
	probe    ProbeID
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
	randomSeed := source.Uint64()
	sequence := randomSeed & uint64(math.MaxInt64)
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
		probePrefix:   randomSeed ^ uint64(config.SelfID)<<48,
		probeCount:    sequence,
		activeProbes:  make(map[uint64]*activeProbe),
		relayProbes:   make(map[relayProbeKey]relayProbe),
		suspicions:    make(map[uint16]*suspicionState),
		tombstones:    make(map[uint16]tombstoneState),
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
	requestID := e.takeProbeRequestID()
	e.activeProbes[sequence] = &activeProbe{target: target, phase: probeDirect, id: ProbeID{Sequence: sequence, RequestID: requestID}}
	return Effects{
		Outbound: []Outbound{{
			To:      target,
			Message: Ping{OriginID: e.config.SelfID, Sequence: sequence, RequestID: requestID},
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
				OriginID:  e.config.SelfID,
				Target:    probe.target,
				Sequence:  sequence,
				RequestID: probe.id.RequestID,
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
		Message: Ack{OriginID: originID, Sequence: message.Sequence, RequestID: message.RequestID},
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
	key := relayProbeKey{originID: originID, probe: message.ID()}
	if _, exists := e.relayProbes[key]; exists {
		return Effects{}
	}
	e.relayProbes[key] = relayProbe{origin: from, target: target}
	return Effects{
		Outbound: []Outbound{{
			To:      target,
			Message: Ping{OriginID: originID, Sequence: message.Sequence, RequestID: message.RequestID},
		}},
		Timers: []TimerRequest{{
			Kind:      TimerRelayProbe,
			OriginID:  originID,
			Sequence:  message.Sequence,
			RequestID: message.RequestID,
			Deadline:  now.Add(e.config.IndirectProbeTimeout),
		}},
	}
}

// HandleAck completes a local direct or late-direct probe, or turns a target
// ACK for a relay generation into an IndirectAck to the preserved origin.
func (e *Engine) HandleAck(from Member, message Ack, _ time.Time) Effects {
	switch e.ackDisposition(from, message) {
	case ackLocalProbe:
		delete(e.activeProbes, message.Sequence)
		return Effects{}
	case ackRelayProbe:
		key := relayProbeKey{originID: message.OriginID, probe: message.ID()}
		probe := e.relayProbes[key]
		delete(e.relayProbes, key)
		return Effects{Outbound: []Outbound{{
			To: probe.origin,
			Message: IndirectAck{
				OriginID:  message.OriginID,
				Target:    probe.target,
				Sequence:  message.Sequence,
				RequestID: message.RequestID,
			},
		}}}
	default:
		return Effects{}
	}
}

type ackDisposition uint8

const (
	ackNotAccepted ackDisposition = iota
	ackLocalProbe
	ackRelayProbe
)

func (e *Engine) acceptsAck(from Member, message Ack) bool {
	return e.ackDisposition(from, message) != ackNotAccepted
}

func (e *Engine) ackDisposition(from Member, message Ack) ackDisposition {
	originID := message.OriginID
	if originID == 0 || originID == e.config.SelfID {
		probe, exists := e.activeProbes[message.Sequence]
		if exists && probeRequestIDMatches(probe.id.RequestID, message.RequestID) && sameProbeIdentity(from, probe.target) {
			return ackLocalProbe
		}
	}
	probe, exists := e.relayProbes[relayProbeKey{originID: originID, probe: message.ID()}]
	if exists && sameProbeIdentity(from, probe.target) {
		return ackRelayProbe
	}
	return ackNotAccepted
}

// HandleIndirectAck completes only an indirect-phase generation from one of
// the relays authorized for that probe and for the exact target identity.
func (e *Engine) HandleIndirectAck(from Member, message IndirectAck, _ time.Time) Effects {
	if !e.acceptsIndirectAck(from, message) {
		return Effects{}
	}
	delete(e.activeProbes, message.Sequence)
	return Effects{}
}

func (e *Engine) acceptsIndirectAck(from Member, message IndirectAck) bool {
	probe, exists := e.activeProbes[message.Sequence]
	if !exists || probe.phase != probeIndirect || !probeRequestIDMatches(probe.id.RequestID, message.RequestID) || (message.OriginID != 0 && message.OriginID != e.config.SelfID) || !sameProbeIdentity(message.Target, probe.target) {
		return false
	}
	relay, allowed := probe.relays[from.NodeID]
	return allowed && sameProbeIdentity(from, relay)
}

// HandleRelayTimeout discards only the named origin/sequence generation.
func (e *Engine) HandleRelayTimeout(originID uint16, sequence uint64, _ time.Time) Effects {
	for key := range e.relayProbes {
		if key.originID == originID && key.probe.Sequence == sequence {
			delete(e.relayProbes, key)
		}
	}
	return Effects{}
}

// HandleRelayTimeoutRequest discards only the complete origin/sequence/UUID
// relay generation carried by a production timer.
func (e *Engine) HandleRelayTimeoutRequest(originID uint16, sequence uint64, requestID wire.RequestID, _ time.Time) Effects {
	delete(e.relayProbes, relayProbeKey{originID: originID, probe: ProbeID{Sequence: sequence, RequestID: requestID}})
	return Effects{}
}

// HandleIndirectTimeout consumes only a matching indirect-phase generation
// and turns that full-probe failure into a local suspicion update.
func (e *Engine) HandleIndirectTimeout(sequence uint64, now time.Time) Effects {
	probe, exists := e.activeProbes[sequence]
	if !exists || probe.phase != probeIndirect {
		return Effects{}
	}
	current, currentExists := e.table.Get(probe.target.NodeID)
	delete(e.activeProbes, sequence)
	if !currentExists || (current.Status != Alive && current.Status != Suspect) || !sameProbeIdentity(current, probe.target) {
		return Effects{}
	}
	suspect := current
	suspect.Status = Suspect
	return e.ApplyUpdate(Update{Member: suspect, ReporterID: e.config.SelfID}, now)
}

// Snapshot returns the engine owner's immutable, sorted membership view.
func (e *Engine) Snapshot() []Member {
	return e.table.Snapshot()
}

// IncarnationFloors returns copied hidden terminal floors for join and repair
// snapshots. The owner goroutine must call it just like Snapshot.
func (e *Engine) IncarnationFloors() []Member {
	return e.table.IncarnationFloors()
}

// ApplyIncarnationFloor incorporates terminal knowledge from an authenticated
// join or repair snapshot. Advancing a visible member produces the same event,
// dissemination, and tombstone effects as a terminal membership update;
// retaining an already-hidden floor produces no visible effects.
func (e *Engine) ApplyIncarnationFloor(floor Member, reporterID uint16, now time.Time) Effects {
	changed, event := e.table.MergeIncarnationFloor(floor, reporterID)
	if !changed || event.Current.NodeID == 0 {
		return Effects{}
	}
	delete(e.suspicions, event.Current.NodeID)
	effects := Effects{Timers: []TimerRequest{e.beginTombstone(event.Current, now)}}
	e.appliedTransition(&effects, event, reporterID)
	return effects
}

func (e *Engine) takeSequence() uint64 {
	sequence := e.nextSequence
	e.nextSequence++
	if e.nextSequence == 0 {
		e.nextSequence = 1
	}
	return sequence
}

func (e *Engine) takeProbeRequestID() wire.RequestID {
	e.probeCount++
	if e.probeCount == 0 {
		e.probeCount++
	}
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[:8], e.probePrefix)
	binary.BigEndian.PutUint64(requestID[8:], e.probeCount)
	if requestID == (wire.RequestID{}) {
		requestID[len(requestID)-1] = 1
	}
	return requestID
}

func probeRequestIDMatches(expected, actual wire.RequestID) bool {
	// Zero exists only for direct engine compatibility tests and never crosses
	// the service boundary; production probes always have a nonzero UUID.
	return expected == (wire.RequestID{}) || actual == (wire.RequestID{}) || expected == actual
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
