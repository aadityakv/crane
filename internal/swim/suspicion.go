package swim

import (
	"math"
	"math/bits"
	"time"
)

const minimumSuspicionDuration = 5 * time.Second

type suspicionState struct {
	incarnation uint64
	deadline    time.Time
	duration    time.Duration
	reporters   map[uint16]struct{}
}

type tombstoneState struct {
	incarnation uint64
	status      Status
	deadline    time.Time
}

// SuspicionDuration returns the cluster-scaled suspicion window with the
// protocol's five-second lower bound. Overflow saturates at the largest
// representable duration rather than wrapping to an early deadline.
func SuspicionDuration(multiplier int, probeInterval time.Duration, aliveMembers int) time.Duration {
	if multiplier <= 0 || probeInterval <= 0 || aliveMembers <= 0 {
		return minimumSuspicionDuration
	}
	levels := bits.Len(uint(aliveMembers))
	if multiplier > math.MaxInt/levels {
		return time.Duration(math.MaxInt64)
	}
	factor := multiplier * levels
	if probeInterval > time.Duration(math.MaxInt64)/time.Duration(factor) {
		return time.Duration(math.MaxInt64)
	}
	duration := time.Duration(factor) * probeInterval
	if duration < minimumSuspicionDuration {
		return minimumSuspicionDuration
	}
	return duration
}

// ApplyUpdate merges one validated membership update, enqueues the resulting
// current record for dissemination, and returns its event and timer effects.
// A current-incarnation self suspicion is refuted with a persisted higher
// incarnation; equal-incarnation Alive updates never refute another member.
func (e *Engine) ApplyUpdate(update Update, now time.Time) Effects {
	if !validUpdate(update) {
		return Effects{}
	}

	current, exists := e.table.Get(update.Member.NodeID)
	if exists && current.NodeID == e.config.SelfID && update.Member.Status == Suspect && update.Member.Incarnation == current.Incarnation && current.Status <= Suspect {
		if current.Incarnation == math.MaxUint64 {
			return Effects{}
		}
		refutation := current
		refutation.Incarnation++
		refutation.Status = Alive
		incarnation := refutation.Incarnation
		effects := Effects{PersistIncarnation: &incarnation}
		changed, event := e.table.Merge(Update{Member: refutation, ReporterID: e.config.SelfID})
		if !changed {
			return Effects{}
		}
		delete(e.suspicions, current.NodeID)
		delete(e.tombstones, current.NodeID)
		e.appliedTransition(&effects, event, e.config.SelfID)
		return effects
	}

	aliveBefore := e.aliveMembers()
	changed, event := e.table.Merge(update)
	if !changed {
		if exists && update.Member.Status == Suspect && current.Status == Suspect && update.Member.Incarnation == current.Incarnation {
			return e.corroborateSuspicion(current, update.ReporterID, now)
		}
		return Effects{}
	}

	effects := Effects{}
	switch event.Current.Status {
	case Alive:
		delete(e.suspicions, event.Current.NodeID)
		delete(e.tombstones, event.Current.NodeID)
	case Suspect:
		delete(e.tombstones, event.Current.NodeID)
		effects.Timers = append(effects.Timers, e.beginSuspicion(event.Current, update.ReporterID, aliveBefore, now))
	case Dead, Left:
		delete(e.suspicions, event.Current.NodeID)
		effects.Timers = append(effects.Timers, e.beginTombstone(event.Current, now))
	}
	e.appliedTransition(&effects, event, update.ReporterID)
	return effects
}

// HandleSuspicionTimeout promotes only the still-current suspected generation
// whose latest (possibly shortened) deadline has arrived.
func (e *Engine) HandleSuspicionTimeout(nodeID uint16, incarnation uint64, now time.Time) Effects {
	suspicion, exists := e.suspicions[nodeID]
	if !exists || suspicion.incarnation != incarnation || now.Before(suspicion.deadline) {
		return Effects{}
	}
	member, exists := e.table.Get(nodeID)
	if !exists || member.Incarnation != incarnation || member.Status != Suspect {
		delete(e.suspicions, nodeID)
		return Effects{}
	}
	delete(e.suspicions, nodeID)
	member.Status = Dead
	return e.ApplyUpdate(Update{Member: member, ReporterID: e.config.SelfID}, now)
}

// ExpireTombstone removes only the terminal record named by the current
// generation-specific tombstone after its retained deadline. Early, stale,
// duplicate, and superseded callbacks are harmless.
func (e *Engine) ExpireTombstone(nodeID uint16, incarnation uint64, status Status, now time.Time) Effects {
	tombstone, exists := e.tombstones[nodeID]
	if !exists || tombstone.incarnation != incarnation || tombstone.status != status || now.Before(tombstone.deadline) {
		return Effects{}
	}
	member, exists := e.table.Get(nodeID)
	if !exists || member.Incarnation != incarnation || member.Status != status {
		delete(e.tombstones, nodeID)
		return Effects{}
	}
	delete(e.tombstones, nodeID)
	delete(e.table.members, nodeID)
	return Effects{}
}

// Leave advances and persists the local incarnation, publishes Left, and
// returns the bounded deadline for graceful dissemination. Repeated leave
// calls after the terminal transition are idempotent.
func (e *Engine) Leave(now time.Time) Effects {
	self, exists := e.table.Get(e.config.SelfID)
	if !exists || self.Status == Left || self.Incarnation == math.MaxUint64 {
		return Effects{}
	}
	aliveBefore := e.aliveMembers()
	self.Incarnation++
	self.Status = Left
	incarnation := self.Incarnation
	effects := Effects{PersistIncarnation: &incarnation}
	changed, event := e.table.Merge(Update{Member: self, ReporterID: e.config.SelfID})
	if !changed {
		return Effects{}
	}
	delete(e.suspicions, self.NodeID)
	effects.Timers = append(effects.Timers, TimerRequest{
		Kind:        TimerLeaveDeadline,
		NodeID:      self.NodeID,
		Incarnation: self.Incarnation,
		Status:      Left,
		Deadline:    addDurationSaturated(now, leaveDuration(e.dissemination.retransmitMultiplier, e.config.ProbeInterval, aliveBefore)),
	})
	effects.Timers = append(effects.Timers, e.beginTombstone(self, now))
	e.appliedTransition(&effects, event, e.config.SelfID)
	return effects
}

func (e *Engine) beginSuspicion(member Member, reporterID uint16, aliveMembers int, now time.Time) TimerRequest {
	duration := SuspicionDuration(int(e.config.SuspicionMultiplier), e.config.ProbeInterval, aliveMembers)
	deadline := addDurationSaturated(now, duration)
	e.suspicions[member.NodeID] = &suspicionState{
		incarnation: member.Incarnation,
		deadline:    deadline,
		duration:    duration,
		reporters:   map[uint16]struct{}{reporterID: {}},
	}
	return suspicionTimer(member.NodeID, member.Incarnation, deadline)
}

func (e *Engine) corroborateSuspicion(member Member, reporterID uint16, now time.Time) Effects {
	suspicion, exists := e.suspicions[member.NodeID]
	if !exists || suspicion.incarnation != member.Incarnation {
		return Effects{}
	}
	if _, duplicate := suspicion.reporters[reporterID]; duplicate {
		return Effects{}
	}
	suspicion.reporters[reporterID] = struct{}{}
	remaining := suspicion.duration / time.Duration(len(suspicion.reporters))
	if remaining < e.config.ProbeInterval {
		remaining = e.config.ProbeInterval
	}
	candidate := addDurationSaturated(now, remaining)
	if !candidate.Before(suspicion.deadline) {
		return Effects{}
	}
	suspicion.deadline = candidate
	return Effects{Timers: []TimerRequest{suspicionTimer(member.NodeID, member.Incarnation, candidate)}}
}

func (e *Engine) beginTombstone(member Member, now time.Time) TimerRequest {
	retention := multiplyDurationSaturated(
		SuspicionDuration(int(e.config.SuspicionMultiplier), e.config.ProbeInterval, e.aliveMembers()),
		10,
	)
	deadline := addDurationSaturated(now, retention)
	e.tombstones[member.NodeID] = tombstoneState{
		incarnation: member.Incarnation,
		status:      member.Status,
		deadline:    deadline,
	}
	return TimerRequest{
		Kind:        TimerTombstone,
		NodeID:      member.NodeID,
		Incarnation: member.Incarnation,
		Status:      member.Status,
		Deadline:    deadline,
	}
}

func (e *Engine) appliedTransition(effects *Effects, event MembershipEvent, reporterID uint16) {
	effects.Events = append(effects.Events, event)
	e.dissemination.Enqueue(Update{Member: event.Current, ReporterID: reporterID}, e.aliveMembers())
	effects.SnapshotRequired = e.dissemination.DigestRequired
}

func (e *Engine) aliveMembers() int {
	alive := 0
	for _, member := range e.table.Snapshot() {
		if member.Status == Alive {
			alive++
		}
	}
	return alive
}

func suspicionTimer(nodeID uint16, incarnation uint64, deadline time.Time) TimerRequest {
	return TimerRequest{Kind: TimerSuspicion, NodeID: nodeID, Incarnation: incarnation, Deadline: deadline}
}

func leaveDuration(retransmitMultiplier int, probeInterval time.Duration, aliveMembers int) time.Duration {
	duration := multiplyDurationSaturated(probeInterval, RetransmitBudget(retransmitMultiplier, aliveMembers))
	if duration < minimumSuspicionDuration {
		return minimumSuspicionDuration
	}
	return duration
}

func multiplyDurationSaturated(duration time.Duration, factor int) time.Duration {
	if duration <= 0 || factor <= 0 {
		return 0
	}
	if duration > time.Duration(math.MaxInt64)/time.Duration(factor) {
		return time.Duration(math.MaxInt64)
	}
	return duration * time.Duration(factor)
}

func addDurationSaturated(now time.Time, duration time.Duration) time.Time {
	deadline := now.Add(duration)
	if duration > 0 && deadline.Before(now) {
		return time.Unix(1<<63-62135596801, 999999999)
	}
	return deadline
}
