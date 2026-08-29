package swim

import (
	"reflect"
	"testing"
	"time"
)

func TestSuspicionFailedFullProbeTransitionsOnlyExactGeneration(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for _, member := range []Member{
		target,
		{NodeID: 3, Status: Alive},
		{NodeID: 4, Status: Alive},
	} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 15, 0, 0, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
	engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	effects := engine.HandleIndirectTimeout(sequence, now.Add(time.Second))

	wantMember := target
	wantMember.Status = Suspect
	if got := engine.table.MustGet(target.NodeID); got != wantMember {
		t.Fatalf("target after failed full probe = %#v, want %#v", got, wantMember)
	}
	wantUpdate := Update{Member: wantMember, ReporterID: self.NodeID}
	wantEvent := MembershipEvent{Previous: target, Current: wantMember, Cause: EventMemberChanged, ReporterID: self.NodeID}
	if !reflect.DeepEqual(effects.Events, []MembershipEvent{wantEvent}) {
		t.Fatalf("events = %#v, want %#v", effects.Events, []MembershipEvent{wantEvent})
	}
	wantTimer := TimerRequest{
		Kind:        TimerSuspicion,
		NodeID:      target.NodeID,
		Incarnation: target.Incarnation,
		Deadline:    now.Add(16 * time.Second),
	}
	if !reflect.DeepEqual(effects.Timers, []TimerRequest{wantTimer}) {
		t.Fatalf("timers = %#v, want %#v", effects.Timers, []TimerRequest{wantTimer})
	}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, 3, countEncoder); !reflect.DeepEqual(batch, []Update{wantUpdate}) {
		t.Fatalf("disseminated updates = %#v, want %#v", batch, []Update{wantUpdate})
	}
	if got := engine.HandleIndirectTimeout(sequence, now.Add(2*time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate indirect timeout effects = %#v, want zero", got)
	}
}

func TestSuspicionDistinctReportersShortenWithoutDuplicatesOrExtensions(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for nodeID := uint16(2); nodeID <= 8; nodeID++ {
		member := Member{NodeID: nodeID, Incarnation: 4, Status: Alive}
		mustMerge(t, engine.table, Update{Member: member, ReporterID: nodeID})
	}
	now := time.Date(2026, 8, 29, 15, 10, 0, 0, time.UTC)
	first := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: self.NodeID}, now)
	if len(first.Timers) != 1 || first.Timers[0].Deadline != now.Add(20*time.Second) {
		t.Fatalf("initial suspicion timers = %#v, want deadline %s", first.Timers, now.Add(20*time.Second))
	}

	second := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: 3}, now.Add(time.Second))
	if len(second.Timers) != 1 || second.Timers[0].Deadline != now.Add(11*time.Second) {
		t.Fatalf("second reporter timers = %#v, want deadline %s", second.Timers, now.Add(11*time.Second))
	}
	if got := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: 3}, now.Add(2*time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate reporter effects = %#v, want zero", got)
	}

	deadline := second.Timers[0].Deadline
	late := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: 4}, deadline.Add(-500*time.Millisecond))
	if !reflect.DeepEqual(late, Effects{}) {
		t.Fatalf("late corroboration effects = %#v, want no deadline extension", late)
	}
	dead := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation, deadline)
	if got := engine.table.MustGet(target.NodeID).Status; got != Dead {
		t.Fatalf("status at shortened deadline = %v, want Dead; effects=%#v", got, dead)
	}
}

func TestSuspicionCorroborationNeverUndercutsOneProbeInterval(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 15, 20, 0, 0, time.UTC)
	effects := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: self.NodeID}, now)
	lastDeadline := effects.Timers[0].Deadline

	for reporterID := uint16(3); reporterID <= 20; reporterID++ {
		effects = engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: reporterID}, now)
		if len(effects.Timers) == 0 {
			continue
		}
		deadline := effects.Timers[0].Deadline
		if deadline.After(lastDeadline) {
			t.Fatalf("reporter %d extended deadline from %s to %s", reporterID, lastDeadline, deadline)
		}
		if deadline.Before(now.Add(engine.config.ProbeInterval)) {
			t.Fatalf("reporter %d shortened deadline to %s, below %s", reporterID, deadline, now.Add(engine.config.ProbeInterval))
		}
		lastDeadline = deadline
	}
	if lastDeadline != now.Add(engine.config.ProbeInterval) {
		t.Fatalf("last deadline = %s, want one probe interval at %s", lastDeadline, now.Add(engine.config.ProbeInterval))
	}
}

func TestSuspicionHigherIncarnationAliveCancelsAndEqualAliveCannotRefute(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "old", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 15, 30, 0, 0, time.UTC)
	engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: self.NodeID}, now)

	equalAlive := target
	equalAlive.Host = "forged"
	equalAlive.BasePort = 9002
	if got := engine.ApplyUpdate(Update{Member: equalAlive, ReporterID: target.NodeID}, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("equal Alive effects = %#v, want zero", got)
	}
	if got := engine.table.MustGet(target.NodeID).Status; got != Suspect {
		t.Fatalf("equal Alive changed status to %v, want Suspect", got)
	}

	newer := Member{NodeID: target.NodeID, Host: "new", BasePort: 8102, Incarnation: 5, Status: Alive}
	effects := engine.ApplyUpdate(Update{Member: newer, ReporterID: target.NodeID}, now.Add(2*time.Second))
	if got := engine.table.MustGet(target.NodeID); got != newer {
		t.Fatalf("member after newer Alive = %#v, want %#v", got, newer)
	}
	if len(effects.Events) != 1 || effects.Events[0].Current != newer {
		t.Fatalf("newer Alive effects = %#v", effects)
	}
	if got := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation, now.Add(30*time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("stale suspicion timeout effects = %#v, want zero", got)
	}
	if got := engine.table.MustGet(target.NodeID); got != newer {
		t.Fatalf("stale timeout changed member to %#v, want %#v", got, newer)
	}
}

func TestSelfSuspicionPersistsBeforePublishingHigherIncarnationAlive(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	engine := newTestEngineWithSelf(self)
	now := time.Date(2026, 8, 29, 15, 40, 0, 0, time.UTC)

	effects := engine.ApplyUpdate(Update{Member: withStatus(self, Suspect), ReporterID: 2}, now)

	want := self
	want.Incarnation = 8
	if got := engine.table.MustGet(self.NodeID); got != want {
		t.Fatalf("self after refutation = %#v, want %#v", got, want)
	}
	if effects.PersistIncarnation == nil || *effects.PersistIncarnation != want.Incarnation {
		t.Fatalf("persist effect = %#v, want %d", effects.PersistIncarnation, want.Incarnation)
	}
	wantEvent := MembershipEvent{Previous: self, Current: want, Cause: EventMemberChanged, ReporterID: self.NodeID}
	if !reflect.DeepEqual(effects.Events, []MembershipEvent{wantEvent}) {
		t.Fatalf("events = %#v, want only refuting Alive %#v", effects.Events, []MembershipEvent{wantEvent})
	}
	wantUpdate := Update{Member: want, ReporterID: self.NodeID}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, 1, countEncoder); !reflect.DeepEqual(batch, []Update{wantUpdate}) {
		t.Fatalf("disseminated updates = %#v, want %#v", batch, []Update{wantUpdate})
	}
	if got := engine.ApplyUpdate(Update{Member: withStatus(self, Suspect), ReporterID: 3}, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("stale self suspicion effects = %#v, want zero", got)
	}
}
