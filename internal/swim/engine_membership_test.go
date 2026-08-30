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

func TestSuspicionIndirectTimeoutRequiresCurrentEligibleTargetIdentity(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	tests := []struct {
		name    string
		current *Member
	}{
		{name: "missing"},
		{name: "higher incarnation", current: memberPointer(withIncarnation(target, 5))},
		{name: "lower incarnation", current: memberPointer(withIncarnation(target, 3))},
		{name: "address mismatch", current: memberPointer(Member{NodeID: target.NodeID, Host: "replacement", BasePort: 9002, Incarnation: target.Incarnation, Status: Alive})},
		{name: "dead", current: memberPointer(withStatus(target, Dead))},
		{name: "left", current: memberPointer(withStatus(target, Left))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := newTestEngineWithSelf(self)
			mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
			now := time.Date(2026, 8, 29, 15, 5, 0, 0, time.UTC)
			sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
			engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

			if test.current == nil {
				delete(engine.table.members, target.NodeID)
			} else {
				engine.table.members[target.NodeID] = *test.current
			}

			effects := engine.HandleIndirectTimeout(sequence, now.Add(time.Second))

			if !reflect.DeepEqual(effects, Effects{}) {
				t.Fatalf("ineligible target effects = %#v, want zero", effects)
			}
			if _, active := engine.activeProbes[sequence]; active {
				t.Fatal("ineligible target probe remained active")
			}
			if test.current == nil {
				if _, exists := engine.table.Get(target.NodeID); exists {
					t.Fatal("missing target was recreated")
				}
			} else if got := engine.table.MustGet(target.NodeID); got != *test.current {
				t.Fatalf("current target changed to %#v, want %#v", got, *test.current)
			}
			if batch := mustTakeForMembers(t, engine.dissemination, 1, engine.aliveMembers(), countEncoder); len(batch) != 0 {
				t.Fatalf("ineligible target dissemination = %#v, want empty", batch)
			}
		})
	}
}

func TestSuspicionStaleIndirectTimeoutCannotRecreateExpiredHigherTerminalGeneration(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 15, 7, 0, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
	engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	terminal := target
	terminal.Incarnation = 5
	terminal.Status = Dead
	terminalEffects := engine.ApplyUpdate(Update{Member: terminal, ReporterID: 3}, now.Add(500*time.Millisecond))
	tombstoneTimer, ok := timerOfKind(terminalEffects, TimerTombstone)
	if !ok {
		t.Fatalf("terminal effects = %#v, want tombstone timer", terminalEffects)
	}
	for send := 0; send < RetransmitBudget(3, engine.aliveMembers()); send++ {
		batch := mustTakeForMembers(t, engine.dissemination, 1, engine.aliveMembers(), countEncoder)
		if !reflect.DeepEqual(batch, []Update{{Member: terminal, ReporterID: 3}}) {
			t.Fatalf("terminal dissemination %d = %#v", send+1, batch)
		}
	}
	engine.ExpireTombstone(terminal.NodeID, terminal.Incarnation, terminal.Status, tombstoneTimer.Deadline)
	if _, exists := engine.table.Get(target.NodeID); exists {
		t.Fatal("terminal generation remained after tombstone expiry")
	}

	effects := engine.HandleIndirectTimeout(sequence, tombstoneTimer.Deadline.Add(time.Millisecond))

	if !reflect.DeepEqual(effects, Effects{}) {
		t.Fatalf("stale indirect timeout effects = %#v, want zero", effects)
	}
	if _, exists := engine.table.Get(target.NodeID); exists {
		t.Fatal("stale indirect timeout recreated expired target generation")
	}
	if _, active := engine.activeProbes[sequence]; active {
		t.Fatal("stale indirect probe remained active")
	}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, engine.aliveMembers(), countEncoder); len(batch) != 0 {
		t.Fatalf("stale indirect timeout dissemination = %#v, want empty", batch)
	}
}

func TestExpiredTombstoneRejectsFreshlyReportedOldAliveWithoutRegossip(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 5, Status: Dead}
	engine := newTestEngineWithSelf(self)
	now := time.Date(2026, 8, 29, 15, 8, 0, 0, time.UTC)
	effects := engine.ApplyUpdate(Update{Member: target, ReporterID: 3}, now)
	timer, ok := timerOfKind(effects, TimerTombstone)
	if !ok {
		t.Fatalf("terminal effects = %#v, want tombstone timer", effects)
	}
	for {
		batch := mustTakeForMembers(t, engine.dissemination, 1, engine.aliveMembers(), countEncoder)
		if len(batch) == 0 {
			break
		}
	}
	engine.ExpireTombstone(target.NodeID, target.Incarnation, target.Status, timer.Deadline)

	staleAlive := target
	staleAlive.Status = Alive
	for reporter := uint16(3); reporter <= 5; reporter++ {
		if got := engine.ApplyUpdate(Update{Member: staleAlive, ReporterID: reporter}, timer.Deadline.Add(time.Duration(reporter)*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
			t.Fatalf("fresh report from %d effects = %#v, want zero", reporter, got)
		}
	}
	if _, exists := engine.table.Get(target.NodeID); exists {
		t.Fatal("freshly reported old Alive became visible after tombstone expiry")
	}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, engine.aliveMembers(), countEncoder); len(batch) != 0 {
		t.Fatalf("freshly reported old Alive was re-gossiped: %#v", batch)
	}
}

func TestEngineAppliesSnapshotFloorAndExportsItAfterVisibleExpiry(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	stale := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 3, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: stale, ReporterID: stale.NodeID})
	floor := stale
	floor.Incarnation = 6
	floor.Status = Dead
	now := time.Date(2026, 8, 29, 15, 9, 0, 0, time.UTC)

	effects := engine.ApplyIncarnationFloor(floor, 3, now)
	if len(effects.Events) != 1 || effects.Events[0].Previous != stale || effects.Events[0].Current != floor {
		t.Fatalf("snapshot floor effects = %#v, want stale-to-terminal transition", effects)
	}
	timer, ok := timerOfKind(effects, TimerTombstone)
	if !ok {
		t.Fatalf("snapshot floor effects = %#v, want tombstone timer", effects)
	}
	engine.ExpireTombstone(floor.NodeID, floor.Incarnation, floor.Status, timer.Deadline)

	if got := engine.Snapshot(); !reflect.DeepEqual(got, []Member{self}) {
		t.Fatalf("visible snapshot after expiry = %#v, want only self", got)
	}
	if got := engine.IncarnationFloors(); !reflect.DeepEqual(got, []Member{floor}) {
		t.Fatalf("exported floors = %#v, want %#v", got, []Member{floor})
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
	third := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: 4}, now.Add(2*time.Second))
	wantThirdDeadline := now.Add(2*time.Second + 20*time.Second/3)
	if len(third.Timers) != 1 || third.Timers[0].Deadline != wantThirdDeadline {
		t.Fatalf("third distinct reporter timers = %#v, want deadline %s", third.Timers, wantThirdDeadline)
	}

	deadline := third.Timers[0].Deadline
	late := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: 5}, deadline.Add(-500*time.Millisecond))
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

func TestSuspicionTimeoutRejectsEarlyAndWrongGenerationBeforeDeadTombstone(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 16, 0, 0, 0, time.UTC)
	suspect := engine.ApplyUpdate(Update{Member: withStatus(target, Suspect), ReporterID: self.NodeID}, now)
	deadline := suspect.Timers[0].Deadline

	if got := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation+1, deadline); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("wrong-generation timeout effects = %#v, want zero", got)
	}
	if got := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation, deadline.Add(-time.Nanosecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("early timeout effects = %#v, want zero", got)
	}
	if got := engine.table.MustGet(target.NodeID).Status; got != Suspect {
		t.Fatalf("status before deadline = %v, want Suspect", got)
	}

	effects := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation, deadline)
	if got := engine.table.MustGet(target.NodeID).Status; got != Dead {
		t.Fatalf("status at deadline = %v, want Dead", got)
	}
	if len(effects.Events) != 1 || effects.Events[0].Current.Status != Dead {
		t.Fatalf("death effects = %#v, want one Dead event", effects)
	}
	wantDeadUpdate := Update{Member: withStatus(target, Dead), ReporterID: self.NodeID}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, 1, countEncoder); !reflect.DeepEqual(batch, []Update{wantDeadUpdate}) {
		t.Fatalf("disseminated updates = %#v, want %#v", batch, []Update{wantDeadUpdate})
	}
	tombstone, ok := timerOfKind(effects, TimerTombstone)
	if !ok {
		t.Fatalf("death effects = %#v, want tombstone timer", effects)
	}
	wantTombstone := TimerRequest{
		Kind:        TimerTombstone,
		NodeID:      target.NodeID,
		Incarnation: target.Incarnation,
		Status:      Dead,
		Deadline:    deadline.Add(50 * time.Second),
	}
	if tombstone != wantTombstone {
		t.Fatalf("tombstone timer = %#v, want %#v", tombstone, wantTombstone)
	}
	if got := engine.HandleSuspicionTimeout(target.NodeID, target.Incarnation, deadline.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate suspicion timeout effects = %#v, want zero", got)
	}
}

func TestTombstoneRetainsUntilExactDeadlineThenExpiresIdempotently(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 16, 10, 0, 0, time.UTC)

	effects := engine.ApplyUpdate(Update{Member: withStatus(target, Dead), ReporterID: 3}, now)
	timer, ok := timerOfKind(effects, TimerTombstone)
	if !ok {
		t.Fatalf("dead effects = %#v, want tombstone timer", effects)
	}
	if timer.Deadline != now.Add(50*time.Second) {
		t.Fatalf("tombstone deadline = %s, want %s", timer.Deadline, now.Add(50*time.Second))
	}
	if got := engine.ApplyUpdate(Update{Member: withStatus(target, Dead), ReporterID: 4}, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate Dead effects = %#v, want zero without retention extension", got)
	}
	if tracked := engine.tombstones[target.NodeID].deadline; tracked != timer.Deadline {
		t.Fatalf("duplicate Dead changed deadline to %s, want %s", tracked, timer.Deadline)
	}
	if got := engine.ExpireTombstone(target.NodeID, target.Incarnation, Dead, timer.Deadline.Add(-time.Nanosecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("early expiry effects = %#v, want zero", got)
	}
	if _, exists := engine.table.Get(target.NodeID); !exists {
		t.Fatal("tombstone disappeared before its deadline")
	}

	if got := engine.ExpireTombstone(target.NodeID, target.Incarnation, Dead, timer.Deadline); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("expiry effects = %#v, want zero", got)
	}
	if _, exists := engine.table.Get(target.NodeID); exists {
		t.Fatal("tombstone remained at its deadline")
	}
	if got := engine.ExpireTombstone(target.NodeID, target.Incarnation, Dead, timer.Deadline.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate expiry effects = %#v, want zero", got)
	}
}

func TestTombstoneRetentionUsesPostTerminalAliveViewAtLogBoundary(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for _, member := range []Member{
		target,
		{NodeID: 3, Status: Alive},
		{NodeID: 4, Status: Alive},
	} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 16, 15, 0, 0, time.UTC)

	effects := engine.ApplyUpdate(Update{Member: withStatus(target, Dead), ReporterID: 3}, now)

	timer, ok := timerOfKind(effects, TimerTombstone)
	if !ok {
		t.Fatalf("terminal effects = %#v, want tombstone timer", effects)
	}
	// After the target becomes terminal, three Alive records remain:
	// ceil(log2(3+1))=2, so suspicion is 10s and retention is exactly 100s.
	// Using the pre-terminal view of four Alive records would incorrectly yield
	// a 150s retention and makes this boundary distinguish the count point.
	if want := now.Add(100 * time.Second); timer.Deadline != want {
		t.Fatalf("tombstone deadline = %s, want post-terminal boundary %s", timer.Deadline, want)
	}
}

func TestTombstoneSeverityReplacementMakesOldTimerHarmless(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 16, 20, 0, 0, time.UTC)
	deadEffects := engine.ApplyUpdate(Update{Member: withStatus(target, Dead), ReporterID: 3}, now)
	deadTimer, _ := timerOfKind(deadEffects, TimerTombstone)

	leftEffects := engine.ApplyUpdate(Update{Member: withStatus(target, Left), ReporterID: 4}, now.Add(time.Second))
	leftTimer, ok := timerOfKind(leftEffects, TimerTombstone)
	if !ok || leftTimer.Status != Left || !leftTimer.Deadline.After(deadTimer.Deadline) {
		t.Fatalf("Left timer = %#v after Dead timer %#v", leftTimer, deadTimer)
	}
	if len(engine.tombstones) != 1 {
		t.Fatalf("tracked tombstones = %d, want one current generation", len(engine.tombstones))
	}
	engine.ExpireTombstone(target.NodeID, target.Incarnation, Dead, deadTimer.Deadline)
	if got := engine.table.MustGet(target.NodeID).Status; got != Left {
		t.Fatalf("old Dead expiry changed status to %v, want Left", got)
	}
	engine.ExpireTombstone(target.NodeID, target.Incarnation, Left, leftTimer.Deadline)
	if _, exists := engine.table.Get(target.NodeID); exists {
		t.Fatal("current Left tombstone remained after expiry")
	}
}

func TestTombstoneOldGenerationCannotEvictHigherIncarnationRejoin(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "old", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 16, 30, 0, 0, time.UTC)
	deadEffects := engine.ApplyUpdate(Update{Member: withStatus(target, Dead), ReporterID: 3}, now)
	deadTimer, _ := timerOfKind(deadEffects, TimerTombstone)

	if got := engine.ApplyUpdate(Update{Member: target, ReporterID: target.NodeID}, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("equal-incarnation resurrection effects = %#v, want zero", got)
	}
	rejoined := Member{NodeID: target.NodeID, Host: "new", BasePort: 8102, Incarnation: 5, Status: Alive}
	engine.ApplyUpdate(Update{Member: rejoined, ReporterID: target.NodeID}, now.Add(2*time.Second))
	engine.ExpireTombstone(target.NodeID, target.Incarnation, Dead, deadTimer.Deadline)
	if got := engine.table.MustGet(target.NodeID); got != rejoined {
		t.Fatalf("old tombstone timer changed rejoined member to %#v, want %#v", got, rejoined)
	}
	if len(engine.tombstones) != 0 {
		t.Fatalf("tracked tombstones after rejoin = %d, want zero", len(engine.tombstones))
	}
}

func TestLeavePersistsHigherIncarnationPublishesLeftAndUsesDerivedDeadline(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for nodeID := uint16(2); nodeID <= 8; nodeID++ {
		mustMerge(t, engine.table, Update{Member: Member{NodeID: nodeID, Status: Alive}, ReporterID: nodeID})
	}
	now := time.Date(2026, 8, 29, 16, 40, 0, 0, time.UTC)

	effects := engine.Leave(now)

	want := self
	want.Incarnation = 8
	want.Status = Left
	if got := engine.table.MustGet(self.NodeID); got != want {
		t.Fatalf("self after leave = %#v, want %#v", got, want)
	}
	if effects.PersistIncarnation == nil || *effects.PersistIncarnation != want.Incarnation {
		t.Fatalf("persist effect = %#v, want %d", effects.PersistIncarnation, want.Incarnation)
	}
	if len(effects.Events) != 1 || effects.Events[0].Previous != self || effects.Events[0].Current != want {
		t.Fatalf("leave events = %#v", effects.Events)
	}
	wantUpdate := Update{Member: want, ReporterID: self.NodeID}
	if batch := mustTakeForMembers(t, engine.dissemination, 1, 7, countEncoder); !reflect.DeepEqual(batch, []Update{wantUpdate}) {
		t.Fatalf("disseminated updates = %#v, want %#v", batch, []Update{wantUpdate})
	}
	leaveTimer, ok := timerOfKind(effects, TimerLeaveDeadline)
	if !ok || leaveTimer.Deadline != now.Add(12*time.Second) {
		t.Fatalf("leave deadline timer = %#v, want %s", leaveTimer, now.Add(12*time.Second))
	}
	if _, ok := timerOfKind(effects, TimerTombstone); !ok {
		t.Fatalf("leave effects = %#v, want tombstone timer", effects)
	}
	if got := engine.Leave(now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate leave effects = %#v, want zero", got)
	}
	if got := engine.table.MustGet(self.NodeID); got != want {
		t.Fatalf("duplicate leave changed self to %#v, want %#v", got, want)
	}
}

func TestSelfDeadAndLeftAreSeverityTransitionsNotSuspicionRefutations(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	engine := newTestEngineWithSelf(self)
	now := time.Date(2026, 8, 29, 16, 50, 0, 0, time.UTC)

	dead := engine.ApplyUpdate(Update{Member: withStatus(self, Dead), ReporterID: 2}, now)
	if got := engine.table.MustGet(self.NodeID); got.Incarnation != self.Incarnation || got.Status != Dead {
		t.Fatalf("self after Dead = %#v, want same-incarnation Dead", got)
	}
	if dead.PersistIncarnation != nil {
		t.Fatalf("Dead persist effect = %#v, want nil", dead.PersistIncarnation)
	}
	left := engine.ApplyUpdate(Update{Member: withStatus(self, Left), ReporterID: 3}, now.Add(time.Second))
	if got := engine.table.MustGet(self.NodeID); got.Incarnation != self.Incarnation || got.Status != Left {
		t.Fatalf("self after Left = %#v, want same-incarnation Left", got)
	}
	if left.PersistIncarnation != nil {
		t.Fatalf("Left persist effect = %#v, want nil", left.PersistIncarnation)
	}
	if got := engine.Leave(now.Add(2 * time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("leave after observed Left effects = %#v, want zero", got)
	}
}

func timerOfKind(effects Effects, kind TimerKind) (TimerRequest, bool) {
	for _, timer := range effects.Timers {
		if timer.Kind == kind {
			return timer, true
		}
	}
	return TimerRequest{}, false
}

func memberPointer(member Member) *Member {
	return &member
}
