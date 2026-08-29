package swim

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/random"
)

func TestNewEngineRejectsInvalidOwnerDependenciesAndTiming(t *testing.T) {
	validConfig := EngineConfig{
		SelfID:               1,
		ProbeInterval:        time.Second,
		DirectProbeTimeout:   300 * time.Millisecond,
		IndirectProbeTimeout: 700 * time.Millisecond,
		IndirectChecks:       3,
		SuspicionMultiplier:  5,
	}
	validTable := NewTable()
	validDissemination := NewDisseminator(32, 3)
	validSource := random.NewLockedSource(1)
	tests := []struct {
		name          string
		config        EngineConfig
		table         *Table
		dissemination *Disseminator
		source        random.Source
	}{
		{name: "zero self", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.SelfID = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "zero probe interval", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.ProbeInterval = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "zero direct timeout", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.DirectProbeTimeout = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "zero indirect timeout", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.IndirectProbeTimeout = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "timeouts exceed interval", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.IndirectProbeTimeout += time.Nanosecond }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "zero indirect checks", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.IndirectChecks = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "zero suspicion multiplier", config: withEngineConfig(validConfig, func(config *EngineConfig) { config.SuspicionMultiplier = 0 }), table: validTable, dissemination: validDissemination, source: validSource},
		{name: "nil table", config: validConfig, table: nil, dissemination: validDissemination, source: validSource},
		{name: "nil dissemination", config: validConfig, table: validTable, dissemination: nil, source: validSource},
		{name: "nil source", config: validConfig, table: validTable, dissemination: validDissemination, source: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if engine, err := NewEngine(test.config, test.table, test.dissemination, test.source); err == nil || engine != nil {
				t.Fatalf("NewEngine returned engine=%#v err=%v, want nil engine and error", engine, err)
			}
		})
	}
}

func TestEngineBeginProbeSelectsAlivePeerAndSchedulesDirectTimeout(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	engine := newTestEngineWithSelf(self)
	engine.source = &scriptedRandom{
		uint64s:      []uint64{41},
		shuffleSwaps: [][][2]int{{{0, 1}}},
	}
	engine.selector.source = engine.source
	peer2 := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 3, Status: Alive}
	peer3 := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 4, Status: Alive}
	mustMerge(t, engine.table, Update{Member: peer2, ReporterID: 2})
	mustMerge(t, engine.table, Update{Member: peer3, ReporterID: 3})
	mustMerge(t, engine.table, Update{Member: Member{NodeID: 4, Status: Dead}, ReporterID: 4})
	mustMerge(t, engine.table, Update{Member: Member{NodeID: 5, Status: Left}, ReporterID: 5})
	mustMerge(t, engine.table, Update{Member: Member{NodeID: 6, Status: Suspect}, ReporterID: 6})
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)

	effects := engine.BeginProbe(now)

	wantOutbound := []Outbound{{
		To:      peer3,
		Message: Ping{OriginID: self.NodeID, Sequence: 41},
	}}
	if !reflect.DeepEqual(effects.Outbound, wantOutbound) {
		t.Fatalf("outbound = %#v, want %#v", effects.Outbound, wantOutbound)
	}
	wantTimers := []TimerRequest{{
		Kind:     TimerDirectProbe,
		Sequence: 41,
		Deadline: now.Add(300 * time.Millisecond),
	}}
	if !reflect.DeepEqual(effects.Timers, wantTimers) {
		t.Fatalf("timers = %#v, want %#v", effects.Timers, wantTimers)
	}
	if effects.Events != nil || effects.PersistIncarnation != nil || effects.SnapshotRequired {
		t.Fatalf("unrelated effects = %#v", effects)
	}
}

func TestEngineProbeSequencesStayNonzeroAndIncreasingAtSeedBoundary(t *testing.T) {
	self := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	table := NewTable()
	mustMerge(t, table, Update{Member: self, ReporterID: self.NodeID})
	mustMerge(t, table, Update{Member: target, ReporterID: target.NodeID})
	engine, err := NewEngine(
		EngineConfig{
			SelfID:               self.NodeID,
			ProbeInterval:        time.Second,
			DirectProbeTimeout:   300 * time.Millisecond,
			IndirectProbeTimeout: 700 * time.Millisecond,
			IndirectChecks:       3,
			SuspicionMultiplier:  5,
		},
		table,
		NewDisseminator(32, 3),
		&scriptedRandom{uint64s: []uint64{math.MaxUint64}},
	)
	if err != nil {
		t.Fatal(err)
	}

	first := engine.BeginProbe(time.Time{}).Outbound[0].Message.(Ping).Sequence
	second := engine.BeginProbe(time.Time{}.Add(time.Second)).Outbound[0].Message.(Ping).Sequence
	if first != math.MaxInt64 || second != uint64(math.MaxInt64)+1 {
		t.Fatalf("sequences = %d, %d, want %d, %d", first, second, uint64(math.MaxInt64), uint64(math.MaxInt64)+1)
	}
}

func TestEngineProbeDirectTimeoutSelectsDistinctRelaysAndIgnoresDuplicate(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	engine := newTestEngineWithSelf(self)
	source := &scriptedRandom{
		shuffleSwaps: [][][2]int{
			nil,
			{{0, 3}, {1, 2}},
		},
	}
	engine.source = source
	engine.selector.source = source
	peers := []Member{
		{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 2, Status: Alive},
		{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 3, Status: Alive},
		{NodeID: 4, Host: "node4", BasePort: 8004, Incarnation: 4, Status: Alive},
		{NodeID: 5, Host: "node5", BasePort: 8005, Incarnation: 5, Status: Alive},
		{NodeID: 6, Host: "node6", BasePort: 8006, Incarnation: 6, Status: Alive},
	}
	for _, peer := range peers {
		mustMerge(t, engine.table, Update{Member: peer, ReporterID: peer.NodeID})
	}
	now := time.Date(2026, 8, 29, 14, 10, 0, 0, time.UTC)
	direct := engine.BeginProbe(now)
	ping := direct.Outbound[0].Message.(Ping)
	if direct.Outbound[0].To != peers[0] {
		t.Fatalf("direct target = %#v, want %#v", direct.Outbound[0].To, peers[0])
	}

	effects := engine.HandleDirectTimeout(ping.Sequence, now.Add(300*time.Millisecond))

	wantRelays := []Member{peers[4], peers[3], peers[2]}
	if len(effects.Outbound) != len(wantRelays) {
		t.Fatalf("indirect sends = %d, want %d", len(effects.Outbound), len(wantRelays))
	}
	for index, outbound := range effects.Outbound {
		request, ok := outbound.Message.(PingReq)
		if !ok {
			t.Fatalf("outbound %d message = %T, want PingReq", index, outbound.Message)
		}
		if outbound.To != wantRelays[index] {
			t.Fatalf("relay %d = %#v, want %#v", index, outbound.To, wantRelays[index])
		}
		if request != (PingReq{OriginID: self.NodeID, Target: peers[0], Sequence: ping.Sequence}) {
			t.Fatalf("request %d = %#v", index, request)
		}
	}
	wantTimers := []TimerRequest{{
		Kind:     TimerIndirectProbe,
		Sequence: ping.Sequence,
		Deadline: now.Add(time.Second),
	}}
	if !reflect.DeepEqual(effects.Timers, wantTimers) {
		t.Fatalf("timers = %#v, want %#v", effects.Timers, wantTimers)
	}
	if got := engine.HandleDirectTimeout(ping.Sequence, now.Add(400*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate direct timeout effects = %#v, want zero", got)
	}
}

func TestEngineProbeDirectAckCancelsProbeAndMakesLateMessagesHarmless(t *testing.T) {
	self := Member{NodeID: 1, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 14, 20, 0, 0, time.UTC)
	begin := engine.BeginProbe(now)
	sequence := begin.Outbound[0].Message.(Ping).Sequence

	if got := engine.HandleAck(target, Ack{OriginID: self.NodeID, Sequence: sequence}, now.Add(10*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("ack effects = %#v, want zero", got)
	}
	if got := engine.HandleAck(target, Ack{OriginID: self.NodeID, Sequence: sequence}, now.Add(20*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate ack effects = %#v, want zero", got)
	}
	if got := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("late direct timeout effects = %#v, want zero", got)
	}
	if got := engine.HandleIndirectTimeout(sequence, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("late indirect timeout effects = %#v, want zero", got)
	}
}

func TestEngineProbeLateDirectAckDuringIndirectPhaseCancelsProbe(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for _, member := range []Member{target, relay} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 14, 22, 0, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
	indirect := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))
	if len(indirect.Outbound) != 1 || len(indirect.Timers) != 1 {
		t.Fatalf("indirect effects = %#v, want one PingReq and timer", indirect)
	}

	engine.HandleAck(target, Ack{OriginID: self.NodeID, Sequence: sequence}, now.Add(500*time.Millisecond))

	if len(engine.activeProbes) != 0 {
		t.Fatalf("active probes after late direct ack = %d, want zero", len(engine.activeProbes))
	}
	if got := engine.HandleIndirectTimeout(sequence, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("indirect timeout after late direct ack = %#v, want zero", got)
	}
}

func TestEngineProbeRejectsSpoofedDirectAckWithoutCancelingProbe(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 14, 25, 0, 0, time.UTC)
	begin := engine.BeginProbe(now)
	sequence := begin.Outbound[0].Message.(Ping).Sequence
	spoofed := target
	spoofed.Host = "attacker"
	spoofed.BasePort = 9999

	engine.HandleAck(spoofed, Ack{OriginID: self.NodeID, Sequence: sequence}, now.Add(10*time.Millisecond))
	effects := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	if len(effects.Timers) != 1 || effects.Timers[0].Kind != TimerIndirectProbe {
		t.Fatalf("effects after spoofed ack = %#v, want active probe's indirect timer", effects)
	}
}

func TestEngineProbeRejectsWrongDirectAcksWithoutCancelingExactSequence(t *testing.T) {
	self := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	other := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 8, Status: Alive}
	tests := []struct {
		name    string
		from    Member
		message func(uint64) Ack
	}{
		{name: "wrong sequence", from: target, message: func(sequence uint64) Ack { return Ack{OriginID: self.NodeID, Sequence: sequence + 1} }},
		{name: "wrong origin", from: target, message: func(sequence uint64) Ack { return Ack{OriginID: other.NodeID, Sequence: sequence} }},
		{name: "wrong node", from: other, message: func(sequence uint64) Ack { return Ack{OriginID: self.NodeID, Sequence: sequence} }},
		{name: "wrong incarnation", from: withIncarnation(target, target.Incarnation-1), message: func(sequence uint64) Ack { return Ack{OriginID: self.NodeID, Sequence: sequence} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := newTestEngineWithSelf(self)
			mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
			mustMerge(t, engine.table, Update{Member: other, ReporterID: other.NodeID})
			now := time.Date(2026, 8, 29, 14, 27, 0, 0, time.UTC)
			sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence

			engine.HandleAck(test.from, test.message(sequence), now.Add(10*time.Millisecond))
			effects := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

			if len(effects.Timers) != 1 || effects.Timers[0].Kind != TimerIndirectProbe {
				t.Fatalf("effects after rejected ack = %#v, want active probe's indirect timer", effects)
			}
		})
	}
}

func TestEngineProbeWithoutRelaysStillWaitsForIndirectGenerationOnce(t *testing.T) {
	self := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 14, 28, 0, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence

	effects := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	if len(effects.Outbound) != 0 {
		t.Fatalf("outbound without relays = %#v, want empty", effects.Outbound)
	}
	if len(effects.Timers) != 1 || effects.Timers[0].Kind != TimerIndirectProbe {
		t.Fatalf("timers without relays = %#v, want one indirect timer", effects.Timers)
	}
	if got := engine.HandleIndirectTimeout(sequence, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("indirect timeout effects = %#v, want zero until Task 10", got)
	}
	if got := engine.HandleIndirectTimeout(sequence, now.Add(2*time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate indirect timeout effects = %#v, want zero", got)
	}
}

func TestIndirectProbeRelaySelectionExcludesSelfTargetAndNonAliveMembers(t *testing.T) {
	self := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	relay := Member{NodeID: 3, Status: Alive}
	engine := newTestEngineWithSelf(self)
	for _, member := range []Member{
		target,
		relay,
		{NodeID: 4, Status: Suspect},
		{NodeID: 5, Status: Dead},
		{NodeID: 6, Status: Left},
	} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 14, 28, 30, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence

	effects := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	if len(effects.Outbound) != 1 || effects.Outbound[0].To != relay {
		t.Fatalf("relay outbound = %#v, want only %#v", effects.Outbound, relay)
	}
}

func TestEngineProbeStaleTimeoutCannotAdvanceNewGeneration(t *testing.T) {
	self := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 14, 29, 0, 0, time.UTC)
	first := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
	engine.HandleAck(target, Ack{OriginID: self.NodeID, Sequence: first}, now.Add(10*time.Millisecond))
	second := engine.BeginProbe(now.Add(time.Second)).Outbound[0].Message.(Ping).Sequence

	if got := engine.HandleDirectTimeout(first, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("stale generation effects = %#v, want zero", got)
	}
	effects := engine.HandleDirectTimeout(second, now.Add(time.Second+300*time.Millisecond))
	if len(effects.Timers) != 1 || effects.Timers[0].Sequence != second || effects.Timers[0].Kind != TimerIndirectProbe {
		t.Fatalf("current generation effects = %#v, want indirect timer for %d", effects, second)
	}
}

func TestEngineProbePingDerivesOmittedOriginFromAuthenticatedSender(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	engine := newTestEngineWithSelf(target)
	mustMerge(t, engine.table, Update{Member: origin, ReporterID: origin.NodeID})

	effects := engine.HandlePing(origin, Ping{Sequence: 93}, time.Time{})

	want := []Outbound{{To: origin, Message: Ack{OriginID: origin.NodeID, Sequence: 93}}}
	if !reflect.DeepEqual(effects.Outbound, want) {
		t.Fatalf("outbound = %#v, want %#v", effects.Outbound, want)
	}
}

func TestEngineProbeDirectAckDerivesOmittedLocalOrigin(t *testing.T) {
	self := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	engine := newTestEngineWithSelf(self)
	mustMerge(t, engine.table, Update{Member: target, ReporterID: target.NodeID})
	now := time.Date(2026, 8, 29, 14, 29, 30, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence

	engine.HandleAck(target, Ack{Sequence: sequence}, now.Add(time.Millisecond))

	if got := engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("timeout after minimal ack = %#v, want zero", got)
	}
}

func TestIndirectProbeRelaysAckToOriginalProbeAndCancelsBothTimeouts(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	originEngine := newTestEngineWithSelf(origin)
	relayEngine := newTestEngineWithSelf(relay)
	targetEngine := newTestEngineWithSelf(target)
	for _, engine := range []*Engine{originEngine, relayEngine, targetEngine} {
		for _, member := range []Member{origin, target, relay} {
			if member.NodeID == engine.config.SelfID {
				continue
			}
			mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
		}
	}
	now := time.Date(2026, 8, 29, 14, 30, 0, 0, time.UTC)
	direct := originEngine.BeginProbe(now)
	sequence := direct.Outbound[0].Message.(Ping).Sequence
	requests := originEngine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))
	request := requests.Outbound[0].Message.(PingReq)

	relayEffects := relayEngine.HandlePingReq(origin, request, now.Add(310*time.Millisecond))
	wantRelayOutbound := []Outbound{{
		To:      target,
		Message: Ping{OriginID: origin.NodeID, Sequence: sequence},
	}}
	if !reflect.DeepEqual(relayEffects.Outbound, wantRelayOutbound) {
		t.Fatalf("relay outbound = %#v, want %#v", relayEffects.Outbound, wantRelayOutbound)
	}
	wantRelayTimers := []TimerRequest{{
		Kind:     TimerRelayProbe,
		OriginID: origin.NodeID,
		Sequence: sequence,
		Deadline: now.Add(1010 * time.Millisecond),
	}}
	if !reflect.DeepEqual(relayEffects.Timers, wantRelayTimers) {
		t.Fatalf("relay timers = %#v, want %#v", relayEffects.Timers, wantRelayTimers)
	}

	targetEffects := targetEngine.HandlePing(relay, relayEffects.Outbound[0].Message.(Ping), now.Add(320*time.Millisecond))
	wantTargetOutbound := []Outbound{{
		To:      relay,
		Message: Ack{OriginID: origin.NodeID, Sequence: sequence},
	}}
	if !reflect.DeepEqual(targetEffects.Outbound, wantTargetOutbound) {
		t.Fatalf("target outbound = %#v, want %#v", targetEffects.Outbound, wantTargetOutbound)
	}

	ackEffects := relayEngine.HandleAck(target, targetEffects.Outbound[0].Message.(Ack), now.Add(330*time.Millisecond))
	wantAckOutbound := []Outbound{{
		To: origin,
		Message: IndirectAck{
			OriginID: origin.NodeID,
			Target:   target,
			Sequence: sequence,
		},
	}}
	if !reflect.DeepEqual(ackEffects.Outbound, wantAckOutbound) {
		t.Fatalf("relayed ack outbound = %#v, want %#v", ackEffects.Outbound, wantAckOutbound)
	}
	if got := relayEngine.HandleRelayTimeout(origin.NodeID, sequence, now.Add(1010*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("late relay timeout effects = %#v, want zero", got)
	}

	if got := originEngine.HandleIndirectAck(relay, ackEffects.Outbound[0].Message.(IndirectAck), now.Add(340*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("indirect ack effects = %#v, want zero", got)
	}
	if got := originEngine.HandleIndirectAck(relay, ackEffects.Outbound[0].Message.(IndirectAck), now.Add(350*time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate indirect ack effects = %#v, want zero", got)
	}
	if got := originEngine.HandleIndirectTimeout(sequence, now.Add(time.Second)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("late indirect timeout effects = %#v, want zero", got)
	}
}

func TestIndirectProbeRejectsUnexpectedRelayAndTargetIdentity(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	impostor := Member{NodeID: 4, Host: "node4", BasePort: 8004, Incarnation: 6, Status: Alive}
	tests := []struct {
		name    string
		from    Member
		message func(uint64) IndirectAck
	}{
		{name: "wrong sequence", from: relay, message: func(sequence uint64) IndirectAck {
			return IndirectAck{OriginID: origin.NodeID, Target: target, Sequence: sequence + 1}
		}},
		{name: "wrong origin", from: relay, message: func(sequence uint64) IndirectAck {
			return IndirectAck{OriginID: impostor.NodeID, Target: target, Sequence: sequence}
		}},
		{name: "unexpected relay", from: impostor, message: func(sequence uint64) IndirectAck {
			return IndirectAck{OriginID: origin.NodeID, Target: target, Sequence: sequence}
		}},
		{name: "wrong relay incarnation", from: withIncarnation(relay, relay.Incarnation-1), message: func(sequence uint64) IndirectAck {
			return IndirectAck{OriginID: origin.NodeID, Target: target, Sequence: sequence}
		}},
		{name: "wrong target incarnation", from: relay, message: func(sequence uint64) IndirectAck {
			return IndirectAck{OriginID: origin.NodeID, Target: withIncarnation(target, target.Incarnation-1), Sequence: sequence}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := newTestEngineWithSelf(origin)
			engine.config.IndirectChecks = 1
			for _, member := range []Member{target, relay, impostor} {
				mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
			}
			now := time.Date(2026, 8, 29, 14, 35, 0, 0, time.UTC)
			sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
			engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

			engine.HandleIndirectAck(test.from, test.message(sequence), now.Add(400*time.Millisecond))

			if len(engine.activeProbes) != 1 {
				t.Fatalf("active probes after rejected indirect ack = %d, want 1", len(engine.activeProbes))
			}
		})
	}
}

func TestIndirectProbeAckDerivesOmittedLocalOrigin(t *testing.T) {
	origin := Member{NodeID: 1, Status: Alive}
	target := Member{NodeID: 2, Status: Alive}
	relay := Member{NodeID: 3, Status: Alive}
	engine := newTestEngineWithSelf(origin)
	for _, member := range []Member{target, relay} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 14, 37, 0, 0, time.UTC)
	sequence := engine.BeginProbe(now).Outbound[0].Message.(Ping).Sequence
	engine.HandleDirectTimeout(sequence, now.Add(300*time.Millisecond))

	engine.HandleIndirectAck(relay, IndirectAck{Target: target, Sequence: sequence}, now.Add(400*time.Millisecond))

	if len(engine.activeProbes) != 0 {
		t.Fatalf("active probes after minimal indirect ack = %d, want zero", len(engine.activeProbes))
	}
}

func TestIndirectRelayRejectsInvalidAndDuplicatePingReq(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	engine := newTestEngineWithSelf(relay)
	for _, member := range []Member{origin, target} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 14, 40, 0, 0, time.UTC)
	valid := PingReq{OriginID: origin.NodeID, Target: target, Sequence: 91}
	invalid := []struct {
		name    string
		from    Member
		request PingReq
	}{
		{name: "zero sequence", from: origin, request: PingReq{OriginID: origin.NodeID, Target: target}},
		{name: "wrong origin", from: origin, request: PingReq{OriginID: target.NodeID, Target: target, Sequence: 91}},
		{name: "spoofed origin endpoint", from: withHost(origin, "attacker"), request: valid},
		{name: "self target", from: origin, request: PingReq{OriginID: origin.NodeID, Target: relay, Sequence: 91}},
		{name: "origin target", from: origin, request: PingReq{OriginID: origin.NodeID, Target: origin, Sequence: 91}},
		{name: "spoofed target", from: origin, request: PingReq{OriginID: origin.NodeID, Target: withHost(target, "attacker"), Sequence: 91}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if got := engine.HandlePingReq(test.from, test.request, now); !reflect.DeepEqual(got, Effects{}) {
				t.Fatalf("invalid PingReq effects = %#v, want zero", got)
			}
		})
	}
	if len(engine.relayProbes) != 0 {
		t.Fatalf("relay probes after invalid requests = %d, want zero", len(engine.relayProbes))
	}
	if got := engine.HandlePingReq(origin, valid, now); len(got.Outbound) != 1 {
		t.Fatalf("valid PingReq effects = %#v, want one ping", got)
	}
	if got := engine.HandlePingReq(origin, valid, now.Add(time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate PingReq effects = %#v, want zero", got)
	}
	if len(engine.relayProbes) != 1 {
		t.Fatalf("relay probes after duplicate = %d, want one", len(engine.relayProbes))
	}
}

func TestIndirectRelayDerivesOmittedOriginFromAuthenticatedPingReqSender(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	engine := newTestEngineWithSelf(relay)
	for _, member := range []Member{origin, target} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}

	effects := engine.HandlePingReq(origin, PingReq{Target: target, Sequence: 92}, time.Time{})

	want := []Outbound{{To: target, Message: Ping{OriginID: origin.NodeID, Sequence: 92}}}
	if !reflect.DeepEqual(effects.Outbound, want) {
		t.Fatalf("outbound = %#v, want %#v", effects.Outbound, want)
	}
}

func TestIndirectRelayStateLivesThroughOriginWindowAndCleanupIsExact(t *testing.T) {
	origin := Member{NodeID: 1, Host: "node1", BasePort: 8001, Incarnation: 7, Status: Alive}
	target := Member{NodeID: 2, Host: "node2", BasePort: 8002, Incarnation: 4, Status: Alive}
	relay := Member{NodeID: 3, Host: "node3", BasePort: 8003, Incarnation: 5, Status: Alive}
	engine := newTestEngineWithSelf(relay)
	for _, member := range []Member{origin, target} {
		mustMerge(t, engine.table, Update{Member: member, ReporterID: member.NodeID})
	}
	now := time.Date(2026, 8, 29, 15, 0, 0, 0, time.UTC)

	lateAckEffects := engine.HandlePingReq(origin, PingReq{Target: target, Sequence: 101}, now)
	wantTimer := TimerRequest{
		Kind:     TimerRelayProbe,
		OriginID: origin.NodeID,
		Sequence: 101,
		Deadline: now.Add(700 * time.Millisecond),
	}
	if !reflect.DeepEqual(lateAckEffects.Timers, []TimerRequest{wantTimer}) {
		t.Fatalf("relay timers = %#v, want %#v", lateAckEffects.Timers, []TimerRequest{wantTimer})
	}
	ackEffects := engine.HandleAck(target, Ack{OriginID: origin.NodeID, Sequence: 101}, now.Add(400*time.Millisecond))
	if len(ackEffects.Outbound) != 1 {
		t.Fatalf("ack after direct duration effects = %#v, want one IndirectAck", ackEffects)
	}
	if _, ok := ackEffects.Outbound[0].Message.(IndirectAck); !ok {
		t.Fatalf("ack after direct duration message = %T, want IndirectAck", ackEffects.Outbound[0].Message)
	}
	if got := engine.HandleRelayTimeout(origin.NodeID, 101, wantTimer.Deadline); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("late cleanup after ack effects = %#v, want zero", got)
	}

	for _, sequence := range []uint64{102, 103} {
		engine.HandlePingReq(origin, PingReq{Target: target, Sequence: sequence}, now.Add(time.Second))
	}
	cleanupDeadline := now.Add(time.Second + 700*time.Millisecond)
	if got := engine.HandleRelayTimeout(origin.NodeID, 102, cleanupDeadline); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("cleanup effects = %#v, want zero", got)
	}
	if got := engine.HandleRelayTimeout(origin.NodeID, 102, cleanupDeadline.Add(time.Millisecond)); !reflect.DeepEqual(got, Effects{}) {
		t.Fatalf("duplicate cleanup effects = %#v, want zero", got)
	}
	if got := engine.HandleAck(target, Ack{OriginID: origin.NodeID, Sequence: 102}, cleanupDeadline.Add(time.Millisecond)); len(got.Outbound) != 0 {
		t.Fatalf("ack after cleanup effects = %#v, want no IndirectAck", got)
	}
	if got := engine.HandleAck(target, Ack{OriginID: origin.NodeID, Sequence: 103}, cleanupDeadline.Add(time.Millisecond)); len(got.Outbound) != 1 {
		t.Fatalf("neighbor generation after cleanup effects = %#v, want one IndirectAck", got)
	}
}

func withEngineConfig(config EngineConfig, mutate func(*EngineConfig)) EngineConfig {
	mutate(&config)
	return config
}

func withIncarnation(member Member, incarnation uint64) Member {
	member.Incarnation = incarnation
	return member
}

func withHost(member Member, host string) Member {
	member.Host = host
	return member
}
