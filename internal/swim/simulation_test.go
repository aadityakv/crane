package swim

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/transport"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestSimulationSlowSubscriberResynchronizesAfterSnapshot(t *testing.T) {
	now := time.Unix(5000, 0)
	network := transport.NewMemoryNetwork()
	configuration := serviceTestConfig(t, 1)
	running := startRunningService(t, configuration, newServiceStore(1), clock.NewManual(now), network, 30)
	subscriptionContext, cancelSubscription := context.WithCancel(context.Background())
	defer cancelSubscription()
	events, err := running.service.Subscribe(subscriptionContext, 1)
	if err != nil {
		t.Fatal(err)
	}
	injector, err := network.Endpoint(config.Endpoint{Host: "subscriber-injector", Port: 9300})
	if err != nil {
		t.Fatal(err)
	}
	defer injector.Close()
	destination, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	clusterID := decodedTestClusterID(t, testClusterID)
	first := Member{NodeID: 2, Host: "127.0.0.2", BasePort: 12000, Incarnation: 1, Status: Alive}
	second := Member{NodeID: 3, Host: "127.0.0.3", BasePort: 13000, Incarnation: 1, Status: Alive}
	overflow := encodeServiceTestFrame(t, authenticator, clusterID, 1, 70, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{
		{Member: first, ReporterID: 1},
		{Member: second, ReporterID: 1},
	}}))
	if err := injector.Send(context.Background(), destination, overflow); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	waitForSnapshot(t, running.service, func(members []Member) bool { return len(members) == 3 })
	select {
	case event := <-events:
		if event.Cause != EventResyncRequired {
			t.Fatalf("slow subscriber event = %#v, want resync marker", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for slow-subscriber resync marker")
	}
	snapshot, err := running.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot[1] != first || snapshot[2] != second {
		t.Fatalf("resynchronization snapshot = %#v", snapshot)
	}

	third := Member{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 1, Status: Alive}
	afterResync := encodeServiceTestFrame(t, authenticator, clusterID, 1, 71, now, wire.MessageSWIMGossip, mustEncodeGob(t, GossipMessage{Updates: []Update{{Member: third, ReporterID: 1}}}))
	if err := injector.Send(context.Background(), destination, afterResync); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	select {
	case event := <-events:
		if event.Current != third || event.Cause != EventMemberChanged {
			t.Fatalf("post-resync event = %#v, want third member", event)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot did not resume slow-subscriber delta delivery")
	}
	running.stop(t)
}

func TestSimulationGracefulLeaveSendsOneRetransmitBudget(t *testing.T) {
	now := time.Unix(5100, 0)
	manualClock := clock.NewManual(now)
	network := transport.NewMemoryNetwork()
	seedConfig := serviceTestConfig(t, 1)
	seed := startRunningService(t, seedConfig, newServiceStore(1), manualClock, network, 31)
	secondConfig := serviceTestConfig(t, 2)
	seedEndpoint, _ := seedConfig.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
	secondConfig.Introducer = seedEndpoint.String()
	second := startRunningService(t, secondConfig, newServiceStore(1), manualClock, network, 32)
	thirdConfig := serviceTestConfig(t, 3)
	thirdConfig.Introducer = seedEndpoint.String()
	baseThird := serviceMemoryDatagram(t, network, thirdConfig)
	recording := newRecordingDatagram(baseThird, wire.NewHMACAuthenticator(testServiceKey()), serviceWireLimits(t))
	thirdStore := newServiceStore(1)
	third := startRunningServiceWithDatagram(t, thirdConfig, thirdStore, manualClock, recording, &scriptedRandom{uint64s: []uint64{1, 2, 3, 4, 5}})

	waitForSnapshot(t, seed.service, func(members []Member) bool { return len(members) == 3 })
	settleMemoryServices(network, seed.service, second.service, third.service)
	recording.clear()
	third.stop(t)

	leftCount := 0
	for _, frame := range recording.frames() {
		for _, update := range updatesFromRecordedFrame(t, frame) {
			if update.Member.NodeID == 3 && update.Member.Status == Left {
				leftCount++
			}
		}
	}
	wantBudget := RetransmitBudget(3, 3)
	if leftCount < wantBudget {
		t.Fatalf("Left transmissions = %d, want at least one budget of %d", leftCount, wantBudget)
	}
	if got, _ := thirdStore.Load(); got != 3 {
		t.Fatalf("durable graceful-leave incarnation = %d, want 3", got)
	}
	seed.stop(t)
	second.stop(t)
}

func TestSimulationDirectProbeSuccess(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	cluster.clearRecordings()
	cluster.advance(time.Second)
	nodeTwoPing := cluster.endpoint(2, config.ServiceSWIMPing)
	nodeOneACK := cluster.endpoint(1, config.ServiceSWIMACK)
	if !cluster.nodes[1].recording.sentTo(wire.MessageSWIMPing, nodeTwoPing) {
		t.Fatal("node 1 did not send its first direct Ping to node 2")
	}
	if !cluster.nodes[2].recording.sentTo(wire.MessageSWIMAck, nodeOneACK) {
		t.Fatal("node 2 did not directly acknowledge node 1")
	}
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.DirectProbeTimeout))
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.IndirectProbeTimeout))
	if member := cluster.waitMember(1, 2, func(member Member) bool { return member.Status == Alive }); member.Status != Alive {
		t.Fatalf("directly responsive node = %#v, want Alive", member)
	}
}

func TestSimulationOneWayPartitionUsesIndirectProbe(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	cluster.clearRecordings()
	nodeOnePing := cluster.endpoint(1, config.ServiceSWIMPing)
	nodeTwoPing := cluster.endpoint(2, config.ServiceSWIMPing)
	cluster.network.Drop(nodeOnePing, nodeTwoPing)
	cluster.advance(time.Second)
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.DirectProbeTimeout))
	nodeThreePing := cluster.endpoint(3, config.ServiceSWIMPing)
	nodeOneACK := cluster.endpoint(1, config.ServiceSWIMACK)
	if !cluster.nodes[1].recording.sentTo(wire.MessageSWIMPingReq, nodeThreePing) {
		t.Fatal("node 1 did not ask node 3 for an indirect probe")
	}
	if !cluster.nodes[3].recording.sentTo(wire.MessageSWIMPing, nodeTwoPing) {
		t.Fatal("node 3 did not relay the probe to node 2")
	}
	if !cluster.nodes[3].recording.sentTo(wire.MessageSWIMIndirectAck, nodeOneACK) {
		t.Fatal("node 3 did not relay the successful acknowledgement")
	}
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.IndirectProbeTimeout))
	member := cluster.waitMember(1, 2, func(member Member) bool { return member.Status == Alive })
	if member.Status != Alive {
		t.Fatalf("indirectly responsive node = %#v, want Alive", member)
	}
	cluster.network.Heal()
	cluster.advance(time.Second)
	cluster.assertRunning(t, 1, 2, 3)
}

func TestSimulationCompleteProbeFailureBecomesDead(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	cluster.network.Drop(cluster.endpoint(1, config.ServiceSWIMPing), cluster.endpoint(2, config.ServiceSWIMPing))
	cluster.network.Drop(cluster.endpoint(3, config.ServiceSWIMPing), cluster.endpoint(2, config.ServiceSWIMPing))
	cluster.advance(time.Second)
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.DirectProbeTimeout))
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.IndirectProbeTimeout))
	suspect := cluster.waitMember(1, 2, func(member Member) bool { return member.Status == Suspect })
	if suspect.Status != Suspect {
		t.Fatalf("failed full probe member = %#v, want Suspect", suspect)
	}
	cluster.advance(SuspicionDuration(5, time.Second, 3))
	dead := cluster.waitMember(1, 2, func(member Member) bool { return member.Status == Dead })
	if dead.Status != Dead || dead.Incarnation != suspect.Incarnation {
		t.Fatalf("expired suspicion = %#v, want Dead generation %#v", dead, suspect)
	}
}

func TestSimulationSuspicionRefutationStaleAndDuplicateUpdates(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	current := cluster.member(t, 2, 2)
	suspect := current
	suspect.Status = Suspect
	cluster.inject(t, 1, 2, 80, GossipMessage{Updates: []Update{{Member: suspect, ReporterID: 1}}})
	refuted := cluster.waitMember(2, 2, func(member Member) bool { return member.Status == Alive && member.Incarnation == current.Incarnation+1 })
	for _, viewer := range []uint16{1, 3} {
		seen := cluster.waitMember(viewer, 2, func(member Member) bool { return member.Status == Alive && member.Incarnation == refuted.Incarnation })
		if seen != refuted {
			t.Fatalf("viewer %d refutation = %#v, want %#v", viewer, seen, refuted)
		}
	}
	staleAlive := current
	staleDead := current
	staleDead.Status = Dead
	cluster.inject(t, 2, 1, 81, GossipMessage{Updates: []Update{
		{Member: staleAlive, ReporterID: 2},
		{Member: staleDead, ReporterID: 2},
	}})
	if got := cluster.member(t, 1, 2); got != refuted {
		t.Fatalf("stale updates replaced refutation: got %#v, want %#v", got, refuted)
	}

	eventsContext, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	events, err := cluster.nodes[1].running.service.Subscribe(eventsContext, 4)
	if err != nil {
		t.Fatal(err)
	}
	newMember := Member{NodeID: 4, Host: "127.0.0.4", BasePort: 14000, Incarnation: 1, Status: Alive}
	cluster.network.Duplicate(cluster.injectorAddress, cluster.endpoint(1, config.ServiceSWIMPing))
	cluster.inject(t, 1, 1, 82, GossipMessage{Updates: []Update{{Member: newMember, ReporterID: 1}}})
	select {
	case event := <-events:
		if event.Current != newMember {
			t.Fatalf("duplicate-delivery event = %#v, want new member", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for duplicated packet's single event")
	}
	select {
	case event := <-events:
		t.Fatalf("duplicated packet published a second event: %#v", event)
	default:
	}
}

func TestSimulationTwoWayPartitionConcurrentSuspicionsHealBySelfRefutation(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	cluster.advance(time.Second)
	cluster.isolate(3)
	cluster.advance(time.Second)
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.DirectProbeTimeout))
	cluster.advance(time.Duration(cluster.nodes[1].configuration.Timing.IndirectProbeTimeout))
	for _, viewer := range []uint16{1, 2} {
		cluster.waitMember(viewer, 3, func(member Member) bool { return member.Status == Suspect })
	}
	cluster.network.Heal()
	cluster.network.Duplicate(cluster.injectorAddress, cluster.endpoint(3, config.ServiceSWIMPing))
	suspect := cluster.member(t, 1, 3)
	cluster.inject(t, 1, 3, 83, GossipMessage{Updates: []Update{{Member: suspect, ReporterID: 1}}})
	for _, viewer := range []uint16{1, 2, 3} {
		alive := cluster.waitMember(viewer, 3, func(member Member) bool { return member.Status == Alive && member.Incarnation == suspect.Incarnation+1 })
		if alive.Status != Alive {
			t.Fatalf("viewer %d healed member = %#v", viewer, alive)
		}
	}
}

func TestSimulationCancellationClosesListenersTimersAndGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for iteration := 0; iteration < 10; iteration++ {
		network := transport.NewMemoryNetwork()
		configuration := serviceTestConfig(t, uint16(iteration+1))
		running := startRunningService(t, configuration, newServiceStore(1), clock.NewManual(time.Unix(8000, 0)), network, int64(100+iteration))
		snapshotEndpoint, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
		idleConnection, err := net.Dial("tcp", snapshotEndpoint.String())
		if err != nil {
			t.Fatal(err)
		}
		running.stop(t)
		if err := idleConnection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := idleConnection.Read(make([]byte, 1)); err == nil {
			t.Fatal("idle TCP connection remained readable after service shutdown")
		}
		_ = idleConnection.Close()
		ping := config.Endpoint{Host: configuration.AdvertiseHost, Port: configuration.BasePort}
		ack := config.Endpoint{Host: configuration.AdvertiseHost, Port: configuration.BasePort + 1}
		reused, err := network.Endpoint(ping, ack)
		if err != nil {
			t.Fatalf("UDP aliases remained registered after shutdown: %v", err)
		}
		_ = reused.Close()
		dialContext, cancelDial := context.WithTimeout(context.Background(), time.Second)
		connection, err := (&net.Dialer{}).DialContext(dialContext, "tcp", snapshotEndpoint.String())
		cancelDial()
		if err == nil {
			_ = connection.Close()
			t.Fatal("snapshot listener accepted a connection after Run returned")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+6 && time.Now().Before(deadline) {
		runtime.GC()
		runtime.Gosched()
	}
	if got := runtime.NumGoroutine(); got > baseline+6 {
		t.Fatalf("goroutines after repeated shutdown = %d, baseline %d", got, baseline)
	}
}

func TestSimulationUnexpectedDatagramClosureFailsService(t *testing.T) {
	network := transport.NewMemoryNetwork()
	configuration := serviceTestConfig(t, 1)
	datagram := serviceMemoryDatagram(t, network, configuration)
	running := startRunningServiceWithDatagram(t, configuration, newServiceStore(1), clock.NewManual(time.Unix(8100, 0)), datagram, &scriptedRandom{uint64s: []uint64{1, 2, 3, 4, 5}})
	if err := datagram.Close(); err != nil {
		t.Fatal(err)
	}
	err := waitServiceResult(t, running.result)
	if !errors.Is(err, transport.ErrDatagramClosed) {
		t.Fatalf("Run error after unexpected datagram closure = %v, want ErrDatagramClosed", err)
	}
	running.markStopped()
}

func TestSimulationGracefulRestartAndSeedFailure(t *testing.T) {
	cluster := newSimulationCluster(t, 3)
	old := cluster.member(t, 1, 3)
	cluster.stopNode(t, 3)
	for _, viewer := range []uint16{1, 2} {
		left := cluster.waitMember(viewer, 3, func(member Member) bool { return member.Status == Left })
		if left.Incarnation != old.Incarnation+1 {
			t.Fatalf("viewer %d graceful leave = %#v, want incarnation %d", viewer, left, old.Incarnation+1)
		}
	}
	cluster.restartNode(t, 3)
	for _, viewer := range []uint16{1, 2, 3} {
		restarted := cluster.waitMember(viewer, 3, func(member Member) bool { return member.Status == Alive && member.Incarnation == old.Incarnation+2 })
		if restarted.Status != Alive {
			t.Fatalf("viewer %d restarted member = %#v", viewer, restarted)
		}
	}

	cluster.stopNode(t, 1)
	for _, viewer := range []uint16{2, 3} {
		cluster.waitMember(viewer, 1, func(member Member) bool { return member.Status == Left })
	}
	cluster.advance(time.Second)
	cluster.assertRunning(t, 2, 3)
	if member := cluster.member(t, 2, 3); member.Status != Alive {
		t.Fatalf("node 2 lost node 3 after seed shutdown: %#v", member)
	}
}

func TestSimulationBoundedDisseminationFallsBackToDigest(t *testing.T) {
	now := time.Unix(7000, 0)
	network := transport.NewMemoryNetwork()
	configuration := serviceTestConfig(t, 1)
	base := serviceMemoryDatagram(t, network, configuration)
	authenticator := wire.NewHMACAuthenticator(testServiceKey())
	recording := newRecordingDatagram(base, authenticator, serviceWireLimits(t))
	running := startRunningServiceWithDatagram(t, configuration, newServiceStore(1), clock.NewManual(now), recording, &scriptedRandom{uint64s: []uint64{1, 2, 3, 4, 5}})
	injector, err := network.Endpoint(config.Endpoint{Host: "bounded-injector", Port: 9500})
	if err != nil {
		t.Fatal(err)
	}
	defer injector.Close()
	destination, _ := configuration.AdvertiseEndpoint(config.ServiceSWIMPing)

	const terminalUpdates = serviceDisseminationMax
	processed := 0
	requestNumber := uint64(1)
	for processed < terminalUpdates {
		// Snapshot is an event-loop barrier between bounded deterministic
		// tranches; keeping the tranche modest also works under -race.
		frames := 32
		remainingUpdates := terminalUpdates - processed
		if maximumFrames := (remainingUpdates + 7) / 8; maximumFrames < frames {
			frames = maximumFrames
		}
		batchUpdates := 0
		for range frames {
			updates := make([]Update, 0, 8)
			for len(updates) < 8 && processed+batchUpdates < terminalUpdates {
				nodeID := uint16(processed + batchUpdates + 2)
				member := Member{NodeID: nodeID, Host: "dead.local", BasePort: 10000, Incarnation: 1, Status: Dead}
				updates = append(updates, Update{Member: member, ReporterID: 1})
				batchUpdates++
			}
			frame := encodeSimulationGossip(t, authenticator, now, requestNumber, GossipMessage{Updates: updates})
			requestNumber++
			if err := injector.Send(context.Background(), destination, frame); err != nil {
				t.Fatal(err)
			}
		}
		if delivered := network.Advance(); delivered != frames {
			t.Fatalf("bounded batch delivered = %d, want %d", delivered, frames)
		}
		processed += batchUpdates
		waitForSnapshot(t, running.service, func(members []Member) bool { return len(members) == processed+1 })
	}

	recording.clear()
	peer := Member{NodeID: 6000, Host: "127.0.0.6", BasePort: 16000, Incarnation: 1, Status: Alive}
	peerFrame := encodeSimulationGossip(t, authenticator, now, requestNumber, GossipMessage{Updates: []Update{{Member: peer, ReporterID: 1}}})
	if err := injector.Send(context.Background(), destination, peerFrame); err != nil {
		t.Fatal(err)
	}
	network.Advance()
	waitForSnapshot(t, running.service, func(members []Member) bool { return len(members) == terminalUpdates+2 })
	peerPing := config.Endpoint{Host: peer.Host, Port: peer.BasePort}
	if !recording.sentTo(wire.MessageSWIMDigest, peerPing) {
		t.Fatal("saturated dissemination queue did not send digest fallback to new active peer")
	}
	select {
	case err := <-running.result:
		t.Fatalf("bounded dissemination stopped service: %v", err)
	default:
	}
	running.stop(t)
}

func encodeSimulationGossip(t *testing.T, authenticator wire.Authenticator, now time.Time, requestNumber uint64, message GossipMessage) []byte {
	t.Helper()
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[8:], requestNumber)
	payload := mustEncodeGob(t, message)
	encoded, err := wire.Encode(wire.Header{
		Version:         wire.Version1,
		Message:         wire.MessageSWIMGossip,
		ClusterID:       decodedTestClusterID(t, testClusterID),
		SenderID:        1,
		RequestID:       requestID,
		TimestampMillis: now.UnixMilli(),
		Codec:           wire.CodecGob,
	}, payload, authenticator, wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type simulationCluster struct {
	t               *testing.T
	clock           *clock.Manual
	network         *transport.MemoryNetwork
	nodes           map[uint16]*simulationNode
	injector        *transport.MemoryDatagram
	injectorAddress config.Endpoint
	seedEndpoint    config.Endpoint
}

type simulationNode struct {
	configuration config.NodeConfig
	store         *serviceStore
	running       *runningService
	recording     *recordingDatagram
	active        bool
	starts        uint64
}

func newSimulationCluster(t *testing.T, size int) *simulationCluster {
	t.Helper()
	start := time.Unix(6000, 0)
	cluster := &simulationCluster{
		t:               t,
		clock:           clock.NewManual(start),
		network:         transport.NewMemoryNetwork(),
		nodes:           make(map[uint16]*simulationNode),
		injectorAddress: config.Endpoint{Host: "simulation-injector", Port: 9400},
	}
	injector, err := cluster.network.Endpoint(cluster.injectorAddress)
	if err != nil {
		t.Fatal(err)
	}
	cluster.injector = injector
	t.Cleanup(func() { _ = injector.Close() })
	for index := 1; index <= size; index++ {
		nodeID := uint16(index)
		configuration := serviceTestConfig(t, nodeID)
		if nodeID == 1 {
			cluster.seedEndpoint, _ = configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
		} else {
			configuration.Introducer = cluster.seedEndpoint.String()
		}
		store := newServiceStore(1)
		cluster.nodes[nodeID] = &simulationNode{configuration: configuration, store: store}
		cluster.startNode(t, nodeID)
		cluster.settle(30)
	}
	for viewer := uint16(1); viewer <= uint16(size); viewer++ {
		cluster.waitSnapshot(viewer, func(members []Member) bool {
			if len(members) != size {
				return false
			}
			for _, member := range members {
				if member.Status != Alive {
					return false
				}
			}
			return true
		})
	}
	cluster.settle(50)
	cluster.clearRecordings()
	return cluster
}

func (c *simulationCluster) startNode(t *testing.T, nodeID uint16) {
	t.Helper()
	node := c.nodes[nodeID]
	node.starts++
	base := serviceMemoryDatagram(t, c.network, node.configuration)
	recording := newRecordingDatagram(base, wire.NewHMACAuthenticator(testServiceKey()), serviceWireLimits(t))
	baseRandom := node.starts * 100
	running := startRunningServiceWithDatagram(t, node.configuration, node.store, c.clock, recording, &scriptedRandom{uint64s: []uint64{baseRandom + 1, baseRandom + 2, baseRandom + 3, baseRandom + 4, baseRandom + 5}})
	node.running = running
	node.recording = recording
	node.active = true
}

func (c *simulationCluster) restartNode(t *testing.T, nodeID uint16) {
	t.Helper()
	c.startNode(t, nodeID)
	c.waitSnapshot(nodeID, func(members []Member) bool {
		member, exists := memberFromSnapshot(members, nodeID)
		return exists && member.Status == Alive
	})
	c.settle(100)
}

func (c *simulationCluster) stopNode(t *testing.T, nodeID uint16) {
	t.Helper()
	node := c.nodes[nodeID]
	if !node.active {
		return
	}
	node.running.stop(t)
	node.active = false
	c.settle(100)
}

func (c *simulationCluster) settle(rounds int) {
	for range rounds {
		c.network.Advance()
		runtime.Gosched()
		for _, node := range c.nodes {
			if !node.active || node.running == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, _ = node.running.service.Snapshot(ctx)
			cancel()
		}
	}
}

func (c *simulationCluster) advance(duration time.Duration) {
	c.clock.Advance(duration)
	c.settle(100)
}

func (c *simulationCluster) waitSnapshot(nodeID uint16, condition func([]Member) bool) []Member {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var last []Member
	var lastError error
	for ctx.Err() == nil {
		c.settle(2)
		snapshot, err := c.nodes[nodeID].running.service.Snapshot(ctx)
		last, lastError = snapshot, err
		if err == nil && condition(snapshot) {
			return snapshot
		}
		runtime.Gosched()
	}
	c.t.Fatalf("node %d snapshot condition not met: %v; last snapshot=%#v error=%v", nodeID, ctx.Err(), last, lastError)
	return nil
}

func (c *simulationCluster) waitMember(viewer, nodeID uint16, condition func(Member) bool) Member {
	c.t.Helper()
	snapshot := c.waitSnapshot(viewer, func(members []Member) bool {
		member, exists := memberFromSnapshot(members, nodeID)
		return exists && condition(member)
	})
	member, _ := memberFromSnapshot(snapshot, nodeID)
	return member
}

func (c *simulationCluster) member(t *testing.T, viewer, nodeID uint16) Member {
	t.Helper()
	snapshot, err := c.nodes[viewer].running.service.Snapshot(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	member, exists := memberFromSnapshot(snapshot, nodeID)
	if !exists {
		t.Fatalf("viewer %d has no member %d in %#v", viewer, nodeID, snapshot)
	}
	return member
}

func memberFromSnapshot(members []Member, nodeID uint16) (Member, bool) {
	for _, member := range members {
		if member.NodeID == nodeID {
			return member, true
		}
	}
	return Member{}, false
}

func (c *simulationCluster) endpoint(nodeID uint16, service config.Service) config.Endpoint {
	endpoint, err := c.nodes[nodeID].configuration.AdvertiseEndpoint(service)
	if err != nil {
		c.t.Fatal(err)
	}
	return endpoint
}

func (c *simulationCluster) isolate(nodeID uint16) {
	for otherID, other := range c.nodes {
		if otherID == nodeID || !other.active {
			continue
		}
		from := c.endpoint(otherID, config.ServiceSWIMPing)
		isolatedFrom := c.endpoint(nodeID, config.ServiceSWIMPing)
		c.network.Drop(from, c.endpoint(nodeID, config.ServiceSWIMPing))
		c.network.Drop(from, c.endpoint(nodeID, config.ServiceSWIMACK))
		c.network.Drop(isolatedFrom, c.endpoint(otherID, config.ServiceSWIMPing))
		c.network.Drop(isolatedFrom, c.endpoint(otherID, config.ServiceSWIMACK))
	}
}

func (c *simulationCluster) inject(t *testing.T, senderID, destinationID uint16, requestNumber uint64, message GossipMessage) {
	t.Helper()
	payload := mustEncodeGob(t, message)
	var requestID wire.RequestID
	binary.BigEndian.PutUint64(requestID[8:], requestNumber)
	encoded, err := wire.Encode(wire.Header{
		Version:         wire.Version1,
		Message:         wire.MessageSWIMGossip,
		ClusterID:       decodedTestClusterID(t, testClusterID),
		SenderID:        senderID,
		RequestID:       requestID,
		TimestampMillis: c.clock.Now().UnixMilli(),
		Codec:           wire.CodecGob,
	}, payload, wire.NewHMACAuthenticator(testServiceKey()), wire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.injector.Send(context.Background(), c.endpoint(destinationID, config.ServiceSWIMPing), encoded); err != nil {
		t.Fatal(err)
	}
	c.network.Advance()
	c.settle(50)
}

func (c *simulationCluster) clearRecordings() {
	for _, node := range c.nodes {
		if node.recording != nil {
			node.recording.clear()
		}
	}
}

func (c *simulationCluster) assertRunning(t *testing.T, nodeIDs ...uint16) {
	t.Helper()
	for _, nodeID := range nodeIDs {
		node := c.nodes[nodeID]
		select {
		case err := <-node.running.result:
			t.Fatalf("node %d stopped unexpectedly: %v", nodeID, err)
		default:
		}
		if _, err := node.running.service.Snapshot(testContext(t)); err != nil {
			t.Fatalf("node %d snapshot while running: %v", nodeID, err)
		}
	}
}

type recordingDatagram struct {
	transport.Datagram
	auth   wire.Authenticator
	limits wire.Limits
	mu     sync.Mutex
	sent   []recordedDatagram
}

type recordedDatagram struct {
	destination config.Endpoint
	frame       wire.Frame
}

func newRecordingDatagram(datagram transport.Datagram, auth wire.Authenticator, limits wire.Limits) *recordingDatagram {
	return &recordingDatagram{Datagram: datagram, auth: auth, limits: limits}
}

func (d *recordingDatagram) Send(ctx context.Context, destination config.Endpoint, payload []byte) error {
	if frame, err := wire.Decode(payload, d.auth, d.limits); err == nil {
		d.mu.Lock()
		d.sent = append(d.sent, recordedDatagram{destination: destination, frame: frame})
		d.mu.Unlock()
	}
	return d.Datagram.Send(ctx, destination, payload)
}

func (d *recordingDatagram) clear() {
	d.mu.Lock()
	d.sent = nil
	d.mu.Unlock()
}

func (d *recordingDatagram) frames() []wire.Frame {
	d.mu.Lock()
	defer d.mu.Unlock()
	frames := make([]wire.Frame, 0, len(d.sent))
	for _, recorded := range d.sent {
		frames = append(frames, recorded.frame)
	}
	return frames
}

func (d *recordingDatagram) sentTo(message wire.MessageType, destination config.Endpoint) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, recorded := range d.sent {
		if recorded.frame.Header.Message == message && recorded.destination == destination {
			return true
		}
	}
	return false
}

func updatesFromRecordedFrame(t *testing.T, frame wire.Frame) []Update {
	t.Helper()
	switch frame.Header.Message {
	case wire.MessageSWIMPing:
		var message PingMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	case wire.MessageSWIMAck:
		var message AckMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	case wire.MessageSWIMPingReq:
		var message PingReqMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	case wire.MessageSWIMIndirectAck:
		var message IndirectAckMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	case wire.MessageSWIMGossip:
		var message GossipMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	case wire.MessageSWIMDigest:
		var message DigestMessage
		if err := wire.DecodeGob(frame.Payload, &message); err != nil {
			t.Fatal(err)
		}
		return message.Updates
	default:
		return nil
	}
}

func settleMemoryServices(network *transport.MemoryNetwork, services ...*Service) {
	for range 50 {
		network.Advance()
		for _, service := range services {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, _ = service.Snapshot(ctx)
			cancel()
		}
	}
}
