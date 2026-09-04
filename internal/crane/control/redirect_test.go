package control

import (
	"sort"
	"testing"

	"crane/internal/crane/model"
	"crane/internal/crane/protocol"
	"crane/internal/crane/state"
	"crane/internal/raft"
)

// redirectProbes returns one instance of every +6 request type.
func redirectProbes(t *testing.T) []protocol.ControlMessage {
	t.Helper()
	page := protocol.ResultPageRequest{JobID: model.JobID{0x52}, ManifestDigest: [32]byte{1}, PageBytes: 1024}
	return []protocol.ControlMessage{
		submitRequestFor(t, 0x51, 1, queryTopology(1)),
		cancelRequestFor(t, 0x51, 2, model.JobID{0x52}, 1),
		statusRequest(model.JobID{0x52}),
		page,
	}
}

func requireRedirect(t *testing.T, response protocol.ControlMessage, want []string) {
	t.Helper()
	redirect, ok := response.(protocol.LeaderRedirect)
	if !ok {
		t.Fatalf("response = %#v, want LeaderRedirect %v", response, want)
	}
	if len(redirect.Endpoints) != len(want) {
		t.Fatalf("redirect endpoints = %v, want %v", redirect.Endpoints, want)
	}
	for index, endpoint := range want {
		if redirect.Endpoints[index] != endpoint {
			t.Fatalf("redirect endpoints = %v, want %v", redirect.Endpoints, want)
		}
	}
}

func staticVoterControlEndpoints() []string {
	endpoints := []string{"127.0.0.1:19106", "127.0.0.2:19206", "127.0.0.3:19306"}
	sort.Strings(endpoints)
	return endpoints
}

func TestFollowerRedirectsToDerivedLeaderEndpoint(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.raft.setLeader(false, 2)
	fixture.start()
	for _, probe := range redirectProbes(t) {
		requireRedirect(t, fixture.exchange(probe), []string{"127.0.0.2:19206"})
	}
	if got := len(fixture.raft.capturedProposals()); got != 0 {
		t.Fatalf("follower proposed %d commands", got)
	}
}

func TestUnknownLeaderRedirectsToStaticVoterEndpoints(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	cases := []struct {
		name string
		hint uint16
	}{
		{"UnknownLeader", 0},
		{"ForeignHint", 42},
		{"SelfHintWithoutLeadership", 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture.raft.setLeader(false, testCase.hint)
			requireRedirect(t, fixture.exchange(statusRequest(model.JobID{0x53})), staticVoterControlEndpoints())
		})
	}
}

func TestNonvoterRedirectsEveryRequestToStaticVoters(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.configuration = controlTestConfig(t, 9)
	options := fixture.options()
	options.Raft = nil
	fixture.buildServiceFrom(options)
	fixture.start()
	for _, probe := range redirectProbes(t) {
		requireRedirect(t, fixture.exchange(probe), staticVoterControlEndpoints())
	}
}

func TestRedirectLeaderLossDuringProposeRedirectsChecked(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()
	// Leadership is lost between the barrier and the proposal: the propose
	// rejection must still produce a checked redirect, never a stale answer.
	fixture.raft.mu.Lock()
	fixture.raft.proposeHook = func([]byte) error {
		fixture.raft.leader = false
		fixture.raft.leaderHint = 3
		return &raft.NotLeaderError{LeaderID: 3}
	}
	fixture.raft.mu.Unlock()
	requireRedirect(t, fixture.exchange(submitRequestFor(t, 0x54, 1, queryTopology(1))), []string{"127.0.0.3:19306"})
	if fixture.wakes.Load() != 0 {
		t.Fatal("redirected mutation woke the coordinator")
	}
}
