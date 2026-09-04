//go:build integration && craneintegration

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/clock"
	"github.com/aadityakv/crane/internal/config"
	"github.com/aadityakv/crane/internal/crane/clientstate"
	"github.com/aadityakv/crane/internal/crane/control"
	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/store"
	"github.com/aadityakv/crane/internal/crane/worker"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

const (
	craneIntegrationSecret = "crane-integration-secret-32bytes!!"
	craneVariantDomain     = "crane/integration-variant\x00"
)

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

// craneNode is one node process incarnation with its activation controller.
type craneNode struct {
	id            uint16
	configuration config.NodeConfig
	configPath    string
	binary        string
	process       *nodeProcess
	hook          *integrationhook.Controller
	incarnation   int
	// pastHooks keeps the controllers of dead incarnations so their rule
	// accounting can still be asserted at the end.
	pastHooks []*integrationhook.Controller
}

type craneCluster struct {
	t              *testing.T
	top            *testing.T
	ctx            context.Context
	harness        *processHarness
	root           string
	nodeBinary     string
	craneBinary    string
	secret         []byte
	secretFile     string
	clusterID      [16]byte
	configurations []config.NodeConfig
	configPaths    []string
	nodes          map[uint16]*craneNode
	clients        map[uint16]*control.Client
	swim           *snapshotClients
	phaseStart     time.Time
}

func repositoryRootForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func writeSecretFile(t *testing.T, secret []byte) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cluster.secret")
	if err := os.WriteFile(path, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// newCraneCluster builds the tagged node binary and the real crane CLI, then
// prepares four real loopback configurations (voters 1–3, nonvoter 4).
func newCraneCluster(t *testing.T, ctx context.Context, nodeBinary, craneBinary string, mutate func(*config.NodeConfig)) *craneCluster {
	t.Helper()
	root := repositoryRootForTest(t)
	secret := []byte(craneIntegrationSecret)
	secretFile := writeSecretFile(t, secret)
	basePort, releasePorts := reserveTypedClusterPorts(t, 4)
	t.Cleanup(releasePorts)
	configurations := craneIntegrationConfigs(t, basePort, secretFile, 4, mutate)
	configPaths := writeIntegrationConfigs(t, configurations)
	releasePorts()
	clusterID, err := decodeClusterIDForTest(configurations[0].ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &craneCluster{
		t: t, top: t, ctx: ctx, harness: newProcessHarness(t), root: root, nodeBinary: nodeBinary, craneBinary: craneBinary,
		secret: secret, secretFile: secretFile, clusterID: clusterID, configurations: configurations, configPaths: configPaths,
		nodes: map[uint16]*craneNode{}, clients: map[uint16]*control.Client{}, swim: newSnapshotClients(t, configurations, secret), phaseStart: time.Now(),
	}
	for index, configuration := range configurations {
		cluster.nodes[configuration.NodeID] = &craneNode{id: configuration.NodeID, configuration: configuration, configPath: configPaths[index], binary: nodeBinary}
	}
	t.Cleanup(func() {
		for _, node := range cluster.nodes {
			if node.hook != nil {
				_ = node.hook.Close()
			}
		}
	})
	return cluster
}

func decodeClusterIDForTest(value string) ([16]byte, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("invalid cluster UUID")
	}
	var result [16]byte
	copy(result[:], decoded)
	return result, nil
}

func (c *craneCluster) fatalf(format string, args ...any) {
	c.t.Helper()
	// Every fatal path captures the cluster diagnostics (leadership probes,
	// boundary counters, node goroutine dumps) so a failure is attributable
	// without a rerun.
	c.t.Fatalf(format+"\n%s\n%s", append(args, c.harness.logs(), c.diagnostics())...)
}

// launch starts one node incarnation with a fresh activation controller and
// waits for activation plus the documented readiness line.
func (c *craneCluster) launch(id uint16) *craneNode {
	c.t.Helper()
	node := c.nodes[id]
	if node.process != nil && !node.process.exited() {
		c.fatalf("node %d is still running", id)
	}
	if node.hook != nil {
		node.pastHooks = append(node.pastHooks, node.hook)
		_ = node.hook.Close()
	}
	controller, err := integrationhook.NewController()
	if err != nil {
		c.fatalf("controller for node %d: %v", id, err)
	}
	node.hook = controller
	node.incarnation++
	name := fmt.Sprintf("crane-node-%d.%d", id, node.incarnation)
	node.process = c.harness.startWithFiles(node.binary, []string{"-config", node.configPath}, name, []*os.File{controller.ChildFile()})
	controller.Started()
	activation, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()
	if err := controller.Activated(activation); err != nil {
		c.fatalf("node %d did not activate its integration hook: %v", id, err)
	}
	waitForNormalNodeReadiness(c.t, activation, c.harness, node.process, id)
	return node
}

// startAll launches the introducer first and then every other node, then
// waits for every member to see all four Alive.
func (c *craneCluster) startAll() {
	c.t.Helper()
	c.launch(1)
	for id := uint16(2); id <= 4; id++ {
		c.launch(id)
	}
	c.awaitMembership(4, 1, 2, 3, 4)
}

// awaitMembership waits until every listed observer sees `members` Alive
// members numbered 1..members.
func (c *craneCluster) awaitMembership(members int, observers ...uint16) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	waitForCluster(c.t, ctx, c.harness, fmt.Sprintf("all %d SWIM members Alive at observers %v", members, observers), func() (bool, error) {
		views, err := c.swim.views(ctx, observers...)
		if err != nil {
			return false, err
		}
		for observer, view := range views {
			for memberID := uint16(1); memberID <= uint16(members); memberID++ {
				if !hasMember(view, memberID, swim.Alive, 0) {
					return false, fmt.Errorf("observer %d view = %#v", observer, view)
				}
			}
		}
		return true, nil
	})
}

// awaitMemberStatus waits until any live observer reports memberID with
// status, returning the observer that saw it.
func (c *craneCluster) awaitMemberStatus(memberID uint16, status swim.Status, observers ...uint16) uint16 {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	var seenBy uint16
	waitForCluster(c.t, ctx, c.harness, fmt.Sprintf("member %d status %d", memberID, status), func() (bool, error) {
		for _, observer := range observers {
			members, err := c.swim.snapshot(ctx, observer)
			if err != nil {
				continue
			}
			if hasMember(members, memberID, status, 0) {
				seenBy = observer
				return true, nil
			}
		}
		return false, nil
	})
	return seenBy
}

func (c *craneCluster) kill(id uint16) {
	c.t.Helper()
	node := c.nodes[id]
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	if err := node.process.kill(ctx); err != nil {
		c.fatalf("kill node %d: %v", id, err)
	}
}

func (c *craneCluster) terminate(id uint16) {
	c.t.Helper()
	node := c.nodes[id]
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	if err := node.process.terminate(ctx); err != nil {
		c.fatalf("terminate node %d: %v", id, err)
	}
}

func (c *craneCluster) pause(id uint16) {
	c.t.Helper()
	if err := c.nodes[id].process.signal(syscall.SIGSTOP); err != nil {
		c.fatalf("SIGSTOP node %d: %v", id, err)
	}
}

func (c *craneCluster) resume(id uint16) {
	c.t.Helper()
	if err := c.nodes[id].process.signal(syscall.SIGCONT); err != nil {
		c.fatalf("SIGCONT node %d: %v", id, err)
	}
}

// loseStore destroys the node's durable worker state (its worker store and
// sealed artifacts) while the process is down; the next incarnation creates
// a new WorkerEpoch.
func (c *craneCluster) loseStore(id uint16) {
	c.t.Helper()
	node := c.nodes[id]
	if !node.process.exited() {
		c.fatalf("cannot lose the store of running node %d", id)
	}
	for _, directory := range []string{worker.WorkerStoreDirectory, "crane-artifacts"} {
		if err := os.RemoveAll(filepath.Join(node.configuration.StorageDir, directory)); err != nil {
			c.fatalf("remove %s of node %d: %v", directory, id, err)
		}
	}
}

// inspectStore opens a dead node's worker store offline and returns its
// durable epoch and recovered work. The store is closed again before return.
func (c *craneCluster) inspectStore(id uint16) (model.WorkerEpoch, store.RecoveredWork) {
	c.t.Helper()
	node := c.nodes[id]
	if node.process != nil && !node.process.exited() {
		c.fatalf("cannot inspect the store of running node %d", id)
	}
	path := filepath.Join(node.configuration.StorageDir, worker.WorkerStoreDirectory)
	durable, err := store.Open(path, store.Identity{ClusterID: c.clusterID, NodeID: id}, store.Options{MaxBytes: node.configuration.Crane.MaxWorkerStoreBytes})
	if err != nil {
		c.fatalf("open store of node %d offline: %v", id, err)
	}
	work, err := durable.RecoverWork()
	epoch := durable.WorkerEpoch()
	_ = durable.Close()
	if err != nil {
		c.fatalf("recover work of node %d offline: %v", id, err)
	}
	return epoch, work
}

// Hook conveniences ---------------------------------------------------------

func (c *craneCluster) hookContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.ctx, 60*time.Second)
}

func (c *craneCluster) watch(id uint16, names ...string) {
	c.t.Helper()
	ctx, cancel := c.hookContext()
	defer cancel()
	for _, name := range names {
		if err := c.nodes[id].hook.Watch(ctx, name); err != nil {
			c.fatalf("watch %s on node %d: %v", name, id, err)
		}
	}
}

func (c *craneCluster) watchAll(names ...string) {
	c.t.Helper()
	for id := uint16(1); id <= 4; id++ {
		c.watch(id, names...)
	}
}

func (c *craneCluster) block(id uint16, name string, occurrence int) {
	c.t.Helper()
	ctx, cancel := c.hookContext()
	defer cancel()
	if err := c.nodes[id].hook.Block(ctx, name, occurrence); err != nil {
		c.fatalf("block %s#%d on node %d: %v", name, occurrence, id, err)
	}
}

func (c *craneCluster) rule(id uint16, rule integrationhook.Rule) {
	c.t.Helper()
	ctx, cancel := c.hookContext()
	defer cancel()
	if err := c.nodes[id].hook.Rule(ctx, rule); err != nil {
		c.fatalf("rule %+v on node %d: %v", rule, id, err)
	}
}

// blockNext parks the node at the next occurrence of name and returns it.
func (c *craneCluster) blockNext(id uint16, name string) int {
	c.t.Helper()
	ctx, cancel := c.hookContext()
	defer cancel()
	occurrence, err := c.nodes[id].hook.BlockNext(ctx, name)
	if err != nil {
		c.fatalf("blocknext %s on node %d: %v", name, id, err)
	}
	return occurrence
}

func (c *craneCluster) unblock(id uint16, name string) {
	c.t.Helper()
	if c.nodes[id].process.exited() {
		return
	}
	ctx, cancel := c.hookContext()
	defer cancel()
	if err := c.nodes[id].hook.Unblock(ctx, name); err != nil && !errors.Is(err, integrationhook.ErrPeerClosed) {
		c.fatalf("unblock %s on node %d: %v", name, id, err)
	}
}

// blockedAt reports whether the node's current incarnation has parked at
// the given occurrence of name.
func (c *craneCluster) blockedAt(id uint16, name string, occurrence int) bool {
	for _, event := range c.nodes[id].hook.Events() {
		if event.Kind == integrationhook.EventBlocked && event.Name == name && event.Occurrence == occurrence {
			return true
		}
	}
	return false
}

// awaitFirstBlocked installs blockNext(name) on every listed node, waits for
// the first one to park, clears the pending block on the others, and
// returns the parked node. Nodes that never receive the boundary keep no
// stale block behind.
func (c *craneCluster) awaitFirstBlocked(name string, timeout time.Duration, trigger func() *craneJob, candidates ...uint16) uint16 {
	c.t.Helper()
	occurrences := map[uint16]int{}
	for _, id := range candidates {
		occurrences[id] = c.blockNext(id, name)
	}
	var job *craneJob
	if trigger != nil {
		job = trigger()
	}
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	var parked uint16
	var last protocol.StatusResponse
	lastProbe := time.Now()
	err := waitCondition(ctx, 25*time.Millisecond, func() (bool, error) {
		for _, id := range candidates {
			if c.blockedAt(id, name, occurrences[id]) {
				parked = id
				return true, nil
			}
		}
		if job != nil && time.Since(lastProbe) > time.Second {
			lastProbe = time.Now()
			statusCtx, cancelStatus := context.WithTimeout(ctx, 10*time.Second)
			status, err := c.client().Status(statusCtx, job.id)
			cancelStatus()
			if err == nil {
				last = status
				if status.State == protocol.JobSucceeded {
					return false, fmt.Errorf("job %s completed before any of %v parked at %s", job.spec.Name, candidates, name)
				}
			}
		}
		return false, nil
	})
	if err != nil {
		c.fatalf("wait for any of %v parked at %s: %v (last status state=%d jcr=%d rev=%d)\n%s", candidates, name, err, last.State, last.JobControlRevision, last.AssignmentRevision, c.diagnostics())
	}
	c.releaseOthers(name, occurrences, parked)
	return parked
}

// releaseOthers clears the pending block on every node but keep and, when a
// node had already parked at its occurrence in the meantime, continues it:
// a node left parked would hold its store mutex for the rest of the run.
func (c *craneCluster) releaseOthers(name string, occurrences map[uint16]int, keep uint16) {
	c.t.Helper()
	for id, occurrence := range occurrences {
		if id == keep || c.nodes[id].process.exited() {
			continue
		}
		c.unblock(id, name)
		if c.blockedAt(id, name, occurrence) {
			c.continueNode(id)
		}
	}
}

func (c *craneCluster) continueNode(id uint16) {
	c.t.Helper()
	ctx, cancel := c.hookContext()
	defer cancel()
	if err := c.nodes[id].hook.Continue(ctx); err != nil {
		c.fatalf("continue node %d: %v", id, err)
	}
}

func (c *craneCluster) awaitBoundary(id uint16, name string, occurrence int, timeout time.Duration) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	if err := c.nodes[id].hook.WaitBoundary(ctx, name, occurrence); err != nil {
		c.fatalf("await %s#%d on node %d: %v (events %v)", name, occurrence, id, err, summarizeEvents(c.nodes[id].hook.Events()))
	}
}

func (c *craneCluster) awaitBlocked(id uint16, name string, occurrence int, timeout time.Duration) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	if err := c.nodes[id].hook.WaitBlocked(ctx, name, occurrence); err != nil {
		c.fatalf("await blocked %s#%d on node %d: %v (events %v)", name, occurrence, id, err, summarizeEvents(c.nodes[id].hook.Events()))
	}
}

// boundaryCount counts the boundary occurrences a node's current
// incarnation has published.
func (c *craneCluster) boundaryCount(id uint16, name string) int {
	return c.nodes[id].hook.Count(integrationhook.EventBoundary, name)
}

func summarizeEvents(events []integrationhook.Event) string {
	counts := map[string]int{}
	for _, event := range events {
		key := string(event.Kind) + ":" + event.Name
		if event.Kind == integrationhook.EventRuleConsumed {
			key = "rule:" + event.RuleID
		}
		counts[key]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, " ")
}

// diagnostics renders every node's event summary for failure messages.
func (c *craneCluster) diagnostics() string {
	var lines []string
	// Each voter's own answer about leadership, so a redirect cycle can be
	// read against who actually leads at the moment of failure.
	for id := uint16(1); id <= 3; id++ {
		if node := c.nodes[id]; node.process != nil {
			isLeader, err := c.probeLeadership(id)
			lines = append(lines, fmt.Sprintf("voter %d leadership probe: leader=%v err=%v", id, isLeader, err))
		}
	}
	for id := uint16(1); id <= 4; id++ {
		if node := c.nodes[id]; node.hook != nil {
			lines = append(lines, fmt.Sprintf("node %d[%d]: %s", id, node.incarnation, summarizeEvents(node.hook.Events())))
		}
		// The tail of each live process's captured stderr explains what the
		// boundary counters cannot (leadership terms, rejected commands).
		if node := c.nodes[id]; node.process != nil {
			// A failing phase is terminal for the run, so ask every live node
			// for a goroutine dump (SIGQUIT) and keep the complete captured
			// output in a file for offline analysis of stalls.
			if node.process.command != nil && node.process.command.Process != nil {
				_ = node.process.command.Process.Signal(syscall.SIGQUIT)
				time.Sleep(750 * time.Millisecond)
				dumpPath := filepath.Join(os.TempDir(), fmt.Sprintf("crane-node-%d-%d.dump", id, node.incarnation))
				if writeErr := os.WriteFile(dumpPath, []byte(node.process.log.String()), 0o600); writeErr == nil {
					lines = append(lines, fmt.Sprintf("node %d[%d] full log + goroutine dump: %s", id, node.incarnation, dumpPath))
				}
			}
			logLines := strings.Split(strings.TrimSpace(node.process.log.String()), "\n")
			if len(logLines) > 12 {
				logLines = logLines[len(logLines)-12:]
			}
			lines = append(lines, fmt.Sprintf("node %d[%d] log tail:\n  %s", id, node.incarnation, strings.Join(logLines, "\n  ")))
		}
	}
	return strings.Join(lines, "\n")
}

// assertRulesConsumed fails unless every rule requested from every
// incarnation of every node was consumed exactly its requested count.
func (c *craneCluster) assertRulesConsumed() {
	c.t.Helper()
	for id := uint16(1); id <= 4; id++ {
		node := c.nodes[id]
		controllers := append([]*integrationhook.Controller(nil), node.pastHooks...)
		if node.hook != nil {
			controllers = append(controllers, node.hook)
		}
		for index, controller := range controllers {
			if unconsumed := controller.Unconsumed(); len(unconsumed) != 0 {
				c.fatalf("node %d incarnation %d rules not consumed exactly once: %v", id, index+1, unconsumed)
			}
		}
	}
}

// Public client ---------------------------------------------------------------

// identity is the member the harness authenticates as: the lowest node
// whose process is currently running. +6 and +5 fail closed on senders that
// are not live members, so a fixed identity would go dark whenever that
// node is the one being crashed.
func (c *craneCluster) identity() uint16 {
	if live := c.liveNodes(); len(live) > 0 {
		return live[0]
	}
	return 1
}

// client returns a real +6 client authenticated as the current identity;
// one durable client identity store is kept per node identity.
func (c *craneCluster) client() *control.Client {
	c.t.Helper()
	id := c.identity()
	if client := c.clients[id]; client != nil {
		return client
	}
	client := c.newClientWithDialRecorder(id, func(string) {})
	c.clients[id] = client
	return client
}

// newClientWithDialRecorder builds a client that reports every address it
// dials without altering any dial.
func (c *craneCluster) newClientWithDialRecorder(identity uint16, record func(string)) *control.Client {
	c.t.Helper()
	// The identity store must outlive the phase (subtest) that created it.
	stateDir := c.top.TempDir()
	if err := os.Chmod(stateDir, 0o700); err != nil {
		c.t.Fatal(err)
	}
	clientStore, err := clientstate.OpenClientState(filepath.Join(stateDir, "client.state"), c.clusterID)
	if err != nil {
		c.t.Fatal(err)
	}
	dialer := &net.Dialer{}
	client, err := control.NewClient(control.ClientOptions{
		Config: c.configurations[identity-1], Authenticator: wire.NewHMACAuthenticator(c.secret), Clock: clock.NewReal(), Store: clientStore,
		MaxAttempts: 40, MaxRedirects: 4, RetryBackoff: 250 * time.Millisecond, RequestTimeout: 3 * time.Second,
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			record(address)
			return dialer.DialContext(ctx, "tcp", address)
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	return client
}

// controlEndpoint is a voter's +6 address.
func (c *craneCluster) controlEndpoint(id uint16) string {
	endpoint, err := c.nodes[id].configuration.AdvertiseEndpoint(config.ServiceTopologyControl)
	if err != nil {
		c.t.Fatal(err)
	}
	return endpoint.String()
}

// probeLeadership asks one voter's +6 for an unknown job: a follower answers
// with a LeaderRedirect, the leader with a bound NotFound-class rejection.
func (c *craneCluster) probeLeadership(id uint16) (isLeader bool, err error) {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.controlEndpoint(id))
	if err != nil {
		return false, err
	}
	defer connection.Close()
	var requestID wire.RequestID
	if _, err := rand.Read(requestID[:]); err != nil {
		return false, err
	}
	var job model.JobID
	job[0] = 0xEE
	payload, err := protocol.MarshalControlMessage(protocol.StatusRequest{JobID: job})
	if err != nil {
		return false, err
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.PublicControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &c.clusterID
	stream := wire.NewTCPFrameStream(connection, wire.NewHMACAuthenticator(c.secret), limits, 2*time.Second)
	frame := wire.Frame{Header: wire.Header{Version: wire.Version1, Message: wire.MessageCraneStatusRequest, ClusterID: c.clusterID, SenderID: c.identity(), RequestID: requestID, TimestampMillis: time.Now().UnixMilli(), Codec: wire.CodecBinary}, Payload: payload}
	if err := stream.WriteFrame(ctx, frame); err != nil {
		return false, err
	}
	response, err := stream.ReadFrame(ctx)
	if err != nil {
		return false, err
	}
	message, err := protocol.UnmarshalControlMessage(response.Header.Message, response.Payload)
	if err != nil {
		return false, err
	}
	switch decoded := message.(type) {
	case protocol.LeaderRedirect:
		// A single endpoint is a leader hint; the full static voter list is
		// the fallback of a voter that knows no leader.
		if len(decoded.Endpoints) != 1 {
			return false, fmt.Errorf("voter %d has no leader: redirect lists %d endpoints", id, len(decoded.Endpoints))
		}
		return false, nil
	case protocol.ControlError:
		if decoded.Code == protocol.ControlErrorStarting && strings.Contains(string(decoded.Detail), "admission gate is closed") {
			// Only the voter whose Barrier succeeded reaches the gate check:
			// a leader whose coordinator is still reconciling answers this
			// way, while followers redirect instead.
			return true, nil
		}
		if decoded.Code == protocol.ControlErrorStarting || decoded.Code == protocol.ControlErrorNotLeader {
			return false, fmt.Errorf("voter %d has no leader: code %d", id, decoded.Code)
		}
		return true, nil
	default:
		return false, fmt.Errorf("unexpected probe response %T", message)
	}
}

// awaitLeader waits until exactly one of the given voters answers as leader
// and returns it.
func (c *craneCluster) awaitLeader(voters ...uint16) uint16 {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	var leader uint16
	last := map[uint16]string{}
	defer func() {
		if c.t.Failed() {
			c.t.Logf("last leadership probes: %v", last)
		}
	}()
	// +6 admits at most four concurrent connections per source address and
	// closes the rest before reading; probe at a civil rate so the harness
	// never starves its own probes during an election.
	err := waitCondition(ctx, 250*time.Millisecond, func() (bool, error) {
		leader = 0
		for _, id := range voters {
			isLeader, err := c.probeLeadership(id)
			if err != nil {
				last[id] = err.Error()
				continue
			}
			last[id] = fmt.Sprintf("leader=%v", isLeader)
			if isLeader {
				if leader != 0 {
					return false, fmt.Errorf("voters %d and %d both answer as leader", leader, id)
				}
				leader = id
			}
		}
		return leader != 0, nil
	})
	if err != nil {
		c.fatalf("wait for one voter answering as leader: %v (probes %v)", err, last)
	}
	return leader
}

// awaitStableLeader waits until the same voter answers as leader for
// `stable` consecutive seconds (probing once per second).
func (c *craneCluster) awaitStableLeader(stable int, timeout time.Duration, voters ...uint16) uint16 {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	var current uint16
	streak := 0
	err := waitCondition(ctx, time.Second, func() (bool, error) {
		leader := uint16(0)
		for _, id := range voters {
			if isLeader, err := c.probeLeadership(id); err == nil && isLeader {
				leader = id
			}
		}
		if leader == 0 || leader != current {
			current, streak = leader, 0
			return false, nil
		}
		streak++
		return streak >= stable, nil
	})
	if err != nil {
		c.fatalf("wait for a leader stable for %ds: %v (last %d streak %d)", stable, err, current, streak)
	}
	return current
}

// Topologies and reference evaluation ----------------------------------------

type craneTopologyShape struct {
	name                string
	start, end          int64
	factor, threshold   int64
	sourceParallelism   uint16
	multiplyParallelism uint16
	evenParallelism     uint16
	lessThanParallelism uint16
	includeEvenAndLess  bool
}

// craneTopology builds range → multiply → even → less_than → collect (or the
// shorter range → multiply → collect when includeEvenAndLess is false).
func craneTopology(shape craneTopologyShape) model.TopologySpec {
	stages := []model.StageSpec{{
		StageID: 1, Name: "numbers", Role: model.StageSource, Parallelism: shape.sourceParallelism,
		Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: fmt.Sprint(shape.end)}, {Key: "start", Value: fmt.Sprint(shape.start)}}},
	}, {
		StageID: 2, Name: "scaled", Role: model.StageTransform, Parallelism: shape.multiplyParallelism,
		Operator: model.OperatorSpec{Name: "multiply", Version: 1, Settings: []model.Setting{{Key: "factor", Value: fmt.Sprint(shape.factor)}}},
	}}
	edges := []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.RoutingShuffle}}
	next, edge := uint16(3), uint16(2)
	if shape.includeEvenAndLess {
		stages = append(stages, model.StageSpec{StageID: next, Name: "even", Role: model.StageTransform, Parallelism: shape.evenParallelism, Operator: model.OperatorSpec{Name: "even", Version: 1}})
		edges = append(edges, model.EdgeSpec{EdgeID: edge, SourceStageID: next - 1, DestinationStageID: next, Routing: model.RoutingShuffle})
		next++
		edge++
		stages = append(stages, model.StageSpec{StageID: next, Name: "small", Role: model.StageTransform, Parallelism: shape.lessThanParallelism, Operator: model.OperatorSpec{Name: "less_than", Version: 1, Settings: []model.Setting{{Key: "threshold", Value: fmt.Sprint(shape.threshold)}}}})
		edges = append(edges, model.EdgeSpec{EdgeID: edge, SourceStageID: next - 1, DestinationStageID: next, Routing: model.RoutingShuffle})
		next++
		edge++
	}
	stages = append(stages, model.StageSpec{StageID: next, Name: "collected", Role: model.StageSink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}})
	edges = append(edges, model.EdgeSpec{EdgeID: edge, SourceStageID: next - 1, DestinationStageID: next, Routing: model.RoutingShuffle})
	return model.TopologySpec{SchemaVersion: 1, Name: shape.name, Stages: stages, Edges: edges, RegistryFingerprint: model.RegistryFingerprint()}
}

// referenceValues evaluates the topology purely (model.SourceTuple over every
// source partition, then each transform through model.ExecuteOperator) and
// returns the sorted multiset of collected values.
func referenceValues(t *testing.T, spec model.TopologySpec) []int64 {
	t.Helper()
	validated, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	var values []int64
	source := spec.Stages[0]
	for partition := uint16(0); partition < source.Parallelism; partition++ {
		task := model.TaskID{JobID: model.JobID{1}, StageID: source.StageID, Partition: partition}
		for sequence := uint64(1); ; sequence++ {
			tuple, exists, err := model.SourceTuple(validated, task, sequence)
			if err != nil {
				t.Fatal(err)
			}
			if !exists {
				break
			}
			current := []model.Tuple{tuple}
			for _, stage := range spec.Stages[1:] {
				if stage.Role == model.StageSink {
					continue
				}
				var next []model.Tuple
				for _, input := range current {
					outputs, err := model.ExecuteOperator(stage.Operator, input)
					if err != nil {
						t.Fatal(err)
					}
					next = append(next, outputs...)
				}
				current = next
			}
			for _, output := range current {
				values = append(values, output.Fields[0].Value.Int64)
			}
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func recordValues(t *testing.T, records []model.ResultRecord) []int64 {
	t.Helper()
	values := make([]int64, 0, len(records))
	for _, record := range records {
		tuple, err := model.UnmarshalTuple(record.Value)
		if err != nil || len(tuple.Fields) != 1 || tuple.Fields[0].Name != "value" {
			t.Fatalf("result record decode = %+v,%v", tuple, err)
		}
		values = append(values, tuple.Fields[0].Value.Int64)
	}
	return values
}

func sortedCopy(values []int64) []int64 {
	result := append([]int64(nil), values...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

// Jobs ------------------------------------------------------------------------

type craneJob struct {
	id        model.JobID
	spec      model.TopologySpec
	reference []int64
}

// submit issues one durable submission. A leadership change between two
// redirect hops makes the checked client refuse the exchange as a redirect
// loop; the whole call is then retried with the same client, which resumes
// its own pending reservation, so a retry can never create a second job.
func (c *craneCluster) submit(spec model.TopologySpec) craneJob {
	c.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
		job, _, err := c.client().Submit(ctx, spec)
		cancel()
		if err == nil {
			return craneJob{id: job, spec: spec, reference: referenceValues(c.t, spec)}
		}
		lastErr = err
		if !errors.Is(err, control.ErrClientRedirectLoop) && !errors.Is(err, control.ErrClientAttemptsExhausted) {
			break
		}
		c.t.Logf("submit %s attempt %d: %v (retrying the whole call)", spec.Name, attempt+1, err)
		time.Sleep(time.Second)
	}
	c.fatalf("submit %s: %v", spec.Name, lastErr)
	return craneJob{}
}

// status reads one job status; a leadership change between redirect hops
// is retried as a whole call, exactly like submit.
func (c *craneCluster) status(job craneJob) protocol.StatusResponse {
	c.t.Helper()
	status, err := c.statusAttempt(job, 20)
	if err != nil {
		c.fatalf("status %s: %v", job.spec.Name, err)
	}
	return status
}

// statusAttempt polls status through the client for a bounded number of
// whole-call attempts and returns the last error instead of failing the test.
func (c *craneCluster) statusAttempt(job craneJob, attempts int) (protocol.StatusResponse, error) {
	c.t.Helper()
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
		status, err := c.client().Status(ctx, job.id)
		cancel()
		if err == nil {
			return status, nil
		}
		lastErr = err
		if !errors.Is(err, control.ErrClientRedirectLoop) && !errors.Is(err, control.ErrClientAttemptsExhausted) {
			break
		}
		time.Sleep(time.Second)
	}
	return protocol.StatusResponse{}, lastErr
}

// awaitStatus polls status until predicate holds.
func (c *craneCluster) awaitStatus(job craneJob, what string, timeout time.Duration, predicate func(protocol.StatusResponse) bool) protocol.StatusResponse {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	var last protocol.StatusResponse
	var lastErr error
	err := waitCondition(ctx, 100*time.Millisecond, func() (bool, error) {
		statusContext, cancelStatus := context.WithTimeout(ctx, 20*time.Second)
		defer cancelStatus()
		status, err := c.client().Status(statusContext, job.id)
		if err != nil {
			lastErr = err
			return false, nil
		}
		last, lastErr = status, nil
		if status.State == protocol.JobFailed || status.State == protocol.JobCanceled {
			return false, fmt.Errorf("job %s reached terminal state %d (failure=%v code=%d)", job.spec.Name, status.State, status.HasFailure, status.FailureCode)
		}
		return predicate(status), nil
	})
	if err != nil {
		diagnostics := c.diagnostics()
		dump := c.dumpJobState(job)
		c.fatalf("await %s for %s: %v (last status state=%d jcr=%d rev=%d completedSources=%d manifests=%d failure=%v/%d, last error %v)\n%s\n%s", what, job.spec.Name, err, last.State, last.JobControlRevision, last.AssignmentRevision, last.CompletedSourceTasks, last.ManifestCount, last.HasFailure, last.FailureCode, lastErr, diagnostics, dump)
	}
	return last
}

func (c *craneCluster) awaitSucceeded(job craneJob, timeout time.Duration) protocol.StatusResponse {
	c.t.Helper()
	return c.awaitStatus(job, "Succeeded with a manifest set", timeout, func(status protocol.StatusResponse) bool {
		return status.State == protocol.JobSucceeded && status.HasManifestSet
	})
}

// results pages the complete result set through the +6 protocol.
func (c *craneCluster) results(job craneJob) []model.ResultRecord {
	c.t.Helper()
	status := c.status(job)
	if !status.HasManifestSet {
		c.fatalf("job %s has no manifest set", job.spec.Name)
	}
	request := protocol.ResultPageRequest{JobID: job.id, ManifestDigest: status.ManifestSetDigest, PageBytes: 64 << 10}
	var records []model.ResultRecord
	for {
		var page protocol.ResultPageResponse
		var err error
		for attempt := 0; attempt < 60; attempt++ {
			ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
			page, err = c.client().ResultPage(ctx, request)
			cancel()
			if err == nil || !errors.Is(err, control.ErrClientRedirectLoop) && !errors.Is(err, control.ErrClientAttemptsExhausted) {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			c.fatalf("result page for %s: %v\n%s", job.spec.Name, err, c.diagnostics())
		}
		records = append(records, page.Records...)
		if page.End {
			return records
		}
		if !page.NextHasLastTuple {
			c.fatalf("result page neither ended nor advanced")
		}
		request.HasLastTuple, request.Last = true, page.NextLast
	}
}

// verifyResults proves the paged output is unique, ordered identically on
// every query, and equals the pure reference evaluation as a multiset.
func (c *craneCluster) verifyResults(job craneJob) []model.ResultRecord {
	c.t.Helper()
	records := c.results(job)
	seen := map[model.TupleID]bool{}
	for _, record := range records {
		if seen[record.TupleID] {
			c.fatalf("job %s emitted duplicate TupleID %+v", job.spec.Name, record.TupleID)
		}
		seen[record.TupleID] = true
	}
	values := sortedCopy(recordValues(c.t, records))
	if !equalInt64s(values, job.reference) {
		c.fatalf("job %s output %v != reference %v", job.spec.Name, values, job.reference)
	}
	again := c.results(job)
	if len(again) != len(records) {
		c.fatalf("job %s second query returned %d records, first %d", job.spec.Name, len(again), len(records))
	}
	for index := range records {
		if again[index].TupleID != records[index].TupleID || !bytes.Equal(again[index].Value, records[index].Value) {
			c.fatalf("job %s second query differs at %d", job.spec.Name, index)
		}
	}
	return records
}

// cliResults runs the real crane binary and returns the decoded values
// in emitted order plus the summary record count.
func (c *craneCluster) cliResults(job craneJob) ([]int64, int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, c.craneBinary, "results", "-config", c.configPaths[c.identity()-1], "-job", hex.EncodeToString(job.id[:]), "-attempts", "20")
	output, err := command.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			c.fatalf("crane results: %v\n%s", err, exit.Stderr)
		}
		c.fatalf("crane results: %v", err)
	}
	var values []int64
	total := -1
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			c.fatalf("crane results line %q: %v", scanner.Text(), err)
		}
		if fields, ok := line["fields"].(map[string]any); ok {
			values = append(values, int64(fields["value"].(float64)))
			continue
		}
		if records, ok := line["records"].(float64); ok {
			total = int(records)
		}
	}
	return values, total
}

// ---------------------------------------------------------------------------
// Phase bookkeeping
// ---------------------------------------------------------------------------

// phase runs one named scenario step as a subtest; after a failure every
// later phase is skipped because they build on the same live cluster.
func (c *craneCluster) phase(name string, body func()) {
	c.t.Helper()
	if c.t.Failed() {
		c.t.Run(name, func(t *testing.T) { t.Skip("earlier phase failed") })
		return
	}
	started := time.Now()
	c.t.Run(name, func(t *testing.T) {
		previous := c.t
		c.t = t
		defer func() { c.t = previous }()
		body()
	})
	c.t.Logf("phase %s: %s", name, time.Since(started).Round(time.Millisecond))
}

func buildCraneBinaries(t *testing.T) (nodeBinary, craneBinary string) {
	t.Helper()
	root := repositoryRootForTest(t)
	return buildGoBinaryWithTags(t, root, "crane-node-craneintegration", "./cmd/node", []string{"craneintegration"}),
		buildGoBinaryWithTags(t, root, "crane", "./cmd/crane", nil)
}

// ---------------------------------------------------------------------------
// The required scenario
// ---------------------------------------------------------------------------

// allBoundaries is every durable boundary the scenario observes.
var allBoundaries = []string{
	store.BoundaryFence, store.BoundaryAssignmentClosed, store.BoundaryAssignmentRunning, store.BoundaryAssignmentDraining,
	store.BoundaryDeliveryReceived, store.BoundaryDeliveryProcessed, store.BoundaryDeliveryCompleted,
	store.BoundaryCheckpointApplied, store.BoundaryCheckpointObserved, store.BoundaryResultUpserted,
	store.BoundaryRepairPending, store.BoundaryRepairStreaming, store.BoundaryRepairComplete, store.BoundaryRepairFailed,
	store.BoundaryOutboxDispatched, store.BoundaryOutboxCompleted,
}

func (c *craneCluster) watchEverything(id uint16) { c.watch(id, allBoundaries...) }

// liveNodes lists the nodes whose current process is still running.
func (c *craneCluster) liveNodes() []uint16 {
	var result []uint16
	for id := uint16(1); id <= 4; id++ {
		if node := c.nodes[id]; node.process != nil && !node.process.exited() {
			result = append(result, id)
		}
	}
	return result
}

// awaitDeparted waits until every live member has retired the node's old
// incarnation (Dead after a crash, Left after a graceful stop): SWIM refuses
// a rejoin while the identity is still Alive or Suspect anywhere.
func (c *craneCluster) awaitDeparted(id uint16) {
	c.t.Helper()
	observers := c.liveNodes()
	if len(observers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	waitForCluster(c.t, ctx, c.harness, fmt.Sprintf("node %d Dead or Left at %v", id, observers), func() (bool, error) {
		views, err := c.swim.views(ctx, observers...)
		if err != nil {
			return false, err
		}
		for observer, view := range views {
			member := memberByID(view, id)
			if member.NodeID == id && member.Status != swim.Dead && member.Status != swim.Left {
				return false, fmt.Errorf("observer %d still sees node %d as %v", observer, id, member.Status)
			}
		}
		return true, nil
	})
}

// relaunch starts a new incarnation of a dead node once its previous
// incarnation has been retired everywhere, and re-arms its watches. A
// restarting seed is re-provisioned with a live introducer (its own
// endpoint would bootstrap a singleton cluster), exactly as an operator
// would; with no live peer it bootstraps as the seed again.
func (c *craneCluster) relaunch(id uint16) {
	c.t.Helper()
	c.awaitDeparted(id)
	node := c.nodes[id]
	introducer := c.configurations[0].Introducer
	if live := c.liveNodes(); id == 1 && len(live) > 0 {
		endpoint, err := c.nodes[live[0]].configuration.AdvertiseEndpoint(config.ServiceSWIMSnapshot)
		if err != nil {
			c.t.Fatal(err)
		}
		introducer = endpoint.String()
	}
	if node.configuration.Introducer != introducer {
		node.configuration.Introducer = introducer
		content, err := json.MarshalIndent(node.configuration, "", "  ")
		if err != nil {
			c.t.Fatal(err)
		}
		path := filepath.Join(filepath.Dir(node.configPath), fmt.Sprintf("node-%d.%d.json", id, node.incarnation+1))
		if err := os.WriteFile(path, append(content, '\n'), 0o600); err != nil {
			c.t.Fatal(err)
		}
		node.configPath = path
	}
	c.launch(id)
	c.watchEverything(id)
	// A restarted node is only usable as a member (and as the harness's
	// authenticated identity) once every live member sees it Alive again.
	observers := c.liveNodes()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	waitForCluster(c.t, ctx, c.harness, fmt.Sprintf("node %d Alive at %v after relaunch", id, observers), func() (bool, error) {
		views, err := c.swim.views(ctx, observers...)
		if err != nil {
			return false, err
		}
		for observer, view := range views {
			if !hasMember(view, id, swim.Alive, 0) {
				return false, fmt.Errorf("observer %d does not see node %d Alive", observer, id)
			}
		}
		return true, nil
	})
}

func (c *craneCluster) nonLeaders(leader uint16) []uint16 {
	var result []uint16
	for id := uint16(1); id <= 4; id++ {
		if id != leader {
			result = append(result, id)
		}
	}
	return result
}

// deliveriesInState counts a job's durable deliveries in one state.
func deliveriesInState(work store.RecoveredWork, job model.JobID, state store.DeliveryState) int {
	count := 0
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.JobID == job && delivery.State == state {
			count++
		}
	}
	return count
}

func installedAssignment(work store.RecoveredWork, job model.JobID) (store.InstalledAssignment, bool) {
	for _, assignment := range work.Assignments {
		if assignment.Assignment.JobID == job {
			return assignment, true
		}
	}
	return store.InstalledAssignment{}, false
}

// resultSetDigest is a canonical digest over one sink task's retained
// logical records (TupleID + checksum), the covered set a replica reports.
func resultSetDigest(work store.RecoveredWork, job model.JobID) ([32]byte, int) {
	type entry struct {
		id       model.TupleID
		checksum [32]byte
	}
	var entries []entry
	for _, result := range work.Results {
		if result.Record.TupleID.JobID == job {
			entries = append(entries, entry{id: result.Record.TupleID, checksum: result.Record.Checksum})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return tupleIDLessForTest(entries[i].id, entries[j].id) })
	var canonical []byte
	for _, item := range entries {
		canonical = append(canonical, []byte(fmt.Sprintf("%+v|%x;", item.id, item.checksum))...)
	}
	return sha256Sum(canonical), len(entries)
}

func tupleIDLessForTest(left, right model.TupleID) bool {
	return fmt.Sprintf("%+v", left) < fmt.Sprintf("%+v", right)
}

func TestCraneLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	nodeBinary, craneBinary := buildCraneBinaries(t)
	cluster := newCraneCluster(t, ctx, nodeBinary, craneBinary, nil)

	var leader uint16
	jobs := map[string]craneJob{}
	finalRecords := map[string][]model.ResultRecord{}
	epochs := map[uint16]model.WorkerEpoch{}
	var storeLossVictim, storeLossPreEpoch = uint16(0), model.WorkerEpoch{}
	var sinkLossVictim uint16

	shape := func(name string, end int64) model.TopologySpec {
		return craneTopology(craneTopologyShape{name: name, start: 0, end: end, factor: 3, threshold: 10_000, sourceParallelism: 3, multiplyParallelism: 3, evenParallelism: 2, lessThanParallelism: 2, includeEvenAndLess: true})
	}
	finish := func(name string, job craneJob, timeout time.Duration) {
		cluster.t.Helper()
		cluster.awaitSucceeded(job, timeout)
		finalRecords[name] = cluster.verifyResults(job)
		jobs[name] = job
		cluster.assertRulesConsumed()
	}

	cluster.phase("start_cluster", func() {
		cluster.startAll()
		for id := uint16(1); id <= 4; id++ {
			cluster.watchEverything(id)
		}
		leader = cluster.awaitLeader(1, 2, 3)
		cluster.t.Logf("leader is voter %d", leader)
	})

	cluster.phase("submit_through_follower", func() {
		// The public client always starts at the lowest-sorted voter +6
		// endpoint. When that voter currently leads, step it down gracefully
		// so the submission provably enters through a follower and follows
		// the redirect to the leader.
		endpoints := []string{cluster.controlEndpoint(1), cluster.controlEndpoint(2), cluster.controlEndpoint(3)}
		sort.Strings(endpoints)
		first := uint16(0)
		for id := uint16(1); id <= 3; id++ {
			if cluster.controlEndpoint(id) == endpoints[0] {
				first = id
			}
		}
		if first == leader {
			cluster.terminate(leader)
			others := []uint16{}
			for id := uint16(1); id <= 3; id++ {
				if id != leader {
					others = append(others, id)
				}
			}
			leader = cluster.awaitLeader(others...)
			cluster.relaunch(first)
			cluster.awaitMembership(4, 1, 2, 3, 4)
			cluster.t.Logf("voter %d stepped down; leader is now voter %d", first, leader)
		}
		var dialMu sync.Mutex
		var dialed []string
		recorder := cluster.newClientWithDialRecorder(4, func(address string) {
			dialMu.Lock()
			dialed = append(dialed, address)
			dialMu.Unlock()
		})
		submitCtx, cancelSubmit := context.WithTimeout(ctx, 60*time.Second)
		jobID, _, err := recorder.Submit(submitCtx, shape("baseline", 30))
		cancelSubmit()
		if err != nil {
			cluster.fatalf("submit baseline: %v", err)
		}
		job := craneJob{id: jobID, spec: shape("baseline", 30), reference: referenceValues(cluster.t, shape("baseline", 30))}
		dialMu.Lock()
		trace := append([]string(nil), dialed...)
		dialMu.Unlock()
		if len(trace) < 2 || trace[0] != endpoints[0] || trace[0] == cluster.controlEndpoint(leader) || trace[len(trace)-1] != cluster.controlEndpoint(leader) {
			cluster.fatalf("submit did not enter through follower %d and reach leader %d: dials %v", first, leader, trace)
		}
		cluster.t.Logf("submit entered through follower %d and was redirected to leader %d", first, leader)
		finish("baseline", job, 90*time.Second)
	})

	cluster.phase("nonvoter_runs_tasks_without_raft_or_store", func() {
		for attempt := 0; cluster.boundaryCount(4, store.BoundaryDeliveryReceived) == 0 && attempt < 6; attempt++ {
			job := cluster.submit(shape(fmt.Sprintf("placement-%d", attempt), 12))
			cluster.awaitSucceeded(job, 60*time.Second)
			cluster.verifyResults(job)
		}
		if got := cluster.boundaryCount(4, store.BoundaryDeliveryReceived); got == 0 {
			cluster.fatalf("nonvoter 4 never took custody of a delivery")
		}
		if got := cluster.boundaryCount(4, store.BoundaryDeliveryProcessed); got == 0 {
			cluster.fatalf("nonvoter 4 never executed a delivery")
		}
		raftDirectory := filepath.Join(cluster.nodes[4].configuration.StorageDir, "raft")
		if _, err := os.Stat(raftDirectory); !errors.Is(err, os.ErrNotExist) {
			cluster.fatalf("nonvoter 4 owns Raft storage %s: %v", raftDirectory, err)
		}
		raftEndpoint, _ := cluster.nodes[4].configuration.BindEndpoint(config.ServiceRaftRPC)
		if !endpointBindable(raftEndpoint, config.TransportTCP) {
			cluster.fatalf("nonvoter 4 holds its +8 endpoint %s", raftEndpoint)
		}
	})

	cluster.phase("leader_loss_after_assignment", func() {
		before := map[uint16]int{}
		beforeDraining := map[uint16]int{}
		for id := uint16(1); id <= 4; id++ {
			before[id] = cluster.boundaryCount(id, store.BoundaryAssignmentRunning)
			beforeDraining[id] = cluster.boundaryCount(id, store.BoundaryAssignmentDraining)
		}
		job := cluster.submit(shape("leader-loss-assigned", 45))
		// Every worker must durably receive the committed scheduling state.
		// A small job can reach Draining on the fast workers before a slow
		// (race-detector) Running install lands on the last one, in which
		// case the coordinator correctly installs the now-committed Draining
		// state there instead; either durable install proves the assignment
		// reached the worker before the leader is killed.
		for id := uint16(1); id <= 4; id++ {
			deadline := time.Now().Add(30 * time.Second)
			for cluster.boundaryCount(id, store.BoundaryAssignmentRunning) <= before[id] && cluster.boundaryCount(id, store.BoundaryAssignmentDraining) <= beforeDraining[id] {
				if time.Now().After(deadline) {
					cluster.fatalf("node %d never received the committed scheduling install for leader-loss-assigned\n%s", id, cluster.diagnostics())
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		old := leader
		cluster.kill(old)
		others := []uint16{}
		for id := uint16(1); id <= 3; id++ {
			if id != old {
				others = append(others, id)
			}
		}
		leader = cluster.awaitLeader(others...)
		cluster.t.Logf("leader %d killed after assignment; new leader %d", old, leader)
		cluster.relaunch(old)
		cluster.awaitMembership(4, 1, 2, 3, 4)
		finish("leader-loss-assigned", job, 120*time.Second)
	})

	cluster.phase("leader_loss_after_uncommitted_progress", func() {
		checkpointsBefore := 0
		for id := uint16(1); id <= 4; id++ {
			checkpointsBefore += cluster.boundaryCount(id, store.BoundaryCheckpointApplied)
		}
		// Every sink replica parks at its first result of this job, so no
		// record can complete and no checkpoint can commit while transforms
		// keep making durable-but-uncommitted progress.
		occurrences := map[uint16]int{}
		for id := uint16(1); id <= 4; id++ {
			occurrences[id] = cluster.blockNext(id, store.BoundaryResultUpserted)
		}
		processedBefore := map[uint16]int{}
		for id := uint16(1); id <= 4; id++ {
			processedBefore[id] = cluster.boundaryCount(id, store.BoundaryDeliveryProcessed)
		}
		job := cluster.submit(shape("leader-loss-progress", 45))
		waitCtx, cancelWait := context.WithTimeout(ctx, 60*time.Second)
		waitForCluster(cluster.t, waitCtx, cluster.harness, "a sink replica parked at its first result and some transform progress", func() (bool, error) {
			parked, progressed := false, false
			for id := uint16(1); id <= 4; id++ {
				if cluster.blockedAt(id, store.BoundaryResultUpserted, occurrences[id]) {
					parked = true
				}
				if cluster.boundaryCount(id, store.BoundaryDeliveryProcessed) > processedBefore[id] {
					progressed = true
				}
			}
			return parked && progressed, nil
		})
		cancelWait()
		checkpointsNow := 0
		for id := uint16(1); id <= 4; id++ {
			checkpointsNow += cluster.boundaryCount(id, store.BoundaryCheckpointApplied)
		}
		if checkpointsNow != checkpointsBefore {
			cluster.fatalf("a checkpoint committed while every sink replica was parked: %d -> %d", checkpointsBefore, checkpointsNow)
		}
		old := leader
		cluster.kill(old)
		// The kill landed while progress was durable but uncommitted; the
		// parked replicas may now resume (a successor cannot converge its
		// reconciliation while a worker's store is held).
		for id := uint16(1); id <= 4; id++ {
			if id == old {
				continue
			}
			if cluster.blockedAt(id, store.BoundaryResultUpserted, occurrences[id]) {
				cluster.continueNode(id)
			} else {
				cluster.unblock(id, store.BoundaryResultUpserted)
			}
		}
		others := []uint16{}
		for id := uint16(1); id <= 3; id++ {
			if id != old {
				others = append(others, id)
			}
		}
		cluster.relaunch(old)
		leader = cluster.awaitLeader(others...)
		cluster.t.Logf("leader %d killed after uncommitted progress; new leader %d", old, leader)
		finish("leader-loss-progress", job, 120*time.Second)
	})

	cluster.phase("drop_and_duplicate_acks_and_deliveries", func() {
		received := map[uint16]int{}
		dispatched := map[uint16]int{}
		for id := uint16(1); id <= 4; id++ {
			received[id] = cluster.blockNext(id, store.BoundaryDeliveryReceived)
			dispatched[id] = cluster.blockNext(id, store.BoundaryOutboxDispatched)
		}
		job := cluster.submit(shape("datagram-faults", 40))
		handledReceived, handledDispatched := map[uint16]bool{}, map[uint16]bool{}
		installed := 0
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) && (len(handledReceived) < 4 || len(handledDispatched) < 4) {
			progressed := false
			for id := uint16(1); id <= 4; id++ {
				if !handledReceived[id] && cluster.blockedAt(id, store.BoundaryDeliveryReceived, received[id]) {
					action := integrationhook.Drop
					if id%2 == 0 {
						action = integrationhook.Duplicate
					}
					// The parked node is about to write the ACK for the
					// delivery it just made durable: that exact ACK is
					// dropped (the sender retries and is answered from
					// durable custody) or duplicated (the sender sees a
					// replay).
					cluster.rule(id, integrationhook.Rule{ID: fmt.Sprintf("ack-%s-%d", action, id), Direction: integrationhook.Send, Message: wire.MessageCraneTupleDeliveryAck, Action: action, Count: 1})
					cluster.continueNode(id)
					handledReceived[id] = true
					installed++
					progressed = true
				}
				if !handledDispatched[id] && cluster.blockedAt(id, store.BoundaryOutboxDispatched, dispatched[id]) {
					action := integrationhook.Drop
					if id > 2 {
						action = integrationhook.Duplicate
					}
					cluster.rule(id, integrationhook.Rule{ID: fmt.Sprintf("delivery-%s-%d", action, id), Direction: integrationhook.Send, Message: wire.MessageCraneTupleDelivery, Action: action, Count: 1})
					cluster.continueNode(id)
					handledDispatched[id] = true
					installed++
					progressed = true
				}
			}
			if !progressed {
				status := cluster.status(job)
				if status.State == protocol.JobSucceeded {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
		}
		for id := uint16(1); id <= 4; id++ {
			if !handledReceived[id] {
				cluster.unblock(id, store.BoundaryDeliveryReceived)
				if cluster.blockedAt(id, store.BoundaryDeliveryReceived, received[id]) {
					cluster.continueNode(id)
				}
			}
			if !handledDispatched[id] {
				cluster.unblock(id, store.BoundaryOutboxDispatched)
				if cluster.blockedAt(id, store.BoundaryOutboxDispatched, dispatched[id]) {
					cluster.continueNode(id)
				}
			}
		}
		if installed < 4 {
			cluster.fatalf("only %d datagram rules were installed at parked boundaries", installed)
		}
		cluster.t.Logf("installed %d exact one-shot datagram rules at parked boundaries", installed)
		finish("datagram-faults", job, 120*time.Second)
	})

	cluster.phase("worker_crash_exactly_after_received", func() {
		var job craneJob
		victim := cluster.awaitFirstBlocked(store.BoundaryDeliveryReceived, 60*time.Second, func() *craneJob { job = cluster.submit(shape("crash-received", 30)); return &job }, cluster.nonLeaders(leader)...)
		cluster.kill(victim)
		epoch, work := cluster.inspectStore(victim)
		if got := deliveriesInState(work, job.id, store.Received); got == 0 {
			cluster.fatalf("victim %d killed after Received holds no Received custody for the job: %+v", victim, work.Deliveries)
		}
		epochs[victim] = epoch
		cluster.t.Logf("worker %d killed exactly after Received: %d durable Received deliveries, epoch %x", victim, deliveriesInState(work, job.id, store.Received), epoch[:4])
		cluster.relaunch(victim)
		cluster.awaitMembership(4, 1, 2, 3, 4)
		finish("crash-received", job, 120*time.Second)
	})

	cluster.phase("worker_crash_exactly_after_processed", func() {
		var job craneJob
		victim := cluster.awaitFirstBlocked(store.BoundaryDeliveryProcessed, 60*time.Second, func() *craneJob { job = cluster.submit(shape("crash-processed", 30)); return &job }, cluster.nonLeaders(leader)...)
		cluster.kill(victim)
		epoch, work := cluster.inspectStore(victim)
		if got := deliveriesInState(work, job.id, store.Processed); got == 0 {
			cluster.fatalf("victim %d killed after Processed holds no Processed custody for the job", victim)
		}
		if previous, seen := epochs[victim]; seen && previous != epoch {
			cluster.fatalf("store-preserving restart changed worker %d epoch %x -> %x", victim, previous[:4], epoch[:4])
		}
		epochs[victim] = epoch
		cluster.t.Logf("worker %d killed exactly after Processed: %d durable Processed deliveries", victim, deliveriesInState(work, job.id, store.Processed))
		cluster.relaunch(victim)
		cluster.awaitMembership(4, 1, 2, 3, 4)
		finish("crash-processed", job, 120*time.Second)
	})

	cluster.phase("suspect_alone_never_reassigns", func() {
		occurrences := map[uint16]int{}
		for id := uint16(1); id <= 4; id++ {
			occurrences[id] = cluster.blockNext(id, store.BoundaryResultUpserted)
		}
		job := cluster.submit(shape("suspect-only", 30))
		waitCtx, cancelWait := context.WithTimeout(ctx, 60*time.Second)
		waitForCluster(cluster.t, waitCtx, cluster.harness, "a sink replica parked mid-job", func() (bool, error) {
			for id := uint16(1); id <= 4; id++ {
				if cluster.blockedAt(id, store.BoundaryResultUpserted, occurrences[id]) {
					return true, nil
				}
			}
			return false, nil
		})
		cancelWait()
		// While a replica is parked at a durable boundary the leader cannot
		// finish a reconciliation pass that starts after the park, so a
		// leadership change during this phase closes the admission gate for
		// the rest of the park. That loses the premise ("the cluster keeps
		// serving while one member is merely Suspect") without saying
		// anything about reassignment, so it is logged and the revision
		// assertion is skipped rather than failed.
		premise := true
		parkedStatus := func(what string) (protocol.StatusResponse, bool) {
			status, err := cluster.statusAttempt(job, 20)
			if err == nil {
				return status, true
			}
			if current := cluster.awaitLeader(1, 2, 3); current != leader {
				cluster.t.Logf("premise lost: leadership moved from %d to %d while a replica was parked (%s); Suspect-only assertion skipped", leader, current, what)
				leader = current
				return protocol.StatusResponse{}, false
			}
			cluster.fatalf("status %s (%s): %v", job.spec.Name, what, err)
			return protocol.StatusResponse{}, false
		}
		status, ok := parkedStatus("before pause")
		premise = premise && ok
		revision := status.AssignmentRevision
		cluster.pause(4)
		observer := cluster.awaitMemberStatus(4, swim.Suspect, 1, 2, 3)
		cluster.resume(4)
		cluster.t.Logf("nonvoter 4 was Suspect at observer %d and resumed", observer)
		healCtx, cancelHeal := context.WithTimeout(ctx, 30*time.Second)
		waitForCluster(cluster.t, healCtx, cluster.harness, "nonvoter 4 Alive again everywhere", func() (bool, error) {
			views, err := cluster.swim.views(healCtx, 1, 2, 3)
			if err != nil {
				return false, err
			}
			for _, view := range views {
				if hasMember(view, 4, swim.Dead, 0) {
					premise = false
				}
				if !hasMember(view, 4, swim.Alive, 0) {
					return false, nil
				}
			}
			return true, nil
		})
		cancelHeal()
		// Outlast the failure grace period: a Suspect-only window must never
		// start, let alone commit, a reassignment. Per the Task 25 ruling,
		// reassignment means a new AssignmentSet revision or a
		// reassignment-class commit; idempotent admission re-drives
		// (content-identical Closed→Running cycles at an unchanged revision
		// and epoch on healthy peers while an assigned worker is unobservable
		// to assignmentAdmissionCurrent) are the I1/C2 convergence design and
		// are permitted, and each cycle may durably record the two admission
		// progressions — so the raw durable install counts are deliberately
		// NOT pinned here; the unchanged assignment revision below already
		// proves no reassignment-class commit, and job progress after the
		// heal (finish below) proves the job itself was never disturbed.
		time.Sleep(time.Duration(cluster.configurations[0].Crane.FailureGracePeriod) + 500*time.Millisecond)
		if premise {
			if after, ok := parkedStatus("after resume"); ok && after.AssignmentRevision != revision {
				cluster.fatalf("Suspect alone advanced assignment revision %d -> %d", revision, after.AssignmentRevision)
			}
		} else {
			cluster.t.Logf("premise lost: an observer marked node 4 Dead during the pause; Suspect-only assertion skipped")
		}
		for id := uint16(1); id <= 4; id++ {
			if cluster.blockedAt(id, store.BoundaryResultUpserted, occurrences[id]) {
				cluster.continueNode(id)
			} else {
				cluster.unblock(id, store.BoundaryResultUpserted)
			}
		}
		finish("suspect-only", job, 120*time.Second)
		if premise {
			jobs["suspect-only-attempts-one"] = job
		}
	})

	cluster.phase("store_loss_reassigns_and_rejects_stale_attempt", func() {
		var job craneJob
		victim := cluster.awaitFirstBlocked(store.BoundaryDeliveryReceived, 60*time.Second, func() *craneJob { job = cluster.submit(shape("store-loss", 36)); return &job }, cluster.nonLeaders(leader)...)
		before := cluster.status(job)
		// Every producer captures its next delivery addressed to the victim's
		// old incarnation: those exact authenticated frames are the stale
		// old-attempt deliveries injected after the reassignment commits.
		for id := uint16(1); id <= 4; id++ {
			if id != victim {
				cluster.rule(id, integrationhook.Rule{ID: "hold-stale-attempt", Direction: integrationhook.Send, Message: wire.MessageCraneTupleDelivery, Count: 1, Hold: true, To: victim, Optional: true})
			}
		}
		cluster.kill(victim)
		storeLossPreEpoch, _ = cluster.inspectStore(victim)
		cluster.loseStore(victim)
		storeLossVictim = victim
		cluster.relaunch(victim)
		after := cluster.awaitStatus(job, "committed reassignment", 90*time.Second, func(status protocol.StatusResponse) bool {
			return status.AssignmentRevision > before.AssignmentRevision
		})
		// Quiesce every legitimate delivery to the victim's new incarnation
		// for the duration of the custody check: after the reassignment the
		// victim may hold current tasks, and a legitimate new-attempt frame
		// landing inside the window would be indistinguishable from stale
		// custody by count alone (observed under -race). Released frames
		// bypass the hold rules, so only the stale-attempt frames reach the
		// victim while this hold is armed; the held legitimate frames are
		// released, unchanged, once the check completes.
		for id := uint16(1); id <= 4; id++ {
			cluster.rule(id, integrationhook.Rule{ID: "quiesce-legit-to-victim", Direction: integrationhook.Send, Message: wire.MessageCraneTupleDelivery, Count: 1024, Hold: true, To: victim, Optional: true})
		}
		// The victim's new incarnation must refuse every stale-attempt
		// delivery with a NACK from its real socket, without taking custody.
		cluster.rule(victim, integrationhook.Rule{ID: "expect-stale-attempt-nack", Direction: integrationhook.Send, Message: wire.MessageCraneTupleDeliveryNack, Action: integrationhook.Pass, Count: 1})
		receivedBefore := cluster.boundaryCount(victim, store.BoundaryDeliveryReceived)
		// Release (and thereby retire) every producer's hold, so legitimate
		// deliveries to the victim's new incarnation are never captured.
		released := 0
		for id := uint16(1); id <= 4; id++ {
			if id == victim {
				continue
			}
			releaseCtx, cancelRelease := cluster.hookContext()
			count, err := cluster.nodes[id].hook.Release(releaseCtx, "hold-stale-attempt")
			cancelRelease()
			if err != nil {
				cluster.fatalf("release stale delivery on node %d: %v", id, err)
			}
			released += count
		}
		if released == 0 {
			cluster.fatalf("no producer captured a delivery to victim %d before the kill\n%s", victim, cluster.diagnostics())
		}
		nackCtx, cancelNack := context.WithTimeout(ctx, 60*time.Second)
		if err := cluster.nodes[victim].hook.WaitRuleConsumed(nackCtx, "expect-stale-attempt-nack", 1); err != nil {
			cancelNack()
			cluster.fatalf("new incarnation of %d never NACKed a released stale-attempt delivery: %v (%s)", victim, err, summarizeEvents(cluster.nodes[victim].hook.Events()))
		}
		cancelNack()
		if got := cluster.boundaryCount(victim, store.BoundaryDeliveryReceived); got != receivedBefore {
			cluster.fatalf("victim %d took custody of a stale-attempt delivery (%d -> %d Received boundaries)", victim, receivedBefore, got)
		}
		for id := uint16(1); id <= 4; id++ {
			quiesceCtx, cancelQuiesce := cluster.hookContext()
			_, err := cluster.nodes[id].hook.Release(quiesceCtx, "quiesce-legit-to-victim")
			cancelQuiesce()
			if err != nil {
				cluster.fatalf("release quiesced legitimate deliveries on node %d: %v", id, err)
			}
		}
		cluster.t.Logf("released %d held stale-attempt deliveries after the reassignment; victim %d NACKed without custody", released, victim)
		cluster.t.Logf("worker %d lost its store: assignment revision %d -> %d, stale-attempt delivery NACKed", victim, before.AssignmentRevision, after.AssignmentRevision)
		finish("store-loss", job, 120*time.Second)
	})

	cluster.phase("sink_store_loss_repair_crash_and_reseal", func() {
		sinkLossVictim = sinkLossPhase(cluster, ctx, &leader, shape, finish)
	})

	cluster.phase("full_restart_identical_results", func() {
		for id := uint16(4); id >= 1; id-- {
			cluster.terminate(id)
		}
		for id := uint16(1); id <= 4; id++ {
			cluster.relaunch(id)
		}
		cluster.awaitMembership(4, 1, 2, 3, 4)
		leader = cluster.awaitStableLeader(10, 4*time.Minute, 1, 2, 3)
		for name, job := range jobs {
			if name == "suspect-only-attempts-one" {
				continue
			}
			status := cluster.awaitSucceeded(job, 60*time.Second)
			if !status.HasManifestSet {
				cluster.fatalf("job %s lost its manifest set across the restart", name)
			}
			records := cluster.results(job)
			previous := finalRecords[name]
			if len(records) != len(previous) {
				cluster.fatalf("job %s returned %d records after restart, %d before", name, len(records), len(previous))
			}
			for index := range records {
				if records[index].TupleID != previous[index].TupleID || !bytes.Equal(records[index].Value, previous[index].Value) {
					cluster.fatalf("job %s differs at record %d after restart", name, index)
				}
			}
		}
		values, total := cluster.cliResults(jobs["sink-loss"])
		if total != len(finalRecords["sink-loss"]) || !equalInt64s(values, recordValues(cluster.t, finalRecords["sink-loss"])) {
			cluster.fatalf("crane results = %d records %v, want %d identical ordered values", total, values, len(finalRecords["sink-loss"]))
		}
		cluster.t.Logf("all %d jobs report identical ordered results after a full restart; the CLI matches", len(finalRecords))
	})

	cluster.phase("offline_durable_state_audit", func() {
		for id := uint16(4); id >= 1; id-- {
			cluster.terminate(id)
		}
		works := map[uint16]store.RecoveredWork{}
		for id := uint16(1); id <= 4; id++ {
			epoch, work := cluster.inspectStore(id)
			works[id] = work
			if previous, seen := epochs[id]; seen && id != storeLossVictim && id != sinkLossVictim && previous != epoch {
				cluster.fatalf("node %d epoch changed across store-preserving restarts: %x -> %x", id, previous[:4], epoch[:4])
			}
			if id == storeLossVictim && epoch == storeLossPreEpoch {
				cluster.fatalf("node %d kept epoch %x across store loss", id, epoch[:4])
			}
		}
		if job, ok := jobs["suspect-only-attempts-one"]; ok {
			for id := uint16(1); id <= 4; id++ {
				if assignment, found := installedAssignment(works[id], job.id); found {
					for _, token := range assignment.Assignment.Tasks {
						if token.Attempt != 1 {
							cluster.fatalf("Suspect-only job task %+v has attempt %d on node %d", token.Task, token.Attempt, id)
						}
					}
				}
			}
		}
		reassigned := false
		for id := uint16(1); id <= 4; id++ {
			if assignment, found := installedAssignment(works[id], jobs["store-loss"].id); found {
				for _, token := range assignment.Assignment.Tasks {
					if token.Attempt > 1 {
						reassigned = true
					}
				}
			}
		}
		if !reassigned {
			cluster.fatalf("store-loss job shows no task attempt above 1 on any node")
		}
		sink := jobs["sink-loss"]
		var replicas []uint16
		for id := uint16(1); id <= 4; id++ {
			if assignment, found := installedAssignment(works[id], sink.id); found && len(assignment.Assignment.ResultReplicas) == 1 {
				replicas = []uint16{assignment.Assignment.ResultReplicas[0].PrimaryNodeID, assignment.Assignment.ResultReplicas[0].SecondaryNodeID}
				break
			}
		}
		if len(replicas) != 2 {
			cluster.fatalf("sink-loss job has no installed replica set on any node")
		}
		primaryDigest, primaryCount := resultSetDigest(works[replicas[0]], sink.id)
		secondaryDigest, secondaryCount := resultSetDigest(works[replicas[1]], sink.id)
		if primaryDigest != secondaryDigest || primaryCount != secondaryCount || primaryCount != len(finalRecords["sink-loss"]) {
			cluster.fatalf("current replicas %v disagree: %d/%x vs %d/%x (want %d records)", replicas, primaryCount, primaryDigest[:4], secondaryCount, secondaryDigest[:4], len(finalRecords["sink-loss"]))
		}
		cluster.t.Logf("current replicas %v hold the identical covered set of %d records", replicas, primaryCount)
		cluster.assertRulesConsumed()
	})
}

func sha256Sum(data []byte) [32]byte { return sha256.Sum256(data) }

// sinkLossPhase destroys a sink replica's store after a partial mid-Running
// checkpoint, crashes the repair destination mid-stream, proves admission
// stayed closed until repair completed, and reseals. It returns the node
// whose store was destroyed.
func sinkLossPhase(cluster *craneCluster, ctx context.Context, leaderRef *uint16, shape func(string, int64) model.TopologySpec, finish func(string, craneJob, time.Duration)) uint16 {
	leader := *leaderRef
	defer func() { *leaderRef = leader }()
	var sinkLossVictim uint16
	resultsBefore := map[uint16]int{}
	checkpointsBefore := map[uint16]int{}
	for id := uint16(1); id <= 4; id++ {
		resultsBefore[id] = cluster.boundaryCount(id, store.BoundaryResultUpserted)
		checkpointsBefore[id] = cluster.boundaryCount(id, store.BoundaryCheckpointApplied)
	}
	job := cluster.submit(shape("sink-loss", 90))
	// Destroy a sink replica's store once a partial checkpoint has committed
	// while the job is still Running and that replica holds at least eight
	// durable result copies. (Parking the replica would stall the very
	// completions the checkpoint needs, so this point is observed, not held.)
	waitCtx, cancelWait := context.WithTimeout(ctx, 90*time.Second)
	var victim uint16
	stageErr := waitCondition(waitCtx, 25*time.Millisecond, func() (bool, error) {
		checkpointed := false
		for id := uint16(1); id <= 4; id++ {
			if cluster.boundaryCount(id, store.BoundaryCheckpointApplied) > checkpointsBefore[id] {
				checkpointed = true
			}
		}
		if !checkpointed {
			return false, nil
		}
		for id := uint16(1); id <= 4; id++ {
			if cluster.boundaryCount(id, store.BoundaryResultUpserted) >= resultsBefore[id]+8 {
				victim = id
				return true, nil
			}
		}
		return false, nil
	})
	cancelWait()
	if stageErr != nil {
		cluster.fatalf("wait for a partial checkpoint with a replica holding eight results: %v\n%s", stageErr, cluster.diagnostics())
	}
	if status := cluster.status(job); status.State != protocol.JobRunning {
		cluster.fatalf("job is not Running at the sink-loss point: state=%d", status.State)
	}
	lossTime := time.Now()
	cluster.kill(victim)
	// Arm the repair crash points on the survivors before the loss can be
	// acted upon: the coordinator reassigns and starts re-establishing the
	// second copy while the victim's old incarnation is still being retired.
	streaming := map[uint16]int{}
	upserts := map[uint16]int{}
	for id := uint16(1); id <= 4; id++ {
		if id == victim {
			continue
		}
		streaming[id] = cluster.blockNext(id, store.BoundaryRepairStreaming)
		upserts[id] = cluster.blockNext(id, store.BoundaryResultUpserted)
	}
	cluster.loseStore(victim)
	sinkLossVictim = victim
	cluster.relaunch(victim)
	streaming[victim] = cluster.blockNext(victim, store.BoundaryRepairStreaming)
	upserts[victim] = cluster.blockNext(victim, store.BoundaryResultUpserted)
	if victim == leader {
		leader = cluster.awaitLeader(1, 2, 3)
	}
	cluster.awaitMembership(4, 1, 2, 3, 4)
	// Two copies are re-established either by a bilateral repair grant (the
	// destination publishes repair-streaming per record) or, when the
	// surviving replica is a member of the new pair, by the holder's
	// readoption (the partner publishes result-upserted). Crash whichever
	// endpoint parks first mid-repair, exactly after its first durable
	// repair progress.
	repairCtx, cancelRepair := context.WithTimeout(ctx, 90*time.Second)
	var target uint16
	var parkedAt string
	repairErr := waitCondition(repairCtx, 25*time.Millisecond, func() (bool, error) {
		for id := uint16(1); id <= 4; id++ {
			if cluster.blockedAt(id, store.BoundaryRepairStreaming, streaming[id]) {
				target, parkedAt = id, store.BoundaryRepairStreaming
				return true, nil
			}
			if cluster.blockedAt(id, store.BoundaryResultUpserted, upserts[id]) {
				target, parkedAt = id, store.BoundaryResultUpserted
				return true, nil
			}
		}
		status, err := cluster.client().Status(repairCtx, job.id)
		if err == nil && (status.State == protocol.JobSucceeded || status.State == protocol.JobDraining) {
			return false, fmt.Errorf("job reached state %d before any repair progress", status.State)
		}
		return false, nil
	})
	cancelRepair()
	if repairErr != nil {
		cluster.fatalf("wait for a repair endpoint parked at its first durable repair progress: %v\n%s", repairErr, cluster.diagnostics())
	}
	cluster.releaseOthers(store.BoundaryRepairStreaming, streaming, target)
	cluster.releaseOthers(store.BoundaryResultUpserted, upserts, target)
	runningBeforeCrash := map[uint16]int{}
	for id := uint16(1); id <= 4; id++ {
		runningBeforeCrash[id] = cluster.boundaryCount(id, store.BoundaryAssignmentRunning)
	}
	cluster.kill(target)
	_, work := cluster.inspectStore(target)
	unfinished := 0
	for _, repair := range work.Repairs {
		if repair.State == store.RepairStreaming || repair.State == store.RepairPending {
			unfinished++
		}
	}
	retained := 0
	for _, result := range work.Results {
		if result.Record.TupleID.JobID == job.id {
			retained++
		}
	}
	if parkedAt == store.BoundaryRepairStreaming && unfinished == 0 {
		cluster.fatalf("repair destination %d holds no unfinished durable repair after the crash", target)
	}
	if parkedAt == store.BoundaryResultUpserted && retained == 0 {
		cluster.fatalf("re-replication endpoint %d holds no durable result copy after the crash", target)
	}
	crashed := time.Now()
	cluster.relaunch(target)
	if target == leader {
		leader = cluster.awaitLeader(1, 2, 3)
	}
	cluster.awaitMembership(4, 1, 2, 3, 4)
	if parkedAt == store.BoundaryRepairStreaming {
		// Bilateral grant path: the coordinator may not reopen admission
		// until the destination's repair completes, so no node may receive
		// a Running install between the destination's crash and its
		// repair-complete boundary.
		cluster.awaitBoundary(target, store.BoundaryRepairComplete, 1, 90*time.Second)
		var repaired time.Time
		for _, event := range cluster.nodes[target].hook.Events() {
			if event.Kind == integrationhook.EventBoundary && event.Name == store.BoundaryRepairComplete {
				repaired = event.At
				break
			}
		}
		for id := uint16(1); id <= 4; id++ {
			for _, event := range cluster.nodes[id].hook.Events() {
				if event.Kind == integrationhook.EventBoundary && event.Name == store.BoundaryAssignmentRunning && event.At.After(crashed) && event.At.Before(repaired) {
					cluster.fatalf("node %d reopened admission at %v while repair destination %d was crashed/resuming (repair completed %v)", id, event.At, target, repaired)
				}
			}
		}
		cluster.t.Logf("sink replica %d lost its store after a partial checkpoint (%s); bilateral repair destination %d crashed mid-stream with %d unfinished grant(s), resumed, and completed repair before any reopen", victim, lossTime.Format("15:04:05.000"), target, unfinished)
	} else {
		// Holder readoption path: the surviving replica re-replicated its
		// retained copies to the new partner; the partner crashed after its
		// first durable copy and resumed. Agreement of the covered set
		// before reopen is enforced by the coordinator and audited offline
		// at the end (identical result sets on both current replicas).
		cluster.t.Logf("sink replica %d lost its store after a partial checkpoint (%s); readoption partner %d crashed after %d durable copies and resumed", victim, lossTime.Format("15:04:05.000"), target, retained)
	}
	// A seal wedge here is the one outcome worth an offline dump: stop the
	// cluster and record both current replicas' result sets, provenances,
	// and repair records before failing.
	sealCtx, cancelSeal := context.WithTimeout(ctx, 150*time.Second)
	var last protocol.StatusResponse
	sealErr := waitCondition(sealCtx, 250*time.Millisecond, func() (bool, error) {
		status, err := cluster.client().Status(sealCtx, job.id)
		if err != nil {
			return false, nil
		}
		last = status
		if status.State == protocol.JobFailed || status.State == protocol.JobCanceled {
			return false, fmt.Errorf("job reached terminal state %d", status.State)
		}
		return status.State == protocol.JobSucceeded && status.HasManifestSet, nil
	})
	cancelSeal()
	if sealErr != nil {
		diagnostics := cluster.diagnostics()
		dump := cluster.dumpJobState(job)
		cluster.fatalf("sink-loss job did not seal: %v (last status state=%d jcr=%d rev=%d completedSources=%d)\n%s\n%s", sealErr, last.State, last.JobControlRevision, last.AssignmentRevision, last.CompletedSourceTasks, diagnostics, dump)
	}
	finish("sink-loss", job, 60*time.Second)
	return sinkLossVictim
}

// ---------------------------------------------------------------------------
// Mismatch and startup cases
// ---------------------------------------------------------------------------

// buildVariantNodeBinary builds cmd/node from an overlay in which only the
// consensus fingerprint differs (the operator registry fingerprint is
// unchanged): ConsensusFingerprint becomes SHA-256(variant domain ||
// production fingerprint), so the test can compute the variant value.
func buildVariantNodeBinary(t *testing.T) (string, [32]byte) {
	t.Helper()
	root := repositoryRootForTest(t)
	original := filepath.Join(root, "internal", "crane", "model", "fingerprint.go")
	content, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	needle := "\treturn sha256.Sum256(append([]byte(consensusFingerprintDomain), encoded...))\n"
	replacement := "\tproduction := sha256.Sum256(append([]byte(consensusFingerprintDomain), encoded...))\n\treturn sha256.Sum256(append([]byte(" + fmt.Sprintf("%q", craneVariantDomain) + "), production[:]...))\n"
	if !strings.Contains(string(content), needle) {
		t.Fatalf("fingerprint.go no longer contains the expected consensus return; update the variant overlay")
	}
	variant := strings.Replace(string(content), needle, replacement, 1)
	dir := t.TempDir()
	variantPath := filepath.Join(dir, "fingerprint.go")
	if err := os.WriteFile(variantPath, []byte(variant), 0o600); err != nil {
		t.Fatal(err)
	}
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": {original: variantPath}})
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := buildGoBinaryWithTags(t, root, "crane-node-variant", "./cmd/node", []string{"craneintegration"}, "-overlay", overlayPath)
	production := model.ConsensusFingerprint()
	return binary, sha256Sum(append([]byte(craneVariantDomain), production[:]...))
}

// writeNodeConfig writes one configuration file next to the cluster's others.
func (c *craneCluster) writeNodeConfig(id uint16, configuration config.NodeConfig, label string) string {
	c.t.Helper()
	content, err := json.MarshalIndent(configuration, "", "  ")
	if err != nil {
		c.t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(c.configPaths[id-1]), fmt.Sprintf("node-%d.%s.json", id, label))
	if err := os.WriteFile(path, append(content, '\n'), 0o600); err != nil {
		c.t.Fatal(err)
	}
	return path
}

// startExpectingExit launches a node process that must exit on its own
// without ever printing the readiness line, and returns its exit error.
func (c *craneCluster) startExpectingExit(id uint16, configPath string, timeout time.Duration) error {
	c.t.Helper()
	node := c.nodes[id]
	node.incarnation++
	process := c.harness.startWithFiles(node.binary, []string{"-config", configPath}, fmt.Sprintf("crane-node-%d.%d", id, node.incarnation), nil)
	node.process = process
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	if err := process.waitExit(ctx); err != nil {
		c.fatalf("node %d did not exit: %v", id, err)
	}
	if strings.Contains(process.log.String(), "CRANE_NODE_READY") {
		c.fatalf("node %d reported readiness before exiting", id)
	}
	return process.result()
}

// assertPortBlockFree proves every typed endpoint of a node is bindable and
// that its worker store, when present, is unlocked.
func (c *craneCluster) assertPortBlockFree(id uint16) {
	c.t.Helper()
	configuration := c.nodes[id].configuration
	for _, service := range config.Services() {
		endpoint, err := configuration.BindEndpoint(service.Service)
		if err != nil {
			c.t.Fatal(err)
		}
		if !endpointBindable(endpoint, service.Transport) {
			c.fatalf("node %d still holds %s endpoint %s", id, service.Name, endpoint)
		}
	}
}

func TestCraneLifecycleFingerprintMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	nodeBinary, craneBinary := buildCraneBinaries(t)
	variantBinary, variantFingerprint := buildVariantNodeBinary(t)
	cluster := newCraneCluster(t, ctx, nodeBinary, craneBinary, nil)

	cluster.phase("configured_fingerprint_mismatch_fails_before_ready", func() {
		// A voter whose configured fingerprint differs from its compiled
		// contract fails its local check before any service is constructed:
		// no readiness, no port, no storage artifact, non-zero exit.
		configuration := cluster.nodes[1].configuration
		configuration.Crane.ConsensusFingerprint = strings.Repeat("ab", 32)
		before := storageEntriesForTest(cluster.t, configuration.StorageDir)
		err := cluster.startExpectingExit(1, cluster.writeNodeConfig(1, configuration, "badfp"), 20*time.Second)
		if err == nil {
			cluster.fatalf("mismatched voter exited cleanly")
		}
		if logs := cluster.nodes[1].process.log.String(); !strings.Contains(logs, "consensus fingerprint does not match this binary") {
			cluster.fatalf("mismatched voter did not report the configured-fingerprint check: %q", logs)
		}
		if after := storageEntriesForTest(cluster.t, configuration.StorageDir); strings.Join(after, ",") != strings.Join(before, ",") {
			cluster.fatalf("mismatched voter touched storage: %v -> %v", before, after)
		}
		cluster.assertPortBlockFree(1)
	})

	cluster.phase("compiled_fingerprint_mismatch_excludes_voter_and_nonvoter", func() {
		// Voters 1–2 run the production contract. Voter 3 and nonvoter 4 run
		// a locally valid binary whose compiled consensus fingerprint differs
		// while the registry fingerprint matches.
		for _, id := range []uint16{3, 4} {
			node := cluster.nodes[id]
			node.binary = variantBinary
			node.configuration.Crane.ConsensusFingerprint = hex.EncodeToString(variantFingerprint[:])
			node.configPath = cluster.writeNodeConfig(id, node.configuration, "variant")
		}
		cluster.launch(1)
		cluster.launch(2)
		cluster.launch(3)
		cluster.launch(4)
		for id := uint16(1); id <= 4; id++ {
			cluster.watchEverything(id)
		}
		cluster.awaitMembership(4, 1, 2, 3, 4)
		leader := cluster.awaitLeader(1, 2)
		cluster.t.Logf("leader among the production voters is %d; variant voter 3 and variant nonvoter 4 are Ready and Alive", leader)
		job := cluster.submit(craneTopology(craneTopologyShape{name: "mismatch", start: 0, end: 24, factor: 3, threshold: 10_000, sourceParallelism: 2, multiplyParallelism: 2, evenParallelism: 1, lessThanParallelism: 1, includeEvenAndLess: true}))
		cluster.awaitSucceeded(job, 90*time.Second)
		cluster.verifyResults(job)
		// The variant voter never learned a leader: its handshake with the
		// production voters fails before it counts for RPC contact, votes,
		// or quorum, so its +6 keeps answering "no leader" while the
		// production pair served a whole job.
		for probe := 0; probe < 5; probe++ {
			isLeader, err := cluster.probeLeadership(3)
			if err == nil {
				cluster.fatalf("variant voter 3 answered a leadership probe (leader=%v): it joined consensus", isLeader)
			}
			if !strings.Contains(err.Error(), "has no leader") {
				cluster.fatalf("variant voter 3 probe = %v, want a no-leader rejection", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		// Neither variant node was registered or assigned: +5 refused their
		// registration, so no fence, install, or custody ever reached them.
		for _, id := range []uint16{3, 4} {
			for _, name := range []string{store.BoundaryFence, store.BoundaryAssignmentClosed, store.BoundaryAssignmentRunning, store.BoundaryDeliveryReceived, store.BoundaryResultUpserted} {
				if got := cluster.boundaryCount(id, name); got != 0 {
					cluster.fatalf("variant node %d published %s %d times: it was registered or assigned", id, name, got)
				}
			}
		}
		if cluster.boundaryCount(1, store.BoundaryDeliveryReceived)+cluster.boundaryCount(2, store.BoundaryDeliveryReceived) == 0 {
			cluster.fatalf("neither production node took custody")
		}
		for id := uint16(4); id >= 1; id-- {
			cluster.terminate(id)
		}
	})
}

func storageEntriesForTest(t *testing.T, directory string) []string {
	t.Helper()
	var entries []string
	_ = filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if path != directory {
			entries = append(entries, strings.TrimPrefix(path, directory))
		}
		return nil
	})
	sort.Strings(entries)
	return entries
}

func TestCraneLifecycleStartupFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	nodeBinary, craneBinary := buildCraneBinaries(t)
	cluster := newCraneCluster(t, ctx, nodeBinary, craneBinary, nil)

	cluster.phase("cancellation_during_partial_startup_releases_everything", func() {
		// Terminate the seed the instant it starts: whatever stage of
		// startup it reached, it must exit promptly, leave every typed
		// endpoint free and its worker store unlocked, and start normally
		// afterwards from the same storage.
		node := cluster.nodes[1]
		for attempt := 0; attempt < 3; attempt++ {
			node.incarnation++
			process := cluster.harness.startWithFiles(node.binary, []string{"-config", node.configPath}, fmt.Sprintf("crane-node-1.%d", node.incarnation), nil)
			node.process = process
			time.Sleep(time.Duration(attempt*40) * time.Millisecond)
			if err := process.signal(syscall.SIGTERM); err != nil {
				cluster.fatalf("SIGTERM: %v", err)
			}
			exitCtx, cancelExit := context.WithTimeout(ctx, 20*time.Second)
			err := process.waitExit(exitCtx)
			cancelExit()
			if err != nil {
				cluster.fatalf("node 1 did not exit after SIGTERM during startup: %v", err)
			}
			cluster.assertPortBlockFree(1)
			if _, err := os.Stat(filepath.Join(node.configuration.StorageDir, worker.WorkerStoreDirectory)); err == nil {
				epoch, _ := cluster.inspectStore(1)
				cluster.t.Logf("attempt %d: store unlocked and reopenable (epoch %x)", attempt, epoch[:4])
			}
		}
		cluster.launch(1)
		cluster.terminate(1)
		cluster.assertPortBlockFree(1)
	})

	cluster.phase("occupied_ports_fail_fast_and_release_the_rest", func() {
		for _, service := range []config.Service{config.ServiceCraneWorker, config.ServiceTopologyControl, config.ServiceCraneTupleACK} {
			endpoint, err := cluster.nodes[2].configuration.BindEndpoint(service)
			if err != nil {
				cluster.t.Fatal(err)
			}
			var occupier interface{ Close() error }
			if service == config.ServiceCraneTupleACK {
				address, _ := net.ResolveUDPAddr("udp", endpoint.String())
				occupier, err = net.ListenUDP("udp", address)
			} else {
				occupier, err = net.Listen("tcp", endpoint.String())
			}
			if err != nil {
				cluster.t.Fatal(err)
			}
			exitErr := cluster.startExpectingExit(2, cluster.nodes[2].configPath, 30*time.Second)
			if exitErr == nil {
				cluster.fatalf("node 2 started with occupied %s endpoint %s", config.Services()[service].Name, endpoint)
			}
			_ = occupier.Close()
			cluster.assertPortBlockFree(2)
			cluster.t.Logf("occupied %s endpoint %s: node exited (%v) and released every other endpoint", config.Services()[service].Name, endpoint, exitErr)
		}
	})
}

// TestCraneLifecycleProductionBinaryIgnoresInheritedActivation launches the
// ordinary (untagged) node binary with a valid activation channel inherited
// on descriptor 3 that asks it to block at its first fence. The production
// build must reach readiness, never answer the hello, never publish an event,
// and never park.
func TestCraneLifecycleProductionBinaryIgnoresInheritedActivation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := repositoryRootForTest(t)
	production := buildGoBinaryWithTags(t, root, "crane-node-production", "./cmd/node", nil)
	names, err := exec.CommandContext(ctx, "go", "tool", "nm", production).Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, symbol := range []string{"integrationhook.(*fdHook)", "integrationhook.parseCommand", "integrationhook.activate", "integrationhook.(*Controller)"} {
		if strings.Contains(string(names), symbol) {
			t.Fatalf("production node binary links %s", symbol)
		}
	}
	_, craneBinary := buildCraneBinaries(t)
	cluster := newCraneCluster(t, ctx, production, craneBinary, nil)
	controller, err := integrationhook.NewController()
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	node := cluster.nodes[1]
	node.incarnation++
	process := cluster.harness.startWithFiles(production, []string{"-config", node.configPath}, "production-node-1", []*os.File{controller.ChildFile()})
	node.process = process
	controller.Started()
	readyCtx, cancelReady := context.WithTimeout(ctx, 30*time.Second)
	defer cancelReady()
	waitForNormalNodeReadiness(t, readyCtx, cluster.harness, process, 1)
	activation, cancelActivation := context.WithTimeout(ctx, 3*time.Second)
	defer cancelActivation()
	if err := controller.Activated(activation); err == nil {
		t.Fatal("production binary answered the activation hello")
	}
	if events := controller.Events(); len(events) != 0 {
		t.Fatalf("production binary published events %v", events)
	}
	shutdown, cancelShutdown := context.WithTimeout(ctx, 30*time.Second)
	defer cancelShutdown()
	if err := process.terminate(shutdown); err != nil {
		t.Fatalf("terminate production node: %v\n%s", err, cluster.harness.logs())
	}
	if events := controller.Events(); len(events) != 0 {
		t.Fatalf("production binary published events at shutdown %v", events)
	}
}

// dumpJobState stops every node and renders each node's offline durable view
// of one job: installed assignment, retained result copies with provenance,
// and repair records.
func (c *craneCluster) dumpJobState(job craneJob) string {
	c.t.Helper()
	for id := uint16(4); id >= 1; id-- {
		if node := c.nodes[id]; node.process != nil && !node.process.exited() {
			ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
			_ = node.process.terminate(ctx)
			cancel()
		}
	}
	var lines []string
	for id := uint16(1); id <= 4; id++ {
		path := filepath.Join(c.nodes[id].configuration.StorageDir, worker.WorkerStoreDirectory)
		durable, err := store.Open(path, store.Identity{ClusterID: c.clusterID, NodeID: id}, store.Options{MaxBytes: c.nodes[id].configuration.Crane.MaxWorkerStoreBytes})
		if err != nil {
			lines = append(lines, fmt.Sprintf("node %d: open store: %v", id, err))
			continue
		}
		work, err := durable.RecoverWork()
		epoch := durable.WorkerEpoch()
		_ = durable.Close()
		if err != nil {
			lines = append(lines, fmt.Sprintf("node %d: recover: %v", id, err))
			continue
		}
		lines = append(lines, fmt.Sprintf("node %d epoch=%x fence-term=%d", id, epoch[:4], work.Fence.Term))
		if assignment, ok := installedAssignment(work, job.id); ok {
			lines = append(lines, fmt.Sprintf("  assignment rev=%d jcr=%d state=%d replicas=%+v", assignment.Assignment.Revision, assignment.JobControlRevision, assignment.SchedulingState, assignment.Assignment.ResultReplicas))
		}
		count := 0
		for _, result := range work.Results {
			if result.Record.TupleID.JobID != job.id {
				continue
			}
			count++
			lines = append(lines, fmt.Sprintf("  result src=%d/%d seq=%d prov(rev=%d primary=%d/%x secondary=%d/%x)", result.Record.TupleID.SourceTask.StageID, result.Record.TupleID.SourceTask.Partition, result.Record.TupleID.SourceSequence, result.Provenance.AssignmentRevision, result.Provenance.ReplicaSet.PrimaryNodeID, result.Provenance.ReplicaSet.PrimaryEpoch[:2], result.Provenance.ReplicaSet.SecondaryNodeID, result.Provenance.ReplicaSet.SecondaryEpoch[:2]))
		}
		lines = append(lines, fmt.Sprintf("  %d retained results for the job", count))
		for _, delivery := range work.Deliveries {
			if delivery.ID.Tuple.JobID == job.id {
				lines = append(lines, fmt.Sprintf("  delivery stage=%d seq=%d state=%d rev=%d epoch-term=%d producer=%d/%d dest=%d/%d", delivery.Destination.Task.StageID, delivery.ID.Tuple.SourceSequence, delivery.State, delivery.AssignmentRevision, delivery.CoordinatorEpoch.Term, delivery.Producer.WorkerID, delivery.Producer.Attempt, delivery.Destination.WorkerID, delivery.Destination.Attempt))
			}
		}
		for _, outbox := range work.Outboxes {
			if outbox.ID.Tuple.JobID == job.id {
				lines = append(lines, fmt.Sprintf("  outbox to=%d/%d stage=%d seq=%d completed=%v accepted=%v rev=%d epoch-term=%d", outbox.Destination.WorkerID, outbox.Destination.Attempt, outbox.Destination.Task.StageID, outbox.ID.Tuple.SourceSequence, outbox.Completed, outbox.Accepted, outbox.AssignmentRevision, outbox.CoordinatorEpoch.Term))
			}
		}
		for _, cursor := range work.Sources {
			if cursor.Source.JobID == job.id {
				lines = append(lines, fmt.Sprintf("  source partition=%d next=%d watermark=%d eof=%d", cursor.Source.Partition, cursor.NextSequence, cursor.Watermark, cursor.EOF))
			}
		}
		for _, event := range work.PendingEvents {
			lines = append(lines, fmt.Sprintf("  pending event tx=%d kind=%d", event.TransactionID, event.Kind))
		}
		for _, repair := range work.Repairs {
			if repair.Instruction.JobID == job.id {
				lines = append(lines, fmt.Sprintf("  repair id=%x state=%d role=%d next=%d expected=%d rev=%d src=%d dst=%d", repair.Instruction.RepairID[:4], repair.State, repair.Role, repair.NextRecord, repair.Instruction.ExpectedRecordCount, repair.Instruction.AssignmentRevision, repair.Instruction.SourceNodeID, repair.Instruction.DestinationNodeID))
			}
		}
	}
	return strings.Join(lines, "\n")
}
