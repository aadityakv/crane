//go:build craneintegration

package integrationhook

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aaditya/cs425mp3/internal/wire"
)

// Enabled reports that this binary can activate the seam from an inherited
// descriptor. It is only ever true under the craneintegration build tag.
const Enabled = true

const (
	// InheritedFD is the only descriptor the tagged build inspects: the
	// harness passes one end of a unix stream socketpair as ExtraFiles[0].
	InheritedFD = 3
	// helloLine must be the first line the harness writes; a socket that
	// does not open with it is left untouched and the hook stays no-op.
	helloLine = "hello craneintegration/1"
	// helloExitOnEOF extends helloLine: the harness additionally demands
	// that the process exit once the activation channel reaches EOF, making
	// the channel the harness's ownership of the process lifetime. Only a
	// harness that launches this process as its child asks for this.
	helloExitOnEOF = "hello craneintegration/1 exit-on-eof"
	// activatedLine is the tagged build's only unsolicited output.
	activatedLine = "activated"
	// helloDeadline bounds how long activation waits for the hello line.
	helloDeadline = 5 * time.Second
)

// ErrPeerClosed reports that the other end of the activation channel is
// gone: the child died, or the harness closed the controller.
var ErrPeerClosed = errors.New("craneintegration peer closed")

// EventKind classifies one line the process publishes to the harness.
type EventKind string

const (
	// EventBoundary is one occurrence of a watched or blocked boundary.
	EventBoundary EventKind = "boundary"
	// EventBlocked reports the process is parked at a boundary awaiting continue.
	EventBlocked EventKind = "blocked"
	// EventResumed reports a blocked boundary was continued.
	EventResumed EventKind = "resumed"
	// EventRuleConsumed reports one datagram rule application.
	EventRuleConsumed EventKind = "rule"
	// EventReleased reports the re-send of every frame a hold rule captured.
	EventReleased EventKind = "released"
)

// Event is one published process observation.
type Event struct {
	Kind EventKind
	// Name is the boundary name for boundary/blocked/resumed events.
	Name string
	// Occurrence is the per-process 1-based count of Name for boundary events.
	Occurrence int
	// RuleID and Consumed describe rule events (Consumed is cumulative).
	RuleID   string
	Consumed int
	// At is the harness receive time.
	At time.Time
}

// actionHold captures the exact outbound frame until the harness releases
// it; it is an internal rule action, never a DatagramAction result.
const actionHold Action = 100

// Rule is one bounded datagram fault: the next Count datagrams matching
// Direction and Message take Action, then the rule is exhausted. Hold is a
// send-only rule that captures the exact frame for a later Release; To
// optionally restricts a hold to datagrams addressed to one node, and
// Optional marks a hold that may legitimately never capture anything.
type Rule struct {
	ID        string
	Direction Direction
	Message   wire.MessageType
	Action    Action
	Count     int
	Hold      bool
	To        uint16
	Optional  bool
}

func (rule Rule) validate() error {
	if rule.ID == "" || strings.ContainsAny(rule.ID, " \n") {
		return errors.New("rule ID must be one non-empty word")
	}
	if rule.Direction != Send && rule.Direction != Receive {
		return errors.New("rule direction must be send or recv")
	}
	if rule.Hold {
		if rule.Direction != Send || rule.Action != Pass {
			return errors.New("hold rules are send-only and carry no other action")
		}
	} else {
		if rule.Action != Pass && rule.Action != Drop && rule.Action != Duplicate {
			return errors.New("rule action must be pass, drop, duplicate, or hold")
		}
		if rule.To != 0 || rule.Optional {
			return errors.New("destination and optional apply to hold rules only")
		}
	}
	if rule.Direction == Receive && rule.Action == Duplicate {
		return errors.New("inbound duplication is not supported; duplicate on the sender")
	}
	if rule.Count <= 0 || rule.Count > 1024 {
		return errors.New("rule count must be 1..1024")
	}
	return nil
}

func (rule Rule) encode() string {
	action := rule.Action.String()
	if rule.Hold {
		action = "hold"
	}
	line := fmt.Sprintf("rule %s %s %s %s %d", rule.ID, rule.Direction, MessageName(rule.Message), action, rule.Count)
	if rule.To != 0 {
		line += fmt.Sprintf(" to=%d", rule.To)
	}
	if rule.Optional {
		line += " optional"
	}
	return line
}

func parseMessage(word string) (wire.MessageType, error) {
	switch word {
	case "delivery":
		return wire.MessageCraneTupleDelivery, nil
	case "ack":
		return wire.MessageCraneTupleDeliveryAck, nil
	case "nack":
		return wire.MessageCraneTupleDeliveryNack, nil
	}
	value, err := strconv.ParseUint(word, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("unknown message %q", word)
	}
	return wire.MessageType(value), nil
}

func parseDirection(word string) (Direction, error) {
	switch word {
	case "send":
		return Send, nil
	case "recv":
		return Receive, nil
	}
	return 0, fmt.Errorf("unknown direction %q", word)
}

func parseAction(word string) (Action, error) {
	switch word {
	case "hold":
		return actionHold, nil
	case "pass":
		return Pass, nil
	case "drop":
		return Drop, nil
	case "duplicate":
		return Duplicate, nil
	}
	return 0, fmt.Errorf("unknown action %q", word)
}

// ---------------------------------------------------------------------------
// Process side
// ---------------------------------------------------------------------------

var (
	loadOnce sync.Once
	loaded   Hook
	// inheritedFile keeps descriptor 3 referenced for the process lifetime so
	// no finalizer closes it behind the connection.
	inheritedFile *os.File
)

// LoadFromInheritedFD activates the seam exactly once from descriptor 3 when
// that descriptor is a unix stream socket whose first line is the exact
// hello; every other process state yields the no-op Hook.
func LoadFromInheritedFD() Hook {
	loadOnce.Do(func() {
		loaded = Noop{}
		// The descriptor stays referenced for the process lifetime whether or
		// not activation succeeds: an unrelated inherited descriptor 3 must
		// never be closed behind its owner by a finalizer.
		inheritedFile = os.NewFile(InheritedFD, "craneintegration")
		if inheritedFile == nil {
			return
		}
		loaded = activate(inheritedFile)
	})
	return loaded
}

// parseHello validates one hello line and reports whether the harness
// demanded orphan control (the process exits once the channel closes).
func parseHello(line string) (exitOnEOF bool, ok bool) {
	switch line {
	case helloLine:
		return false, true
	case helloExitOnEOF:
		return true, true
	}
	return false, false
}

// activate validates the channel and performs the hello handshake; anything
// but an exact handshake leaves the process with the no-op Hook. A hello that
// demands exit-on-EOF makes the activation channel the harness's ownership of
// this process: once it closes, the process exits instead of leaking.
func activate(file *os.File) Hook {
	info, err := file.Stat()
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return Noop{}
	}
	conn, err := net.FileConn(file)
	if err != nil {
		return Noop{}
	}
	if err := conn.SetReadDeadline(time.Now().Add(helloDeadline)); err != nil {
		_ = conn.Close()
		return Noop{}
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return Noop{}
	}
	exitOnEOF, ok := parseHello(strings.TrimRight(line, "\r\n"))
	if !ok {
		_ = conn.Close()
		return Noop{}
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return Noop{}
	}
	hook := &fdHook{conn: conn, exitOnEOF: exitOnEOF, watch: map[string]bool{}, blocks: map[string]int{}, counts: map[string]int{}}
	if err := hook.writeLine(activatedLine); err != nil {
		_ = conn.Close()
		return Noop{}
	}
	go hook.readCommands(reader)
	return hook
}

type ruleState struct {
	rule      Rule
	remaining int
	consumed  int
	held      []func()
	released  int
}

// fdHook is the tagged Hook bound to the inherited activation channel.
type fdHook struct {
	conn net.Conn
	// exitOnEOF makes EOF on the activation channel terminate the process.
	exitOnEOF bool

	writeMu sync.Mutex

	mu      sync.Mutex
	watch   map[string]bool
	blocks  map[string]int
	counts  map[string]int
	rules   []*ruleState
	blocked []chan struct{}
	closed  bool
}

func (hook *fdHook) writeLine(line string) error {
	hook.writeMu.Lock()
	defer hook.writeMu.Unlock()
	_, err := io.WriteString(hook.conn, line+"\n")
	return err
}

// DurableBoundary publishes watched boundaries and parks at a blocked one.
func (hook *fdHook) DurableBoundary(name string) {
	hook.mu.Lock()
	hook.counts[name]++
	occurrence := hook.counts[name]
	blockAt, hasBlock := hook.blocks[name]
	if hook.watch[name] || hasBlock {
		_ = hook.writeLine(fmt.Sprintf("%s %s %d", EventBoundary, name, occurrence))
	}
	if !hasBlock || blockAt != occurrence || hook.closed {
		hook.mu.Unlock()
		return
	}
	delete(hook.blocks, name)
	release := make(chan struct{})
	hook.blocked = append(hook.blocked, release)
	_ = hook.writeLine(fmt.Sprintf("%s %s %d", EventBlocked, name, occurrence))
	hook.mu.Unlock()
	<-release
	_ = hook.writeLine(fmt.Sprintf("%s %s %d", EventResumed, name, occurrence))
}

// HoldDatagram captures one outbound frame for the first matching
// unexhausted hold rule; the harness releases it later.
func (hook *fdHook) HoldDatagram(message wire.MessageType, destination uint16, resend func()) bool {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	for _, state := range hook.rules {
		if !state.rule.Hold || state.remaining == 0 || state.rule.Message != message || state.rule.To != 0 && state.rule.To != destination {
			continue
		}
		state.remaining--
		state.consumed++
		state.held = append(state.held, resend)
		_ = hook.writeLine(fmt.Sprintf("%s %s consumed %d", EventRuleConsumed, state.rule.ID, state.consumed))
		return true
	}
	return false
}

// DatagramAction consumes the first matching unexhausted rule.
func (hook *fdHook) DatagramAction(direction Direction, message wire.MessageType) Action {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	for _, state := range hook.rules {
		if state.rule.Hold || state.remaining == 0 || state.rule.Direction != direction || state.rule.Message != message {
			continue
		}
		state.remaining--
		state.consumed++
		_ = hook.writeLine(fmt.Sprintf("%s %s consumed %d", EventRuleConsumed, state.rule.ID, state.consumed))
		return state.rule.Action
	}
	return Pass
}

func (hook *fdHook) readCommands(reader *bufio.Reader) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			hook.mu.Lock()
			hook.closed = true
			// A harness that is gone can never continue a blocked boundary;
			// release everything so parked call sites return.
			for _, release := range hook.blocked {
				close(release)
			}
			hook.blocked = nil
			hook.mu.Unlock()
			if hook.exitOnEOF {
				// The activation channel is the harness's ownership of this
				// process: once its other end is gone the process is orphaned
				// (the harness may have died without running its cleanup), so
				// it exits instead of leaking sockets and durable locks. The
				// store is crash-safe by construction; this is exactly the
				// crash the scenario suite elsewhere inflicts on purpose.
				fmt.Fprintln(os.Stderr, "craneintegration: activation channel closed; exiting")
				os.Exit(1)
			}
			return
		}
		sequence, detail, refusal := hook.apply(strings.TrimRight(line, "\r\n"))
		switch {
		case refusal != "":
			_ = hook.writeLine(fmt.Sprintf("error %d %s", sequence, refusal))
		case detail != "":
			_ = hook.writeLine(fmt.Sprintf("ack %d %s", sequence, detail))
		default:
			_ = hook.writeLine(fmt.Sprintf("ack %d", sequence))
		}
	}
}

// apply executes one "<seq> <command> ..." line and returns its sequence,
// an optional ack detail, and an empty refusal on success.
func (hook *fdHook) apply(line string) (uint64, string, string) {
	words := strings.Fields(line)
	if len(words) < 2 {
		return 0, "", "command requires a sequence and a verb"
	}
	sequence, err := strconv.ParseUint(words[0], 10, 64)
	if err != nil {
		return 0, "", "invalid sequence"
	}
	command, err := parseCommand(words[1:])
	if err != nil {
		return sequence, "", err.Error()
	}
	hook.mu.Lock()
	defer hook.mu.Unlock()
	switch command.verb {
	case "watch":
		hook.watch[command.name] = true
	case "block":
		if hook.counts[command.name] >= command.occurrence {
			return sequence, "", fmt.Sprintf("boundary %s already reached occurrence %d", command.name, hook.counts[command.name])
		}
		hook.blocks[command.name] = command.occurrence
		hook.watch[command.name] = true
	case "blocknext":
		occurrence := hook.counts[command.name] + 1
		hook.blocks[command.name] = occurrence
		hook.watch[command.name] = true
		return sequence, strconv.Itoa(occurrence), ""
	case "unblock":
		delete(hook.blocks, command.name)
	case "rule":
		for _, existing := range hook.rules {
			if existing.rule.ID == command.rule.ID {
				return sequence, "", "duplicate rule ID"
			}
		}
		hook.rules = append(hook.rules, &ruleState{rule: command.rule, remaining: command.rule.Count})
	case "continue":
		for _, release := range hook.blocked {
			close(release)
		}
		hook.blocked = nil
	case "release":
		for _, state := range hook.rules {
			if state.rule.ID != command.name {
				continue
			}
			held := state.held
			state.held = nil
			state.released += len(held)
			// Releasing retires the hold: a rule whose budget was larger than
			// the frames it captured must not keep swallowing later frames.
			state.remaining = 0
			// A released hold is retired: it captures nothing further.
			state.remaining = 0
			for _, resend := range held {
				go resend()
			}
			_ = hook.writeLine(fmt.Sprintf("%s %s %d", EventReleased, state.rule.ID, state.released))
			return sequence, strconv.Itoa(len(held)), ""
		}
		return sequence, "", "unknown rule"
	}
	return sequence, "", ""
}

type command struct {
	verb       string
	name       string
	occurrence int
	rule       Rule
}

// parseCommand strictly parses one command's words after its sequence.
func parseCommand(words []string) (command, error) {
	switch words[0] {
	case "watch":
		if len(words) != 2 {
			return command{}, errors.New("watch <boundary>")
		}
		return command{verb: "watch", name: words[1]}, nil
	case "block":
		if len(words) != 3 {
			return command{}, errors.New("block <boundary> <occurrence>")
		}
		occurrence, err := strconv.Atoi(words[2])
		if err != nil || occurrence <= 0 {
			return command{}, errors.New("block occurrence must be a positive integer")
		}
		return command{verb: "block", name: words[1], occurrence: occurrence}, nil
	case "blocknext", "unblock", "release":
		if len(words) != 2 {
			return command{}, fmt.Errorf("%s <name>", words[0])
		}
		return command{verb: words[0], name: words[1]}, nil
	case "rule":
		if len(words) < 6 || len(words) > 8 {
			return command{}, errors.New("rule <id> <send|recv> <message> <pass|drop|duplicate|hold> <count> [to=<node>] [optional]")
		}
		direction, err := parseDirection(words[2])
		if err != nil {
			return command{}, err
		}
		message, err := parseMessage(words[3])
		if err != nil {
			return command{}, err
		}
		action, err := parseAction(words[4])
		if err != nil {
			return command{}, err
		}
		count, err := strconv.Atoi(words[5])
		if err != nil {
			return command{}, errors.New("rule count must be an integer")
		}
		rule := Rule{ID: words[1], Direction: direction, Message: message, Action: action, Count: count}
		if action == actionHold {
			rule.Hold, rule.Action = true, Pass
		}
		for _, extra := range words[6:] {
			switch {
			case extra == "optional":
				rule.Optional = true
			case strings.HasPrefix(extra, "to="):
				to, err := strconv.ParseUint(strings.TrimPrefix(extra, "to="), 10, 16)
				if err != nil || to == 0 {
					return command{}, errors.New("hold destination must be a nonzero node ID")
				}
				rule.To = uint16(to)
			default:
				return command{}, fmt.Errorf("unknown rule option %q", extra)
			}
		}
		if err := rule.validate(); err != nil {
			return command{}, err
		}
		return command{verb: "rule", rule: rule}, nil
	case "continue":
		if len(words) != 1 {
			return command{}, errors.New("continue takes no arguments")
		}
		return command{verb: "continue"}, nil
	default:
		return command{}, fmt.Errorf("unknown command %q", words[0])
	}
}

// ---------------------------------------------------------------------------
// Harness side
// ---------------------------------------------------------------------------

// Controller is the harness end of one process's activation channel.
type Controller struct {
	conn  net.Conn
	child *os.File

	writeMu  sync.Mutex
	sequence uint64

	mu        sync.Mutex
	cond      *sync.Cond
	activated bool
	closed    bool
	readErr   error
	events    []Event
	acks      map[uint64]string
	acked     map[uint64]bool
	details   map[uint64]string
	rules     map[string]*ruleState
	ruleOrder []string
}

// NewController creates the socketpair and starts reading events. The child
// end must be passed as ExtraFiles[0] (descriptor 3) to exactly one process,
// after which Started must be called. The hello demands exit-on-EOF: the
// child exits when the channel closes, so closing the controller is also the
// orphan-control path when a harness dies without running its cleanup.
func NewController() (*Controller, error) {
	return newController(helloExitOnEOF)
}

// NewControllerWithoutOrphanExit creates a controller whose child keeps
// running after the channel closes. In-process users that hold the child
// socket in the same process need this form.
func NewControllerWithoutOrphanExit() (*Controller, error) {
	return newController(helloLine)
}

func newController(hello string) (*Controller, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("craneintegration socketpair: %w", err)
	}
	parentFile := os.NewFile(uintptr(fds[0]), "craneintegration-parent")
	child := os.NewFile(uintptr(fds[1]), "craneintegration-child")
	conn, err := net.FileConn(parentFile)
	_ = parentFile.Close()
	if err != nil {
		_ = child.Close()
		return nil, fmt.Errorf("craneintegration parent connection: %w", err)
	}
	controller := &Controller{conn: conn, child: child, acks: map[uint64]string{}, acked: map[uint64]bool{}, details: map[uint64]string{}, rules: map[string]*ruleState{}}
	controller.cond = sync.NewCond(&controller.mu)
	if _, err := io.WriteString(conn, hello+"\n"); err != nil {
		_ = conn.Close()
		_ = child.Close()
		return nil, fmt.Errorf("craneintegration hello: %w", err)
	}
	go controller.readEvents()
	return controller, nil
}

// ChildFile is the descriptor to inherit as ExtraFiles[0].
func (controller *Controller) ChildFile() *os.File { return controller.child }

// Started releases the harness copy of the child descriptor so the channel
// reports EOF exactly when the child process is gone.
func (controller *Controller) Started() {
	if controller.child != nil {
		_ = controller.child.Close()
		controller.child = nil
	}
}

// Close tears the channel down. The child observes the closed channel and
// exits on its own (the activation channel is the harness's ownership of the
// process), so closing the controller is also the orphan-control path when a
// test dies without running its cleanup.
func (controller *Controller) Close() error {
	controller.Started()
	err := controller.conn.Close()
	controller.mu.Lock()
	controller.closed = true
	controller.cond.Broadcast()
	controller.mu.Unlock()
	return err
}

func (controller *Controller) readEvents() {
	reader := bufio.NewReader(controller.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			controller.mu.Lock()
			controller.readErr = ErrPeerClosed
			controller.cond.Broadcast()
			controller.mu.Unlock()
			return
		}
		controller.consume(strings.TrimRight(line, "\r\n"))
	}
}

func (controller *Controller) consume(line string) {
	words := strings.Fields(line)
	if len(words) == 0 {
		return
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	defer controller.cond.Broadcast()
	switch words[0] {
	case activatedLine:
		controller.activated = true
	case "ack":
		if len(words) >= 2 {
			if sequence, err := strconv.ParseUint(words[1], 10, 64); err == nil {
				controller.acked[sequence] = true
				controller.acks[sequence] = ""
				if len(words) > 2 {
					controller.details[sequence] = strings.Join(words[2:], " ")
				}
			}
		}
	case "error":
		if len(words) >= 3 {
			if sequence, err := strconv.ParseUint(words[1], 10, 64); err == nil {
				controller.acked[sequence] = true
				controller.acks[sequence] = strings.Join(words[2:], " ")
			}
		}
	case string(EventBoundary), string(EventBlocked), string(EventResumed):
		if len(words) != 3 {
			return
		}
		occurrence, err := strconv.Atoi(words[2])
		if err != nil {
			return
		}
		controller.events = append(controller.events, Event{Kind: EventKind(words[0]), Name: words[1], Occurrence: occurrence, At: time.Now()})
	case string(EventReleased):
		if len(words) != 3 {
			return
		}
		released, err := strconv.Atoi(words[2])
		if err != nil {
			return
		}
		if state := controller.rules[words[1]]; state != nil {
			state.released = released
		}
		controller.events = append(controller.events, Event{Kind: EventReleased, RuleID: words[1], Consumed: released, At: time.Now()})
	case string(EventRuleConsumed):
		if len(words) != 4 || words[2] != "consumed" {
			return
		}
		consumed, err := strconv.Atoi(words[3])
		if err != nil {
			return
		}
		if state := controller.rules[words[1]]; state != nil {
			state.consumed = consumed
		}
		controller.events = append(controller.events, Event{Kind: EventRuleConsumed, RuleID: words[1], Consumed: consumed, At: time.Now()})
	}
}

// sendRaw writes one sequenced command and waits for its ack or refusal.
func (controller *Controller) sendRaw(ctx context.Context, body string) error {
	_, err := controller.sendCommand(ctx, body)
	return err
}

// sendCommand writes one sequenced command and returns the ack detail.
func (controller *Controller) sendCommand(ctx context.Context, body string) (string, error) {
	controller.writeMu.Lock()
	controller.sequence++
	sequence := controller.sequence
	_, err := io.WriteString(controller.conn, fmt.Sprintf("%d %s\n", sequence, body))
	controller.writeMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrPeerClosed, err)
	}
	var detail string
	err = controller.wait(ctx, func() (bool, error) {
		if !controller.acked[sequence] {
			return false, nil
		}
		if refusal := controller.acks[sequence]; refusal != "" {
			return true, fmt.Errorf("command %q refused: %s", body, refusal)
		}
		detail = controller.details[sequence]
		return true, nil
	})
	return detail, err
}

// wait blocks on the condition under the controller lock until done, the
// context ends, or the peer closes.
func (controller *Controller) wait(ctx context.Context, done func() (bool, error)) error {
	stop := context.AfterFunc(ctx, func() {
		controller.mu.Lock()
		controller.cond.Broadcast()
		controller.mu.Unlock()
	})
	defer stop()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	for {
		finished, err := done()
		if finished || err != nil {
			return err
		}
		if controller.readErr != nil || controller.closed {
			return ErrPeerClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		controller.cond.Wait()
	}
}

// Activated waits for the process to answer the hello.
func (controller *Controller) Activated(ctx context.Context) error {
	return controller.wait(ctx, func() (bool, error) { return controller.activated, nil })
}

// Watch publishes every occurrence of name.
func (controller *Controller) Watch(ctx context.Context, name string) error {
	return controller.sendRaw(ctx, "watch "+name)
}

// Block parks the process at the given occurrence of name until Continue.
func (controller *Controller) Block(ctx context.Context, name string, occurrence int) error {
	return controller.sendRaw(ctx, fmt.Sprintf("block %s %d", name, occurrence))
}

// BlockNext parks the process at the next occurrence of name, whatever the
// current count, and returns that occurrence.
func (controller *Controller) BlockNext(ctx context.Context, name string) (int, error) {
	detail, err := controller.sendCommand(ctx, "blocknext "+name)
	if err != nil {
		return 0, err
	}
	occurrence, err := strconv.Atoi(detail)
	if err != nil {
		return 0, fmt.Errorf("blocknext ack %q: %w", detail, err)
	}
	return occurrence, nil
}

// Unblock removes a pending block on name so it can never fire later; a
// boundary already parked stays parked until Continue.
func (controller *Controller) Unblock(ctx context.Context, name string) error {
	return controller.sendRaw(ctx, "unblock "+name)
}

// Rule installs one bounded datagram fault and tracks its consumption.
func (controller *Controller) Rule(ctx context.Context, rule Rule) error {
	if err := rule.validate(); err != nil {
		return err
	}
	controller.mu.Lock()
	if _, exists := controller.rules[rule.ID]; exists {
		controller.mu.Unlock()
		return fmt.Errorf("rule %q already requested", rule.ID)
	}
	controller.rules[rule.ID] = &ruleState{rule: rule, remaining: rule.Count}
	controller.ruleOrder = append(controller.ruleOrder, rule.ID)
	controller.mu.Unlock()
	err := controller.sendRaw(ctx, rule.encode())
	if err != nil {
		controller.mu.Lock()
		delete(controller.rules, rule.ID)
		controller.mu.Unlock()
	}
	return err
}

// Release re-sends every frame the hold rule captured, from the node's own
// socket, and returns how many were released.
func (controller *Controller) Release(ctx context.Context, id string) (int, error) {
	detail, err := controller.sendCommand(ctx, "release "+id)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(detail)
}

// Continue releases the currently blocked boundary.
func (controller *Controller) Continue(ctx context.Context) error {
	return controller.sendRaw(ctx, "continue")
}

// Events returns a snapshot of every event received so far.
func (controller *Controller) Events() []Event {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return append([]Event(nil), controller.events...)
}

// Count returns how many events of kind carry name.
func (controller *Controller) Count(kind EventKind, name string) int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return countEvents(controller.events, kind, name)
}

func countEvents(events []Event, kind EventKind, name string) int {
	count := 0
	for _, event := range events {
		if event.Kind == kind && event.Name == name {
			count++
		}
	}
	return count
}

// WaitEvents blocks until predicate holds over the event history.
func (controller *Controller) WaitEvents(ctx context.Context, predicate func([]Event) bool) error {
	return controller.wait(ctx, func() (bool, error) { return predicate(controller.events), nil })
}

// WaitBoundary waits for the given occurrence of a watched boundary.
func (controller *Controller) WaitBoundary(ctx context.Context, name string, occurrence int) error {
	return controller.WaitEvents(ctx, func(events []Event) bool {
		for _, event := range events {
			if event.Kind == EventBoundary && event.Name == name && event.Occurrence >= occurrence {
				return true
			}
		}
		return false
	})
}

// WaitBlocked waits for the process to park at the given occurrence.
func (controller *Controller) WaitBlocked(ctx context.Context, name string, occurrence int) error {
	return controller.WaitEvents(ctx, func(events []Event) bool {
		for _, event := range events {
			if event.Kind == EventBlocked && event.Name == name && event.Occurrence == occurrence {
				return true
			}
		}
		return false
	})
}

// WaitRuleConsumed waits until rule id has been consumed at least count times.
func (controller *Controller) WaitRuleConsumed(ctx context.Context, id string, count int) error {
	return controller.wait(ctx, func() (bool, error) {
		state := controller.rules[id]
		if state == nil {
			return true, fmt.Errorf("rule %q was never requested", id)
		}
		return state.consumed >= count, nil
	})
}

// Consumed reports how many times rule id has fired.
func (controller *Controller) Consumed(id string) int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if state := controller.rules[id]; state != nil {
		return state.consumed
	}
	return 0
}

// Unconsumed lists every requested rule whose consumption differs from its
// requested count, in request order. An optional hold may capture nothing;
// whatever a hold captured must also have been released.
func (controller *Controller) Unconsumed() []string {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	var result []string
	for _, id := range controller.ruleOrder {
		state := controller.rules[id]
		if state == nil {
			continue
		}
		if state.rule.Hold {
			if state.consumed != state.released || !state.rule.Optional && state.consumed != state.rule.Count {
				result = append(result, id)
			}
			continue
		}
		if state.consumed != state.rule.Count {
			result = append(result, id)
		}
	}
	return result
}

// Exited reports whether the channel has reported the peer gone.
func (controller *Controller) Exited() bool {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.readErr != nil
}
