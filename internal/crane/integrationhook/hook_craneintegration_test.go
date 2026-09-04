//go:build craneintegration

package integrationhook

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/wire"
)

// newHookPair activates one hook over a socketpair exactly as an inherited
// descriptor would, returning the harness-side controller. The in-process
// child must not exit on channel close, so the plain hello is used.
func newHookPair(t *testing.T) (*Controller, Hook) {
	t.Helper()
	controller, err := NewControllerWithoutOrphanExit()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controller.Close() })
	child := controller.ChildFile()
	hook := activate(child)
	controller.Started()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Activated(ctx); err != nil {
		t.Fatalf("activation: %v", err)
	}
	return controller, hook
}

func TestActivationRequiresExactHelloOnASocket(t *testing.T) {
	// A regular file is never an activation channel.
	regular, err := os.CreateTemp(t.TempDir(), "not-a-socket")
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()
	if _, ok := activate(regular).(Noop); !ok {
		t.Fatal("regular file activated the hook")
	}
	// A socket without the exact hello line is refused.
	parent, child, err := socketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	go func() { _, _ = parent.Write([]byte("hello something-else/1\n")) }()
	if _, ok := activate(child).(Noop); !ok {
		t.Fatal("wrong hello activated the hook")
	}
}

func TestHelloSelectsExitOnEOF(t *testing.T) {
	// The plain hello leaves the process running after the channel closes;
	// only the harness's orphan-control hello demands exit-on-EOF. Parsing is
	// proven directly: activating an exit-on-EOF hook inside this process
	// would let its socket's finalizer close terminate the whole test run.
	plainExit, ok := parseHello(helloLine)
	if !ok || plainExit {
		t.Fatalf("parseHello(%q) = %v, %v; want false, true", helloLine, plainExit, ok)
	}
	exitOnEOF, ok := parseHello(helloExitOnEOF)
	if !ok || !exitOnEOF {
		t.Fatalf("parseHello(%q) = %v, %v; want true, true", helloExitOnEOF, exitOnEOF, ok)
	}
	if _, ok := parseHello("hello craneintegration/1 exit-on-eof extra"); ok {
		t.Fatal("parseHello accepted an unknown suffix")
	}
}

func TestBoundaryEventsBlockUntilContinueAndCountOccurrences(t *testing.T) {
	controller, hook := newHookPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Watch(ctx, "delivery-received"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Block(ctx, "delivery-processed", 2); err != nil {
		t.Fatal(err)
	}
	// Unwatched boundaries are silent.
	hook.DurableBoundary("fence")
	hook.DurableBoundary("delivery-received")
	hook.DurableBoundary("delivery-processed")
	if err := controller.WaitBoundary(ctx, "delivery-received", 1); err != nil {
		t.Fatal(err)
	}
	if err := controller.WaitBoundary(ctx, "delivery-processed", 1); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		hook.DurableBoundary("delivery-processed")
		close(released)
	}()
	if err := controller.WaitBlocked(ctx, "delivery-processed", 2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
		t.Fatal("second occurrence did not block")
	case <-time.After(100 * time.Millisecond):
	}
	if err := controller.Continue(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("continue did not release the blocked boundary")
	}
	// The block was one-shot: a third occurrence passes.
	hook.DurableBoundary("delivery-processed")
	if err := controller.WaitBoundary(ctx, "delivery-processed", 3); err != nil {
		t.Fatal(err)
	}
	if got := controller.Count(EventBoundary, "fence"); got != 0 {
		t.Fatalf("unwatched boundary published %d events", got)
	}
}

func TestDatagramRulesAreBoundedConsumedExactlyOnceAndReported(t *testing.T) {
	controller, hook := newHookPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Rule(ctx, Rule{ID: "drop-ack", Direction: Send, Message: wire.MessageCraneTupleDeliveryAck, Action: Drop, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Rule(ctx, Rule{ID: "dup-delivery", Direction: Send, Message: wire.MessageCraneTupleDelivery, Action: Duplicate, Count: 2}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Rule(ctx, Rule{ID: "drop-recv", Direction: Receive, Message: wire.MessageCraneTupleDelivery, Action: Drop, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryNack); got != Pass {
		t.Fatalf("unmatched type action = %v", got)
	}
	if got := hook.DatagramAction(Receive, wire.MessageCraneTupleDeliveryAck); got != Pass {
		t.Fatalf("unmatched direction action = %v", got)
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryAck); got != Drop {
		t.Fatalf("first ACK action = %v, want Drop", got)
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryAck); got != Pass {
		t.Fatalf("second ACK action = %v, want Pass (rule exhausted)", got)
	}
	for i := 0; i < 2; i++ {
		if got := hook.DatagramAction(Send, wire.MessageCraneTupleDelivery); got != Duplicate {
			t.Fatalf("delivery %d action = %v, want Duplicate", i, got)
		}
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDelivery); got != Pass {
		t.Fatalf("third delivery action = %v, want Pass", got)
	}
	if err := controller.WaitRuleConsumed(ctx, "dup-delivery", 2); err != nil {
		t.Fatal(err)
	}
	if err := controller.WaitRuleConsumed(ctx, "drop-ack", 1); err != nil {
		t.Fatal(err)
	}
	unconsumed := controller.Unconsumed()
	if len(unconsumed) != 1 || unconsumed[0] != "drop-recv" {
		t.Fatalf("unconsumed = %v, want [drop-recv]", unconsumed)
	}
	if got := hook.DatagramAction(Receive, wire.MessageCraneTupleDelivery); got != Drop {
		t.Fatalf("receive action = %v, want Drop", got)
	}
	if err := controller.WaitRuleConsumed(ctx, "drop-recv", 1); err != nil {
		t.Fatal(err)
	}
	if unconsumed := controller.Unconsumed(); len(unconsumed) != 0 {
		t.Fatalf("unconsumed = %v, want none", unconsumed)
	}
}

func TestBlockNextUnblockAndPassRules(t *testing.T) {
	controller, hook := newHookPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hook.DurableBoundary("result-upserted")
	hook.DurableBoundary("result-upserted")
	occurrence, err := controller.BlockNext(ctx, "result-upserted")
	if err != nil || occurrence != 3 {
		t.Fatalf("BlockNext = %d,%v, want 3", occurrence, err)
	}
	// A cleared block never fires.
	if err := controller.Unblock(ctx, "result-upserted"); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		hook.DurableBoundary("result-upserted")
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("unblocked boundary still parked")
	}
	if err := controller.WaitBoundary(ctx, "result-upserted", 3); err != nil {
		t.Fatal(err)
	}
	// A pass rule is a consumed-once observation that changes nothing.
	if err := controller.Rule(ctx, Rule{ID: "expect-nack", Direction: Send, Message: wire.MessageCraneTupleDeliveryNack, Action: Pass, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryNack); got != Pass {
		t.Fatalf("pass rule action = %v", got)
	}
	if err := controller.WaitRuleConsumed(ctx, "expect-nack", 1); err != nil {
		t.Fatal(err)
	}
	if unconsumed := controller.Unconsumed(); len(unconsumed) != 0 {
		t.Fatalf("unconsumed = %v", unconsumed)
	}
}

func TestHoldRulesCaptureExactFramesUntilReleased(t *testing.T) {
	controller, hook := newHookPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, ok := hook.(DatagramHolder)
	if !ok {
		t.Fatal("tagged hook does not implement DatagramHolder")
	}
	if err := controller.Rule(ctx, Rule{ID: "stale", Direction: Send, Message: wire.MessageCraneTupleDelivery, Count: 1, Hold: true, To: 4, Optional: true}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Rule(ctx, Rule{ID: "never", Direction: Send, Message: wire.MessageCraneTupleDelivery, Count: 1, Hold: true, To: 9, Optional: true}); err != nil {
		t.Fatal(err)
	}
	resent := make(chan struct{}, 4)
	resend := func() { resent <- struct{}{} }
	if holder.HoldDatagram(wire.MessageCraneTupleDeliveryAck, 4, resend) {
		t.Fatal("hold captured a non-matching message type")
	}
	if holder.HoldDatagram(wire.MessageCraneTupleDelivery, 2, resend) {
		t.Fatal("hold captured a datagram to another destination")
	}
	if !holder.HoldDatagram(wire.MessageCraneTupleDelivery, 4, resend) {
		t.Fatal("hold did not capture the matching datagram")
	}
	if holder.HoldDatagram(wire.MessageCraneTupleDelivery, 4, resend) {
		t.Fatal("exhausted hold captured a second datagram")
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDelivery); got != Pass {
		t.Fatalf("hold rule leaked into DatagramAction: %v", got)
	}
	if err := controller.WaitRuleConsumed(ctx, "stale", 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resent:
		t.Fatal("held frame was re-sent before release")
	case <-time.After(50 * time.Millisecond):
	}
	if unconsumed := controller.Unconsumed(); len(unconsumed) != 1 || unconsumed[0] != "stale" {
		t.Fatalf("captured-but-unreleased hold not reported: %v", unconsumed)
	}
	released, err := controller.Release(ctx, "stale")
	if err != nil || released != 1 {
		t.Fatalf("Release = %d,%v", released, err)
	}
	select {
	case <-resent:
	case <-time.After(2 * time.Second):
		t.Fatal("release did not re-send the held frame")
	}
	if err := controller.WaitEvents(ctx, func(events []Event) bool {
		for _, event := range events {
			if event.Kind == EventReleased && event.RuleID == "stale" && event.Consumed == 1 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if unconsumed := controller.Unconsumed(); len(unconsumed) != 0 {
		t.Fatalf("unconsumed = %v, want none (optional hold captured nothing)", unconsumed)
	}
	// A released hold is retired and captures nothing further.
	if holder.HoldDatagram(wire.MessageCraneTupleDelivery, 4, resend) {
		t.Fatal("released hold captured a later datagram")
	}
	if _, err := controller.Release(ctx, "missing"); err == nil {
		t.Fatal("release of an unknown rule accepted")
	}
}

func TestMalformedCommandsAreRefusedWithoutChangingState(t *testing.T) {
	controller, hook := newHookPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, raw := range []string{"block", "block name notanumber", "blocknext", "unblock a b", "rule x send delivery explode 1", "rule x sideways delivery drop 1", "rule x send delivery drop 0", "rule x recv delivery duplicate 1", "rule x recv delivery hold 1", "rule x send delivery drop 1 to=4", "rule x send delivery hold 1 to=0", "rule x send delivery hold 1 bogus"} {
		if err := controller.sendRaw(ctx, raw); err == nil {
			t.Fatalf("command %q accepted", raw)
		}
	}
	if err := controller.Rule(ctx, Rule{ID: "x", Direction: Send, Message: wire.MessageCraneTupleDelivery, Action: Drop, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Rule(ctx, Rule{ID: "x", Direction: Send, Message: wire.MessageCraneTupleDelivery, Action: Drop, Count: 1}); err == nil {
		t.Fatal("duplicate rule ID accepted")
	}
	if got := hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryAck); got != Pass {
		t.Fatalf("refused rules changed datagrams: %v", got)
	}
}

func TestChildDeathIsObservedAndPendingWaitsFail(t *testing.T) {
	controller, err := NewController()
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	child := controller.ChildFile()
	controller.Started()
	_ = child.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controller.Activated(ctx); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Activated after child death = %v, want ErrPeerClosed", err)
	}
	if err := controller.WaitBoundary(ctx, "anything", 1); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("WaitBoundary after child death = %v, want ErrPeerClosed", err)
	}
}

func TestLoadFromInheritedFDWithoutDescriptorIsNoop(t *testing.T) {
	// The test process has no test-owned descriptor 3 activation channel; the
	// loader must degrade to the no-op hook rather than fail or block.
	hook := LoadFromInheritedFD()
	if _, ok := hook.(Noop); !ok {
		t.Fatalf("LoadFromInheritedFD() = %T without an inherited channel, want Noop", hook)
	}
}

// socketPair returns a raw unix stream pair with nothing written on it.
func socketPair() (net.Conn, *os.File, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	parentFile := os.NewFile(uintptr(fds[0]), "parent")
	child := os.NewFile(uintptr(fds[1]), "child")
	parent, err := net.FileConn(parentFile)
	_ = parentFile.Close()
	if err != nil {
		_ = child.Close()
		return nil, nil, err
	}
	return parent, child, nil
}
