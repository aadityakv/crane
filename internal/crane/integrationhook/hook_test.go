package integrationhook

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crane/internal/wire"
)

// TestNoopHookPassesEveryDatagramAndNeverBlocks pins the production
// contract: the no-op hook consults no environment, no descriptor, and no
// registry; every datagram passes and every boundary returns immediately.
func TestNoopHookPassesEveryDatagramAndNeverBlocks(t *testing.T) {
	var hook Hook = Noop{}
	for _, direction := range []Direction{Send, Receive} {
		for _, message := range []wire.MessageType{wire.MessageCraneTupleDelivery, wire.MessageCraneTupleDeliveryAck, wire.MessageCraneTupleDeliveryNack, 0, 255} {
			if action := hook.DatagramAction(direction, message); action != Pass {
				t.Fatalf("Noop.DatagramAction(%v, %d) = %v, want Pass", direction, message, action)
			}
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, name := range []string{"", "delivery-received", "anything"} {
			hook.DurableBoundary(name)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Noop.DurableBoundary blocked")
	}
}

// TestLoadFromInheritedFDIsNoopInOrdinaryBuild proves the ordinary build has
// no activation path at all: loading returns the no-op hook regardless of
// process state, and the package reports the seam disabled.
func TestLoadFromInheritedFDIsNoopInOrdinaryBuild(t *testing.T) {
	if Enabled {
		t.Skip("craneintegration build tag active")
	}
	hook := LoadFromInheritedFD()
	if _, ok := hook.(Noop); !ok {
		t.Fatalf("LoadFromInheritedFD() = %T, want Noop", hook)
	}
	if hook.DatagramAction(Send, wire.MessageCraneTupleDeliveryAck) != Pass {
		t.Fatal("ordinary build altered a datagram")
	}
}

// TestProductionNodeBinaryContainsNoActivationSymbols builds cmd/node without
// the tag and proves the linked binary carries no event registry, activation
// parser, command reader, or controller; the tagged build must carry them so
// the symbol scan is not vacuous.
func TestProductionNodeBinaryContainsNoActivationSymbols(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two node binaries")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	taggedSymbols := []string{"integrationhook.(*fdHook)", "integrationhook.parseCommand", "integrationhook.(*Controller)", "integrationhook.activate"}
	production := buildNodeSymbols(t, root, nil)
	for _, symbol := range taggedSymbols {
		if strings.Contains(production, symbol) {
			t.Fatalf("production node binary links %q", symbol)
		}
	}
	if strings.Contains(production, "craneintegration") {
		t.Fatal("production node binary references the craneintegration activation protocol")
	}
	tagged := buildNodeSymbols(t, root, []string{"-tags", "craneintegration"})
	for _, symbol := range taggedSymbols[:2] {
		if !strings.Contains(tagged, symbol) {
			t.Fatalf("tagged node binary does not link %q; the production scan is vacuous", symbol)
		}
	}
}

func buildNodeSymbols(t *testing.T, root string, extra []string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "node")
	args := append([]string{"build"}, extra...)
	args = append(args, "-o", binary, "./cmd/node")
	build := exec.CommandContext(ctx, "go", args...)
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, output)
	}
	names := exec.CommandContext(ctx, "go", "tool", "nm", binary)
	names.Dir = root
	output, err := names.Output()
	if err != nil {
		t.Fatalf("go tool nm: %v", err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal(err)
	}
	return string(output)
}
