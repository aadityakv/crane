package runtime

import (
	"testing"

	"github.com/aadityakv/crane/internal/crane/integrationhook"
	"github.com/aadityakv/crane/internal/wire"
)

type markerHook struct{ integrationhook.Noop }

func (markerHook) DatagramAction(integrationhook.Direction, wire.MessageType) integrationhook.Action {
	return integrationhook.Pass
}

// TestNewThreadsIntegrationHookIntoWorkerComposition proves the runtime hands
// exactly one hook to the worker service (store boundaries and the real +5
// paths) and that the ordinary build's default is the production no-op
// loaded from LoadFromInheritedFD, never an activated seam.
func TestNewThreadsIntegrationHookIntoWorkerComposition(t *testing.T) {
	configuration := runtimeTestConfig(t, 1, reserveRuntimeBasePort(t))
	prepareRuntimeStorage(t, configuration)

	dependencies := runtimeTestDependencies()
	dependencies.Hook = markerHook{}
	runtime, err := New(configuration, dependencies)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if runtime.workerOptions.Hook != integrationhook.Hook(markerHook{}) {
		t.Fatalf("worker hook = %T, want the injected hook", runtime.workerOptions.Hook)
	}

	plain, err := New(configuration, runtimeTestDependencies())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if integrationhook.Enabled {
		t.Skip("craneintegration build: default hook depends on descriptor 3")
	}
	if _, ok := plain.workerOptions.Hook.(integrationhook.Noop); !ok {
		t.Fatalf("default worker hook = %T, want integrationhook.Noop", plain.workerOptions.Hook)
	}
}
