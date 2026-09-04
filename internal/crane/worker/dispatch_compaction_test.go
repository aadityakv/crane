package worker

import (
	"strings"
	"testing"
	"time"

	"github.com/aadityakv/crane/internal/crane/store"
)

// TestDispatchStartAfterCheckpointCompactionIsBenignSkip pins the real-process
// finding of Task 25: a sender job queued for an outbox that a committed
// checkpoint then compacts must be answered with a no-send skip, exactly like
// a Completed ACK overtaking a queued dispatch, never with the owner-fatal
// "unknown outbox" error that terminates the whole worker process. An
// identity no committed watermark covers stays a fatal invariant violation.
func TestDispatchStartAfterCheckpointCompactionIsBenignSkip(t *testing.T) {
	fixture := workerFixture(t)
	engine, err := NewEngine(testEngineOptions(newFakeRepository(fixture), admissionGateForTupleTest(t, fixture), &fakeSender{}))
	if err != nil {
		t.Fatal(err)
	}
	delivery := fixture.delivery(t, 3)
	outboxID := delivery.ID
	outboxID.EdgeID = 2
	// The source cursor durably covers sequence 3: compaction removed the
	// outbox from the owner map before the sender's handshake arrived.
	engine.sources[delivery.ID.Tuple.SourceTask] = store.SourceCursor{Source: delivery.ID.Tuple.SourceTask, NextSequence: 4, Watermark: 3, EOF: 16}
	response := engine.handleDispatchStart(dispatchStart{id: outboxID, started: time.Unix(100, 0), response: make(chan dispatchResponse, 1)})
	if response.err != nil || response.disposition != dispatchSkip {
		t.Fatalf("compacted outbox dispatch = %+v, want benign skip", response)
	}

	uncovered := fixture.delivery(t, 9).ID
	uncovered.EdgeID = 2
	response = engine.handleDispatchStart(dispatchStart{id: uncovered, started: time.Unix(100, 0), response: make(chan dispatchResponse, 1)})
	if response.err == nil || !strings.Contains(response.err.Error(), "unknown outbox") {
		t.Fatalf("uncovered unknown outbox dispatch = %+v, want the fatal invariant error", response)
	}
}
