package state

import (
	"bytes"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
)

func TestClientSequenceExecutesOnceCachesOwnedResultAndRejectsReuseStaleGap(t *testing.T) {
	machine := NewMachine()
	client := model.ClientID{1}
	digest1 := [32]byte{1}
	var mutations int
	result := []byte("first-result")
	first := applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, digest1, func() mutationPlan {
		return mutationPlan{result: result, commit: func() { mutations++ }}
	})
	if mutations != 1 || !bytes.Equal(first, result) {
		t.Fatalf("first=%x mutations=%d", first, mutations)
	}
	result[0] ^= 0xff
	retry := applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, digest1, func() mutationPlan {
		t.Fatal("retry executed")
		return mutationPlan{}
	})
	if !bytes.Equal(retry, []byte("first-result")) {
		t.Fatalf("retry=%q", retry)
	}
	retry[0] ^= 0xff
	again := applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, digest1, func() mutationPlan { t.Fatal("retry executed"); return mutationPlan{} })
	if !bytes.Equal(again, []byte("first-result")) {
		t.Fatalf("cached result aliased caller: %q", again)
	}

	reuse := mustResult(t, applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, [32]byte{2}, func() mutationPlan { t.Fatal("reuse executed"); return mutationPlan{} }))
	if reuse.Code != ResultIdentityReuse {
		t.Fatalf("reuse=%#v", reuse)
	}
	gap := mustResult(t, applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 3}, [32]byte{3}, func() mutationPlan { t.Fatal("gap executed"); return mutationPlan{} }))
	if gap.Code != ResultSkippedRequest {
		t.Fatalf("gap=%#v", gap)
	}
	second := applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 2}, [32]byte{4}, func() mutationPlan {
		return mutationPlan{result: []byte("second"), commit: func() { mutations++ }}
	})
	if !bytes.Equal(second, []byte("second")) || mutations != 2 {
		t.Fatalf("second=%q mutations=%d", second, mutations)
	}
	stale := mustResult(t, applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, digest1, func() mutationPlan { t.Fatal("stale executed"); return mutationPlan{} }))
	if stale.Code != ResultStaleRequest {
		t.Fatalf("stale=%#v", stale)
	}
}

func TestClientCapacity1024Rejects1025BeforeExecutionWithoutEviction(t *testing.T) {
	machine := NewMachine()
	for value := 1; value <= int(model.StateCommandMaxClientSessionsV1); value++ {
		client := model.ClientID{byte(value >> 8), byte(value)}
		got := applyClientTest(t, machine, model.ClientRequestID{ClientID: client, Sequence: 1}, [32]byte{byte(value), 1}, func() mutationPlan { return mutationPlan{result: []byte{1}} })
		if !bytes.Equal(got, []byte{1}) {
			t.Fatalf("client %d result=%x", value, got)
		}
	}
	var prepared bool
	extra := model.ClientID{0xff, 0xff}
	rejected := mustResult(t, applyClientTest(t, machine, model.ClientRequestID{ClientID: extra, Sequence: 1}, [32]byte{9}, func() mutationPlan {
		prepared = true
		return mutationPlan{result: []byte{1}}
	}))
	if rejected.Code != ResultCapacityExhausted || prepared || len(machine.clients) != 1024 {
		t.Fatalf("capacity=%#v prepared=%t clients=%d", rejected, prepared, len(machine.clients))
	}
	first := model.ClientID{0, 1}
	replay := applyClientTest(t, machine, model.ClientRequestID{ClientID: first, Sequence: 1}, [32]byte{1, 1}, func() mutationPlan { t.Fatal("evicted first client"); return mutationPlan{} })
	if !bytes.Equal(replay, []byte{1}) {
		t.Fatalf("first client was not retained: %x", replay)
	}
}

func TestClientCachedResult65536ExactAndPlusOneRejectedBeforeMutation(t *testing.T) {
	machine := NewMachine()
	maximum := make([]byte, model.StateCommandMaxCachedResultBytesV1)
	maximum[0], maximum[len(maximum)-1] = 1, 2
	var commits int
	got := applyClientTest(t, machine, model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}, [32]byte{1}, func() mutationPlan {
		return mutationPlan{result: maximum, commit: func() { commits++ }}
	})
	if !bytes.Equal(got, maximum) || commits != 1 {
		t.Fatalf("max result len=%d commits=%d", len(got), commits)
	}
	tooLarge := make([]byte, model.StateCommandMaxCachedResultBytesV1+1)
	tooLarge[0] = 1
	rejected := mustResult(t, applyClientTest(t, machine, model.ClientRequestID{ClientID: model.ClientID{2}, Sequence: 1}, [32]byte{2}, func() mutationPlan {
		return mutationPlan{result: tooLarge, commit: func() { commits++ }}
	}))
	if rejected.Code != ResultResultTooLarge || commits != 1 {
		t.Fatalf("oversize=%#v commits=%d", rejected, commits)
	}
	if len(machine.clients) != 2 {
		t.Fatalf("oversize deterministic rejection was not cached: clients=%d", len(machine.clients))
	}
	replay := applyClientTest(t, machine, model.ClientRequestID{ClientID: model.ClientID{2}, Sequence: 1}, [32]byte{2}, func() mutationPlan { t.Fatal("oversize replay executed"); return mutationPlan{} })
	if !bytes.Equal(replay, mustMarshalResult(t, rejected)) {
		t.Fatalf("oversize replay=%x", replay)
	}
}

func TestClientInvalidIdentityRejectsBeforeExecution(t *testing.T) {
	machine := NewMachine()
	tests := []struct {
		request model.ClientRequestID
		digest  [32]byte
	}{
		{request: model.ClientRequestID{Sequence: 1}, digest: [32]byte{1}},
		{request: model.ClientRequestID{ClientID: model.ClientID{1}}, digest: [32]byte{1}},
		{request: model.ClientRequestID{ClientID: model.ClientID{1}, Sequence: 1}},
	}
	for _, test := range tests {
		prepared := false
		machine.mu.Lock()
		_, err := machine.applyClientLocked(test.request, test.digest, func() (mutationPlan, error) { prepared = true; return mutationPlan{}, nil })
		machine.mu.Unlock()
		if err == nil || prepared {
			t.Fatalf("invalid identity request=%#v digest=%x err=%v prepared=%t", test.request, test.digest, err, prepared)
		}
	}
}

func applyClientTest(t *testing.T, machine *Machine, request model.ClientRequestID, digest [32]byte, prepare func() mutationPlan) []byte {
	t.Helper()
	machine.mu.Lock()
	defer machine.mu.Unlock()
	result, err := machine.applyClientLocked(request, digest, func() (mutationPlan, error) { return prepare(), nil })
	if err != nil {
		t.Fatalf("applyClientLocked: %v", err)
	}
	return result
}

func mustMarshalResult(t *testing.T, result CommandResult) []byte {
	t.Helper()
	encoded, err := MarshalCommandResult(result)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
