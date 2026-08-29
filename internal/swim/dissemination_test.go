package swim

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestDisseminatorPrioritizesAndBoundsEncodedBatch(t *testing.T) {
	d := NewDisseminator(32, 3)
	d.Enqueue(updateFor(2, 1, Alive), 8)
	d.Enqueue(updateFor(3, 1, Dead), 8)
	d.Enqueue(updateFor(4, 1, Suspect), 8)
	encode := func(updates []Update) ([]byte, error) { return json.Marshal(updates) }

	batch, err := d.Take(150, 8, encode)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) == 0 || batch[0].Member.Status != Dead {
		t.Fatalf("batch order = %#v", batch)
	}
	encoded, err := encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 150 {
		t.Fatalf("encoded batch length = %d", len(encoded))
	}
}

func TestDisseminatorCoalescesByMembershipFreshness(t *testing.T) {
	d := NewDisseminator(8, 1)
	original := updateFor(2, 4, Alive)
	original.Member.Host = "original"
	original.Member.BasePort = 8002
	d.Enqueue(original, 1)

	stale := updateFor(2, 3, Left)
	stale.Member.Host = "stale"
	d.Enqueue(stale, 1)
	d.Enqueue(updateFor(2, 4, Alive), 1)

	suspect := updateFor(2, 4, Suspect)
	suspect.Member.Host = "forged"
	suspect.Member.BasePort = 9002
	suspect.ReporterID = 9
	d.Enqueue(suspect, 1)

	batch := mustTake(t, d, 1, countEncoder)
	want := original
	want.Member.Status = Suspect
	want.ReporterID = 9
	if !reflect.DeepEqual(batch, []Update{want}) {
		t.Fatalf("coalesced batch = %#v, want %#v", batch, []Update{want})
	}
}

func TestDisseminatorNewIncarnationReplacesWholeUpdateAndBudget(t *testing.T) {
	d := NewDisseminator(8, 1)
	d.Enqueue(updateFor(2, 4, Dead), 1)

	newer := updateFor(2, 5, Alive)
	newer.Member.Host = "replacement"
	newer.Member.BasePort = 8102
	newer.ReporterID = 7
	d.Enqueue(newer, 3)

	first := mustTakeForMembers(t, d, 3, 3, countEncoder)
	second := mustTakeForMembers(t, d, 3, 3, countEncoder)
	if !reflect.DeepEqual(first, []Update{newer}) || !reflect.DeepEqual(second, []Update{newer}) {
		t.Fatalf("replacement batches = %#v then %#v, want %#v twice", first, second, []Update{newer})
	}
	if got := mustTakeForMembers(t, d, 3, 3, countEncoder); len(got) != 0 {
		t.Fatalf("batch after replacement budget exhausted = %#v, want empty", got)
	}
}

func TestDisseminatorOrdersSeverityThenEnqueueAge(t *testing.T) {
	d := NewDisseminator(8, 1)
	d.Enqueue(updateFor(8, 1, Alive), 1)
	d.Enqueue(updateFor(9, 1, Alive), 1)
	d.Enqueue(updateFor(5, 1, Suspect), 1)
	d.Enqueue(updateFor(4, 1, Dead), 1)
	d.Enqueue(updateFor(3, 1, Left), 1)

	// Replacing node 8 makes its current Alive information newer than node 9's.
	d.Enqueue(updateFor(8, 2, Alive), 1)

	batch := mustTake(t, d, 5, countEncoder)
	wantIDs := []uint16{3, 4, 5, 9, 8}
	if got := nodeIDs(batch); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("batch node IDs = %v, want %v", got, wantIDs)
	}
}

func TestDisseminatorFallsBackToNodeIDWhenEnqueueSequenceSaturates(t *testing.T) {
	d := NewDisseminator(3, 1)
	d.nextSequence = math.MaxUint64
	for _, nodeID := range []uint16{9, 2, 5} {
		d.Enqueue(updateFor(nodeID, 1, Alive), 1)
	}

	batch := mustTake(t, d, 3, countEncoder)
	if got, want := nodeIDs(batch), []uint16{2, 5, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saturated-sequence batch node IDs = %v, want %v", got, want)
	}
}

func TestDisseminatorHonorsExactByteBoundaryAndRetainsOversizeItem(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 1)
	encode := fixedBytesPerUpdate(7)

	batch, err := d.Take(6, 1, encode)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 0 {
		t.Fatalf("oversize batch = %#v, want empty", batch)
	}

	batch = mustTake(t, d, 7, encode)
	if !reflect.DeepEqual(batch, []Update{update}) {
		t.Fatalf("exact-boundary batch = %#v, want %#v", batch, []Update{update})
	}
	if got := mustTake(t, d, 7, encode); len(got) != 0 {
		t.Fatalf("batch after exact-boundary transmission = %#v, want empty", got)
	}
}

func TestDisseminatorOnlyDecrementsTransmittedItems(t *testing.T) {
	d := NewDisseminator(3, 1)
	for _, nodeID := range []uint16{2, 3, 4} {
		d.Enqueue(updateFor(nodeID, 1, Alive), 1)
	}

	encode := fixedBytesPerUpdate(10)
	for _, wantID := range []uint16{2, 3, 4} {
		batch := mustTake(t, d, 10, encode)
		if got := nodeIDs(batch); !reflect.DeepEqual(got, []uint16{wantID}) {
			t.Fatalf("batch node IDs = %v, want [%d]", got, wantID)
		}
	}
	if got := mustTake(t, d, 10, encode); len(got) != 0 {
		t.Fatalf("batch after all budgets exhausted = %#v, want empty", got)
	}
}

func TestDisseminatorEncoderErrorDoesNotDecrementBudget(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 1)
	wantErr := errors.New("encode failed")

	batch, err := d.Take(100, 1, func([]Update) ([]byte, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Take error = %v, want %v", err, wantErr)
	}
	if batch != nil {
		t.Fatalf("batch on encoder error = %#v, want nil", batch)
	}
	if got := mustTake(t, d, 1, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
		t.Fatalf("batch after encoder error = %#v, want %#v", got, []Update{update})
	}
}

func TestDisseminatorRejectsNilEncoderWithoutDecrementingBudget(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 1)

	batch, err := d.Take(100, 1, nil)
	if err == nil || batch != nil {
		t.Fatalf("nil encoder returned batch=%#v err=%v, want nil batch and error", batch, err)
	}
	if got := mustTake(t, d, 1, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
		t.Fatalf("batch after nil encoder = %#v, want %#v", got, []Update{update})
	}
}

func TestDisseminatorUsesGrownBudgetWhileItemRemainsPending(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 1)

	for send := 0; send < 2; send++ {
		if got := mustTakeForMembers(t, d, 1, 3, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
			t.Fatalf("send %d batch = %#v, want %#v", send+1, got, []Update{update})
		}
	}
	if got := mustTakeForMembers(t, d, 1, 3, countEncoder); len(got) != 0 {
		t.Fatalf("batch after grown-cluster budget exhausted = %#v, want empty", got)
	}
}

func TestDisseminatorShrinkRemovesItemAlreadyAtCurrentBudget(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 8)

	for send := 0; send < 2; send++ {
		if got := mustTakeForMembers(t, d, 1, 8, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
			t.Fatalf("send %d batch = %#v, want %#v", send+1, got, []Update{update})
		}
	}
	if got := mustTakeForMembers(t, d, 1, 1, countEncoder); len(got) != 0 {
		t.Fatalf("batch after shrink below sent count = %#v, want empty", got)
	}
	if got := mustTakeForMembers(t, d, 1, 8, countEncoder); len(got) != 0 {
		t.Fatalf("batch after regrowth of removed item = %#v, want empty", got)
	}
}

func TestDisseminatorShrinkAllowsOnlyCurrentBudgetRemainder(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 8)

	if got := mustTakeForMembers(t, d, 1, 8, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
		t.Fatalf("initial batch = %#v, want %#v", got, []Update{update})
	}
	if got := mustTakeForMembers(t, d, 1, 3, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
		t.Fatalf("last current-budget batch = %#v, want %#v", got, []Update{update})
	}
	if got := mustTakeForMembers(t, d, 1, 8, countEncoder); len(got) != 0 {
		t.Fatalf("batch after shrink exhausted item = %#v, want empty", got)
	}
}

func TestDisseminatorShrinkPrunesExhaustedItemWithZeroByteBatch(t *testing.T) {
	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 8)

	for send := 0; send < 2; send++ {
		if got := mustTakeForMembers(t, d, 1, 8, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
			t.Fatalf("send %d batch = %#v, want %#v", send+1, got, []Update{update})
		}
	}
	if got := mustTakeForMembers(t, d, 0, 1, countEncoder); len(got) != 0 {
		t.Fatalf("zero-byte shrink batch = %#v, want empty", got)
	}
	if got := mustTakeForMembers(t, d, 1, 8, countEncoder); len(got) != 0 {
		t.Fatalf("batch after zero-byte shrink pruning = %#v, want empty", got)
	}
}

func TestDisseminatorOverflowRequiresDigestWithoutEvictingCurrentState(t *testing.T) {
	d := NewDisseminator(2, 1)
	d.Enqueue(updateFor(2, 1, Alive), 1)
	d.Enqueue(updateFor(3, 1, Alive), 1)

	// Superseding an existing node still fits because it coalesces in place.
	d.Enqueue(updateFor(2, 2, Dead), 1)
	if d.DigestRequired {
		t.Fatal("coalesced replacement unexpectedly required a digest")
	}

	d.Enqueue(updateFor(4, 1, Left), 1)
	if !d.DigestRequired {
		t.Fatal("overflow did not require a digest")
	}
	batch := mustTake(t, d, 2, countEncoder)
	if got, want := nodeIDs(batch), []uint16{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch after overflow = %v, want retained current nodes %v", got, want)
	}
}

func TestDisseminatorRemainsBoundedUnderDistinctUpdates(t *testing.T) {
	d := NewDisseminator(4, 1)
	for nodeID := uint16(1); nodeID <= 1_000; nodeID++ {
		d.Enqueue(updateFor(nodeID, 1, Alive), 1)
	}
	if got := len(d.items); got != 4 {
		t.Fatalf("pending item count = %d, want 4", got)
	}
	if !d.DigestRequired {
		t.Fatal("bounded overflow did not require a digest")
	}
}

func TestDisseminatorRejectsInvalidUpdates(t *testing.T) {
	d := NewDisseminator(4, 1)
	for _, update := range []Update{
		{Member: Member{NodeID: 0, Status: Alive}, ReporterID: 1},
		{Member: Member{NodeID: 1, Status: Alive}, ReporterID: 0},
		{Member: Member{NodeID: 1, Incarnation: 0, Status: Alive}, ReporterID: 1},
		{Member: Member{NodeID: 1, Status: Status(255)}, ReporterID: 1},
	} {
		d.Enqueue(update, 1)
	}
	if got := len(d.items); got != 0 {
		t.Fatalf("invalid updates retained %d items, want 0", got)
	}
	if d.DigestRequired {
		t.Fatal("invalid update required a digest")
	}
}

func TestDisseminatorHandlesZeroAndInvalidBoundsExplicitly(t *testing.T) {
	for _, maxItems := range []int{0, -1} {
		d := NewDisseminator(maxItems, 1)
		d.Enqueue(updateFor(2, 1, Alive), 1)
		if !d.DigestRequired {
			t.Fatalf("maxItems %d did not require a digest", maxItems)
		}
		if got := mustTake(t, d, 100, countEncoder); len(got) != 0 {
			t.Fatalf("maxItems %d batch = %#v, want empty", maxItems, got)
		}
	}

	d := NewDisseminator(1, 1)
	update := updateFor(2, 1, Alive)
	d.Enqueue(update, 1)
	if batch, err := d.Take(-1, 1, countEncoder); err == nil || batch != nil {
		t.Fatalf("negative byte bound returned batch=%#v err=%v, want nil batch and error", batch, err)
	}
	if got := mustTake(t, d, 0, countEncoder); len(got) != 0 {
		t.Fatalf("zero-byte batch = %#v, want empty", got)
	}
	if got := mustTake(t, d, 1, countEncoder); !reflect.DeepEqual(got, []Update{update}) {
		t.Fatalf("batch after invalid bounds = %#v, want %#v", got, []Update{update})
	}

	zeroBudget := NewDisseminator(1, 0)
	zeroBudget.Enqueue(update, 1)
	if !zeroBudget.DigestRequired || len(zeroBudget.items) != 0 {
		t.Fatalf("zero retransmit budget state = digest:%v items:%d, want digest and no items", zeroBudget.DigestRequired, len(zeroBudget.items))
	}
}

func TestRetransmitBudgetUsesCeilingLog2(t *testing.T) {
	tests := []struct {
		name       string
		multiplier int
		members    int
		want       int
	}{
		{name: "empty", multiplier: 3, members: 0, want: 0},
		{name: "one", multiplier: 3, members: 1, want: 3},
		{name: "two", multiplier: 3, members: 2, want: 6},
		{name: "three", multiplier: 3, members: 3, want: 6},
		{name: "eight", multiplier: 3, members: 8, want: 12},
		{name: "zero_multiplier", multiplier: 0, members: 8, want: 0},
		{name: "negative_multiplier", multiplier: -1, members: 8, want: 0},
		{name: "negative_members", multiplier: 3, members: -1, want: 0},
		{name: "saturates", multiplier: math.MaxInt, members: math.MaxInt, want: math.MaxInt},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := RetransmitBudget(test.multiplier, test.members); got != test.want {
				t.Fatalf("RetransmitBudget(%d, %d) = %d, want %d", test.multiplier, test.members, got, test.want)
			}
		})
	}
}

func updateFor(nodeID uint16, incarnation uint64, status Status) Update {
	return Update{
		Member: Member{
			NodeID:      nodeID,
			Host:        "node",
			BasePort:    8000,
			Incarnation: incarnation,
			Status:      status,
		},
		ReporterID: 1,
	}
}

func mustTake(t *testing.T, d *Disseminator, maxBytes int, encode func([]Update) ([]byte, error)) []Update {
	return mustTakeForMembers(t, d, maxBytes, 1, encode)
}

func mustTakeForMembers(t *testing.T, d *Disseminator, maxBytes int, aliveMembers int, encode func([]Update) ([]byte, error)) []Update {
	t.Helper()
	batch, err := d.Take(maxBytes, aliveMembers, encode)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func countEncoder(updates []Update) ([]byte, error) {
	return make([]byte, len(updates)), nil
}

func fixedBytesPerUpdate(size int) func([]Update) ([]byte, error) {
	return func(updates []Update) ([]byte, error) {
		return make([]byte, size*len(updates)), nil
	}
}

func nodeIDs(updates []Update) []uint16 {
	ids := make([]uint16, len(updates))
	for index, update := range updates {
		ids[index] = update.Member.NodeID
	}
	return ids
}
