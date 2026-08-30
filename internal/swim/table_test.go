package swim

import (
	"reflect"
	"testing"
)

func TestTableMergeUsesIncarnationThenSeverity(t *testing.T) {
	table := NewTable()
	alive := Member{NodeID: 2, Host: "node2", BasePort: 8000, Incarnation: 4, Status: Alive}
	mustMerge(t, table, Update{Member: alive, ReporterID: 2})
	mustMerge(t, table, Update{Member: withStatus(alive, Suspect), ReporterID: 1})
	if got := table.MustGet(2).Status; got != Suspect {
		t.Fatalf("status = %v, want %v", got, Suspect)
	}

	changed, event := table.Merge(Update{
		Member:     Member{NodeID: 2, Host: "stale", BasePort: 9000, Incarnation: 3, Status: Dead},
		ReporterID: 3,
	})
	if changed {
		t.Fatal("stale higher-severity update replaced newer incarnation")
	}
	if event != (MembershipEvent{}) {
		t.Fatalf("stale update event = %#v, want zero value", event)
	}
	wantAfterStale := withStatus(alive, Suspect)
	if got := mustGet(t, table, 2); got != wantAfterStale {
		t.Fatalf("member after stale update = %#v, want %#v", got, wantAfterStale)
	}

	newer := Member{NodeID: 2, Host: "replacement", BasePort: 8100, Incarnation: 5, Status: Alive}
	mustMerge(t, table, Update{Member: newer, ReporterID: 2})
	if got := mustGet(t, table, 2); got != newer {
		t.Fatalf("member = %#v, want %#v", got, newer)
	}
}

func TestTableMergeRejectsZeroReporterWithoutMutation(t *testing.T) {
	table := NewTable()
	original := Member{NodeID: 6, Host: "node6", BasePort: 8006, Incarnation: 3, Status: Alive}
	mustMerge(t, table, Update{Member: original, ReporterID: 6})

	changed, event := table.Merge(Update{
		Member:     Member{NodeID: 6, Host: "replacement", BasePort: 9006, Incarnation: 4, Status: Dead},
		ReporterID: 0,
	})
	if changed {
		t.Fatal("zero-reporter update changed the table")
	}
	if event != (MembershipEvent{}) {
		t.Fatalf("zero-reporter event = %#v, want zero value", event)
	}
	if got := mustGet(t, table, 6); got != original {
		t.Fatalf("member after zero-reporter update = %#v, want %#v", got, original)
	}
}

func TestTableMergeEqualIncarnationUsesExactStatusPrecedence(t *testing.T) {
	statuses := []Status{Alive, Suspect, Dead, Left}
	for index, status := range statuses {
		table := NewTable()
		base := Member{NodeID: 7, Host: "node7", BasePort: 8007, Incarnation: 9, Status: status}
		mustMerge(t, table, Update{Member: base, ReporterID: 7})

		for lower := 0; lower < index; lower++ {
			changed, _ := table.Merge(Update{Member: withStatus(base, statuses[lower]), ReporterID: 3})
			if changed {
				t.Fatalf("%v replaced by lower-severity %v", status, statuses[lower])
			}
		}
		if index+1 < len(statuses) {
			higher := withStatus(base, statuses[index+1])
			mustMerge(t, table, Update{Member: higher, ReporterID: 4})
			if got := mustGet(t, table, 7).Status; got != higher.Status {
				t.Fatalf("status = %v, want %v", got, higher.Status)
			}
		}
	}
}

func TestTableMergeDoesNotChangeAddressAtEqualIncarnation(t *testing.T) {
	table := NewTable()
	base := Member{NodeID: 3, Host: "original", BasePort: 8003, Incarnation: 12, Status: Alive}
	mustMerge(t, table, Update{Member: base, ReporterID: 3})

	changed, event := table.Merge(Update{
		Member:     Member{NodeID: 3, Host: "forged", BasePort: 9003, Incarnation: 12, Status: Suspect},
		ReporterID: 4,
	})
	if !changed {
		t.Fatal("higher-severity status was not merged")
	}
	want := withStatus(base, Suspect)
	if got := mustGet(t, table, 3); got != want {
		t.Fatalf("member = %#v, want %#v", got, want)
	}
	if event.Current != want {
		t.Fatalf("event current = %#v, want %#v", event.Current, want)
	}
}

func TestTableMergeEventCarriesReporterAndCopiedTransition(t *testing.T) {
	table := NewTable()
	member := Member{NodeID: 8, Host: "node8", BasePort: 8008, Incarnation: 2, Status: Alive}
	changed, event := table.Merge(Update{Member: member, ReporterID: 42})
	if !changed {
		t.Fatal("initial member was not merged")
	}
	if event.Previous != (Member{}) || event.Current != member {
		t.Fatalf("event transition = %#v -> %#v", event.Previous, event.Current)
	}
	if event.Cause != EventMemberChanged {
		t.Fatalf("event cause = %v, want %v", event.Cause, EventMemberChanged)
	}
	if event.ReporterID != 42 {
		t.Fatalf("reporter = %d, want 42", event.ReporterID)
	}

	event.Current.Host = "mutated"
	if got := mustGet(t, table, 8).Host; got != "node8" {
		t.Fatalf("event mutation changed table host to %q", got)
	}

	changed, event = table.Merge(Update{Member: member, ReporterID: 99})
	if changed || event != (MembershipEvent{}) {
		t.Fatalf("reporter-only duplicate changed table: changed=%v event=%#v", changed, event)
	}
}

func TestTableMergeRejectsInvalidIdentityAndStatus(t *testing.T) {
	table := NewTable()
	for _, update := range []Update{
		{Member: Member{NodeID: 0, Status: Alive}, ReporterID: 1},
		{Member: Member{NodeID: 1, Status: Status(255)}, ReporterID: 1},
	} {
		changed, event := table.Merge(update)
		if changed || event != (MembershipEvent{}) {
			t.Fatalf("invalid update merged: %#v", update)
		}
	}
	if got := table.Snapshot(); len(got) != 0 {
		t.Fatalf("snapshot = %#v, want empty", got)
	}
}

func TestTableSnapshotIsSortedAndIsolated(t *testing.T) {
	table := NewTable()
	for _, nodeID := range []uint16{9, 2, 5} {
		mustMerge(t, table, Update{
			Member:     Member{NodeID: nodeID, Host: "node", BasePort: 8000 + nodeID, Incarnation: 1, Status: Alive},
			ReporterID: nodeID,
		})
	}

	snapshot := table.Snapshot()
	wantIDs := []uint16{2, 5, 9}
	gotIDs := []uint16{snapshot[0].NodeID, snapshot[1].NodeID, snapshot[2].NodeID}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("snapshot node IDs = %v, want %v", gotIDs, wantIDs)
	}

	snapshot[0].Host = "mutated"
	fresh := table.Snapshot()
	if len(fresh) != 3 || fresh[0].Host != "node" {
		t.Fatalf("snapshot mutation leaked into table: %#v", fresh)
	}

	member, ok := table.Get(2)
	if !ok {
		t.Fatal("member 2 missing")
	}
	member.Host = "also-mutated"
	if got := mustGet(t, table, 2).Host; got != "node" {
		t.Fatalf("Get mutation changed table host to %q", got)
	}
	if _, ok := table.Get(0); ok {
		t.Fatal("Get reported zero node ID present")
	}
}

func TestTableTerminalFloorSurvivesVisibleExpiryAndRejectsOldAlive(t *testing.T) {
	table := NewTable()
	terminal := Member{NodeID: 12, Host: "node12", BasePort: 8012, Incarnation: 9, Status: Dead}
	mustMerge(t, table, Update{Member: terminal, ReporterID: 3})

	if !table.ExpireTerminal(terminal) {
		t.Fatal("ExpireTerminal did not remove the exact visible tombstone")
	}
	if _, exists := table.Get(terminal.NodeID); exists {
		t.Fatal("visible tombstone remained after expiry")
	}
	if got := table.IncarnationFloors(); !reflect.DeepEqual(got, []Member{terminal}) {
		t.Fatalf("incarnation floors = %#v, want %#v", got, []Member{terminal})
	}

	staleAlive := terminal
	staleAlive.Status = Alive
	if changed, event := table.Merge(Update{Member: staleAlive, ReporterID: staleAlive.NodeID}); changed || event != (MembershipEvent{}) {
		t.Fatalf("old Alive crossed retained floor: changed=%v event=%#v", changed, event)
	}
	if _, exists := table.Get(terminal.NodeID); exists {
		t.Fatal("old Alive recreated a visible member")
	}

	newerAlive := staleAlive
	newerAlive.Incarnation++
	mustMerge(t, table, Update{Member: newerAlive, ReporterID: newerAlive.NodeID})
	if got := table.MustGet(newerAlive.NodeID); got != newerAlive {
		t.Fatalf("newer Alive = %#v, want %#v", got, newerAlive)
	}
}

func TestTableMergeIncarnationFloorRepairsStaleVisibleMember(t *testing.T) {
	table := NewTable()
	stale := Member{NodeID: 13, Host: "node13", BasePort: 8013, Incarnation: 4, Status: Alive}
	mustMerge(t, table, Update{Member: stale, ReporterID: stale.NodeID})
	floor := stale
	floor.Incarnation = 6
	floor.Status = Left

	changed, event := table.MergeIncarnationFloor(floor, 2)
	if !changed {
		t.Fatal("newer snapshot floor was not retained")
	}
	if event.Previous != stale || event.Current != floor {
		t.Fatalf("floor repair transition = %#v -> %#v, want %#v -> %#v", event.Previous, event.Current, stale, floor)
	}
	if got := table.MustGet(floor.NodeID); got != floor {
		t.Fatalf("visible repaired member = %#v, want %#v", got, floor)
	}

	if !table.ExpireTerminal(floor) {
		t.Fatal("repaired terminal record did not expire")
	}
	copyOfFloors := table.IncarnationFloors()
	copyOfFloors[0].Incarnation = 1
	if got := table.IncarnationFloors(); len(got) != 1 || got[0] != floor {
		t.Fatalf("floor snapshot mutation leaked into table: %#v", got)
	}
}

func withStatus(member Member, status Status) Member {
	member.Status = status
	return member
}

func mustMerge(t *testing.T, table *Table, update Update) MembershipEvent {
	t.Helper()
	changed, event := table.Merge(update)
	if !changed {
		t.Fatalf("update was not merged: %#v", update)
	}
	return event
}

func mustGet(t *testing.T, table *Table, nodeID uint16) Member {
	t.Helper()
	member, ok := table.Get(nodeID)
	if !ok {
		t.Fatalf("member %d missing", nodeID)
	}
	return member
}
