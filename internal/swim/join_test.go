package swim

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestPrepareJoinUsesPersistedAndSnapshotMaximum(t *testing.T) {
	store := &recordingIncarnationStore{loaded: 6}
	self := Member{NodeID: 2, Host: "new", BasePort: 8200, Status: Dead}
	snapshot := []Member{
		{NodeID: 1, Host: "seed", BasePort: 8100, Incarnation: 9, Status: Alive},
		{NodeID: 2, Host: "old", BasePort: 8101, Incarnation: 4, Status: Dead},
	}
	wantSnapshot := append([]Member(nil), snapshot...)

	got, err := PrepareJoin(store, snapshot, self)
	if err != nil {
		t.Fatalf("PrepareJoin() error = %v", err)
	}
	want := self
	want.Incarnation = 7
	want.Status = Alive
	if got != want {
		t.Fatalf("PrepareJoin() = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(store.stored, []uint64{7}) {
		t.Fatalf("stored incarnations = %v, want [7]", store.stored)
	}
	if !reflect.DeepEqual(snapshot, wantSnapshot) {
		t.Fatalf("PrepareJoin mutated snapshot: got %#v, want %#v", snapshot, wantSnapshot)
	}
	if self.Incarnation != 0 || self.Status != Dead {
		t.Fatalf("PrepareJoin mutated self input: %#v", self)
	}
}

func TestPrepareJoinUsesHighestMatchingSnapshotRecord(t *testing.T) {
	store := &recordingIncarnationStore{}
	self := Member{NodeID: 2, Host: "node2", BasePort: 8200}
	snapshot := []Member{
		{NodeID: 2, Host: "old-a", BasePort: 8100, Incarnation: 4, Status: Dead},
		{NodeID: 2, Host: "old-b", BasePort: 8101, Incarnation: 9, Status: Left},
	}

	got, err := PrepareJoin(store, snapshot, self)
	if err != nil {
		t.Fatalf("PrepareJoin() error = %v", err)
	}
	if got.Incarnation != 10 {
		t.Fatalf("joining incarnation = %d, want 10", got.Incarnation)
	}
}

func TestPrepareJoinUsesRetainedIncarnationFloor(t *testing.T) {
	store := &recordingIncarnationStore{loaded: 3}
	self := Member{NodeID: 7, Host: "node7.example", BasePort: 8700}
	floors := []Member{{NodeID: 7, Host: "old-node7.example", BasePort: 8600, Incarnation: 11, Status: Left}}

	got, err := PrepareJoinWithFloors(store, nil, floors, self)
	if err != nil {
		t.Fatalf("PrepareJoinWithFloors() error = %v", err)
	}
	if got.Incarnation != 12 || got.Status != Alive {
		t.Fatalf("prepared member = %#v, want Alive incarnation 12", got)
	}
	if !reflect.DeepEqual(store.stored, []uint64{12}) {
		t.Fatalf("stored incarnations = %v, want [12]", store.stored)
	}
}

func TestPrepareJoinRequiresRecoverableIdentityState(t *testing.T) {
	store := &recordingIncarnationStore{}
	self := Member{NodeID: 2, Host: "node2", BasePort: 8200}
	snapshot := []Member{{NodeID: 1, Host: "seed", BasePort: 8100, Incarnation: 3, Status: Alive}}

	got, err := PrepareJoin(store, snapshot, self)
	if !errors.Is(err, ErrIncarnationRecoveryRequired) {
		t.Fatalf("PrepareJoin() error = %v, want ErrIncarnationRecoveryRequired", err)
	}
	if got != (Member{}) {
		t.Fatalf("PrepareJoin() member = %#v, want zero", got)
	}
	if len(store.stored) != 0 {
		t.Fatalf("stored incarnations = %v, want none", store.stored)
	}
}

func TestPrepareJoinRejectsIncarnationOverflow(t *testing.T) {
	store := &recordingIncarnationStore{loaded: math.MaxUint64}
	self := Member{NodeID: 2, Host: "node2", BasePort: 8200}

	got, err := PrepareJoin(store, nil, self)
	if !errors.Is(err, ErrIncarnationOverflow) {
		t.Fatalf("PrepareJoin() error = %v, want ErrIncarnationOverflow", err)
	}
	if got != (Member{}) {
		t.Fatalf("PrepareJoin() member = %#v, want zero", got)
	}
	if len(store.stored) != 0 {
		t.Fatalf("stored incarnations = %v, want none", store.stored)
	}
}

func TestPrepareJoinPersistsBeforeReturningAlive(t *testing.T) {
	store := newBlockingIncarnationStore(6)
	self := Member{NodeID: 2, Host: "node2", BasePort: 8200}
	type result struct {
		member Member
		err    error
	}
	resultReady := make(chan result, 1)
	go func() {
		member, err := PrepareJoin(store, nil, self)
		resultReady <- result{member: member, err: err}
	}()

	if got := <-store.entered; got != 7 {
		t.Fatalf("Store() value = %d, want 7", got)
	}
	select {
	case got := <-resultReady:
		t.Fatalf("PrepareJoin returned before Store completed: %#v", got)
	default:
	}
	close(store.release)
	got := <-resultReady
	if got.err != nil {
		t.Fatalf("PrepareJoin() error = %v", got.err)
	}
	if got.member.Status != Alive || got.member.Incarnation != 7 {
		t.Fatalf("PrepareJoin() member = %#v, want Alive incarnation 7", got.member)
	}
}

func TestPrepareJoinDoesNotReturnAliveWhenPersistenceFails(t *testing.T) {
	persistFailure := errors.New("injected persistence failure")
	store := &recordingIncarnationStore{loaded: 6, storeErr: persistFailure}
	self := Member{NodeID: 2, Host: "node2", BasePort: 8200}

	got, err := PrepareJoin(store, nil, self)
	if !errors.Is(err, persistFailure) {
		t.Fatalf("PrepareJoin() error = %v, want persistence failure", err)
	}
	if got != (Member{}) {
		t.Fatalf("PrepareJoin() member = %#v, want zero", got)
	}
}

func TestValidateJoinRejectsConcurrentIdentity(t *testing.T) {
	table := NewTable()
	mustMerge(t, table, Update{Member: Member{NodeID: 2, Host: "old", BasePort: 8100, Incarnation: 4, Status: Alive}, ReporterID: 2})
	announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "new", BasePort: 8200, Incarnation: 5, Status: Alive}}
	if err := ValidateJoinAnnouncement(table, announce); !errors.Is(err, ErrDuplicateNodeID) {
		t.Fatalf("join error = %v, want ErrDuplicateNodeID", err)
	}
}

func TestValidateJoinRejectsSuspectIdentity(t *testing.T) {
	table := NewTable()
	mustMerge(t, table, Update{Member: Member{NodeID: 2, Host: "old", BasePort: 8100, Incarnation: 4, Status: Suspect}, ReporterID: 1})
	announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "new", BasePort: 8200, Incarnation: 5, Status: Alive}}

	if err := ValidateJoinAnnouncement(table, announce); !errors.Is(err, ErrDuplicateNodeID) {
		t.Fatalf("join error = %v, want ErrDuplicateNodeID", err)
	}
}

func TestValidateJoinRejectsInvalidAnnouncement(t *testing.T) {
	valid := Member{NodeID: 2, Host: "node-2.example.test", BasePort: 8200, Incarnation: 5, Status: Alive}
	tests := []struct {
		name   string
		mutate func(*Member)
	}{
		{name: "zero_node_id", mutate: func(member *Member) { member.NodeID = 0 }},
		{name: "zero_incarnation", mutate: func(member *Member) { member.Incarnation = 0 }},
		{name: "suspect_status", mutate: func(member *Member) { member.Status = Suspect }},
		{name: "dead_status", mutate: func(member *Member) { member.Status = Dead }},
		{name: "left_status", mutate: func(member *Member) { member.Status = Left }},
		{name: "unknown_status", mutate: func(member *Member) { member.Status = Status(255) }},
		{name: "empty_host", mutate: func(member *Member) { member.Host = "" }},
		{name: "malformed_host", mutate: func(member *Member) { member.Host = "bad host" }},
		{name: "host_with_port", mutate: func(member *Member) { member.Host = "node2:9000" }},
		{name: "wildcard_ipv4", mutate: func(member *Member) { member.Host = "0.0.0.0" }},
		{name: "wildcard_ipv6", mutate: func(member *Member) { member.Host = "::" }},
		{name: "zero_base_port", mutate: func(member *Member) { member.BasePort = 0 }},
		{name: "base_port_service_overflow", mutate: func(member *Member) { member.BasePort = 65528 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			member := valid
			tt.mutate(&member)
			if err := ValidateJoinAnnouncement(NewTable(), JoinAnnounce{Member: member}); !errors.Is(err, ErrInvalidJoinAnnouncement) {
				t.Fatalf("join error = %v, want ErrInvalidJoinAnnouncement", err)
			}
		})
	}
}

func TestValidateJoinAcceptsValidDNSAndIPv6Endpoints(t *testing.T) {
	for _, host := range []string{"node-2.example.test", "node-2.example.test.", "2001:db8::2"} {
		announce := JoinAnnounce{Member: Member{NodeID: 2, Host: host, BasePort: 65527, Incarnation: 5, Status: Alive}}
		if err := ValidateJoinAnnouncement(NewTable(), announce); err != nil {
			t.Fatalf("ValidateJoinAnnouncement(host %q) error = %v", host, err)
		}
	}
}

func TestValidateJoinRequiresHigherGenerationThanTerminalRecord(t *testing.T) {
	for _, status := range []Status{Dead, Left} {
		for _, incarnation := range []uint64{3, 4} {
			t.Run(joinValidationCaseName(status, incarnation), func(t *testing.T) {
				table := NewTable()
				mustMerge(t, table, Update{
					Member:     Member{NodeID: 2, Host: "old", BasePort: 8100, Incarnation: 4, Status: status},
					ReporterID: 1,
				})
				announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "new", BasePort: 8200, Incarnation: incarnation, Status: Alive}}

				if err := ValidateJoinAnnouncement(table, announce); !errors.Is(err, ErrStaleJoinIncarnation) {
					t.Fatalf("join error = %v, want ErrStaleJoinIncarnation", err)
				}
			})
		}
	}
}

func TestValidateJoinAcceptsHigherGenerationThanTerminalRecordWithoutMutation(t *testing.T) {
	table := NewTable()
	terminal := Member{NodeID: 2, Host: "old", BasePort: 8100, Incarnation: 4, Status: Dead}
	mustMerge(t, table, Update{Member: terminal, ReporterID: 1})
	announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "new", BasePort: 8200, Incarnation: 5, Status: Alive}}

	if err := ValidateJoinAnnouncement(table, announce); err != nil {
		t.Fatalf("join error = %v", err)
	}
	if got := table.MustGet(2); got != terminal {
		t.Fatalf("validation mutated table: got %#v, want %#v", got, terminal)
	}
}

func TestValidateJoinRequiresHigherGenerationThanExpiredTerminalFloor(t *testing.T) {
	table := NewTable()
	terminal := Member{NodeID: 2, Host: "old", BasePort: 8100, Incarnation: 8, Status: Dead}
	mustMerge(t, table, Update{Member: terminal, ReporterID: 1})
	if !table.ExpireTerminal(terminal) {
		t.Fatal("terminal record did not expire")
	}
	if _, exists := table.Get(terminal.NodeID); exists {
		t.Fatal("expired terminal record remained visible")
	}

	announce := JoinAnnounce{Member: Member{NodeID: 2, Host: "new", BasePort: 8200, Incarnation: 8, Status: Alive}}
	if err := ValidateJoinAnnouncement(table, announce); !errors.Is(err, ErrStaleJoinIncarnation) {
		t.Fatalf("join error = %v, want ErrStaleJoinIncarnation", err)
	}
}

func TestJoinMessagesRoundTripThroughGob(t *testing.T) {
	member := Member{NodeID: 2, Host: "2001:db8::2", BasePort: 8200, Incarnation: 5, Status: Alive}
	floor := Member{NodeID: 3, Host: "2001:db8::3", BasePort: 8300, Incarnation: 8, Status: Left}
	tests := []struct {
		name  string
		value any
		fresh func() any
	}{
		{name: "request", value: JoinRequest{NodeID: 2}, fresh: func() any { return new(JoinRequest) }},
		{name: "snapshot", value: JoinSnapshot{Members: []Member{member}, Floors: []Member{floor}}, fresh: func() any { return new(JoinSnapshot) }},
		{name: "announce", value: JoinAnnounce{Member: member}, fresh: func() any { return new(JoinAnnounce) }},
		{name: "accepted", value: JoinAccepted{Member: member}, fresh: func() any { return new(JoinAccepted) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var encoded bytes.Buffer
			if err := gob.NewEncoder(&encoded).Encode(tt.value); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			decoded := tt.fresh()
			if err := gob.NewDecoder(&encoded).Decode(decoded); err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			want := reflect.ValueOf(tt.value)
			got := reflect.ValueOf(decoded).Elem()
			if !reflect.DeepEqual(got.Interface(), want.Interface()) {
				t.Fatalf("round trip = %#v, want %#v", got.Interface(), want.Interface())
			}
		})
	}
}

type recordingIncarnationStore struct {
	loaded   uint64
	loads    int
	loadErr  error
	storeErr error
	stored   []uint64
}

func (s *recordingIncarnationStore) Load() (uint64, error) {
	s.loads++
	return s.loaded, s.loadErr
}

func (s *recordingIncarnationStore) Store(value uint64) error {
	s.stored = append(s.stored, value)
	return s.storeErr
}

type blockingIncarnationStore struct {
	loaded  uint64
	entered chan uint64
	release chan struct{}
	once    sync.Once
}

func newBlockingIncarnationStore(loaded uint64) *blockingIncarnationStore {
	return &blockingIncarnationStore{
		loaded:  loaded,
		entered: make(chan uint64, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingIncarnationStore) Load() (uint64, error) {
	return s.loaded, nil
}

func (s *blockingIncarnationStore) Store(value uint64) error {
	s.once.Do(func() { s.entered <- value })
	<-s.release
	return nil
}

func joinValidationCaseName(status Status, incarnation uint64) string {
	statusName := map[Status]string{Dead: "dead", Left: "left"}[status]
	incarnationName := map[uint64]string{3: "lower", 4: "equal"}[incarnation]
	return statusName + "_" + incarnationName
}
