package swim

import (
	"reflect"
	"testing"
)

func TestProbeSelectorVisitsEveryAlivePeerBeforeScriptedReshuffle(t *testing.T) {
	source := &scriptedRandom{
		shuffleSwaps: [][][2]int{
			{{0, 2}},
			{{0, 1}},
		},
	}
	selector := probeSelector{source: source}
	members := []Member{
		{NodeID: 1, Status: Alive},
		{NodeID: 2, Status: Alive},
		{NodeID: 3, Status: Alive},
		{NodeID: 4, Status: Alive},
		{NodeID: 5, Status: Suspect},
		{NodeID: 6, Status: Dead},
		{NodeID: 7, Status: Left},
	}

	var got []uint16
	for range 6 {
		member, ok := selector.next(members, 1)
		if !ok {
			t.Fatal("selector exhausted while alive peers remained")
		}
		got = append(got, member.NodeID)
	}

	want := []uint16{4, 3, 2, 3, 2, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("probe order = %v, want %v", got, want)
	}
	if source.shuffleCalls != 2 {
		t.Fatalf("shuffle calls = %d, want 2", source.shuffleCalls)
	}
}

type scriptedRandom struct {
	uint64s      []uint64
	uint64Index  int
	shuffleSwaps [][][2]int
	shuffleCalls int
}

func (s *scriptedRandom) Uint64() uint64 {
	if s.uint64Index >= len(s.uint64s) {
		return 1
	}
	value := s.uint64s[s.uint64Index]
	s.uint64Index++
	return value
}

func (s *scriptedRandom) Intn(n int) int {
	return 0
}

func (s *scriptedRandom) Shuffle(n int, swap func(i, j int)) {
	if s.shuffleCalls < len(s.shuffleSwaps) {
		for _, indexes := range s.shuffleSwaps[s.shuffleCalls] {
			if indexes[0] >= n || indexes[1] >= n {
				panic("scripted shuffle index out of range")
			}
			swap(indexes[0], indexes[1])
		}
	}
	s.shuffleCalls++
}
