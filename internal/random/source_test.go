package random

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

func TestLockedSourceShuffleIsDeterministic(t *testing.T) {
	left := []int{0, 1, 2, 3, 4, 5, 6, 7}
	right := append([]int(nil), left...)
	first := NewLockedSource(42)
	second := NewLockedSource(42)
	first.Shuffle(len(left), func(i, j int) { left[i], left[j] = left[j], left[i] })
	second.Shuffle(len(right), func(i, j int) { right[i], right[j] = right[j], right[i] })
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("same seed produced different shuffles: %v and %v", left, right)
	}
}

func TestLockedSourceMatchesSeededRand(t *testing.T) {
	got := NewLockedSource(7)
	want := rand.New(rand.NewSource(7))
	for i := 0; i < 10; i++ {
		if actual, expected := got.Uint64(), want.Uint64(); actual != expected {
			t.Fatalf("Uint64 #%d = %d, want %d", i, actual, expected)
		}
		if actual, expected := got.Intn(100), want.Intn(100); actual != expected {
			t.Fatalf("Intn #%d = %d, want %d", i, actual, expected)
		}
	}
}

func TestLockedSourceConcurrentUse(t *testing.T) {
	source := NewLockedSource(42)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				source.Uint64()
				source.Intn(100)
				source.Shuffle(4, func(i, j int) {})
			}
		}()
	}
	wg.Wait()
}
