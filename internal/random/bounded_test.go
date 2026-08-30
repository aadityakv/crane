package random

import "testing"

type scriptedSource struct {
	values []uint64
	index  int
}

func (s *scriptedSource) Uint64() uint64 {
	value := s.values[s.index]
	s.index++
	return value
}

func TestUint64nRejectsZeroBound(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Uint64n did not panic for zero bound")
		}
	}()
	Uint64n(&scriptedSource{values: []uint64{0}}, 0)
}

func TestUint64nSupportsMaximumUint64BoundWithoutIntConversion(t *testing.T) {
	source := &scriptedSource{values: []uint64{^uint64(0) - 1}}
	if got, want := Uint64n(source, ^uint64(0)), ^uint64(0)-1; got != want {
		t.Fatalf("Uint64n(max) = %d, want %d", got, want)
	}
}

func TestUint64nUsesRejectionSamplingForNonPowerOfTwoBounds(t *testing.T) {
	// For n=10, values 0..5 are discarded after the threshold translation;
	// returning 6 would reveal modulo-biased sampling instead of retrying.
	source := &scriptedSource{values: []uint64{5, 16}}
	if got, want := Uint64n(source, 10), uint64(6); got != want {
		t.Fatalf("Uint64n() = %d, want %d", got, want)
	}
	if source.index != 2 {
		t.Fatalf("Uint64n consumed %d values, want rejection then retry", source.index)
	}
}

func TestUint64nUsesScriptedSourceForPowerOfTwoBound(t *testing.T) {
	source := &scriptedSource{values: []uint64{13}}
	if got, want := Uint64n(source, 8), uint64(5); got != want {
		t.Fatalf("Uint64n() = %d, want %d", got, want)
	}
}
