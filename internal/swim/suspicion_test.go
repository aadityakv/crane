package swim

import (
	"testing"
	"time"
)

func TestSuspicionDurationUsesExactCeilingLogarithmAndFiveSecondFloor(t *testing.T) {
	tests := []struct {
		name          string
		multiplier    int
		probeInterval time.Duration
		aliveMembers  int
		want          time.Duration
	}{
		{name: "floor for one member", multiplier: 1, probeInterval: time.Second, aliveMembers: 1, want: 5 * time.Second},
		{name: "exact power of two", multiplier: 5, probeInterval: time.Second, aliveMembers: 3, want: 10 * time.Second},
		{name: "ceiling above power of two", multiplier: 5, probeInterval: time.Second, aliveMembers: 4, want: 15 * time.Second},
		{name: "subsecond interval", multiplier: 4, probeInterval: 750 * time.Millisecond, aliveMembers: 8, want: 12 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SuspicionDuration(test.multiplier, test.probeInterval, test.aliveMembers); got != test.want {
				t.Fatalf("SuspicionDuration(%d, %s, %d) = %s, want %s", test.multiplier, test.probeInterval, test.aliveMembers, got, test.want)
			}
		})
	}
}
