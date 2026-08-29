package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDurationMarshalJSONUsesStrictDurationString(t *testing.T) {
	encoded, err := json.Marshal(Duration(1500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"1.5s"` {
		t.Fatalf("Marshal duration = %s, want %q", encoded, `"1.5s"`)
	}
	var decoded Duration
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal marshaled duration: %v", err)
	}
	if decoded != Duration(1500*time.Millisecond) {
		t.Fatalf("round-trip duration = %v", decoded)
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Duration
		ok    bool
	}{
		{name: "positive duration string", input: `"300ms"`, want: 300 * time.Millisecond, ok: true},
		{name: "zero duration", input: `"0s"`},
		{name: "negative duration", input: `"-1s"`},
		{name: "numeric duration", input: `1000000000`},
		{name: "invalid duration string", input: `"soon"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var duration Duration
			err := json.Unmarshal([]byte(tt.input), &duration)
			if tt.ok {
				if err != nil {
					t.Fatalf("Unmarshal(%s): %v", tt.input, err)
				}
				if time.Duration(duration) != tt.want {
					t.Fatalf("duration = %v, want %v", duration, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("Unmarshal(%s) succeeded", tt.input)
			}
		})
	}
}
