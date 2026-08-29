package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a positive configuration duration decoded from a JSON string.
type Duration time.Duration

// UnmarshalJSON decodes a positive duration string accepted by time.ParseDuration.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("duration must be a JSON string: %w", err)
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}
	if parsed <= 0 {
		return fmt.Errorf("duration must be greater than zero")
	}
	*d = Duration(parsed)
	return nil
}
