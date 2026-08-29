// Package testutil contains bounded helpers shared by integration and process tests.
package testutil

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WaitFor checks condition immediately and then at interval until it matches or ctx ends.
// Condition errors are treated as transient diagnostics and the latest is retained on timeout.
func WaitFor(ctx context.Context, interval time.Duration, condition func() (bool, error)) error {
	if ctx == nil {
		return errors.New("wait context is nil")
	}
	if interval <= 0 {
		return fmt.Errorf("wait interval must be greater than zero")
	}
	if condition == nil {
		return errors.New("wait condition is nil")
	}

	matched, lastError := condition()
	if matched {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if lastError != nil {
				return errors.Join(ctx.Err(), fmt.Errorf("last condition error: %w", lastError))
			}
			return ctx.Err()
		case <-ticker.C:
			var err error
			matched, err = condition()
			if matched {
				return nil
			}
			if err != nil {
				lastError = err
			}
		}
	}
}
