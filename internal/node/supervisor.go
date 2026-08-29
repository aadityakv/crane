package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Supervisor starts related services, waits for their readiness, and stops
// every service when its parent context is canceled or a service fails.
type Supervisor struct {
	services []Service
}

type serviceState struct {
	service Service
	ready   <-chan struct{}
}

type completion struct {
	index      int
	err        error
	ready      bool
	contextErr error
}

// NewSupervisor constructs a Supervisor for services. It does not start any
// services; Run validates service names before it starts them.
func NewSupervisor(services ...Service) *Supervisor {
	return &Supervisor{services: append([]Service(nil), services...)}
}

// Run starts every service and waits until each reports readiness. It cancels
// and joins all services after parent-context cancellation or the first
// service failure. Returning before readiness is a startup failure.
func (s *Supervisor) Run(ctx context.Context) error {
	if err := validateServices(s.services); err != nil {
		return err
	}
	if len(s.services) == 0 {
		return nil
	}

	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	states := make([]serviceState, len(s.services))
	for index, service := range s.services {
		states[index] = serviceState{service: service, ready: service.Ready()}
	}

	readyEvents := make(chan int, len(states))
	completions := make(chan completion, len(states))
	var readinessWG sync.WaitGroup
	for index, state := range states {
		readinessWG.Add(1)
		go func() {
			defer readinessWG.Done()
			select {
			case <-state.ready:
				readyEvents <- index
			case <-serviceCtx.Done():
			}
		}()
		go func() {
			err := state.service.Run(serviceCtx)
			completions <- completion{
				index:      index,
				err:        err,
				ready:      channelClosed(state.ready),
				contextErr: ctx.Err(),
			}
		}()
	}

	ready := make([]bool, len(states))
	readyCount := 0
	completed := make([]completion, len(states))
	completedCount := 0
	shutdown := false
	markReady := func(index int) {
		if !ready[index] {
			ready[index] = true
			readyCount++
		}
	}
	record := func(result completion) {
		completed[result.index] = result
		completedCount++
		if result.ready {
			markReady(result.index)
		}
	}

	for !shutdown && readyCount < len(states) {
		select {
		case index := <-readyEvents:
			markReady(index)
		case result := <-completions:
			record(result)
			shutdown = true
		case <-ctx.Done():
			shutdown = true
		}
	}

	for !shutdown {
		select {
		case result := <-completions:
			record(result)
			shutdown = true
		case <-ctx.Done():
			shutdown = true
		}
	}

	cancel()
	readinessWG.Wait()
	for completedCount < len(states) {
		record(<-completions)
	}
	return selectFailure(states, completed)
}

func validateServices(services []Service) error {
	names := make(map[string]struct{}, len(services))
	for index, service := range services {
		if service == nil {
			return fmt.Errorf("service at index %d is nil", index)
		}
		name := service.Name()
		if _, exists := names[name]; exists {
			return fmt.Errorf("duplicate service name %q", name)
		}
		names[name] = struct{}{}
	}
	return nil
}

func startupFailure(name string, err error) error {
	if err == nil {
		return fmt.Errorf("service %q exited before reporting ready", name)
	}
	return fmt.Errorf("service %q failed before reporting ready: %w", name, err)
}

func selectFailure(states []serviceState, completed []completion) error {
	for index, state := range states {
		result := completed[index]
		if isCancellation(result.err, result.contextErr) {
			continue
		}
		if !result.ready {
			return startupFailure(state.service.Name(), result.err)
		}
		return runningFailure(state.service.Name(), result.err)
	}
	return nil
}

func runningFailure(name string, err error) error {
	if err == nil {
		return fmt.Errorf("service %q exited unexpectedly", name)
	}
	return fmt.Errorf("service %q failed: %w", name, err)
}

func isCancellation(err, contextErr error) bool {
	if err == nil && contextErr != nil {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) && errors.Is(contextErr, context.DeadlineExceeded)
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
