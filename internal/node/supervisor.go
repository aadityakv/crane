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

	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type serviceState struct {
		service Service
		ready   <-chan struct{}
	}
	type completion struct {
		index int
		err   error
	}

	states := make([]serviceState, len(s.services))
	for index, service := range s.services {
		states[index] = serviceState{service: service, ready: service.Ready()}
	}

	readyEvents := make(chan int, len(states))
	completions := make(chan completion, len(states))
	var readinessWG sync.WaitGroup
	for index, state := range states {
		readinessWG.Go(func() {
			select {
			case <-state.ready:
				readyEvents <- index
			case <-serviceCtx.Done():
			}
		})
		go func() {
			completions <- completion{index: index, err: state.service.Run(serviceCtx)}
		}()
	}

	ready := make([]bool, len(states))
	readyCount := 0
	completedCount := 0
	markReady := func(index int) {
		if !ready[index] {
			ready[index] = true
			readyCount++
		}
	}
	readyNow := func(index int) bool {
		select {
		case <-states[index].ready:
			markReady(index)
			return true
		default:
			return false
		}
	}
	join := func(cause error) error {
		cancel()
		readinessWG.Wait()
		for completedCount < len(states) {
			<-completions
			completedCount++
		}
		return cause
	}

	for readyCount < len(states) {
		select {
		case index := <-readyEvents:
			markReady(index)
		case result := <-completions:
			completedCount++
			if readyNow(result.index) {
				if err := runningFailure(states[result.index].service.Name(), result.err, serviceCtx.Err()); err != nil {
					return join(err)
				}
				return join(nil)
			}
			if serviceCtx.Err() != nil {
				return join(nil)
			}
			return join(startupFailure(states[result.index].service.Name(), result.err))
		case <-ctx.Done():
			return join(nil)
		}
	}

	readinessWG.Wait()
	for completedCount < len(states) {
		select {
		case result := <-completions:
			completedCount++
			if err := runningFailure(states[result.index].service.Name(), result.err, serviceCtx.Err()); err != nil {
				return join(err)
			}
			return join(nil)
		case <-ctx.Done():
			return join(nil)
		}
	}

	return join(nil)
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

func runningFailure(name string, err, contextErr error) error {
	if contextErr != nil && isCancellation(err) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("service %q exited unexpectedly", name)
	}
	if isCancellation(err) {
		return fmt.Errorf("service %q exited unexpectedly: %w", name, err)
	}
	return fmt.Errorf("service %q failed: %w", name, err)
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
