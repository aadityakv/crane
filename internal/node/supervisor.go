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
	services  []Service
	ready     chan struct{}
	readyOnce sync.Once
}

type serviceState struct {
	service Service
	ready   <-chan struct{}
}

type completion struct {
	index                 int
	err                   error
	ready                 bool
	cancellationInitiated bool
	parentCause           error
}

var errSupervisorShutdown = errors.New("supervisor initiated shutdown")

type supervisorCausality struct {
	mu                       sync.Mutex
	readiness                []<-chan struct{}
	cancellationInitiated    bool
	completionBeforeAllReady bool
}

func newSupervisorCausality(readiness ...<-chan struct{}) *supervisorCausality {
	return &supervisorCausality{readiness: append([]<-chan struct{}(nil), readiness...)}
}

func (c *supervisorCausality) linearizeCompletion(index int, err error, parent context.Context) completion {
	c.mu.Lock()
	defer c.mu.Unlock()
	serviceReady, allReady := c.snapshotReadyLocked(index)
	if !allReady {
		c.completionBeforeAllReady = true
	}
	return completion{
		index:                 index,
		err:                   err,
		ready:                 serviceReady,
		cancellationInitiated: c.cancellationInitiated,
		parentCause:           context.Cause(parent),
	}
}

func (c *supervisorCausality) publishReady(parent context.Context, publish func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.completionBeforeAllReady || c.cancellationInitiated || context.Cause(parent) != nil || !c.allReadyLocked() {
		return false
	}
	publish()
	return true
}

func (c *supervisorCausality) snapshotReadyLocked(completingIndex int) (bool, bool) {
	allReady := len(c.readiness) > 0
	completingReady := false
	for index, ready := range c.readiness {
		closed := channelClosed(ready)
		if index == completingIndex {
			completingReady = closed
		}
		allReady = allReady && closed
	}
	return completingReady, allReady
}

func (c *supervisorCausality) allReadyLocked() bool {
	_, allReady := c.snapshotReadyLocked(-1)
	return allReady
}

func (c *supervisorCausality) initiateCancellation(cancel context.CancelCauseFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancellationInitiated = true
	cancel(errSupervisorShutdown)
}

// NewSupervisor constructs a Supervisor for services. It does not start any
// services; Run validates service names before it starts them.
func NewSupervisor(services ...Service) *Supervisor {
	return &Supervisor{services: append([]Service(nil), services...), ready: make(chan struct{})}
}

// Ready closes exactly once after every declared service has causally reported
// readiness during a successful Run startup.
func (s *Supervisor) Ready() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ready
}

// Run starts every service and waits until each reports readiness. It cancels
// and joins all services after parent-context cancellation or the first
// service failure. Returning before readiness is a startup failure.
func (s *Supervisor) Run(ctx context.Context) error {
	if err := validateServices(s.services); err != nil {
		return err
	}
	if len(s.services) == 0 {
		s.readyOnce.Do(func() { close(s.ready) })
		return nil
	}

	states := make([]serviceState, len(s.services))
	readiness := make([]<-chan struct{}, len(s.services))
	for index, service := range s.services {
		states[index] = serviceState{service: service, ready: service.Ready()}
		readiness[index] = states[index].ready
	}
	serviceCtx, cancel := context.WithCancelCause(ctx)
	causality := newSupervisorCausality(readiness...)

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
			completions <- causality.linearizeCompletion(index, err, ctx)
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
	if !s.publishAggregateReady(ctx, causality) {
		shutdown = true
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

	causality.initiateCancellation(cancel)
	readinessWG.Wait()
	for completedCount < len(states) {
		record(<-completions)
	}
	return selectFailure(states, completed)
}

func (s *Supervisor) publishAggregateReady(ctx context.Context, causality *supervisorCausality) bool {
	return causality.publishReady(ctx, func() { s.readyOnce.Do(func() { close(s.ready) }) })
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
		if isCancellation(result) {
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

func isCancellation(result completion) bool {
	if result.err == nil && (result.cancellationInitiated || result.parentCause != nil) {
		return true
	}
	if errors.Is(result.err, context.Canceled) {
		return true
	}
	return errors.Is(result.err, context.DeadlineExceeded) && errors.Is(result.parentCause, context.DeadlineExceeded)
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
