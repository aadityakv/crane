package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSupervisorReturnsImmediatelyWithNoServices(t *testing.T) {
	supervisor := NewSupervisor()
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(context.Background()) }()

	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil for an empty service set", err)
	}
	assertChannelClosed(t, supervisor.Ready(), "empty supervisor readiness")
}

func TestSupervisorReadyClosesOnlyAfterEveryServiceIsCausallyReady(t *testing.T) {
	first := newFakeService("first")
	second := newFakeService("second")
	supervisor := NewSupervisor(first, second)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-first.started
	<-second.started
	first.markReady()
	assertChannelOpen(t, supervisor.Ready(), "aggregate readiness after only first service")
	second.markReady()
	assertChannelClosed(t, supervisor.Ready(), "aggregate readiness after every service")
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
}

func TestSupervisorReadyAcceptsPreclosedChildReadiness(t *testing.T) {
	first := newFakeService("first")
	second := newFakeService("second")
	first.markReady()
	second.markReady()
	supervisor := NewSupervisor(first, second)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-first.started
	<-second.started
	assertChannelClosed(t, supervisor.Ready(), "aggregate readiness for preclosed children")
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
}

func TestSupervisorReadyRemainsOpenWhenServiceFailsBeforeReadiness(t *testing.T) {
	failed := newFakeService("failed")
	sibling := newFakeService("sibling")
	sibling.markReady()
	sibling.waitAfterCancellation = make(chan struct{})
	supervisor := NewSupervisor(failed, sibling)
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(context.Background()) }()

	<-failed.started
	<-sibling.started
	failure := errors.New("bind failed")
	failed.fail(failure)
	<-sibling.canceled
	assertChannelOpen(t, supervisor.Ready(), "aggregate readiness after early startup failure")
	assertChannelOpen(t, sibling.returned, "sibling return before release")
	close(sibling.waitAfterCancellation)
	if err := <-result; !errors.Is(err, failure) {
		t.Fatalf("Run error = %v, want startup failure %v", err, failure)
	}
	assertChannelOpen(t, supervisor.Ready(), "aggregate readiness after failed Run")
	assertChannelClosed(t, sibling.returned, "sibling joined before supervisor return")
}

func TestSupervisorCausalityRejectsAggregateReadyAfterCompletionBeforeBufferedReadiness(t *testing.T) {
	causality := &supervisorCausality{}
	childReady := make(chan struct{})
	aggregateReady := make(chan struct{})
	result := causality.linearizeCompletion(0, errors.New("startup failed"), childReady, context.Background())
	close(childReady)

	if result.ready {
		t.Fatal("completion observed child ready even though completion linearized first")
	}
	if causality.publishReady(context.Background(), func() { close(aggregateReady) }) {
		t.Fatal("aggregate readiness published after a completion was already linearized")
	}
	assertChannelOpen(t, aggregateReady, "aggregate readiness after causally early completion")
}

func TestSupervisorCausalityPublishesAggregateReadyWhenReadinessClosesBeforeCompletion(t *testing.T) {
	causality := &supervisorCausality{}
	childReady := make(chan struct{})
	aggregateReady := make(chan struct{})
	close(childReady)
	result := causality.linearizeCompletion(0, errors.New("running failure"), childReady, context.Background())

	if !result.ready {
		t.Fatal("completion did not observe readiness that causally closed first")
	}
	if !causality.publishReady(context.Background(), func() { close(aggregateReady) }) {
		t.Fatal("aggregate readiness was suppressed by a completion that followed child readiness")
	}
	assertChannelClosed(t, aggregateReady, "aggregate readiness after causally later completion")
}

func TestSupervisorCancellationAfterReadyJoinsEveryService(t *testing.T) {
	first := newFakeService("first")
	second := newFakeService("second")
	first.markReady()
	second.markReady()
	first.waitAfterCancellation = make(chan struct{})
	second.waitAfterCancellation = make(chan struct{})
	supervisor := NewSupervisor(first, second)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-first.started
	<-second.started
	assertChannelClosed(t, supervisor.Ready(), "aggregate readiness before cancellation")
	cancel()
	<-first.canceled
	<-second.canceled
	close(first.waitAfterCancellation)
	assertChannelOpen(t, result, "supervisor return while second service drains")
	close(second.waitAfterCancellation)
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after cancellation", err)
	}
	assertChannelClosed(t, first.returned, "first joined")
	assertChannelClosed(t, second.returned, "second joined")
}

func TestSupervisorValidationFailureLeavesReadyOpenAndStartsNothing(t *testing.T) {
	first := newFakeService("duplicate")
	second := newFakeService("duplicate")
	supervisor := NewSupervisor(first, second)
	if err := supervisor.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with duplicate service names")
	}
	assertChannelOpen(t, supervisor.Ready(), "aggregate readiness after validation failure")
	assertChannelOpen(t, first.started, "first service start")
	assertChannelOpen(t, second.started, "second service start")
}

func TestSupervisorWaitsForEveryServiceToBecomeReady(t *testing.T) {
	first := newFakeService("first")
	second := newFakeService("second")
	supervisor := NewSupervisor(first, second)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-first.started
	<-second.started
	first.markReady()
	assertRunBlockedBefore(t, result, func() { second.markReady() })
	assertRunBlockedBefore(t, result, cancel)

	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
	<-first.returned
	<-second.returned
}

func TestSupervisorCancelsSiblingsOnServiceFailure(t *testing.T) {
	failed := newFakeService("failed")
	sibling := newFakeService("sibling")
	supervisor := NewSupervisor(failed, sibling)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-failed.started
	<-sibling.started
	failed.markReady()
	sibling.markReady()
	failure := errors.New("listener failed")
	failed.fail(failure)

	if err := <-result; !errors.Is(err, failure) || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("Run error = %v, want failure from failed service", err)
	}
	<-sibling.canceled
	<-sibling.returned
}

func TestSupervisorClassifiesReturnBeforeReadinessAsStartupFailure(t *testing.T) {
	service := newFakeService("listener")
	supervisor := NewSupervisor(service)
	result := make(chan error, 1)
	startupFailure := errors.New("bind failed")
	go func() { result <- supervisor.Run(context.Background()) }()

	<-service.started
	service.fail(startupFailure)

	if err := <-result; !errors.Is(err, startupFailure) || !strings.Contains(err.Error(), "before reporting ready") {
		t.Fatalf("Run error = %v, want startup failure retaining %v", err, startupFailure)
	}
	<-service.returned
}

func TestSupervisorTreatsCanceledReturnBeforeReadinessAsNormalShutdown(t *testing.T) {
	canceled := newFakeService("canceled")
	sibling := newFakeService("sibling")
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(canceled, sibling).Run(context.Background()) }()

	<-canceled.started
	<-sibling.started
	canceled.fail(context.Canceled)

	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil for cancellation-only shutdown", err)
	}
	<-sibling.canceled
	<-sibling.returned
}

func TestSupervisorTreatsCanceledReturnAfterReadinessAsNormalShutdown(t *testing.T) {
	canceled := newFakeService("canceled")
	sibling := newFakeService("sibling")
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(canceled, sibling).Run(context.Background()) }()

	<-canceled.started
	<-sibling.started
	canceled.markReady()
	sibling.markReady()
	canceled.fail(context.Canceled)

	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil for cancellation-only shutdown", err)
	}
	<-sibling.canceled
	<-sibling.returned
}

func TestSupervisorTreatsNilReturnAfterParentCancellationAsNormalShutdown(t *testing.T) {
	service := newFakeService("graceful")
	service.returnNilAfterCancellation = true
	service.markReady()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(service).Run(ctx) }()

	<-service.started
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after graceful cancellation cleanup", err)
	}
}

func TestSupervisorPreservesNonCancellationFailureAfterParentCancellation(t *testing.T) {
	service := newFakeService("flush")
	service.markReady()
	failure := errors.New("flush failed")
	service.returnAfterCancellation = failure
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(service).Run(ctx) }()

	<-service.started
	cancel()
	if err := <-result; !errors.Is(err, failure) {
		t.Fatalf("Run error = %v, want cancellation-time failure %v", err, failure)
	}
}

func TestSupervisorSelectsFailureByServiceDeclarationOrder(t *testing.T) {
	earlier := newFakeService("earlier")
	later := newFakeService("later")
	earlierFailure := errors.New("earlier failed")
	laterFailure := errors.New("later failed")
	earlier.returnAfterCancellation = earlierFailure
	earlier.markReady()
	later.markReady()
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(earlier, later).Run(context.Background()) }()

	<-earlier.started
	<-later.started
	later.fail(laterFailure)

	if err := <-result; !errors.Is(err, earlierFailure) {
		t.Fatalf("Run error = %v, want earlier declared failure %v", err, earlierFailure)
	}
}

func TestSupervisorDoesNotLetEarlierNilCancellationMaskLaterFailure(t *testing.T) {
	earlier := newFakeService("earlier")
	earlier.returnNilAfterCancellation = true
	later := newFakeService("later")
	earlier.markReady()
	later.markReady()
	failure := errors.New("later listener failed")
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(earlier, later).Run(context.Background()) }()

	<-earlier.started
	<-later.started
	later.fail(failure)

	if err := <-result; !errors.Is(err, failure) || !strings.Contains(err.Error(), "later") {
		t.Fatalf("Run error = %v, want later failure %v", err, failure)
	}
	<-earlier.canceled
	<-earlier.returned
}

func TestSupervisorKeepsNilCompletionLinearizedBeforeCancellationUnexpectedWhenDeliveryIsDelayed(t *testing.T) {
	causality := &supervisorCausality{}
	ready := make(chan struct{})
	close(ready)
	linearized := make(chan struct{})
	releaseDelivery := make(chan struct{})
	earlierDelivery := make(chan completion, 1)
	go func() {
		result := causality.linearizeCompletion(0, nil, ready, context.Background())
		close(linearized)
		<-releaseDelivery
		earlierDelivery <- result
	}()
	<-linearized

	laterFailure := errors.New("later listener failed")
	later := causality.linearizeCompletion(1, laterFailure, ready, context.Background())
	derivedContext, cancel := context.WithCancelCause(context.Background())
	causality.initiateCancellation(cancel)
	if !errors.Is(context.Cause(derivedContext), errSupervisorShutdown) {
		t.Fatalf("derived context cause = %v, want supervisor shutdown", context.Cause(derivedContext))
	}
	close(releaseDelivery)
	earlier := <-earlierDelivery

	states := []serviceState{
		{service: newFakeService("earlier")},
		{service: newFakeService("later")},
	}
	err := selectFailure(states, []completion{earlier, later})
	if err == nil || !strings.Contains(err.Error(), "earlier") || !strings.Contains(err.Error(), "exited unexpectedly") {
		t.Fatalf("selectFailure error = %v, want earlier unexpected nil completion", err)
	}
	if errors.Is(err, laterFailure) {
		t.Fatalf("selectFailure error = %v, later failure masked earlier unexpected completion", err)
	}
}

func TestSupervisorRejectsDuplicateServiceNames(t *testing.T) {
	first := newFakeService("duplicate")
	second := newFakeService("duplicate")

	err := NewSupervisor(first, second).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Run error = %v, want duplicate service-name error", err)
	}
	select {
	case <-first.started:
		t.Fatal("first duplicate service was started")
	default:
	}
	select {
	case <-second.started:
		t.Fatal("second duplicate service was started")
	default:
	}
}

func TestSupervisorAcceptsReadinessClosedBeforeRun(t *testing.T) {
	service := newFakeService("already-ready")
	service.markReady()
	supervisor := NewSupervisor(service)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-service.started
	assertRunBlockedBefore(t, result, cancel)
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
}

func TestSupervisorJoinsEveryServiceBeforeReturning(t *testing.T) {
	service := newFakeService("draining")
	service.markReady()
	service.waitAfterCancellation = make(chan struct{})
	supervisor := NewSupervisor(service)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan lifecycleEvent, 2)
	service.events = events
	result := make(chan error)
	go func() {
		err := supervisor.Run(ctx)
		events <- supervisorReturned
		result <- err
	}()

	<-service.started
	cancel()
	<-service.canceled
	close(service.waitAfterCancellation)
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
	if got, want := <-events, serviceFinished; got != want {
		t.Fatalf("first lifecycle event = %q, want %q", got, want)
	}
	if got, want := <-events, supervisorReturned; got != want {
		t.Fatalf("second lifecycle event = %q, want %q", got, want)
	}
	<-service.returned
}

func assertRunBlockedBefore(t *testing.T, result <-chan error, release func()) {
	t.Helper()
	type gateOutcome struct {
		returned bool
		err      error
	}
	checked := make(chan struct{})
	earlyResult := make(chan gateOutcome, 1)
	go func() {
		select {
		case err := <-result:
			earlyResult <- gateOutcome{returned: true, err: err}
		case <-checked:
			earlyResult <- gateOutcome{}
		}
	}()
	close(checked)
	if outcome := <-earlyResult; outcome.returned {
		t.Fatalf("Run returned before release with error %v", outcome.err)
	}
	release()
}

func assertChannelOpen[T any](t *testing.T, channel <-chan T, description string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatalf("%s is closed or readable, want open and blocked", description)
	default:
	}
}

func assertChannelClosed[T any](t *testing.T, channel <-chan T, description string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestSupervisorTreatsDeadlineReturnFromCanceledParentAsNormalShutdown(t *testing.T) {
	service := newFakeService("deadline")
	service.markReady()
	parent := newControlledContext()
	result := make(chan error, 1)
	go func() { result <- NewSupervisor(service).Run(parent) }()

	<-service.started
	parent.cancel(context.DeadlineExceeded)
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil for parent deadline cancellation", err)
	}
}

type controlledContext struct {
	mu   sync.Mutex
	done chan struct{}
	err  error
}

func newControlledContext() *controlledContext {
	return &controlledContext{done: make(chan struct{})}
}

func (c *controlledContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *controlledContext) Done() <-chan struct{} {
	return c.done
}

func (c *controlledContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *controlledContext) Value(key any) any {
	return nil
}

func (c *controlledContext) cancel(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	close(c.done)
}

type fakeService struct {
	name                       string
	ready                      chan struct{}
	started                    chan struct{}
	finish                     chan error
	canceled                   chan struct{}
	returned                   chan struct{}
	readyOnce                  sync.Once
	canceledOnce               sync.Once
	returnedOnce               sync.Once
	waitAfterCancellation      chan struct{}
	returnAfterCancellation    error
	returnNilAfterCancellation bool
	events                     chan<- lifecycleEvent
}

func newFakeService(name string) *fakeService {
	return &fakeService{
		name:     name,
		ready:    make(chan struct{}),
		started:  make(chan struct{}),
		finish:   make(chan error, 1),
		canceled: make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (s *fakeService) Name() string {
	return s.name
}

func (s *fakeService) Ready() <-chan struct{} {
	return s.ready
}

func (s *fakeService) Run(ctx context.Context) error {
	close(s.started)
	defer s.returnedOnce.Do(func() { close(s.returned) })

	select {
	case err := <-s.finish:
		return s.finishWith(err)
	case <-ctx.Done():
		s.canceledOnce.Do(func() { close(s.canceled) })
		if s.waitAfterCancellation != nil {
			<-s.waitAfterCancellation
		}
		if s.returnAfterCancellation != nil {
			return s.finishWith(s.returnAfterCancellation)
		}
		if s.returnNilAfterCancellation {
			return s.finishWith(nil)
		}
		return s.finishWith(ctx.Err())
	}
}

type lifecycleEvent string

const (
	serviceFinished    lifecycleEvent = "service-finished"
	supervisorReturned lifecycleEvent = "supervisor-returned"
)

func (s *fakeService) finishWith(err error) error {
	if s.events != nil {
		s.events <- serviceFinished
	}
	return err
}

func (s *fakeService) markReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *fakeService) fail(err error) {
	s.finish <- err
}
