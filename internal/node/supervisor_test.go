package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	result := make(chan error)
	go func() { result <- supervisor.Run(ctx) }()
	gate := observeResult(result)

	<-service.started
	cancel()
	<-service.canceled
	if err := gate.releaseAfterBlocked(t, func() { close(service.waitAfterCancellation) }); err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
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

type resultGate struct {
	allowCheck chan struct{}
	observed   chan gateOutcome
	final      chan error
}

type gateOutcome struct {
	returned bool
	err      error
}

func observeResult(result <-chan error) *resultGate {
	gate := &resultGate{
		allowCheck: make(chan struct{}),
		observed:   make(chan gateOutcome, 1),
		final:      make(chan error, 1),
	}
	go func() {
		select {
		case err := <-result:
			gate.observed <- gateOutcome{returned: true, err: err}
		case <-gate.allowCheck:
			gate.observed <- gateOutcome{}
			gate.final <- <-result
		}
	}()
	return gate
}

func (g *resultGate) releaseAfterBlocked(t *testing.T, release func()) error {
	t.Helper()
	close(g.allowCheck)
	if outcome := <-g.observed; outcome.returned {
		t.Fatalf("Run returned before release with error %v", outcome.err)
	}
	release()
	return <-g.final
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
	return c.err
}

func (c *controlledContext) Value(key any) any {
	return nil
}

func (c *controlledContext) cancel(err error) {
	c.err = err
	close(c.done)
}

type fakeService struct {
	name                    string
	ready                   chan struct{}
	started                 chan struct{}
	finish                  chan error
	canceled                chan struct{}
	returned                chan struct{}
	readyOnce               sync.Once
	canceledOnce            sync.Once
	returnedOnce            sync.Once
	waitAfterCancellation   chan struct{}
	returnAfterCancellation error
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
		return err
	case <-ctx.Done():
		s.canceledOnce.Do(func() { close(s.canceled) })
		if s.waitAfterCancellation != nil {
			<-s.waitAfterCancellation
		}
		if s.returnAfterCancellation != nil {
			return s.returnAfterCancellation
		}
		return ctx.Err()
	}
}

func (s *fakeService) markReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *fakeService) fail(err error) {
	s.finish <- err
}
