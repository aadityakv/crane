package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
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
	assertNotReturned(t, result)
	second.markReady()
	assertNotReturned(t, result)

	cancel()
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
	assertNotReturned(t, result)
	cancel()
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
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()

	<-service.started
	cancel()
	<-service.canceled
	assertNotReturned(t, result)
	close(service.waitAfterCancellation)
	if err := <-result; err != nil {
		t.Fatalf("Run error = %v, want nil after parent cancellation", err)
	}
	<-service.returned
}

func assertNotReturned(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("Run returned early with error %v", err)
	default:
	}
}

type fakeService struct {
	name                  string
	ready                 chan struct{}
	started               chan struct{}
	finish                chan error
	canceled              chan struct{}
	returned              chan struct{}
	readyOnce             sync.Once
	canceledOnce          sync.Once
	returnedOnce          sync.Once
	waitAfterCancellation chan struct{}
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
		return ctx.Err()
	}
}

func (s *fakeService) markReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *fakeService) fail(err error) {
	s.finish <- err
}
