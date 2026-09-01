package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/crane/admission"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/store"
)

var (
	// ErrNotReady reports an engine whose one-time durable recovery has not
	// completed or whose owner loop has already stopped.
	ErrNotReady = errors.New("Crane worker engine is not ready")
	// ErrNotRunning reports admission under a missing, closed, or stale durable
	// job installation.
	ErrNotRunning = errors.New("Crane job is not durably Running at the current fence")
)

// OperatorExecutor performs one deterministic built-in operator invocation.
// The context permits bounded workers to join engine cancellation.
type OperatorExecutor func(context.Context, model.OperatorSpec, model.Tuple) ([]model.Tuple, error)

// EngineOptions fixes the worker engine's durable dependencies, shared gate,
// deterministic clock, retry schedules, and local queue bounds.
type EngineOptions struct {
	// Repository is the sole durable worker authority.
	Repository Repository
	// Sender transmits only records already present in a durable outbox.
	Sender Sender
	// Replicator reserves the independently reviewed slice-B boundary.
	Replicator ResultReplicator
	// Gate is the caller-owned process gate shared with worker services.
	Gate *admission.Gate
	// Clock schedules every retry and reconciliation deterministically.
	Clock clock.Clock
	// Execute optionally replaces the built-in deterministic operator runner.
	Execute OperatorExecutor
	// MaxExecutors bounds concurrent deterministic operator work.
	MaxExecutors uint16
	// MaxPendingWork bounds queued commands and operator results.
	MaxPendingWork uint16
	// MaxPendingOutboxes bounds source emission and queued sends.
	MaxPendingOutboxes uint16
	// AcceptedRetryInterval retries custody until Accepted is observed.
	AcceptedRetryInterval time.Duration
	// CompletedRetryInterval retries retained payload after Accepted while
	// waiting for Completed.
	CompletedRetryInterval time.Duration
}

// Engine owns one serialized durable state publisher plus bounded operator and
// sender workers. Constructors create no goroutine, timer, file, or socket.
type Engine struct {
	repository         Repository
	sender             Sender
	replicator         ResultReplicator
	gate               *admission.Gate
	clock              clock.Clock
	execute            OperatorExecutor
	maxExecutors       int
	maxPendingOutboxes int
	acceptedRetry      time.Duration
	completedRetry     time.Duration

	ready      chan struct{}
	done       chan struct{}
	events     chan model.WorkerEvent
	commands   chan any
	started    atomic.Bool
	readyState atomic.Bool

	executorJobs    chan executionJob
	executorResults chan executionResult
	sendJobs        chan sendJob
	sendResults     chan sendResult
	workers         sync.WaitGroup

	deliveries        map[model.DeliveryID]store.DeliveryRecord
	outboxes          map[model.DeliveryID]*ownedOutbox
	parents           map[model.DeliveryID]map[model.DeliveryID]struct{}
	sources           map[model.TaskID]store.SourceCursor
	executing         map[model.DeliveryID]struct{}
	failedTasks       map[model.TaskID]struct{}
	jobs              map[model.JobID]struct{}
	eventQueue        []model.WorkerEvent
	nextTransactionID uint64
	localNode         uint16
	localEpoch        model.WorkerEpoch
}

// NewEngine validates and retains the caller's exact dependencies without
// opening resources or starting background work.
func NewEngine(options EngineOptions) (*Engine, error) {
	if options.Repository == nil || options.Sender == nil || options.Gate == nil || options.Clock == nil {
		return nil, errors.New("worker engine requires repository, sender, shared gate, and clock")
	}
	if options.MaxExecutors == 0 {
		options.MaxExecutors = 1
	}
	if options.MaxPendingWork == 0 {
		options.MaxPendingWork = 64
	}
	if options.MaxPendingOutboxes == 0 {
		options.MaxPendingOutboxes = 64
	}
	if uint64(options.MaxExecutors) > model.LimitsV1().MaxWorkerSlots || int(options.MaxPendingWork) > store.MaxTransactionRecords || uint64(options.MaxPendingOutboxes) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("worker engine queue bound exceeds v1 limits")
	}
	if options.AcceptedRetryInterval == 0 {
		options.AcceptedRetryInterval = 200 * time.Millisecond
	}
	if options.CompletedRetryInterval == 0 {
		options.CompletedRetryInterval = time.Second
	}
	if options.AcceptedRetryInterval <= 0 || options.CompletedRetryInterval <= options.AcceptedRetryInterval {
		return nil, errors.New("worker engine retry intervals are invalid")
	}
	if options.Execute == nil {
		options.Execute = func(_ context.Context, operator model.OperatorSpec, tuple model.Tuple) ([]model.Tuple, error) {
			return model.ExecuteOperator(operator, tuple)
		}
	}
	return &Engine{
		repository: options.Repository, sender: options.Sender, replicator: options.Replicator,
		gate: options.Gate, clock: options.Clock, execute: options.Execute,
		maxExecutors: int(options.MaxExecutors), maxPendingOutboxes: int(options.MaxPendingOutboxes),
		acceptedRetry: options.AcceptedRetryInterval, completedRetry: options.CompletedRetryInterval,
		ready: make(chan struct{}), done: make(chan struct{}), events: make(chan model.WorkerEvent, store.MaxTransactionRecords),
		commands:     make(chan any, options.MaxPendingWork),
		executorJobs: make(chan executionJob, options.MaxPendingWork), executorResults: make(chan executionResult, options.MaxPendingWork),
		sendJobs: make(chan sendJob, options.MaxPendingOutboxes), sendResults: make(chan sendResult, options.MaxPendingOutboxes),
		deliveries: make(map[model.DeliveryID]store.DeliveryRecord), outboxes: make(map[model.DeliveryID]*ownedOutbox),
		parents: make(map[model.DeliveryID]map[model.DeliveryID]struct{}), sources: make(map[model.TaskID]store.SourceCursor),
		executing: make(map[model.DeliveryID]struct{}), failedTasks: make(map[model.TaskID]struct{}),
		jobs: make(map[model.JobID]struct{}),
	}, nil
}

// Ready closes after exactly one complete owned recovery snapshot has been
// consumed and its resumable work has been admitted to the owner.
func (engine *Engine) Ready() <-chan struct{} { return engine.ready }

// Events publishes recovered and newly durable worker events without changing
// their globally ordered transaction identities.
func (engine *Engine) Events() <-chan model.WorkerEvent { return engine.events }

// Run performs one recovery and owns every subsequent state publication until
// cancellation. It joins all bounded workers before returning.
func (engine *Engine) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil engine context")
	}
	if !engine.started.CompareAndSwap(false, true) {
		return errors.New("worker engine Run called more than once")
	}
	work, err := engine.repository.RecoverWork()
	if err != nil {
		close(engine.done)
		return err
	}
	runContext, cancel := context.WithCancel(ctx)
	engine.startWorkers(runContext)
	if err = engine.consumeRecovery(work); err != nil {
		cancel()
		engine.stopWorkers()
		close(engine.done)
		return err
	}
	if err = engine.reconcile(runContext, engine.clock.Now()); err != nil {
		cancel()
		engine.stopWorkers()
		close(engine.done)
		return err
	}
	timer := engine.clock.NewTimer(engine.acceptedRetry)
	timerActive := true
	engine.readyState.Store(true)
	close(engine.ready)
	defer func() {
		if timerActive {
			timer.Stop()
		}
		engine.readyState.Store(false)
		cancel()
		engine.stopWorkers()
		engine.failPendingCommands(ctx.Err())
		close(engine.events)
		close(engine.done)
	}()

	for {
		now := engine.clock.Now()
		if err := engine.reconcile(runContext, now); err != nil {
			return err
		}
		delay := engine.nextWake(now)
		if timerActive {
			timer.Stop()
		}
		timer.Reset(delay)
		timerActive = true
		var eventOutput chan model.WorkerEvent
		var nextEvent model.WorkerEvent
		if len(engine.eventQueue) > 0 {
			eventOutput, nextEvent = engine.events, engine.eventQueue[0]
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case command := <-engine.commands:
			engine.handleCommand(runContext, command)
		case result := <-engine.executorResults:
			if err := engine.handleExecutionResult(result); err != nil {
				return err
			}
		case result := <-engine.sendResults:
			engine.handleSendResult(result)
		case eventOutput <- nextEvent:
			engine.eventQueue = engine.eventQueue[1:]
		case <-timer.C():
			timerActive = false
		}
	}
}

func (engine *Engine) consumeRecovery(work store.RecoveredWork) error {
	engine.nextTransactionID = work.NextTransactionID
	if engine.nextTransactionID == 0 {
		return errors.New("recovery returned zero next worker transaction")
	}
	engine.localNode, engine.localEpoch = engine.repository.LocalIdentity()
	if engine.localNode == 0 || engine.localEpoch.Validate() != nil {
		return errors.New("repository returned invalid local worker identity")
	}
	for _, assignment := range work.Assignments {
		engine.jobs[assignment.Assignment.JobID] = struct{}{}
	}
	for _, cursor := range work.Sources {
		engine.sources[cursor.Source] = cursor
	}
	for _, delivery := range work.Deliveries {
		owned := delivery.Clone()
		engine.deliveries[owned.ID] = owned
		for _, outboxID := range owned.OutboxIDs {
			if engine.parents[outboxID] == nil {
				engine.parents[outboxID] = make(map[model.DeliveryID]struct{})
			}
			engine.parents[outboxID][owned.ID] = struct{}{}
		}
	}
	for _, outbox := range work.Outboxes {
		owned := outbox.Clone()
		engine.outboxes[owned.ID] = &ownedOutbox{record: owned, nextAttempt: engine.clock.Now()}
	}
	engine.eventQueue = append(engine.eventQueue, work.PendingEvents...)
	for _, event := range work.PendingEvents {
		if event.Failure != nil {
			engine.failedTasks[event.Failure.Task.Task] = struct{}{}
		}
	}
	return nil
}

func (engine *Engine) startWorkers(ctx context.Context) {
	for index := 0; index < engine.maxExecutors; index++ {
		engine.workers.Add(1)
		go engine.executorWorker(ctx)
	}
	engine.workers.Add(1)
	go engine.senderWorker(ctx)
}

func (engine *Engine) stopWorkers() {
	close(engine.executorJobs)
	close(engine.sendJobs)
	engine.workers.Wait()
}

func (engine *Engine) failPendingCommands(cause error) {
	if cause == nil {
		cause = ErrNotReady
	}
	for {
		select {
		case command := <-engine.commands:
			switch value := command.(type) {
			case deliveryCommand:
				value.response <- deliveryResponse{err: cause}
			case ackCommand:
				value.response <- cause
			}
		default:
			return
		}
	}
}

func (engine *Engine) nextWake(now time.Time) time.Duration {
	deadline := now.Add(engine.acceptedRetry)
	for _, outbox := range engine.outboxes {
		if outbox.record.Completed || outbox.sending {
			continue
		}
		if outbox.nextAttempt.Before(deadline) {
			deadline = outbox.nextAttempt
		}
	}
	delay := deadline.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

func (engine *Engine) handleCommand(ctx context.Context, command any) {
	switch value := command.(type) {
	case deliveryCommand:
		ack, err := engine.receiveDelivery(ctx, value.message)
		value.response <- deliveryResponse{ack: ack, err: err}
	case ackCommand:
		value.response <- engine.receiveACK(value.ack)
	}
}

func (engine *Engine) reconcile(ctx context.Context, now time.Time) error {
	if err := engine.completeSatisfiedParents(); err != nil {
		return err
	}
	engine.scheduleRecoveredExecutions(ctx)
	if err := engine.emitSources(ctx); err != nil {
		return err
	}
	engine.scheduleOutboxes(ctx, now)
	return nil
}

func (engine *Engine) currentRunning(job model.JobID) (store.InstalledAssignment, bool) {
	assignment, ok := engine.repository.InstalledAssignment(job)
	if !ok || assignment.SchedulingState != model.Running || assignment.CoordinatorEpoch != engine.repository.CurrentFence() {
		return store.InstalledAssignment{}, false
	}
	return assignment, true
}

func (engine *Engine) enqueue(command any, ctx context.Context) error {
	if !engine.readyState.Load() {
		return ErrNotReady
	}
	select {
	case engine.commands <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.done:
		return ErrNotReady
	}
}

func (engine *Engine) ownerError(operation string, err error) error {
	return fmt.Errorf("worker engine %s: %w", operation, err)
}
