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
	ErrNotReady = errors.New("crane worker engine is not ready")
	// ErrNotRunning reports admission under a missing, closed, or stale durable
	// job installation.
	ErrNotRunning = errors.New("crane job is not durably Running at the current fence")
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
	dispatchStarts  chan dispatchStart
	resultJobs      chan resultJob
	resultResponses chan resultResponse
	workers         sync.WaitGroup

	deliveries        map[model.DeliveryID]store.DeliveryRecord
	outboxes          map[model.DeliveryID]*ownedOutbox
	parents           map[model.DeliveryID]map[model.DeliveryID]struct{}
	sources           map[model.TaskID]store.SourceCursor
	executing         map[model.DeliveryID]struct{}
	failedTasks       map[model.TaskID]struct{}
	jobs              map[model.JobID]struct{}
	eventQueue        []model.WorkerEvent
	completionReports map[model.TaskID]model.WorkerEvent
	results           map[resultIdentity]*ownedResult
	readoptEnvelopes  map[model.JobID]resultEnvelope
	readoptPending    map[model.JobID]struct{}
	nextTransactionID uint64
	localNode         uint16
	localEpoch        model.WorkerEpoch
}

type assignmentCommand struct {
	job      model.JobID
	response chan error
}

// NewEngine validates and retains the caller's exact dependencies without
// opening resources or starting background work.
func NewEngine(options EngineOptions) (*Engine, error) {
	if options.Repository == nil || options.Sender == nil || options.Replicator == nil || options.Gate == nil || options.Clock == nil {
		return nil, errors.New("worker engine requires repository, sender, result replicator, shared gate, and clock")
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
		dispatchStarts: make(chan dispatchStart, options.MaxPendingOutboxes),
		resultJobs:     make(chan resultJob, options.MaxPendingWork), resultResponses: make(chan resultResponse, options.MaxPendingWork),
		deliveries: make(map[model.DeliveryID]store.DeliveryRecord), outboxes: make(map[model.DeliveryID]*ownedOutbox),
		parents: make(map[model.DeliveryID]map[model.DeliveryID]struct{}), sources: make(map[model.TaskID]store.SourceCursor),
		executing: make(map[model.DeliveryID]struct{}), failedTasks: make(map[model.TaskID]struct{}),
		jobs:              make(map[model.JobID]struct{}),
		results:           make(map[resultIdentity]*ownedResult),
		readoptEnvelopes:  make(map[model.JobID]resultEnvelope),
		readoptPending:    make(map[model.JobID]struct{}),
		completionReports: make(map[model.TaskID]model.WorkerEvent),
	}, nil
}

// Ready closes after exactly one complete owned recovery snapshot has been
// consumed and its resumable work has been admitted to the owner.
func (engine *Engine) Ready() <-chan struct{} { return engine.ready }

// Events publishes recovered and newly durable worker events without changing
// their globally ordered transaction identities.
func (engine *Engine) Events() <-chan model.WorkerEvent { return engine.events }

// ReconcileAssignment observes one already-durable post-recovery assignment
// transition on the serialized owner. Task 17 calls it only after persistence.
func (engine *Engine) ReconcileAssignment(ctx context.Context, job model.JobID) error {
	if ctx == nil {
		return errors.New("nil assignment reconciliation context")
	}
	if err := job.Validate(); err != nil {
		return err
	}
	response := make(chan error, 1)
	if err := engine.enqueue(assignmentCommand{job: job, response: response}, ctx); err != nil {
		return err
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-engine.done:
		return ErrNotReady
	}
}

// Run performs one recovery and owns every subsequent state publication until
// cancellation. It joins all bounded workers before returning.
func (engine *Engine) Run(ctx context.Context) (runErr error) {
	if ctx == nil {
		return errors.New("nil engine context")
	}
	if !engine.started.CompareAndSwap(false, true) {
		return errors.New("worker engine Run called more than once")
	}
	var cancel context.CancelFunc
	var timer clock.Timer
	workersStarted, timerActive := false, false
	defer func() {
		if timerActive {
			timer.Stop()
		}
		engine.readyState.Store(false)
		if cancel != nil {
			cancel()
		}
		if workersStarted {
			engine.stopWorkers()
			engine.drainExecutionResults()
		}
		engine.failPendingCommands(runErr)
		close(engine.events)
		close(engine.done)
	}()
	work, err := engine.repository.RecoverWork()
	if err != nil {
		return err
	}
	if err = engine.consumeRecovery(work); err != nil {
		return err
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	engine.startWorkers(runContext)
	workersStarted = true
	if err = engine.reconcile(runContext, engine.clock.Now()); err != nil {
		return err
	}
	timer = engine.clock.NewTimer(engine.nextWake(engine.clock.Now()))
	timerActive = true
	engine.readyState.Store(true)
	close(engine.ready)

	firstSelect := true
	for {
		if firstSelect {
			firstSelect = false
		} else {
			now := engine.clock.Now()
			if err := engine.reconcile(runContext, now); err != nil {
				return err
			}
			if timerActive {
				timer.Stop()
			}
			timer.Reset(engine.nextWake(engine.clock.Now()))
			// A manual (or heavily delayed real) clock may cross an absolute
			// durable deadline between computing a relative delay and Reset.
			// Re-arm immediately when that happened so retries never slip by
			// one full relative interval.
			if engine.nextWake(engine.clock.Now()) == 0 {
				timer.Stop()
				timer.Reset(0)
			}
			timerActive = true
		}
		var eventOutput chan model.WorkerEvent
		var nextEvent model.WorkerEvent
		if len(engine.eventQueue) > 0 {
			eventOutput, nextEvent = engine.events, cloneWorkerEvent(engine.eventQueue[0])
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
		case result := <-engine.resultResponses:
			if err := engine.handleResultResponse(result); err != nil {
				return err
			}
		case start := <-engine.dispatchStarts:
			response := engine.handleDispatchStart(start)
			start.response <- response
			if response.err != nil {
				return response.err
			}
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
		engine.outboxes[owned.ID] = &ownedOutbox{record: owned}
	}
	for _, stored := range work.Results {
		owned := &ownedResult{record: cloneResultRecord(stored.Record), provenance: stored.Provenance}
		engine.results[resultID(stored.Record)] = owned
	}
	for _, event := range work.PendingEvents {
		owned := cloneWorkerEvent(event)
		engine.eventQueue = append(engine.eventQueue, owned)
		if owned.Completion != nil {
			engine.completionReports[owned.Completion.Source] = owned
		}
		if owned.Failure != nil {
			engine.failedTasks[owned.Failure.Task.Task] = struct{}{}
		}
	}
	return nil
}

func cloneWorkerEvent(event model.WorkerEvent) model.WorkerEvent {
	if event.Completion != nil {
		completion := *event.Completion
		event.Completion = &completion
	}
	if event.Failure != nil {
		failure := *event.Failure
		event.Failure = &failure
	}
	return event
}

func (engine *Engine) startWorkers(ctx context.Context) {
	for index := 0; index < engine.maxExecutors; index++ {
		engine.workers.Add(1)
		go engine.executorWorker(ctx)
	}
	engine.workers.Add(1)
	go engine.senderWorker(ctx)
	engine.workers.Add(1)
	go engine.resultWorker(ctx)
}

func (engine *Engine) stopWorkers() {
	close(engine.executorJobs)
	close(engine.sendJobs)
	close(engine.resultJobs)
	engine.workers.Wait()
}

func (engine *Engine) drainExecutionResults() {
	for {
		select {
		case result := <-engine.executorResults:
			result.job.release()
		default:
			return
		}
	}
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
				value.release()
				value.response <- deliveryResponse{err: cause}
			case ackCommand:
				value.response <- cause
			case assignmentCommand:
				value.response <- cause
			case checkpointCommand:
				value.response <- cause
			case eventAckCommand:
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
		if outbox.record.RetryDeadlineUnixNano == 0 {
			return 0
		}
		candidate := time.Unix(0, outbox.record.RetryDeadlineUnixNano)
		if candidate.Before(deadline) {
			deadline = candidate
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
		defer value.release()
		ack, err := engine.receiveDelivery(ctx, value.message)
		value.response <- deliveryResponse{ack: ack, err: err}
	case ackCommand:
		value.response <- engine.receiveACK(value.ack)
	case assignmentCommand:
		assignment, ok := engine.currentRunning(value.job)
		if ok && assignment.Assignment.JobID == value.job {
			engine.jobs[value.job] = struct{}{}
		} else {
			delete(engine.jobs, value.job)
		}
		// Every install re-evaluates the job's retained result copies against
		// the (possibly partner-changing) installed envelope.
		engine.readoptPending[value.job] = struct{}{}
		value.response <- nil
	case checkpointCommand:
		value.response <- engine.applyCheckpoint(value.notice)
	case eventAckCommand:
		value.response <- engine.acknowledgeEvents(value.through)
	}
}

func (engine *Engine) reconcile(ctx context.Context, now time.Time) error {
	if err := engine.completeSatisfiedParents(); err != nil {
		return err
	}
	engine.scheduleRecoveredExecutions(ctx)
	if err := engine.reconcileResults(ctx); err != nil {
		return err
	}
	if err := engine.emitSources(ctx); err != nil {
		return err
	}
	engine.scheduleOutboxes(ctx, now)
	return engine.publishContiguousCompletions()
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
