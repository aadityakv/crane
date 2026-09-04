package swim

import (
	"container/heap"
	"time"

	"github.com/aadityakv/crane/internal/wire"
)

type serviceTimerKey struct {
	probe     bool
	kind      TimerKind
	originID  uint16
	nodeID    uint16
	sequence  uint64
	requestID wire.RequestID
}

type scheduledServiceTimer struct {
	key      serviceTimerKey
	deadline time.Time
	event    timerServiceEvent
	order    uint64
	index    int
}

type serviceTimerHeap []*scheduledServiceTimer

func (h serviceTimerHeap) Len() int { return len(h) }

func (h serviceTimerHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].order < h[j].order
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h serviceTimerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *serviceTimerHeap) Push(value any) {
	timer := value.(*scheduledServiceTimer)
	timer.index = len(*h)
	*h = append(*h, timer)
}

func (h *serviceTimerHeap) Pop() any {
	old := *h
	last := len(old) - 1
	timer := old[last]
	old[last] = nil
	timer.index = -1
	*h = old[:last]
	return timer
}

type serviceTimerScheduler struct {
	queue   serviceTimerHeap
	entries map[serviceTimerKey]*scheduledServiceTimer
	order   uint64
}

func (s *serviceTimerScheduler) upsert(key serviceTimerKey, deadline time.Time, event timerServiceEvent) {
	if s.entries == nil {
		s.entries = make(map[serviceTimerKey]*scheduledServiceTimer)
	}
	s.order++
	if s.order == 0 {
		s.order++
	}
	if current, exists := s.entries[key]; exists {
		current.deadline = deadline
		current.event = event
		current.order = s.order
		heap.Fix(&s.queue, current.index)
		return
	}
	timer := &scheduledServiceTimer{key: key, deadline: deadline, event: event, order: s.order}
	s.entries[key] = timer
	heap.Push(&s.queue, timer)
}

func (s *serviceTimerScheduler) cancel(key serviceTimerKey) {
	current, exists := s.entries[key]
	if !exists {
		return
	}
	delete(s.entries, key)
	heap.Remove(&s.queue, current.index)
}

func (s *serviceTimerScheduler) nextDeadline() (time.Time, bool) {
	if len(s.queue) == 0 {
		return time.Time{}, false
	}
	return s.queue[0].deadline, true
}

func (s *serviceTimerScheduler) popDue(now time.Time) []timerServiceEvent {
	var events []timerServiceEvent
	for len(s.queue) > 0 && !s.queue[0].deadline.After(now) {
		current := heap.Pop(&s.queue).(*scheduledServiceTimer)
		delete(s.entries, current.key)
		events = append(events, current.event)
	}
	return events
}

func probeCycleTimerKey() serviceTimerKey {
	return serviceTimerKey{probe: true}
}

func serviceTimerKeyFor(request TimerRequest) serviceTimerKey {
	switch request.Kind {
	case TimerDirectProbe, TimerIndirectProbe:
		return serviceTimerKey{kind: TimerDirectProbe, sequence: request.Sequence, requestID: request.RequestID}
	case TimerRelayProbe:
		return serviceTimerKey{kind: TimerRelayProbe, originID: request.OriginID, sequence: request.Sequence, requestID: request.RequestID}
	case TimerSuspicion:
		return serviceTimerKey{kind: TimerSuspicion, nodeID: request.NodeID}
	case TimerTombstone:
		return serviceTimerKey{kind: TimerTombstone, nodeID: request.NodeID}
	default:
		return serviceTimerKey{kind: request.Kind, nodeID: request.NodeID, originID: request.OriginID, sequence: request.Sequence, requestID: request.RequestID}
	}
}

func (l *serviceLoop) timerChannel() <-chan time.Time {
	if l.clockTimer == nil {
		return nil
	}
	return l.clockTimer.C()
}

func (l *serviceLoop) armClockTimer() {
	deadline, exists := l.timerScheduler.nextDeadline()
	if l.clockTimer != nil {
		l.clockTimer.Stop()
		l.clockTimer = nil
	}
	if !exists {
		return
	}
	duration := deadline.Sub(l.service.options.Clock.Now())
	if duration < 0 {
		duration = 0
	}
	l.clockTimer = l.service.options.Clock.NewTimer(duration)
}

func (l *serviceLoop) stopClockTimer() {
	if l.clockTimer != nil {
		l.clockTimer.Stop()
		l.clockTimer = nil
	}
}

func (l *serviceLoop) scheduleClockEvent(key serviceTimerKey, deadline time.Time, event timerServiceEvent) {
	l.timerScheduler.upsert(key, deadline, event)
	l.armClockTimer()
}

func (l *serviceLoop) cancelClockEvent(key serviceTimerKey) {
	l.timerScheduler.cancel(key)
	l.armClockTimer()
}

func (l *serviceLoop) dispatchDueTimers() error {
	l.clockTimer = nil
	events := l.timerScheduler.popDue(l.service.options.Clock.Now())
	for _, event := range events {
		if err := l.handleTimer(event); err != nil {
			return err
		}
	}
	l.armClockTimer()
	return nil
}
