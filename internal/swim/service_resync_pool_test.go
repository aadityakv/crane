package swim

import (
	"context"
	"crane/internal/testutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crane/internal/config"
)

func TestSnapshotResyncUsesBoundedWorkerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	entered := make(chan struct{}, serviceResyncWorkers)
	var active atomic.Int32
	var maximum atomic.Int32
	loop := &serviceLoop{
		service:       &Service{events: make(chan serviceEvent, serviceEventQueueSize), done: make(chan struct{})},
		workerContext: ctx,
		workers:       &workers,
		resyncing:     make(map[uint16]bool),
		resyncJobs:    make(chan snapshotResyncJob, serviceResyncQueueSize),
		beginSnapshot: func(ctx context.Context, _ config.Endpoint, _ uint16) (*pendingSnapshot, error) {
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			active.Add(-1)
			return nil, ctx.Err()
		},
	}
	loop.startSnapshotResyncWorkers()

	for nodeID := uint16(1); nodeID <= 1_000; nodeID++ {
		loop.startSnapshotResync(Member{NodeID: nodeID, Host: "127.0.0.1", BasePort: 12000, Incarnation: 1, Status: Alive})
	}
	for worker := 0; worker < serviceResyncWorkers; worker++ {
		select {
		case <-entered:
		case <-time.After(testutil.Scale(time.Second)):
			t.Fatalf("resync worker %d did not start", worker+1)
		}
	}
	if got := maximum.Load(); got != serviceResyncWorkers {
		t.Fatalf("maximum concurrent resyncs = %d, want %d", got, serviceResyncWorkers)
	}
	if got := len(loop.resyncJobs); got != serviceResyncQueueSize {
		t.Fatalf("queued resyncs = %d, want bounded capacity %d", got, serviceResyncQueueSize)
	}
	if got, want := len(loop.resyncing), serviceResyncWorkers+serviceResyncQueueSize; got != want {
		t.Fatalf("tracked resyncs = %d, want active + queued = %d", got, want)
	}

	cancel()
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(testutil.Scale(time.Second)):
		t.Fatal("resync workers did not stop after cancellation")
	}
}
