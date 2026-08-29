package swim

import (
	"sync"
	"testing"
	"time"
)

func TestSubscriptionOverflowSignalsResyncWithoutBlocking(t *testing.T) {
	subscriptions := NewSubscriptions()
	id, events := subscriptions.Subscribe(1)

	first := membershipEvent(2, 1, Alive)
	second := membershipEvent(2, 1, Suspect)
	subscriptions.Publish(first)

	published := make(chan struct{})
	go func() {
		subscriptions.Publish(second)
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("publisher blocked behind a full subscriber")
	}

	if got := receiveEvent(t, events); got.Cause != EventResyncRequired {
		t.Fatalf("overflow event = %#v, want resync marker", got)
	}
	subscriptions.Publish(membershipEvent(2, 2, Alive))
	assertNoEvent(t, events)

	subscriptions.MarkResynchronized(id)
	want := membershipEvent(2, 2, Alive)
	subscriptions.Publish(want)
	if got := receiveEvent(t, events); got != want {
		t.Fatalf("event after resync = %#v, want %#v", got, want)
	}
}

func TestSubscriptionOverflowOnlyResynchronizesSlowSubscriber(t *testing.T) {
	subscriptions := NewSubscriptions()
	_, slow := subscriptions.Subscribe(1)
	_, fast := subscriptions.Subscribe(4)

	first := membershipEvent(4, 1, Alive)
	second := membershipEvent(4, 1, Dead)
	subscriptions.Publish(first)
	if got := receiveEvent(t, fast); got != first {
		t.Fatalf("first fast event = %#v, want %#v", got, first)
	}
	subscriptions.Publish(second)

	if got := receiveEvent(t, slow); got.Cause != EventResyncRequired {
		t.Fatalf("slow event = %#v, want resync marker", got)
	}
	if got := receiveEvent(t, fast); got != second {
		t.Fatalf("second fast event = %#v, want %#v", got, second)
	}
}

func TestSubscriptionUnsubscribeClosesOnlySelectedChannel(t *testing.T) {
	subscriptions := NewSubscriptions()
	firstID, first := subscriptions.Subscribe(2)
	_, second := subscriptions.Subscribe(2)

	subscriptions.Unsubscribe(firstID)
	subscriptions.Unsubscribe(firstID)
	if _, ok := <-first; ok {
		t.Fatal("unsubscribed channel is open")
	}

	want := membershipEvent(5, 3, Suspect)
	subscriptions.Publish(want)
	if got := receiveEvent(t, second); got != want {
		t.Fatalf("remaining subscriber event = %#v, want %#v", got, want)
	}
}

func TestSubscriptionsCloseIsIdempotentAndRejectsNewSubscribers(t *testing.T) {
	subscriptions := NewSubscriptions()
	_, existing := subscriptions.Subscribe(0)
	subscriptions.Close()
	subscriptions.Close()
	if _, ok := <-existing; ok {
		t.Fatal("existing subscriber remained open after Close")
	}

	id, afterClose := subscriptions.Subscribe(1)
	if id != 0 {
		t.Fatalf("subscription ID after Close = %d, want 0", id)
	}
	if _, ok := <-afterClose; ok {
		t.Fatal("subscriber created after Close is open")
	}
	subscriptions.Publish(membershipEvent(9, 1, Alive))
}

func TestSubscriptionsConcurrentPublishUnsubscribeAndClose(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		subscriptions := NewSubscriptions()
		const subscriberCount = 24
		ids := make([]uint64, 0, subscriberCount)
		var readers sync.WaitGroup
		for index := 0; index < subscriberCount; index++ {
			id, events := subscriptions.Subscribe(1 + index%3)
			ids = append(ids, id)
			readers.Add(1)
			go func() {
				defer readers.Done()
				for range events {
				}
			}()
		}

		start := make(chan struct{})
		var operations sync.WaitGroup
		for publisher := 0; publisher < 4; publisher++ {
			operations.Add(1)
			go func(nodeOffset int) {
				defer operations.Done()
				<-start
				for sequence := 1; sequence <= 200; sequence++ {
					subscriptions.Publish(membershipEvent(uint16(nodeOffset+1), uint64(sequence), Alive))
				}
			}(publisher)
		}
		operations.Add(2)
		go func() {
			defer operations.Done()
			<-start
			for _, id := range ids {
				subscriptions.MarkResynchronized(id)
			}
		}()
		go func() {
			defer operations.Done()
			<-start
			for index, id := range ids {
				if index%2 == 0 {
					subscriptions.Unsubscribe(id)
				}
			}
			subscriptions.Close()
		}()

		close(start)
		operations.Wait()
		subscriptions.Close()
		readers.Wait()
	}
}

func membershipEvent(nodeID uint16, incarnation uint64, status Status) MembershipEvent {
	return MembershipEvent{
		Current:    Member{NodeID: nodeID, Host: "node", BasePort: 8000, Incarnation: incarnation, Status: status},
		Cause:      EventMemberChanged,
		ReporterID: 1,
	}
}

func receiveEvent(t *testing.T, events <-chan MembershipEvent) MembershipEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("event channel closed unexpectedly")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for membership event")
		return MembershipEvent{}
	}
}

func assertNoEvent(t *testing.T, events <-chan MembershipEvent) {
	t.Helper()
	select {
	case event, ok := <-events:
		t.Fatalf("unexpected event after overflow: %#v (open=%v)", event, ok)
	default:
	}
}
