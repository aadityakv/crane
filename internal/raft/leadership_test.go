package raft

import (
	"context"
	"errors"
	"testing"
)

func TestLeadershipSubscriptionSnapshotThenStreamHasNoGapAndSuppressesSameState(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancel, runResult := startTask8Follower(t, options)
	defer stopTask8Node(t, cancel, runResult)

	subscription, err := node.SubscribeLeadership(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := subscription.Snapshot(), (LeadershipEvent{Sequence: 1, Role: RoleFollower, LocalID: 1}); got != want {
		t.Fatalf("leadership snapshot = %#v, want %#v", got, want)
	}
	request := AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1}
	if err := node.SubmitRPC(context.Background(), 2, request); err != nil {
		t.Fatal(err)
	}
	if got, want := <-subscription.Events(), (LeadershipEvent{Sequence: 2, Term: 1, Role: RoleFollower, LeaderID: 2, LocalID: 1}); got != want {
		t.Fatalf("leadership delta = %#v, want %#v", got, want)
	}
	if err := node.SubmitRPC(context.Background(), 2, request); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-subscription.Events():
		t.Fatalf("identical leadership state emitted delta %#v", event)
	default:
	}
}

func TestLeadershipSubscriptionOverflowRequiresExplicitResynchronization(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancel, runResult := startTask8Follower(t, options)
	defer stopTask8Node(t, cancel, runResult)

	subscription, err := node.SubscribeLeadership(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SubmitRPC(context.Background(), 2, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := node.SubmitRPC(context.Background(), 3, AppendEntriesRequest{LeaderID: 3, Term: 2, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	<-subscription.Done()
	if !errors.Is(subscription.Err(), ErrLeadershipResyncRequired) {
		t.Fatalf("subscription error = %v, want ErrLeadershipResyncRequired", subscription.Err())
	}
	if got := <-subscription.Events(); got.Sequence != 2 || got.Term != 1 {
		t.Fatalf("buffered delta before overflow = %#v, want sequence 2 term 1", got)
	}
	if _, open := <-subscription.Events(); open {
		t.Fatal("leadership event stream remained open after overflow")
	}
}

func TestLeadershipSubscriptionCapacityUnsubscribeContextAndShutdown(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancelNode, runResult := startTask8Follower(t, options)
	for _, capacity := range []int{0, MaxLeadershipSubscriptionCapacity + 1} {
		if _, err := node.SubscribeLeadership(context.Background(), capacity); !errors.Is(err, ErrInvalidLeadershipCapacity) {
			t.Fatalf("SubscribeLeadership capacity %d error = %v", capacity, err)
		}
	}

	unsubscribed, err := node.SubscribeLeadership(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	unsubscribed.Unsubscribe()
	<-unsubscribed.Done()
	if unsubscribed.Err() != nil {
		t.Fatalf("unsubscribed error = %v, want nil", unsubscribed.Err())
	}

	subscriptionCtx, cancelSubscription := context.WithCancel(context.Background())
	canceled, err := node.SubscribeLeadership(subscriptionCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	cancelSubscription()
	<-canceled.Done()
	if !errors.Is(canceled.Err(), context.Canceled) {
		t.Fatalf("context-canceled subscription error = %v", canceled.Err())
	}

	shutdown, err := node.SubscribeLeadership(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	cancelNode()
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
	<-shutdown.Done()
	if !errors.Is(shutdown.Err(), ErrStopped) {
		t.Fatalf("shutdown subscription error = %v, want ErrStopped", shutdown.Err())
	}
}

func TestLeadershipSequenceExhaustionFailsClosedWithoutWrappedDelta(t *testing.T) {
	options, _, _, _ := task8NodeOptions(t, RecoveredState{})
	node, cancel, runResult := startTask8Follower(t, options)
	defer cancel()
	node.leadership.Sequence = ^uint64(0)
	rpcResult := make(chan error, 1)
	go func() {
		rpcResult <- node.SubmitRPC(context.Background(), 2, AppendEntriesRequest{LeaderID: 2, Term: 1, Generation: 1})
	}()
	if err := <-runResult; !errors.Is(err, ErrLeadershipSequenceOverflow) {
		t.Fatalf("Run error = %v, want ErrLeadershipSequenceOverflow", err)
	}
	if err := <-rpcResult; !errors.Is(err, ErrStopped) {
		t.Fatalf("SubmitRPC error = %v, want ErrStopped after terminal exhaustion", err)
	}
}

func startTask8Follower(t *testing.T, options NodeOptions) (*Node, context.CancelFunc, <-chan error) {
	t.Helper()
	node, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- node.Run(ctx) }()
	<-node.Ready()
	return node, cancel, runResult
}
