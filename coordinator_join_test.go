package broker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestCoordinatorStopsPeersOnErroneousReturnAfterDrain(t *testing.T) {
	primary, cleanup := errors.New("order failure"), errors.New("independent cleanup failure")
	known, allowReturn, peerReady, inspectPeer := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	peerState := make(chan error, 1)
	joined := make(chan struct{})
	coordinator := broker.NewCoordinator()
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil })
	first := consumer{validate: func() error { return nil }, run: func(context.Context) error {
		close(known)
		waitForSignal(t, allowReturn)
		return primary
	}}
	peer := consumer{validate: func() error { return nil }, run: func(ctx context.Context) error {
		close(peerReady)
		waitForSignal(t, inspectPeer)
		peerState <- ctx.Err()
		waitForSignal(t, ctx.Done())
		close(joined)
		return cleanup
	}}
	if err := coordinator.Add("failed", func() (broker.Consumer, error) { return first, nil }, h); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("peer", func() (broker.Consumer, error) { return peer, nil }, h); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	waitForSignal(t, known)
	waitForSignal(t, peerReady)
	close(inspectPeer)
	select {
	case err := <-peerState:
		if err != nil {
			t.Errorf("peer stopped before failed Run returned: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(allowReturn)
	select {
	case err := <-result:
		if !errors.Is(err, primary) || !errors.Is(err, cleanup) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-joined:
	default:
		t.Fatal("coordinator returned before peer joined")
	}
}

func TestFiniteSuccessfulConsumerDoesNotStopPeers(t *testing.T) {
	ready, finiteReturning, peerObserved := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	c := broker.NewCoordinator()
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil })
	finite := consumer{validate: func() error { return nil }, run: func(context.Context) error {
		waitForSignal(t, ready)
		close(finiteReturning)
		return nil
	}}
	peer := consumer{validate: func() error { return nil }, run: func(work context.Context) error {
		close(ready)
		waitForSignal(t, finiteReturning)
		if work.Err() != nil {
			t.Error("successful finite source stopped peer")
		}
		close(peerObserved)
		waitForSignal(t, work.Done())
		return work.Err()
	}}
	if err := c.Add("finite", func() (broker.Consumer, error) { return finite, nil }, h); err != nil {
		t.Fatal(err)
	}
	if err := c.Add("peer", func() (broker.Consumer, error) { return peer, nil }, h); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- c.Run(ctx) }()
	waitForSignal(t, peerObserved)
	stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator did not join")
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out at lifecycle barrier")
	}
}
