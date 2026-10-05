package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNativeAsyncPublishSession(t *testing.T) {
	conn := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runAsyncPublish(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := conn.FlushWithContext(ctx); err != nil {
		t.Fatalf("borrowed connection unusable: %v", err)
	}
}

func TestNativeFutureFailureRetainsCause(t *testing.T) {
	conn := nativeConnection(t)
	js, err := conn.JetStream(nats.PublishAsyncTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer js.CleanupPublisher()
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	}()
	future, err := js.PublishAsync(stream+".events", []byte("payload"), nats.ExpectStream("not-the-stream"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = awaitPublish(ctx, future)
	var api *nats.APIError
	if !errors.As(err, &api) || api.ErrorCode != 10060 {
		t.Fatalf("expected original native stream mismatch: %v", err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err == nil {
		t.Fatal("completion hid individual failure")
	}
}

func TestNativeFutureCleanupResolvesPending(t *testing.T) {
	conn := nativeConnection(t)
	received := make(chan struct{}, 1)
	subject := nats.NewInbox()
	sub, err := conn.Subscribe(subject, func(*nats.Msg) { received <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cleanupErr := sub.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	if err = conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(1))
	if err != nil {
		t.Fatal(err)
	}
	defer js.CleanupPublisher()
	pending, err := js.PublishAsync(subject, []byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = js.PublishAsync(subject, []byte("blocked"), nats.StallWait(20*time.Millisecond))
	if !errors.Is(err, nats.ErrTooManyStalledMsgs) {
		t.Fatalf("bounded native backpressure: %v", err)
	}
	js.CleanupPublisher()
	_, err = awaitPublish(ctx, pending)
	if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
		t.Fatalf("cleanup lost pending cause: %v", err)
	}
	if js.PublishAsyncPending() != 0 {
		t.Fatal("cleanup retained pending publications")
	}
	if err = conn.FlushWithContext(ctx); err != nil {
		t.Fatalf("cleanup closed borrowed connection: %v", err)
	}
}
