package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

func TestNativePushProfile(t *testing.T) {
	connection := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := runPush(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if connection.IsClosed() {
		t.Fatal("push profile closed borrowed connection")
	}
}

func TestNativePushDrainJoinsHeldCallback(t *testing.T) {
	connection := nativeConnection(t)
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	subject := stream + ".held"
	if _,
		err = js.AddConsumer(stream,
		&nats.ConsumerConfig{Durable: "held",
			DeliverSubject: nats.NewInbox(),
			FilterSubject:  subject,
			AckPolicy:      nats.AckExplicitPolicy},
		nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	callbackErr := make(chan error, 1)
	subscription, err := js.Subscribe(subject, func(message *nats.Msg) {
		callbackErr <- handlePush(ctx,
			message,
			broker.NewTypedHandler(broker.DecodeJSON[pushCommand],
				func(context.Context,
					pushCommand) error {
					close(entered)
					<-release
					return nil
				}))
	}, nats.Bind(stream, "held"), nats.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	subscription.SetClosedHandler(func(string) { close(closed) })
	if _, err = js.Publish(subject, []byte(`{"id":"held"}`), nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	drained := make(chan error, 1)
	go func() { drained <- drainPush(subscription, closed) }()
	select {
	case err = <-drained:
		t.Errorf("drain returned before callback joined: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err = <-drained:
		if err != nil {
			t.Error(err)
		}
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	if err = <-callbackErr; err != nil {
		t.Fatal(err)
	}
	info, err := js.ConsumerInfo(stream, "held", nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("callback effect was not followed by confirmed ACK: %+v", info)
	}
}

func TestNativePushApplicationFailureRetainsCauseAndDetachedData(t *testing.T) {
	connection := nativeConnection(t)
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	subject := stream + ".rejected"
	if _, err = js.AddConsumer(stream, &nats.ConsumerConfig{Durable: "rejected", DeliverSubject: nats.NewInbox(), FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy}, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	marker := errors.New("application order field rejected")
	failures := make(chan error, 1)
	subscription, err := js.Subscribe(subject, func(message *nats.Msg) {
		failures <- handlePush(ctx, message, broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			detached := delivery.Message()
			message.Data[0] = 'X'
			message.Header.Set("Example", "mutated")
			if string(detached.Body) != `{}` || len(detached.Headers) != 1 || string(detached.Headers[0].Value) != "original" {
				t.Error("push application data aliases native delivery")
			}
			return broker.Handled{}, marker
		}))
	}, nats.Bind(stream, "rejected"), nats.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	subscription.SetClosedHandler(func(string) { close(closed) })
	defer func() {
		if drainErr := drainPush(subscription, closed); drainErr != nil {
			t.Error(drainErr)
		}
	}()
	if _, err = js.PublishMsg(&nats.Msg{Subject: subject, Data: []byte(`{}`),
		Header: nats.Header{"Example": []string{"original"}}}, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-failures:
		if !errors.Is(err, marker) {
			t.Fatalf("callback cause lost: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	info, err := js.ConsumerInfo(stream, "rejected", nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("failed callback was acknowledged: %+v", info)
	}
}
