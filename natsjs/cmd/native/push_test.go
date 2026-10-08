package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
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
	entered, release := make(chan struct{}), make(chan struct{})
	consumer, err := natsjs.NewPushConsumer(connection, natsjs.ConsumerConfig{Stream: stream, Consumer: "held", Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	drained := make(chan error, 1)
	go func() {
		drained <- consumer.Run(runCtx, broker.NewTypedHandler(broker.DecodeJSON[pushCommand],
			func(context.Context, pushCommand) error { close(entered); <-release; return nil }))
	}()
	if _, err = js.Publish(subject, []byte(`{"id":"held"}`), nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	stop()
	select {
	case err = <-drained:
		t.Errorf("drain returned before callback joined: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err = <-drained:
		if !onlyCancellation(err) {
			t.Error(err)
		}
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
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
	var retained broker.Delivery
	consumer, err := natsjs.NewPushConsumer(connection, natsjs.ConsumerConfig{Stream: stream, Consumer: "rejected", Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		failures <- consumer.Run(ctx, broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			retained = delivery
			return broker.Handled{}, marker
		}))
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
	// Detached delivery metadata remains usable after Run and its subscription join.
	if retained == nil || string(retained.Message().Body) != `{}` || retained.Source() != stream+"/rejected" {
		t.Fatal("received data or source changed")
	}
	if got := retained.(natsjs.SubjectReader).Subject(); got != subject {
		t.Fatalf("subject = %q", got)
	}
	headers := retained.Message().Headers
	if len(headers) != 1 || headers[0].Name != "Example" || string(headers[0].Value) != "original" {
		t.Fatalf("received headers changed: %+v", headers)
	}
	metadata := retained.(natsjs.MetadataReader).Metadata()
	metadata.Stream = "changed"
	if retained.(natsjs.MetadataReader).Metadata().Stream != stream {
		t.Fatal("metadata aliases caller snapshot")
	}
	info, err := js.ConsumerInfo(stream, "rejected", nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("failed callback was acknowledged: %+v", info)
	}
}
