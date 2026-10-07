package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

func TestNativeIteratorProfile(t *testing.T) {
	connection := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := runIterator(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if connection.IsClosed() {
		t.Fatal("iterator profile closed borrowed connection")
	}
}

func TestNativeIteratorCallbackFailureLeavesDeliveryUnconfirmed(t *testing.T) {
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
	subject := stream + ".orders"
	durable := "iterator_failure"
	if _,
		err = js.AddConsumer(stream,
		&nats.ConsumerConfig{Durable: durable,
			DeliverSubject: nats.NewInbox(),
			FilterSubject:  subject,
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Minute},
		nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	subscription, err := js.SubscribeSync(subject, nats.Bind(stream, durable))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := subscription.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if _, err = js.PublishMsg(&nats.Msg{Subject: subject, Data: []byte(`{"value":1}`),
		Header: nats.Header{"Example": []string{"original"}}}, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	message, err := subscription.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	marker := errors.New("application field value rejected")
	err = handleIterator(ctx, message, broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		detached := delivery.Message()
		message.Data[0] = 'X'
		message.Header.Set("Example", "mutated")
		if string(detached.Body) != `{"value":1}` || len(detached.Headers) != 1 || string(detached.Headers[0].Value) != "original" {
			t.Error("iterator application data aliases native message")
		}
		return broker.Handled{}, marker
	}))
	if !errors.Is(err, marker) {
		t.Fatalf("callback cause lost: %v", err)
	}
	info, err := js.ConsumerInfo(stream, durable, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("failed application message was confirmed: %+v", info)
	}
}

func TestNativeIteratorTerminalFailurePreservesCause(t *testing.T) {
	admin := nativeConnection(t)
	receiver := nativeConnection(t)
	js, err := admin.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		t.Fatal(err)
	}
	receiving, err := receiver.JetStream(nats.MaxWait(operationTimeout))
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
	subject := stream + ".terminal"
	if _,
		err = js.AddConsumer(stream,
		&nats.ConsumerConfig{Durable: "terminal",
			DeliverSubject: nats.NewInbox(),
			FilterSubject:  subject,
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Minute},
		nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	subscription, err := receiving.SubscribeSync(subject, nats.Bind(stream, "terminal"), nats.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(subject, []byte(`{}`), nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	message, err := subscription.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	effects := 0
	err = handleIterator(ctx, message, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		effects++
		receiver.Close()
		return broker.Handled{}, nil
	}))
	if !errors.Is(err, nats.ErrConnectionClosed) || effects != 1 {
		t.Fatalf("terminal cause/effect lost: %v count=%d", err, effects)
	}
	info, err := js.ConsumerInfo(stream, "terminal", nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("uncertain terminal operation took opposite action: %+v", info)
	}
}
