package azuresb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEmulatorConcurrentPublicationAndBorrowedOwnership(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sender, err := client.NewSender(queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, sender.Close) })
	publisher, err := adapter.NewPublisher(sender, adapter.PublisherConfig{Source: "concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	const count = 12
	failures := make(chan error, count)
	expected := make(map[string][]byte, count)
	var workers sync.WaitGroup
	for index := range count {
		id := fmt.Sprintf("concurrent-%d-%d", time.Now().UnixNano(), index)
		body := []byte{0, 0xff, 1}
		expected[id] = body
		workers.Add(1)
		go func() {
			defer workers.Done()
			failures <- publisher.Publish(ctx, broker.Message{ID: id, Body: body})
		}()
	}
	workers.Wait()
	close(failures)
	for publishErr := range failures {
		if publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	completed := 0
	newReceiver := func() (adapter.Receiver, error) { return client.NewReceiverForQueue(queue, nil) }
	consumer, err := adapter.NewConsumer(newReceiver, adapter.ConsumerConfig{
		Source: "concurrent", ReceiveTimeout: time.Second,
		Observer: &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
			if o.Operation == adapter.OperationComplete && o.Outcome == adapter.OutcomeAccepted {
				completed++
				if completed == count {
					stop()
				}
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.Run(runCtx, broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
		message := d.Message()
		body, ok := expected[message.ID]
		if !ok || !bytes.Equal(body, message.Body) {
			return nil, errors.New("concurrent logical ID/body differs or duplicated")
		}
		delete(expected, message.ID)
		return broker.Handled{}, nil
	}))
	if !onlyEmulatorCancellation(err) || completed != count || len(expected) != 0 {
		t.Fatalf("concurrent roundtrip completed=%d remaining=%d: %v", completed, len(expected), err)
	}
	verifyBorrowedResources(t, ctx, publisher, newReceiver)
}

func TestEmulatorNativeFailuresPreserveSDKCause(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entity := fmt.Sprintf("%s-missing-%d", queue, time.Now().UnixNano())
	sender, err := client.NewSender(entity, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, sender.Close) })
	publisher, err := adapter.NewPublisher(sender, adapter.PublisherConfig{Source: "missing-source"})
	if err != nil {
		t.Fatal(err)
	}
	assertMissingEntityFailure(t, publisher.Publish(ctx, broker.Message{ID: "missing-entity", Body: []byte("body")}), adapter.OperationPublish)
	consumer, err := adapter.NewConsumer(
		func() (adapter.Receiver, error) { return client.NewReceiverForQueue(entity, nil) },
		adapter.ConsumerConfig{Source: "missing-source"})
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("missing entity invoked callback")
		return broker.Handled{}, nil
	}))
	assertMissingEntityFailure(t, err, adapter.OperationReceive)
}

func assertMissingEntityFailure(t *testing.T, err error, operation string) {
	t.Helper()
	var native *amqp.Error
	var contextual *adapter.OperationError
	if !errors.As(err, &native) || native.Condition != amqp.ErrCondNotFound || !errors.As(err, &contextual) ||
		contextual.Operation != operation || contextual.Source != "missing-source" {
		t.Fatalf("native type/code and operation/source must survive: %v", err)
	}
	if !errors.Is(err, native) {
		t.Fatal("native object identity lost")
	}
}

func verifyBorrowedResources(
	t *testing.T, ctx context.Context, publisher *adapter.Publisher, newReceiver func() (adapter.Receiver, error),
) {
	t.Helper()
	var err error
	// A completed Run closed only its receiver. Its caller-owned client and
	// the publisher's borrowed sender still perform native operations.
	if err = publisher.Publish(ctx, broker.Message{ID: "borrowed-resources", Body: []byte("still-open")}); err != nil {
		t.Fatal(err)
	}
	check, err := newReceiver()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, check.Close) })
	messages, err := check.ReceiveMessages(ctx, 1, nil)
	if err != nil || len(messages) != 1 {
		t.Fatalf("borrowed client read: count=%d cause=%v", len(messages), err)
	}
	if messages[0].MessageID != "borrowed-resources" {
		t.Fatal("unexpected readback identity")
	}
	if err = check.CompleteMessage(ctx, messages[0], nil); err != nil {
		t.Fatal(err)
	}
}
