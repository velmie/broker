package azuresb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEmulatorDeadLetterRoutes(t *testing.T) {
	client, queue, topic, subscription := emulatorClient(t)
	for _, source := range emulatorSubqueueSources(client, queue, topic, subscription) {
		t.Run(source.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			id := sendSubqueueFixture(t, ctx, client, source.destination)
			seedDeadLetter(t, ctx, source, id)
			consumeSubqueueFixture(t, source, azservicebus.SubQueueDeadLetter, id, "fixture-rejection")
			assertEmulatorSubqueueEmpty(t, source, 0)
		})
	}
}

// This check establishes native transfer-subqueue addressing, not forwarding
// failure or transfer dead-letter production. Dedicated fixtures start empty.
func TestEmulatorTransferSubqueueAddresses(t *testing.T) {
	client, queue, topic, subscription := emulatorClient(t)
	for _, source := range emulatorSubqueueSources(client, queue, topic, subscription) {
		t.Run(source.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			consumer, err := adapter.NewConsumer(func() (adapter.Receiver, error) {
				return source.open(&azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueTransfer})
			}, adapter.ConsumerConfig{Source: source.name, ReceiveTimeout: 100 * time.Millisecond,
				OperationTimeout: time.Second, CloseTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			var callbacks int
			err = consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				callbacks++
				return nil, errors.New("expected an empty transfer-subqueue fixture")
			}))
			if !errors.Is(err, context.DeadlineExceeded) || callbacks != 0 {
				t.Fatalf("transfer-subqueue idle receive: callbacks=%d error=%v", callbacks, err)
			}
		})
	}
}

type emulatorSubqueueSource struct {
	name        string
	destination string
	open        func(*azservicebus.ReceiverOptions) (*azservicebus.Receiver, error)
}

func emulatorSubqueueSources(client *azservicebus.Client, queue, topic, subscription string) []emulatorSubqueueSource {
	return []emulatorSubqueueSource{
		{name: "queue", destination: queue, open: func(options *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
			return client.NewReceiverForQueue(queue, options)
		}},
		{name: "subscription", destination: topic, open: func(options *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
			return client.NewReceiverForSubscription(topic, subscription, options)
		}},
	}
}

func sendSubqueueFixture(t *testing.T, ctx context.Context, client *azservicebus.Client, destination string) string {
	t.Helper()
	id := fmt.Sprintf("subqueue-%d", time.Now().UnixNano())
	sender, err := client.NewSender(destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeEmulatorResource(t, sender.Close)
	if err = sender.SendMessage(ctx, &azservicebus.Message{MessageID: &id, Body: []byte(`{"amount":42}`)}, nil); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedDeadLetter(t *testing.T, ctx context.Context, source emulatorSubqueueSource, id string) {
	t.Helper()
	receiver, err := source.open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeEmulatorResource(t, receiver.Close)
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil || len(messages) != 1 {
		t.Fatalf("receive seed: count=%d error=%v", len(messages), err)
	}
	if messages[0].MessageID != id {
		t.Fatal("unexpected seed message")
	}
	reason, description := "fixture-rejection", "controlled native dead-letter fixture"
	err = receiver.DeadLetterMessage(ctx, messages[0], &azservicebus.DeadLetterOptions{
		Reason: &reason, ErrorDescription: &description,
	})
	if err != nil {
		t.Fatalf("native dead-letter: %v", err)
	}
}

func consumeSubqueueFixture(
	t *testing.T, source emulatorSubqueueSource, subqueue azservicebus.SubQueue, id, reason string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	options := &azservicebus.ReceiverOptions{SubQueue: subqueue}
	var callbacks, completions int
	consumer, err := adapter.NewConsumer(func() (adapter.Receiver, error) {
		return source.open(options)
	}, adapter.ConsumerConfig{Source: source.name, ReceiveTimeout: time.Second,
		OperationTimeout: 5 * time.Second, CloseTimeout: 5 * time.Second,
		Observer: &adapter.Observer{Record: func(_ context.Context, event adapter.Observation) {
			if event.Operation == adapter.OperationComplete && event.Outcome == adapter.OutcomeAccepted {
				completions++
				cancel()
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.Run(ctx, broker.NewTypedDeliveryHandler(broker.DecodeJSON[emulatorOrder],
		func(_ context.Context, delivery broker.Delivery, value emulatorOrder) (broker.Disposition, error) {
			callbacks++
			if delivery.Message().ID != id || value.Amount != 42 {
				return nil, errors.New("dead-letter identity or typed body differs")
			}
			metadata := delivery.(adapter.MetadataReader).Metadata()
			if metadata.ApplicationProperties["DeadLetterReason"] != reason {
				return nil, fmt.Errorf("dead-letter reason differs: %v", metadata.ApplicationProperties["DeadLetterReason"])
			}
			return broker.Handled{}, nil
		}))
	if !onlyEmulatorCancellation(err) || callbacks != 1 || completions != 1 {
		t.Fatalf("subqueue: callbacks=%d completions=%d error=%v", callbacks, completions, err)
	}
	assertEmulatorSubqueueEmpty(t, source, subqueue)
}

func assertEmulatorSubqueueEmpty(t *testing.T, source emulatorSubqueueSource, subqueue azservicebus.SubQueue) {
	t.Helper()
	receiver, err := source.open(&azservicebus.ReceiverOptions{SubQueue: subqueue})
	if err != nil {
		t.Fatal(err)
	}
	defer closeEmulatorResource(t, receiver.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if len(messages) != 0 || err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("subqueue empty readback: count=%d error=%v", len(messages), err)
	}
}
