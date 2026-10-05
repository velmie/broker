package azuresb_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

type emulatorOrder struct {
	Amount int `json:"amount"`
}

func TestEmulatorTypedRoutes(t *testing.T) {
	client, queue, topic, subscription := emulatorClient(t)
	for _, route := range []emulatorRoute{
		{name: "queue", mode: azservicebus.ReceiveModePeekLock},
		{name: "subscription", topic: true, mode: azservicebus.ReceiveModePeekLock},
		{name: "receive-and-delete", mode: azservicebus.ReceiveModeReceiveAndDelete},
	} {
		t.Run(route.name, func(t *testing.T) {
			exerciseEmulatorRoute(t, client, queue, topic, subscription, route)
		})
	}
}

type emulatorRoute struct {
	name  string
	topic bool
	mode  azservicebus.ReceiveMode
}

//nolint:gocyclo // One real-service route checks publication, two delivery modes, native retry and confirmed cleanup together.
func exerciseEmulatorRoute(t *testing.T, client *azservicebus.Client, queue, topic, subscription string, route emulatorRoute) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	destination := queue
	if route.topic {
		destination = topic
	}
	sender, err := client.NewSender(destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, sender.Close) })
	options := azservicebus.ReceiverOptions{ReceiveMode: route.mode}
	newReceiver := func() (adapter.Receiver, error) {
		if route.topic {
			return client.NewReceiverForSubscription(topic, subscription, &options)
		}
		return client.NewReceiverForQueue(queue, &options)
	}
	publisher, err := adapter.NewPublisher(sender, adapter.PublisherConfig{Source: route.name, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("order-%d", time.Now().UnixNano())
	publish := broker.NewTypedPublisher[emulatorOrder](broker.EncoderFunc(json.Marshal), publisher,
		func(context.Context, emulatorOrder) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: id, Headers: []broker.Header{
				{Name: "Correlation-Id", Value: []byte("correlation-example")},
				{Name: "binary-base64", Value: []byte(base64.StdEncoding.EncodeToString([]byte{0xff, 0, 1}))},
			}}, nil
		})
	if err = publish(ctx, emulatorOrder{Amount: 42}); err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var calls uint64
	var completed, abandoned int
	var retained broker.Message
	observer := &adapter.Observer{Record: func(_ context.Context, observation adapter.Observation) {
		if observation.Outcome != adapter.OutcomeAccepted {
			return
		}
		switch observation.Operation {
		case adapter.OperationComplete:
			completed++
			if calls != 2 {
				t.Error("completion preceded successful second effect")
			}
			stop()
		case adapter.OperationAbandon:
			abandoned++
		}
	}}
	consumer, err := adapter.NewConsumer(newReceiver, adapter.ConsumerConfig{
		Source: route.name, ReceiveMode: route.mode, ReceiveTimeout: time.Second,
		OperationTimeout: 5 * time.Second, CloseTimeout: 5 * time.Second, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	requirements := []broker.Requirement{broker.AttemptsRequirement{}}
	if route.mode == azservicebus.ReceiveModePeekLock {
		requirements = append(requirements, broker.RedeliveryRequirement{})
	}
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[emulatorOrder],
		func(_ context.Context, delivery broker.Delivery, value emulatorOrder) (broker.Disposition, error) {
			calls++
			retained = delivery.Message()
			if deliveryErr := checkEmulatorDelivery(delivery, value, id, calls); deliveryErr != nil {
				return nil, deliveryErr
			}
			if route.mode == azservicebus.ReceiveModeReceiveAndDelete {
				stop()
				return broker.Handled{}, nil
			}
			if calls == 1 {
				return broker.RetryAfter{Cause: errors.New("synthetic transient effect")}, nil
			}
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirements...))
	if err = consumer.Run(runCtx, handler); !onlyEmulatorCancellation(err) {
		t.Fatalf("joined run: %v", err)
	}
	if route.mode == azservicebus.ReceiveModePeekLock {
		if calls != 2 || completed != 1 || abandoned != 1 {
			t.Fatalf("calls=%d complete=%d abandon=%d", calls, completed, abandoned)
		}
	} else if calls != 1 || completed != 0 || abandoned != 0 {
		t.Fatalf("destructive receive settled again: calls=%d complete=%d abandon=%d", calls, completed, abandoned)
	}
	if retained.ID != id || !bytes.Contains(retained.Body, []byte("42")) {
		t.Fatal("retained application data changed after receiver closure")
	}
	check, err := newReceiver()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, check.Close) })
	emptyCtx, stopEmpty := context.WithTimeout(ctx, 500*time.Millisecond)
	defer stopEmpty()
	messages, receiveErr := check.ReceiveMessages(emptyCtx, 1, nil)
	if len(messages) != 0 || receiveErr != nil && !errors.Is(receiveErr, context.DeadlineExceeded) {
		t.Fatalf("post-completion readback: count=%d cause=%v", len(messages), receiveErr)
	}
}

func emulatorClient(t *testing.T) (client *azservicebus.Client, queue, topic, subscription string) {
	t.Helper()
	connection := os.Getenv("AZURESB_TEST_CONNECTION_STRING")
	if connection == "" {
		t.Skip("set AZURESB_TEST_CONNECTION_STRING and dedicated emulator entity names to run Service Bus transport checks")
	}
	if !strings.Contains(strings.ToLower(connection), "usedevelopmentemulator=true") {
		t.Fatal("transport fixture requires an explicitly configured development emulator")
	}
	queue, topic, subscription = os.Getenv("AZURESB_TEST_QUEUE"), os.Getenv("AZURESB_TEST_TOPIC"), os.Getenv("AZURESB_TEST_SUBSCRIPTION")
	if queue == "" || topic == "" || subscription == "" {
		t.Fatal("AZURESB_TEST_QUEUE, AZURESB_TEST_TOPIC and AZURESB_TEST_SUBSCRIPTION are required dedicated fixtures")
	}
	client, err := azservicebus.NewClientFromConnectionString(connection,
		&azservicebus.ClientOptions{RetryOptions: azservicebus.RetryOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, client.Close) })
	return client, queue, topic, subscription
}

func closeEmulatorResource(t *testing.T, closeResource func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := closeResource(ctx); err != nil {
		t.Errorf("owned SDK resource close: %v", err)
	}
}

func onlyEmulatorCancellation(err error) bool {
	if err == nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !onlyEmulatorCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyEmulatorCancellation(wrapped.Unwrap())
	}
	return err == context.Canceled
}

func checkEmulatorDelivery(delivery broker.Delivery, value emulatorOrder, id string, calls uint64) error {
	message := delivery.Message()
	if value.Amount != 42 || message.ID != id {
		return errors.New("typed payload or logical identity differs")
	}
	var binaryFound, correlationFound bool
	for _, header := range message.Headers {
		if header.Name == "binary-base64" {
			decoded, err := base64.StdEncoding.DecodeString(string(header.Value))
			if err != nil {
				return fmt.Errorf("binary-base64: %w", err)
			}
			binaryFound = bytes.Equal(decoded, []byte{0xff, 0, 1})
		}
		if header.Name == "Correlation-Id" {
			correlationFound = string(header.Value) == "correlation-example"
		}
	}
	if !binaryFound || !correlationFound {
		return errors.New("native metadata round trip differs")
	}
	attempt := delivery.(broker.AttemptReader).Attempt()
	if !attempt.Known || attempt.Number != calls {
		return fmt.Errorf("unexpected native attempt: %+v", attempt)
	}
	return nil
}
