package azuresb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEmulatorNativeSchedulingAndDeferredProcessing(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sender := nativeEmulatorSender(t, client, queue)
	receiver, err := client.NewReceiverForQueue(queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, receiver.Close) })
	native := nativeEmulatorMessage(t)
	subject, contentType := "order.created", "application/json"
	native.Subject, native.ContentType = &subject, &contentType
	sequences, err := sender.ScheduleMessages(ctx, []*azservicebus.Message{native}, time.Now().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 1 {
		t.Fatalf("scheduled sequences: %v", sequences)
	}
	received, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := onlyNativeMessage(t, received)
	checkNativeMessageControls(t, message, subject, contentType)
	var snapshot adapter.Metadata
	processor, err := adapter.NewProcessor(broker.NewTypedDeliveryHandler(broker.DecodeJSON[emulatorOrder],
		func(_ context.Context, delivery broker.Delivery, order emulatorOrder) (broker.Disposition, error) {
			if order.Amount != 42 || delivery.Source() != queue || delivery.Message().ID != *native.MessageID {
				t.Error("typed processing lost message data or source")
			}
			snapshot = delivery.(adapter.MetadataReader).Metadata()
			return broker.Handled{}, nil
		}), queue)
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(ctx, message); err != nil {
		t.Fatal(err)
	}
	if message.SequenceNumber == nil || snapshot.SequenceNumber != *message.SequenceNumber {
		t.Fatal("sequence metadata lost")
	}
	snapshot.ApplicationProperties["id"] = "detached"
	if message.ApplicationProperties["id"] != *native.MessageID {
		t.Fatal("metadata mutation changed native message")
	}
	// Defer succeeds only while the original SDK delivery retains settlement authority.
	if err = receiver.DeferMessage(ctx, message, nil); err != nil {
		t.Fatal(err)
	}
	deferred, err := receiver.ReceiveDeferredMessages(ctx, []int64{*message.SequenceNumber}, nil)
	if err != nil {
		t.Fatal(err)
	}
	restored := onlyNativeMessage(t, deferred)
	if restored.MessageID != message.MessageID {
		t.Fatal("deferred receive returned a different message")
	}
	if err = receiver.CompleteMessage(ctx, restored, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEmulatorNativeSessionProcessing(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	sessionQueue := os.Getenv("AZURESB_TEST_SESSION_QUEUE")
	if sessionQueue == queue {
		t.Fatal("session fixture must use a separate session-enabled queue")
	}
	queue = sessionQueue
	if queue == "" {
		t.Skip("set AZURESB_TEST_SESSION_QUEUE to a dedicated session-enabled fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sender := nativeEmulatorSender(t, client, queue)
	native := nativeEmulatorMessage(t)
	session := fmt.Sprintf("session-%d", time.Now().UnixNano())
	native.SessionID = &session
	if err := sender.SendMessage(ctx, native, nil); err != nil {
		t.Fatal(err)
	}
	receiver, err := client.AcceptSessionForQueue(ctx, queue, session, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, receiver.Close) })
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := onlyNativeMessage(t, messages)
	calls := 0
	processor, err := adapter.NewProcessor(broker.NewTypedHandler(broker.DecodeJSON[emulatorOrder],
		func(_ context.Context, order emulatorOrder) error {
			calls++
			if order.Amount != 42 {
				t.Error("session payload changed")
			}
			return nil
		}), queue)
	if err != nil {
		t.Fatal(err)
	}
	// The caller renews the session lock, not the individual message lock.
	if err = receiver.RenewSessionLock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(ctx, message); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || message.SessionID == nil || *message.SessionID != session {
		t.Fatal("session processing changed identity")
	}
	if err = receiver.CompleteMessage(ctx, message, nil); err != nil {
		t.Fatal(err)
	}
	// The borrowed client remains usable for native batching after processing.
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.AddMessage(native, nil); err != nil {
		t.Fatal(err)
	}
	if err = sender.SendMessageBatch(ctx, batch, nil); err != nil {
		t.Fatal(err)
	}
	messages, err = receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = receiver.CompleteMessage(ctx, onlyNativeMessage(t, messages), nil); err != nil {
		t.Fatal(err)
	}
}

func nativeEmulatorSender(t *testing.T, client *azservicebus.Client, queue string) *azservicebus.Sender {
	t.Helper()
	sender, err := client.NewSender(queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, sender.Close) })
	return sender
}

func nativeEmulatorMessage(t *testing.T) *azservicebus.Message {
	t.Helper()
	message, err := adapter.EncodeMessage(broker.Message{ID: fmt.Sprintf("native-%d", time.Now().UnixNano()), Body: []byte(`{"amount":42}`)})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func onlyNativeMessage(t *testing.T, messages []*azservicebus.ReceivedMessage) *azservicebus.ReceivedMessage {
	t.Helper()
	if len(messages) != 1 {
		t.Fatalf("received %d messages, want one", len(messages))
	}
	return messages[0]
}

func checkNativeMessageControls(t *testing.T, message *azservicebus.ReceivedMessage, subject, contentType string) {
	t.Helper()
	if message.Subject == nil || *message.Subject != subject || message.ContentType == nil || *message.ContentType != contentType {
		t.Fatal("native publication controls were lost")
	}
}
