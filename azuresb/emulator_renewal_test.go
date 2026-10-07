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

func TestEmulatorLockRenewal(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	id := publishLockFixture(t, client, queue)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var renewals, completions int
	consumer, err := adapter.NewConsumer(func() (adapter.Receiver, error) {
		return client.NewReceiverForQueue(queue, nil)
	}, adapter.ConsumerConfig{Source: "lock-renewal", ReceiveTimeout: time.Second,
		OperationTimeout: 5 * time.Second, CloseTimeout: 5 * time.Second,
		Observer: &adapter.Observer{Record: func(_ context.Context, event adapter.Observation) {
			if event.Outcome != adapter.OutcomeAccepted {
				return
			}
			switch event.Operation {
			case "renew":
				renewals++
			case adapter.OperationComplete:
				completions++
				cancel()
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var originalExpiry time.Time
	err = consumer.Run(ctx, broker.NewHandler(func(work context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		if delivery.Message().ID != id {
			return nil, errors.New("unexpected fixture message")
		}
		metadata := delivery.(adapter.MetadataReader)
		originalExpiry = metadata.Metadata().LockedUntil
		remaining := time.Until(originalExpiry)
		if remaining < 10*time.Second || remaining > 75*time.Second {
			return nil, fmt.Errorf("fixture requires a live lock of 10..75s, remaining %s", remaining)
		}
		timer := time.NewTimer(remaining + 250*time.Millisecond)
		defer timer.Stop()
		select {
		case <-work.Done():
			return nil, context.Cause(work)
		case <-timer.C:
		}
		if metadata.Metadata().LockedUntil != originalExpiry {
			return nil, errors.New("native renewal mutated the delivery snapshot")
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: 5 * time.Second})))
	if !onlyEmulatorCancellation(err) {
		t.Fatalf("renewed route: %v", err)
	}
	if renewals == 0 || completions != 1 || originalExpiry.IsZero() || time.Now().Before(originalExpiry) {
		t.Fatalf("renewals=%d completions=%d original expiry=%v", renewals, completions, originalExpiry)
	}
	assertEmulatorQueueEmpty(t, client, queue)
}

func TestEmulatorLockLossSuppressesSettlement(t *testing.T) {
	client, queue, _, _ := emulatorClient(t)
	id := publishLockFixture(t, client, queue)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var receiver *expiredLockReceiver
	consumer, err := adapter.NewConsumer(func() (adapter.Receiver, error) {
		native, err := client.NewReceiverForQueue(queue, nil)
		if err != nil {
			return nil, err
		}
		receiver = &expiredLockReceiver{Receiver: native}
		return receiver, nil
	}, adapter.ConsumerConfig{Source: "lock-loss", ReceiveTimeout: time.Second,
		OperationTimeout: 75 * time.Second, CloseTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	err = consumer.Run(ctx, broker.NewHandler(func(work context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		calls++
		if delivery.Message().ID != id {
			return nil, errors.New("unexpected fixture message")
		}
		<-work.Done()
		// A successful application return after cancellation cannot authorize a
		// terminal operation on the lost lock.
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
	var nativeError *azservicebus.Error
	if receiver == nil || receiver.failure == nil || !errors.Is(err, receiver.failure) ||
		!errors.As(err, &nativeError) || nativeError.Code != azservicebus.CodeLockLost {
		t.Fatalf("expected original native lock-loss error: %v", err)
	}
	if calls != 1 || receiver.terminals != 0 {
		t.Fatalf("callbacks=%d native terminals=%d", calls, receiver.terminals)
	}
	cleanupLockFixture(t, client, queue, id)
	assertEmulatorQueueEmpty(t, client, queue)
}

// The fixture delays the first actual renewal until the original lock expires.
// The SDK call then obtains a real service error rather than a fabricated cause.
type expiredLockReceiver struct {
	*azservicebus.Receiver
	failure   error
	terminals int
}

func (r *expiredLockReceiver) RenewMessageLock(
	ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.RenewMessageLockOptions,
) error {
	if message.LockedUntil == nil {
		return errors.New("fixture requires a native lock deadline")
	}
	remaining := time.Until(*message.LockedUntil)
	if remaining < 0 || remaining > 75*time.Second {
		return errors.New("fixture requires a live lock no longer than 75s")
	}
	timer := time.NewTimer(remaining + 250*time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
	}
	r.failure = r.Receiver.RenewMessageLock(ctx, message, options)
	return r.failure
}

func (r *expiredLockReceiver) CompleteMessage(
	ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions,
) error {
	r.terminals++
	return r.Receiver.CompleteMessage(ctx, message, options)
}

func (r *expiredLockReceiver) AbandonMessage(
	ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.AbandonMessageOptions,
) error {
	r.terminals++
	return r.Receiver.AbandonMessage(ctx, message, options)
}

func publishLockFixture(t *testing.T, client *azservicebus.Client, queue string) string {
	t.Helper()
	id := fmt.Sprintf("lock-%d", time.Now().UnixNano())
	sender, err := client.NewSender(queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeEmulatorResource(t, sender.Close) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = sender.SendMessage(ctx, &azservicebus.Message{MessageID: &id, Body: []byte(`{"amount":42}`)}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupLockFixture(t, client, queue, id) })
	return id
}

func cleanupLockFixture(t *testing.T, client *azservicebus.Client, queue, id string) {
	t.Helper()
	receiver, err := client.NewReceiverForQueue(queue, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeEmulatorResource(t, receiver.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Error(err)
		return
	}
	for _, message := range messages {
		if message.MessageID != id {
			t.Error("cleanup found an unrelated message in the dedicated queue")
			return
		}
		settle, stop := context.WithTimeout(context.Background(), 5*time.Second)
		err = receiver.CompleteMessage(settle, message, nil)
		stop()
		if err != nil {
			t.Errorf("fixture cleanup: %v", err)
		}
	}
}

func assertEmulatorQueueEmpty(t *testing.T, client *azservicebus.Client, queue string) {
	t.Helper()
	receiver, err := client.NewReceiverForQueue(queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeEmulatorResource(t, receiver.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if len(messages) != 0 || err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty readback: count=%d error=%v", len(messages), err)
	}
}
