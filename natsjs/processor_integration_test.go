package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestProcessorLeavesNativePullSettlementToCaller(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	subscription, err := js.PullSubscribe(config.Subject, "", nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := subscription.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if _, err = js.Publish(config.Subject, []byte(`{"ID":"first"}`)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	messages, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	var callbacks int
	processor, err := natsjs.NewProcessor(broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{ ID string }],
		func(_ context.Context, delivery broker.Delivery, value struct{ ID string }) (broker.Disposition, error) {
			callbacks++
			if value.ID != "first" || delivery.Source() != config.Stream+"/"+config.Consumer ||
				delivery.(natsjs.SubjectReader).Subject() != config.Subject || !delivery.(broker.AttemptReader).Attempt().Known {
				t.Error("native processing lost typed data or metadata")
			}
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{})), config.Stream+"/"+config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(ctx, messages[0]); err != nil {
		t.Fatal(err)
	}
	assertPending(t, js, config, 1)
	if err = messages[0].AckSync(nats.Context(ctx)); err != nil {
		t.Fatalf("caller did not retain settlement authority: %v", err)
	}
	assertPending(t, js, config, 0)
	if callbacks != 1 || !subscription.IsValid() || nc.IsClosed() {
		t.Fatalf("callbacks=%d subscription=%v connection closed=%v", callbacks, subscription.IsValid(), nc.IsClosed())
	}
}

func TestProcessorRejectsManagedRequirementsBeforeEffects(t *testing.T) {
	for _, requirement := range []broker.Requirement{
		broker.RedeliveryRequirement{}, broker.KeepAliveRequirement{Interval: time.Second}, natsjs.TerminationRequirement{},
	} {
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unsupported requirement reached callback")
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirement))
		_, err := natsjs.NewProcessor(handler, "ORDERS/worker")
		var field *broker.FieldError
		if !errors.As(err, &field) || field.Field != "Requirements" {
			t.Fatalf("requirement %T accepted or diagnostic lost: %v", requirement, err)
		}
	}
}

func TestProcessorRejectsDispositionAndPreservesCancellation(t *testing.T) {
	_, js, config := provision(t, time.Second)
	subscription, err := js.PullSubscribe(config.Subject, "", nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := subscription.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if _, err = js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	messages, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	processor, err := natsjs.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.RetryAfter{}, nil
	}), config.Stream+"/"+config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	var field *broker.FieldError
	if err = processor.Process(ctx, messages[0]); !errors.As(err, &field) || field.Field != "Disposition" {
		t.Fatalf("unsupported native outcome accepted: %v", err)
	}
	cause := errors.New("caller ended native processing")
	canceled, stop := context.WithCancelCause(ctx)
	stop(cause)
	if err = processor.Process(canceled, messages[0]); !errors.Is(err, cause) {
		t.Fatalf("cancellation cause lost: %v", err)
	}
	assertPending(t, js, config, 1)
}
