package azuresb_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestProcessorDetachesNativeMessage(t *testing.T) {
	native := &azservicebus.ReceivedMessage{Body: []byte(`{"ID":"order"}`), MessageID: "event", DeliveryCount: 2,
		ApplicationProperties: map[string]any{"tag": []byte("value")},
	}
	calls := 0
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{ ID string }],
		func(_ context.Context, delivery broker.Delivery, value struct{ ID string }) (broker.Disposition, error) {
			calls++
			if value.ID != "order" || delivery.Message().ID != "event" || delivery.Source() != "orders" ||
				delivery.(broker.AttemptReader).Attempt() != (broker.Attempt{Known: true, Number: 2}) {
				t.Error("native identity lost")
			}
			metadata := delivery.(adapter.MetadataReader).Metadata()
			metadata.ApplicationProperties["tag"].([]byte)[0] = 'X'
			delivery.Message().Body[0] = 'Y'
			delivery.Message().Headers[0].Value[0] = 'Z'
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}))
	processor, err := adapter.NewProcessor(handler, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(context.Background(), native); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || string(native.Body) != `{"ID":"order"}` || string(native.ApplicationProperties["tag"].([]byte)) != "value" {
		t.Fatalf("processing changed native settlement message: %+v", native)
	}
}

func TestProcessorRejectsUnsupportedRequirementsAndSources(t *testing.T) {
	for _, requirement := range []broker.Requirement{broker.RedeliveryRequirement{}, broker.KeepAliveRequirement{Interval: time.Second}, nil} {
		_, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unsupported requirement reached callback")
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirement)), "orders")
		assertProcessorField(t, err, "Requirements", "validate")
	}
	for _, source := range []string{"", "private\nsource", strings.Repeat("x", 129)} {
		_, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.Handled{}, nil
		}), source)
		assertProcessorField(t, err, "Source", "validate")
	}
}

func TestProcessorPreservesCallbackAndCancellationCauses(t *testing.T) {
	callbackFailure, canceled := errors.New("business failed"), errors.New("caller stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	calls := 0
	processor, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		cancel(canceled)
		return broker.Handled{}, callbackFailure
	}), "orders")
	if err != nil {
		t.Fatal(err)
	}
	native := &azservicebus.ReceivedMessage{Body: []byte("body"), MessageID: "event"}
	err = processor.Process(ctx, native)
	if !errors.Is(err, callbackFailure) || !errors.Is(err, canceled) {
		t.Fatalf("causes lost: %v", err)
	}
	if err = processor.Process(ctx, native); !errors.Is(err, canceled) || calls != 1 {
		t.Fatalf("canceled callback: calls=%d err=%v", calls, err)
	}
}

func TestProcessorRejectsNativeDispositionAndMalformedMetadata(t *testing.T) {
	for _, disposition := range []broker.Disposition{nil, broker.RetryAfter{}} {
		processor, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return disposition, nil
		}), "orders")
		if err != nil {
			t.Fatal(err)
		}
		assertProcessorField(t, processor.Process(context.Background(), &azservicebus.ReceivedMessage{Body: []byte("body"), MessageID: "event"}),
			"Disposition", adapter.OperationDisposition)
		assertProcessorField(t, processor.Process(context.Background(), &azservicebus.ReceivedMessage{MessageID: "event",
			ApplicationProperties: map[string]any{"id": "other"}}), "ApplicationProperties.id", adapter.OperationMetadata)
	}
}

func TestProcessorDoesNotInventMissingAttempt(t *testing.T) {
	processor, err := adapter.NewProcessor(broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		if delivery.(broker.AttemptReader).Attempt().Known {
			t.Error("invented native delivery count")
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.AttemptsRequirement{})), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(context.Background(), &azservicebus.ReceivedMessage{Body: []byte("body"), MessageID: "event"}); err != nil {
		t.Fatal(err)
	}
}

func assertProcessorField(t *testing.T, err error, fieldName, operationName string) {
	t.Helper()
	var field *adapter.FieldError
	var operation *adapter.OperationError
	if !errors.As(err, &field) || field.Field != fieldName || !errors.As(err, &operation) || operation.Operation != operationName {
		t.Fatalf("expected %s/%s: %v", operationName, fieldName, err)
	}
}
