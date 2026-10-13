package sqs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestProcessorDetachesNativeDelivery(t *testing.T) {
	native := &types.Message{Body: aws.String(`{"ID":"order"}`), ReceiptHandle: aws.String("receipt"), MessageId: aws.String("service-id"),
		Attributes:        map[string]string{"ApproximateReceiveCount": "2"},
		MessageAttributes: map[string]types.MessageAttributeValue{"tag": {DataType: aws.String("Binary"), BinaryValue: []byte("value")}},
	}
	var retained adapter.Metadata
	calls := 0
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{ ID string }],
		func(_ context.Context, d broker.Delivery, value struct{ ID string }) (broker.Disposition, error) {
			calls++
			if value.ID != "order" || d.Source() != "orders" || d.(broker.AttemptReader).Attempt() != (broker.Attempt{Known: true, Number: 2}) {
				t.Error("native delivery identity lost")
			}
			retained = d.(adapter.MetadataReader).Metadata()
			d.Message().Body[0] = 'x'
			d.Message().Headers[0].Value[0] = 'X'
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}))
	processor, err := adapter.NewProcessor(handler, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(context.Background(), native); err != nil {
		t.Fatal(err)
	}
	retained.Attributes["ApproximateReceiveCount"] = "9"
	retained.MessageAttributes["tag"].BinaryValue[0] = 'Y'
	if calls != 1 || *native.Body != `{"ID":"order"}` || native.Attributes["ApproximateReceiveCount"] != "2" ||
		string(native.MessageAttributes["tag"].BinaryValue) != "value" {
		t.Fatalf("native message mutated: %+v", native)
	}
}

func TestProcessorRejectsUnsupportedRequirementsAndSources(t *testing.T) {
	for _, requirement := range []broker.Requirement{broker.RedeliveryRequirement{}, broker.KeepAliveRequirement{Interval: time.Second}, nil} {
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unsupported capability invoked handler")
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirement))
		_, err := adapter.NewProcessor(handler, "orders")
		assertProcessorField(t, err, "Requirements", "validate")
	}
	for _, source := range []string{"", "https://private/path", "bad\nsource", strings.Repeat("x", 129)} {
		_, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.Handled{}, nil
		}), source)
		assertProcessorField(t, err, "Source", "validate")
	}
}

func TestProcessorRetainsFailureAndCancellation(t *testing.T) {
	failure, cancellation := errors.New("effect failed"), errors.New("caller stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	calls := 0
	processor, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		cancel(cancellation)
		return broker.Handled{}, failure
	}), "orders")
	if err != nil {
		t.Fatal(err)
	}
	native := &types.Message{Body: aws.String("event"), ReceiptHandle: aws.String("receipt")}
	err = processor.Process(ctx, native)
	if !errors.Is(err, failure) || !errors.Is(err, cancellation) {
		t.Fatalf("causes lost: %v", err)
	}
	if err = processor.Process(ctx, native); !errors.Is(err, cancellation) || calls != 1 {
		t.Fatalf("canceled processing: calls=%d err=%v", calls, err)
	}
}

func TestProcessorRejectsDispositionAndMissingReceipt(t *testing.T) {
	for _, disposition := range []broker.Disposition{nil, broker.RetryAfter{}} {
		processor, err := adapter.NewProcessor(broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return disposition, nil
		}), "orders")
		if err != nil {
			t.Fatal(err)
		}
		assertProcessorField(t, processor.Process(context.Background(), &types.Message{
			Body: aws.String("event"), ReceiptHandle: aws.String("receipt")}),
			"Disposition", adapter.OperationDisposition)
		assertProcessorField(t, processor.Process(context.Background(), &types.Message{
			Body: aws.String("event")}),
			"ReceiptHandle", adapter.OperationMetadata)
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

func TestProcessorAttemptRemainsUnknownWithoutNativeAttribute(t *testing.T) {
	processor, err := adapter.NewProcessor(broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		if delivery.(broker.AttemptReader).Attempt().Known {
			t.Error("missing native count was invented")
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.AttemptsRequirement{})), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(context.Background(), &types.Message{
		Body: aws.String("event"), ReceiptHandle: aws.String("receipt")}); err != nil {
		t.Fatal(err)
	}
}
