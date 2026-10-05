package azuresb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
)

type nativeSenderFunc func(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error

func (f nativeSenderFunc) SendMessage(ctx context.Context, message *azservicebus.Message, options *azservicebus.SendMessageOptions) error {
	return f(ctx, message, options)
}

func TestPublisherNativeMappingDoesNotAliasInput(t *testing.T) {
	original := broker.Message{ID: "event-1", Body: []byte{0, 255}, Headers: []broker.Header{
		{Name: "traceparent", Value: []byte("trace")}, {Name: "encoded", Value: []byte("/wE=")},
	}}
	var received *azservicebus.Message
	sender := nativeSenderFunc(func(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
		received = m
		if m.MessageID == nil || *m.MessageID != "event-1" || m.ApplicationProperties["id"] != "event-1" ||
			m.ApplicationProperties["traceparent"] != "trace" || m.ApplicationProperties["encoded"] != "/wE=" {
			t.Fatalf("native mapping: %+v", m)
		}
		m.Body[0] = 42
		m.ApplicationProperties["encoded"] = "mutated"
		*m.MessageID = "mutated"
		return nil
	})
	publisher, err := NewPublisher(sender, PublisherConfig{Source: "orders/publish"})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	if received == nil ||
		original.ID != "event-1" ||
		!bytes.Equal(original.Body,
			[]byte{0,
				255}) ||
		string(original.Headers[1].Value) != "/wE=" {
		t.Fatal("native sender mutated caller storage")
	}
}

func TestPublisherRejectsAmbiguousMetadataBeforeNativeEffects(t *testing.T) {
	calls := 0
	publisher, err := NewPublisher(nativeSenderFunc(func(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error {
		calls++
		return nil
	}), PublisherConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []broker.Message{
		{ID: "event", Headers: []broker.Header{{Name: "id", Value: []byte("other")}}},
		{ID: "event", Headers: []broker.Header{{Name: "ID", Value: []byte("event")}}},
		{ID: "event", Headers: []broker.Header{{Name: "same", Value: []byte("a")}, {Name: "same", Value: []byte("b")}}},
		{ID: string([]byte{255})},
		{ID: "event", Headers: []broker.Header{{Name: string([]byte{255}), Value: []byte("value")}}},
	}
	for i, input := range cases {
		err = publisher.Publish(context.Background(), input)
		var field *FieldError
		var operation *OperationError
		if !errors.As(err, &field) || !errors.As(err, &operation) || operation.Source != "orders" || field.Field == "" {
			t.Errorf("case%d field/source missing: %v", i, err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid metadata reached native sender")
	}
}

func TestDetachPreservesScalarPropertiesAndSnapshots(t *testing.T) {
	locked := time.Now().UTC().Truncate(time.Millisecond)
	enqueued := locked.Add(-time.Minute)
	sequence := int64(7)
	uuid := amqp.UUID{1, 2, 3}
	native := &azservicebus.ReceivedMessage{MessageID: "event",
		Body:           []byte("body"),
		DeliveryCount:  2,
		LockedUntil:    &locked,
		EnqueuedTime:   &enqueued,
		SequenceNumber: &sequence,
		ApplicationProperties: map[string]any{
			"id": "event",
			"binary": []byte{0,
				255},
			"count":    int64(-42),
			"flag":     true,
			"fraction": float64(1.5),
			"when":     enqueued,
			"uuid":     uuid,
			"symbol":   amqp.Symbol("label"),
		}}
	delivery, err := detach(native, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Message().ID != "event" || delivery.Source() != "orders" || delivery.Attempt() != (broker.Attempt{Known: true, Number: 2}) {
		t.Fatalf("delivery: %+v", delivery)
	}
	values := map[string][]byte{}
	for _, h := range delivery.Message().Headers {
		values[h.Name] = h.Value
	}
	if string(values["count"]) != "-42" ||
		string(values["flag"]) != "true" ||
		string(values["fraction"]) != "1.5" ||
		string(values["when"]) != enqueued.Format(time.RFC3339Nano) ||
		string(values["symbol"]) != "label" {
		t.Fatalf("scalar header conversion: %v", values)
	}
	metadata := delivery.Metadata()
	if !reflect.DeepEqual(metadata.ApplicationProperties["uuid"],
		uuid) ||
		!reflect.DeepEqual(metadata.ApplicationProperties["count"],
			int64(-42)) {
		t.Fatal("native scalar types lost")
	}
	native.Body[0] = 'x'
	native.ApplicationProperties["binary"].([]byte)[0] = 42
	locked = locked.Add(time.Hour)
	metadata.ApplicationProperties["binary"].([]byte)[1] = 42
	if string(delivery.Message().Body) != "body" ||
		!bytes.Equal(delivery.Metadata().ApplicationProperties["binary"].([]byte),
			[]byte{0,
				255}) ||
		!delivery.Metadata().LockedUntil.Equal(metadata.LockedUntil) {
		t.Fatal("native or metadata mutation changed retained delivery")
	}
}

func TestDetachRejectsAmbiguousOrUnsupportedProperties(t *testing.T) {
	for _, props := range []map[string]any{{"id": "conflict"},
		{"ID": "event"},
		{"id": []byte("event")},
		{"nested": map[string]any{"sensitive": "value"}},
		{"bad": []string{"value"}},
		{"bad": func() {}},
		{"text": string([]byte{255})}} {
		_, err := detach(&azservicebus.ReceivedMessage{MessageID: "event", ApplicationProperties: props}, "orders")
		var field *FieldError
		if !errors.As(err, &field) || field.Field == "" || errors.Unwrap(field) == nil {
			t.Fatalf("invalid property lost field/cause: %v", err)
		}
	}
	delivery, err := detach(&azservicebus.ReceivedMessage{}, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Attempt().Known || delivery.Message().ID != "" {
		t.Fatal("invented attempt or identity")
	}
}

func TestDetachNilNativeDeliveryIsMetadataFailure(t *testing.T) {
	_, err := detach(nil, "orders")
	var field *FieldError
	if !errors.As(err, &field) || field.Field != "Message" {
		t.Fatalf("nil native delivery lost field: %v", err)
	}
}

func TestPublisherRequiresExplicitIdentityBeforeNativeSend(t *testing.T) {
	calls := 0
	publisher, err := NewPublisher(nativeSenderFunc(func(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error {
		calls++
		return nil
	}), PublisherConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	err = publisher.Publish(context.Background(), broker.Message{Body: []byte("body")})
	var field *FieldError
	if !errors.As(err, &field) || field.Field != "ID" || calls != 0 {
		t.Fatalf("missing logical identity reached native service: calls=%d err=%v", calls, err)
	}
}

func TestPublisherRejectsNativeBinaryPropertyBeforeSend(t *testing.T) {
	calls := 0
	publisher, err := NewPublisher(nativeSenderFunc(func(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error {
		calls++
		return nil
	}), PublisherConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	err = publisher.Publish(context.Background(),
		broker.Message{ID: "event",
			Body: []byte{0,
				255},
			Headers: []broker.Header{{Name: "opaque",
				Value: []byte{255,
					1}}}})
	var field *FieldError
	if !errors.As(err, &field) || field.Field != "ApplicationProperties.opaque" || calls != 0 {
		t.Fatalf("unsupported native binary property reached service: calls=%d err=%v", calls, err)
	}
}

func TestDetachUnsupportedPropertyRetainsNativeType(t *testing.T) {
	_,
		err := detach(&azservicebus.ReceivedMessage{MessageID: "event",
		ApplicationProperties: map[string]any{"nested": map[string]any{"secret": "value"}}},
		"orders")
	var field *FieldError
	if !errors.As(err,
		&field) ||
		field.Field != "ApplicationProperties.nested" ||
		field.ValueType != "map[string]interface {}" ||
		field.Cause == nil {
		t.Fatalf("unsupported property type/field/cause lost: %v", err)
	}
}
