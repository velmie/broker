package broker_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/velmie/broker"
)

func TestNewDeliveryRunsTypedHandler(t *testing.T) {
	message := broker.Message{
		ID:      "order-1",
		Body:    []byte(`{"ID":"order-1"}`),
		Headers: []broker.Header{{Name: "Content-Type", Value: []byte("application/json")}},
	}
	input := broker.NewDelivery(message, "orders")
	called := false
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[command],
		func(_ context.Context, received broker.Delivery, value command) (broker.Disposition, error) {
			called = true
			if value.ID != message.ID || received.Source() != "orders" {
				t.Fatalf("received command %q from %q", value.ID, received.Source())
			}
			if !reflect.DeepEqual(received.Message(), message) {
				t.Fatalf("received message %#v, want %#v", received.Message(), message)
			}
			return broker.Handled{}, nil
		})
	result, err := handler.Handle(context.Background(), input)
	if err != nil || !called {
		t.Fatalf("handle: called=%v, error=%v", called, err)
	}
	if _, ok := result.(broker.Handled); !ok {
		t.Fatalf("result %T, want broker.Handled", result)
	}
	if _, ok := input.(broker.AttemptReader); ok {
		t.Fatal("plain delivery exposes transport attempt metadata")
	}
}

func TestNewDeliverySharesApplicationOwnedStorage(t *testing.T) {
	message := broker.Message{
		Body:    []byte("one"),
		Headers: []broker.Header{{Name: "Kind", Value: []byte("one")}},
	}
	input := broker.NewDelivery(message, "orders")
	message.Body[0] = 'O'
	message.Headers[0].Name = "Event"
	message.Headers[0].Value[0] = 'O'
	received := input.Message()
	if string(received.Body) != "One" || received.Headers[0].Name != "Event" || string(received.Headers[0].Value) != "One" {
		t.Fatalf("delivery did not retain application storage: %#v", received)
	}
	received.Body[0] = 'o'
	received.Headers[0].Value[0] = 'o'
	if string(message.Body) != "one" || string(message.Headers[0].Value) != "one" {
		t.Fatal("returned message no longer shares application storage")
	}
}
