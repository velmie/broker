package azuresb

import (
	"context"
	"errors"
	"testing"

	"github.com/velmie/broker"
)

func TestRejectRequirementFailsCoordinatorPreflight(t *testing.T) {
	consumer, err := NewConsumer(func() (Receiver, error) {
		t.Error("preflight acquired a receiver")
		return nil, nil
	}, ConsumerConfig{
		Source: "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Validate(broker.RejectRequirement{}); err == nil {
		t.Fatal("Validate accepted RejectRequirement")
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("preflight invoked the handler")
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.RejectRequirement{}))
	coordinator := broker.NewCoordinator()
	if err = coordinator.Add("orders", func() (broker.Consumer, error) { return consumer, nil }, handler); err != nil {
		t.Fatal(err)
	}
	err = coordinator.Run(context.Background())
	var registration *broker.RegistrationError
	if !errors.As(err, &registration) || registration.Operation != "validate" {
		t.Fatalf("expected preflight validation failure, got %v", err)
	}
}
