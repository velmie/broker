package broker_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/velmie/broker"
)

func TestMiddlewareOrderRequirementsAndOutcome(t *testing.T) {
	cause := errors.New("business evidence")
	result := broker.RetryAfter{Cause: cause}
	var order []string
	wrap := func(name string) broker.HandlerMiddleware {
		return broker.HandlerMiddleware{
			Requirements: []broker.Requirement{broker.RedeliveryRequirement{}},
			Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
				return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
					order = append(order, name+" enter")
					value, err := next(ctx, delivery)
					order = append(order, name+" exit")
					return value, err
				}
			},
		}
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return result, cause })
	first, second := wrap("first"), wrap("second")
	wrapped := h.WithMiddleware(first, second).WithMiddleware(wrap("outer"))
	first.Requirements[0] = broker.AttemptsRequirement{}
	declarations := wrapped.Requirements()
	declarations[0] = broker.AttemptsRequirement{}
	for _, requirement := range wrapped.Requirements() {
		if _, ok := requirement.(broker.RedeliveryRequirement); !ok {
			t.Fatalf("declaration changed: %T", requirement)
		}
	}
	if len(h.Requirements()) != 0 {
		t.Fatal("original handler was mutated")
	}
	got, err := wrapped.Handle(context.Background(), delivery{})
	if got != result || err != cause {
		t.Fatalf("outcome changed: %#v, %v", got, err)
	}
	want := []string{"outer enter", "first enter", "second enter", "second exit", "first exit", "outer exit"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
}

func TestCoordinatorValidatesAllSourcesBeforeRun(t *testing.T) {
	cause := errors.New("unsupported requirement")
	coordinator := broker.NewCoordinator()
	started := false
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("callback before preflight")
		return nil, nil
	})
	good := consumer{validate: func() error { return nil }, run: func(context.Context) error { started = true; return nil }}
	bad := consumer{validate: func() error { return cause }, run: good.run}
	if err := coordinator.Add("good", func() (broker.Consumer, error) { return good, nil }, h); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("unsupported", func() (broker.Consumer, error) { return bad, nil }, h); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Run(context.Background()); !errors.Is(err, cause) || started {
		t.Fatalf("preflight: %v, started=%v", err, started)
	}
	if err := coordinator.Add("late", func() (broker.Consumer, error) { return good, nil }, h); err == nil {
		t.Fatal("registration after freeze")
	}
	if err := coordinator.Run(context.Background()); err == nil {
		t.Fatal("second coordinator run")
	}
}

func TestRequirementOptionSnapshotsItsList(t *testing.T) {
	values := []broker.Requirement{broker.AttemptsRequirement{}}
	option := broker.WithRequirements(values...)
	values[0] = broker.RedeliveryRequirement{}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil }, option)
	if _, ok := h.Requirements()[0].(broker.AttemptsRequirement); !ok {
		t.Fatal("option retained mutable list")
	}
}
