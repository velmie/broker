package natsjs_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestInvalidOrUndeclaredResultsLeaveDeliveryPending(t *testing.T) {
	var nilHandled *broker.Handled
	tests := []struct {
		name         string
		result       broker.Disposition
		requirements []broker.Requirement
	}{
		{"nil", nil, nil},
		{"typed_nil", nilHandled, nil},
		{"external", externalResult{}, nil},
		{"undeclared_termination", natsjs.Terminate{}, nil},
		{"undeclared_retry", broker.RetryAfter{}, nil},
		{"negative_delay", broker.RetryAfter{Delay: -time.Nanosecond}, []broker.Requirement{broker.RedeliveryRequirement{}}},
		{"overflow_delay", broker.RetryAfter{Delay: time.Duration(math.MaxInt64)}, []broker.Requirement{broker.RedeliveryRequirement{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			var calls, operations int
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
					operations++
				}
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls++ // Validation cannot undo an effect that already happened.
				return test.result, nil
			}, broker.WithRequirements(test.requirements...))
			result := startConsumer(t, nc, js, config, context.Background(), h, `{}`)
			if err := receive(t, result); err == nil || calls != 1 || operations != 0 {
				t.Fatalf("result=%v calls=%d operations=%d", err, calls, operations)
			}
			assertPending(t, js, config, 1)
		})
	}
}

func TestDecodingRejectsMalformedAndNullPointersBeforeBusiness(t *testing.T) {
	for _, body := range []string{`{`, `null`} {
		t.Run(body, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			h := broker.NewTypedHandler(broker.DecodeJSON[*struct{}], func(context.Context, *struct{}) error {
				t.Error("invalid DTO reached business")
				return nil
			})
			err := receive(t, startConsumer(t, nc, js, config, context.Background(), h, body))
			var decode *broker.DecodeError
			if !errors.As(err, &decode) || decode.Cause == nil {
				t.Fatalf("decode cause: %v", err)
			}
			assertPending(t, js, config, 1)
		})
	}
}

func TestConflictingRequirementsFailBeforeStartup(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	config.Observer.Record = func(context.Context, natsjs.Observation) { t.Error("runtime started before local validation") }
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]broker.Requirement{
		{broker.KeepAliveRequirement{Interval: time.Second}, broker.KeepAliveRequirement{Interval: 2 * time.Second}},
		{broker.KeepAliveRequirement{Interval: 2 * time.Second}, broker.KeepAliveRequirement{Interval: time.Second}},
		{broker.KeepAliveRequirement{Interval: time.Second}, broker.KeepAliveRequirement{}},
		{broker.KeepAliveRequirement{}, broker.KeepAliveRequirement{Interval: time.Second}},
		{externalRequirement{}}, {nil},
	} {
		h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return nil, nil
		}, broker.WithRequirements(values...))
		if err := c.Run(context.Background(), h); err == nil {
			t.Fatalf("accepted %T requirements", values[0])
		}
	}
	repeated := []broker.Requirement{
		broker.RedeliveryRequirement{}, broker.RedeliveryRequirement{}, broker.AttemptsRequirement{}, broker.AttemptsRequirement{},
	}
	if err := c.Validate(repeated...); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupportedNativeRegistrationPreventsEveryRun(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	var runs int
	config.Observer.Record = func(context.Context, natsjs.Observation) { runs++ }
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("preflight admitted callback")
		return natsjs.Terminate{}, nil
	},
		broker.WithRequirements(natsjs.TerminationRequirement{}))
	coordinator := broker.NewCoordinator()
	if err := coordinator.Add("supported", func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, config) }, h); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("unsupported", func() (broker.Consumer, error) { return rejectingConsumer{}, nil }, h); err != nil {
		t.Fatal(err)
	}
	err := coordinator.Run(context.Background())
	if err == nil || runs != 0 {
		t.Fatalf("preflight result: %v, runs=%d", err, runs)
	}
	if !strings.Contains(err.Error(), "unsupported validate") || !strings.Contains(err.Error(), "TerminationRequirement") {
		t.Fatalf("local preflight: %v runs=%d", err, runs)
	}
}

func TestStartupOnlyBindsExistingCompatibleConsumer(t *testing.T) {
	for _, mode := range []string{"missing", "ack_none", "wrong_filter", "backoff", "batch_limit"} {
		t.Run(mode, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			remote, err := js.ConsumerInfo(config.Stream, config.Consumer)
			if err != nil {
				t.Fatal(err)
			}
			if err = js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "ack_none":
				remote.Config.AckPolicy = nats.AckNonePolicy
			case "wrong_filter":
				config.Subject += ".different"
			case "backoff":
				remote.Config.BackOff = []time.Duration{time.Second}
				remote.Config.MaxDeliver = 3
			case "batch_limit":
				remote.Config.MaxRequestBatch = 1
				config.BatchSize = 2
			}
			if mode != "missing" {
				if _, err = js.AddConsumer(config.Stream, &remote.Config); err != nil {
					t.Fatal(err)
				}
			}
			c, err := natsjs.NewConsumer(nc, config)
			if err != nil {
				t.Fatal(err)
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("invalid binding reached callback")
				return broker.Handled{}, nil
			})
			if err = c.Run(context.Background(), h); err == nil {
				t.Fatal("incompatible startup accepted")
			}
			if mode == "missing" {
				if _, err = js.ConsumerInfo(config.Stream, config.Consumer); !errors.Is(err, nats.ErrConsumerNotFound) {
					t.Fatalf("startup provisioned a consumer: %v", err)
				}
			}
		})
	}
}

func TestRetryRejectsExhaustedDeliveryLimit(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.MaxDeliver = 1
	if _, err = js.UpdateConsumer(config.Stream, &info.Config); err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.RetryAfter{}, nil },
		broker.WithRequirements(broker.RedeliveryRequirement{}))
	err = receive(t, startConsumer(t, nc, js, config, context.Background(), h, `{}`))
	if err == nil || !strings.Contains(err.Error(), "MaxDeliver") {
		t.Fatalf("delivery limit: %v", err)
	}
	assertPending(t, js, config, 1)
}

func TestInvalidShutdownAndOperationConfigurationFailsLocally(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	for _, policy := range []broker.ShutdownPolicy{
		{Mode: broker.ShutdownMode(99)}, {GracePeriod: -time.Second}, {Mode: broker.ShutdownCancel, GracePeriod: time.Second},
	} {
		config.Shutdown = policy
		if _, err := natsjs.NewConsumer(nc, config); err == nil {
			t.Fatalf("invalid policy accepted: %+v", policy)
		}
	}
	config.Shutdown = broker.ShutdownPolicy{}
	config.OperationTimeout = -time.Second
	if _, err := natsjs.NewConsumer(nc, config); err == nil {
		t.Fatal("negative operation budget accepted")
	}
}

type externalResult struct{}

func (externalResult) DeliveryDisposition() {}

type externalRequirement struct{}

func (externalRequirement) DeliveryRequirement() {}

type rejectingConsumer struct{}

func (rejectingConsumer) Validate(requirements ...broker.Requirement) error {
	for _, requirement := range requirements {
		if _, ok := requirement.(natsjs.TerminationRequirement); ok {
			return errors.New("TerminationRequirement: unsupported")
		}
	}
	return nil
}
func (rejectingConsumer) Run(context.Context, broker.Handler) error {
	return errors.New("unexpected unsupported run")
}
