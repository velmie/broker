package azuresb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestConsumerSettlementRoute(t *testing.T) {
	reason := errors.New("requested retry")
	for _, tc := range []struct {
		name              string
		mode              azservicebus.ReceiveMode
		result            broker.Disposition
		requirements      []broker.Requirement
		complete, abandon int
		invalid           bool
	}{
		{name: "complete", result: broker.Handled{},
			complete: 1},
		{name: "abandon", result: broker.RetryAfter{Cause: reason},
			requirements: []broker.Requirement{broker.RedeliveryRequirement{}}, abandon: 1},
		{name: "receive-and-delete", mode: azservicebus.ReceiveModeReceiveAndDelete, result: broker.Handled{}},
		{name: "undeclared-retry", result: broker.RetryAfter{}, invalid: true},
		{name: "delayed-retry", result: broker.RetryAfter{Delay: time.Second},
			requirements: []broker.Requirement{broker.RedeliveryRequirement{}}, invalid: true},
		{name: "negative-retry", result: broker.RetryAfter{Delay: -time.Second},
			requirements: []broker.Requirement{broker.RedeliveryRequirement{}}, invalid: true},
		{name: "nil-disposition", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, completes, abandons, closes := 0, 0, 0, 0
			r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				calls++
				return []*azservicebus.ReceivedMessage{{MessageID: "logical", Body: []byte(`{"value":1}`), DeliveryCount: 1}}, nil
			},
				complete: func(ctx context.Context) error {
					if ctx.Err() != nil {
						t.Fatal("terminal inherited canceled run")
					}
					completes++
					return nil
				}, abandon: func(context.Context) error { abandons++; return nil },
				close: func(ctx context.Context) error {
					if ctx.Err() != nil {
						t.Fatal("close inherited canceled run")
					}
					closes++
					return nil
				}}
			var records []Observation
			c, err := NewConsumer(func() (Receiver, error) { return r, nil },
				ConsumerConfig{Source: "orders", ReceiveMode: tc.mode,

					Observer: &Observer{Record: func(_ context.Context, o Observation) { records = append(records, o) }}})
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(ctx, broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
				if d.Message().ID != "logical" {
					t.Fatal("logical identity lost")
				}
				cancel()
				return tc.result, nil
			}, broker.WithRequirements(tc.requirements...)))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("missing lifecycle cause: %v", err)
			}
			if completes != tc.complete || abandons != tc.abandon || closes != 1 || calls != 1 {
				t.Fatalf("receive/complete/abandon/close=%d/%d/%d/%d", calls, completes, abandons, closes)
			}
			if tc.invalid {
				var op *OperationError
				if !errors.As(err, &op) {
					t.Fatalf("invalid disposition missing diagnostic: %v", err)
				}
			}
			if tc.abandon == 1 {
				if errors.Is(err, reason) {
					t.Fatal("successful retry cause became failure")
				}
				seen := false
				for _, o := range records {
					if o.Operation == OperationAbandon && errors.Is(o.Cause, reason) && o.Err == nil {
						seen = true
					}
				}
				if !seen {
					t.Fatal("abandon lost original retry cause")
				}
			}
		})
	}
}

func TestConsumerRequirementsBeforeAcquisition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        azservicebus.ReceiveMode
		requirement broker.Requirement
	}{
		{name: "destructive-keepalive", mode: azservicebus.ReceiveModeReceiveAndDelete,
			requirement: broker.KeepAliveRequirement{Interval: time.Second}},
		{name: "destructive-retry", mode: azservicebus.ReceiveModeReceiveAndDelete, requirement: broker.RedeliveryRequirement{}},
		{name: "unknown", requirement: routeUnknownRequirement{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c, err := NewConsumer(func() (Receiver, error) { calls++; return &routeReceiver{}, nil },
				ConsumerConfig{Source: "orders", ReceiveMode: tc.mode})
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Fatal("callback reached")
				return broker.Handled{}, nil
			}, broker.WithRequirements(tc.requirement)))
			if err == nil || calls != 0 {
				t.Fatalf("error=%v acquisitions=%d", err, calls)
			}
		})
	}
}

func TestConsumerJoinsOriginalReceiveAndCloseFailures(t *testing.T) {
	receiveErr, closeErr := errors.New("receive native cause"), errors.New("close native cause")
	acquisitions, closes := 0, 0
	c, err := NewConsumer(func() (Receiver, error) {
		acquisitions++
		return &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) { return nil, receiveErr },
			close: func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded cleanup")
				}
				closes++
				return closeErr
			}}, nil
	},
		ConsumerConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Fatal("unexpected callback")
			return nil, nil
		}))
		if !errors.Is(err, receiveErr) || !errors.Is(err, closeErr) {
			t.Fatalf("original causes lost: %v", err)
		}
	}
	if acquisitions != 2 || closes != 2 {
		t.Fatalf("acquisitions=%d closes=%d", acquisitions, closes)
	}
}

func TestConsumerConfigurationBeforeAcquisition(t *testing.T) {
	for _, config := range []ConsumerConfig{
		{Source: ""},
		{Source: "secret\nsource"},
		{Source: "orders", ReceiveMode: azservicebus.ReceiveMode(99)},
		{Source: "orders", ReceiveTimeout: -time.Second},
		{Source: "orders", OperationTimeout: -time.Second},
		{Source: "orders", CloseTimeout: -time.Second},
		{Source: "orders", Shutdown: broker.ShutdownPolicy{Mode: broker.ShutdownMode(99)}},
		{Source: "orders", Shutdown: broker.ShutdownPolicy{Mode: broker.ShutdownCancel, GracePeriod: time.Second}},
	} {
		calls := 0
		_, err := NewConsumer(func() (Receiver, error) { calls++; return nil, nil }, config)
		if err == nil || calls != 0 {
			t.Fatalf("invalid config accepted or acquired: error=%v calls=%d", err, calls)
		}
	}
}

func TestConsumerCallbackAndTerminalFailuresNeverChooseOppositeAction(t *testing.T) {
	marker := errors.New("original failure")
	for _, callbackFails := range []bool{true, false} {
		t.Run(map[bool]string{true: "application", false: "complete"}[callbackFails], func(t *testing.T) {
			completes, abandons := 0, 0
			r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
			},
				complete: func(context.Context) error { completes++; return marker }, abandon: func(context.Context) error { abandons++; return nil }}
			var records []Observation
			c, err := NewConsumer(func() (Receiver, error) { return r, nil },
				ConsumerConfig{Source: "orders",

					Observer: &Observer{Record: func(_ context.Context, o Observation) { records = append(records, o) }}})
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				if callbackFails {
					return broker.RetryAfter{}, marker
				}
				return broker.Handled{}, nil
			}, broker.WithRequirements(broker.RedeliveryRequirement{})))
			expected := 1
			if callbackFails {
				expected = 0
			}
			if !errors.Is(err, marker) || completes != expected || abandons != 0 {
				t.Fatalf("error=%v complete=%d abandon=%d", err, completes, abandons)
			}
			seen := false
			for _, o := range records {
				if errors.Is(o.Err, marker) {
					seen = true
					if !callbackFails && o.Outcome != OutcomeUnknown {
						t.Fatalf("unconfirmed native result reported %s", o.Outcome)
					}
				}
			}
			if !seen {
				t.Fatal("observer lost original cause")
			}
		})
	}
}

func TestConsumerFactoryFailureRetainsCause(t *testing.T) {
	cause := errors.New("acquisition failure")
	var observed error
	c, err := NewConsumer(func() (Receiver, error) { return nil, cause },
		ConsumerConfig{Source: "orders",
			Observer: &Observer{Record: func(_ context.Context, o Observation) {
				if o.Operation == OperationAcquire {
					observed = o.Err
				}
			}}})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Fatal("callback after failed acquisition")
		return nil, nil
	}))
	if !errors.Is(err, cause) || !errors.Is(observed, cause) {
		t.Fatalf("returned=%v observed=%v", err, observed)
	}
}

func TestConsumerObserverPanicDoesNotSuppressCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	marker := errors.New("observer panic")
	completes := 0
	receiver := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
		return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
	}, complete: func(context.Context) error { completes++; return nil }}
	c, err := NewConsumer(func() (Receiver, error) { return receiver, nil },
		ConsumerConfig{Source: "orders",
			Observer: &Observer{Record: func(context.Context, Observation) { panic(marker) }}})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		cancel()
		return broker.Handled{}, nil
	}))
	var panicErr *ObserverPanicError
	if !errors.As(err, &panicErr) || !errors.Is(err, marker) || completes != 1 {
		t.Fatalf("error=%v completions=%d", err, completes)
	}
}

type routeUnknownRequirement struct{}

func (routeUnknownRequirement) DeliveryRequirement() {}

type routeReceiver struct {
	receive  func(context.Context) ([]*azservicebus.ReceivedMessage, error)
	complete func(context.Context) error
	abandon  func(context.Context) error
	close    func(context.Context) error
}

func (r *routeReceiver) ReceiveMessages(
	ctx context.Context, _ int, _ *azservicebus.ReceiveMessagesOptions,
) ([]*azservicebus.ReceivedMessage, error) {
	return r.receive(ctx)
}
func (r *routeReceiver) CompleteMessage(
	ctx context.Context, _ *azservicebus.ReceivedMessage, _ *azservicebus.CompleteMessageOptions,
) error {
	if r.complete != nil {
		return r.complete(ctx)
	}
	return nil
}
func (r *routeReceiver) AbandonMessage(ctx context.Context, _ *azservicebus.ReceivedMessage, _ *azservicebus.AbandonMessageOptions) error {
	if r.abandon != nil {
		return r.abandon(ctx)
	}
	return nil
}
func (r *routeReceiver) Close(ctx context.Context) error {
	if r.close != nil {
		return r.close(ctx)
	}
	return nil
}

func (r *routeReceiver) RenewMessageLock(context.Context, *azservicebus.ReceivedMessage, *azservicebus.RenewMessageLockOptions) error {
	return nil
}
