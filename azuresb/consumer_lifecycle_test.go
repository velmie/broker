package azuresb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestConsumerForcedShutdownSuppressesUnadmittedComplete(t *testing.T) {
	for _, policy := range []broker.ShutdownPolicy{{Mode: broker.ShutdownCancel}, {GracePeriod: time.Millisecond}} {
		t.Run(policyName(policy), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			stop := errors.New("shutdown owner")
			completed, closed := false, false
			r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
			},
				complete: func(context.Context) error { completed = true; return nil },
				close:    func(context.Context) error { closed = true; return nil }}
			c, err := NewConsumer(func() (Receiver, error) { return r, nil },
				ConsumerConfig{Source: "orders", Shutdown: policy})
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
				cancel(stop)
				select {
				case <-work.Done():
				case <-time.After(time.Second):
					t.Fatal("admitted callback not canceled")
				}
				return broker.Handled{}, nil
			}))
			if !errors.Is(err, stop) || completed || !closed {
				t.Fatalf("result=%v complete=%v closed=%v", err, completed, closed)
			}
		})
	}
}

func TestConsumerAdmittedCompleteJoinsBeforeClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admitted, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
		return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
	},
		complete: func(native context.Context) error {
			close(admitted)
			<-release
			if native.Err() != nil {
				return native.Err()
			}
			return nil
		},
		close: func(context.Context) error { close(closed); return nil }}
	c, err := NewConsumer(func() (Receiver, error) { return r, nil },
		ConsumerConfig{Source: "orders", Shutdown: broker.ShutdownPolicy{Mode: broker.ShutdownCancel}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- c.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.Handled{}, nil
		}))
	}()
	<-admitted
	cancel()
	select {
	case <-closed:
		t.Fatal("receiver closed before terminal joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("result=%v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("receiver close not joined")
	}
}

func TestConsumerLateReceiveSuppressesCallbackAndCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := false
	r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
		cancel()
		return []*azservicebus.ReceivedMessage{{MessageID: "late"}}, nil
	},
		close: func(context.Context) error { closed = true; return nil }}
	c, err := NewConsumer(func() (Receiver, error) { return r, nil },
		ConsumerConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Fatal("late message admitted")
		return nil, nil
	}))
	if !errors.Is(err, context.Canceled) || !closed {
		t.Fatalf("result=%v close=%v", err, closed)
	}
}

func TestConsumerCallbackPanicClosesReceiverBeforeUnwind(t *testing.T) {
	marker := errors.New("application panic")
	closeFailure := errors.New("cleanup during panic")
	closed, observed := false, false
	r := &routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
		return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
	},
		close: func(context.Context) error { closed = true; return closeFailure }}
	c, err := NewConsumer(func() (Receiver, error) { return r, nil },
		ConsumerConfig{Source: "orders",
			Observer: &Observer{Record: func(_ context.Context, o Observation) {
				if o.Operation == OperationClose && errors.Is(o.Err, closeFailure) {
					observed = true
				}
			}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if got := recover(); got != marker || !closed || !observed {
			t.Errorf("panic=%v receiver closed=%v", got, closed)
		}
	}()
	_ = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { panic(marker) }))
	t.Fatal("application panic was hidden")
}

func TestConsumerEmptyPollAndMixedFailure(t *testing.T) {
	foreign := errors.New("independent native failure")
	calls := 0
	r := &routeReceiver{receive: func(ctx context.Context) ([]*azservicebus.ReceivedMessage, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		<-ctx.Done()
		if calls == 2 {
			return nil, ctx.Err()
		}
		return nil, errors.Join(ctx.Err(), foreign)
	}}
	c, err := NewConsumer(func() (Receiver, error) { return r, nil },
		ConsumerConfig{Source: "orders", ReceiveTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Fatal("unexpected callback")
		return nil, nil
	}))
	if !errors.Is(err, foreign) || calls != 3 {
		t.Fatalf("receive calls=%d error=%v", calls, err)
	}
}

func TestConsumerConcurrentRunDoesNotAcquire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	acquisitions := 0
	r := &routeReceiver{receive: func(ctx context.Context) ([]*azservicebus.ReceivedMessage, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	c, err := NewConsumer(func() (Receiver, error) { acquisitions++; return r, nil },
		ConsumerConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil })
	finished := make(chan error, 1)
	go func() { finished <- c.Run(ctx, handler) }()
	<-entered
	if err := c.Run(context.Background(), handler); err == nil {
		t.Fatal("concurrent run accepted")
	}
	cancel()
	<-finished
	if acquisitions != 1 {
		t.Fatalf("acquisitions=%d", acquisitions)
	}
}

func policyName(policy broker.ShutdownPolicy) string {
	if policy.Mode == broker.ShutdownCancel {
		return "cancel"
	}
	return "grace-expiry"
}
