package azuresb

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestRunServiceStopResults(t *testing.T) {
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil })
	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errors.New("stop"))
		c, _ := NewConsumer(func() (Receiver, error) { t.Fatal("acquired"); return nil, nil }, ConsumerConfig{
			Source: "orders",
		})
		if err := c.RunService(ctx, handler); err != nil {
			t.Fatal(err)
		}
	})
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "custom-cause", true: "deadline"}[deadline], func(t *testing.T) {
			var ctx context.Context
			var stop context.CancelFunc
			if deadline {
				ctx, stop = context.WithTimeout(context.Background(), 20*time.Millisecond)
			} else {
				var cancel context.CancelCauseFunc
				ctx, cancel = context.WithCancelCause(context.Background())
				stop = func() { cancel(errors.New("stop")) }
			}
			defer stop()
			r := &routeReceiver{
				receive: func(poll context.Context) ([]*azservicebus.ReceivedMessage, error) {
					if !deadline {
						stop()
					}
					<-poll.Done()
					return nil, poll.Err()
				},
			}
			c, _ := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{
				Source: "orders",
			})
			if err := c.RunService(ctx, handler); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunServiceCallbackAndCleanupResults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		callback    error
		closeErr    error
		cooperative bool
	}{
		{
			name:        "cooperative",
			cooperative: true,
		},
		{
			name:     "callback",
			callback: errors.New("callback failed"),
		},
		{
			name:        "close",
			closeErr:    errors.New("close failed"),
			cooperative: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &routeReceiver{
				receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
					return []*azservicebus.ReceivedMessage{{
						MessageID: "one",
					}}, nil
				},
				close: func(context.Context) error { return tc.closeErr },
			}
			c, _ := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{
				Source: "orders",
				Shutdown: broker.ShutdownPolicy{
					Mode: broker.ShutdownCancel,
				},
			})
			err := c.RunService(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
				cancel()
				<-work.Done()
				if tc.cooperative {
					return nil, work.Err()
				}
				return nil, errors.Join(work.Err(), tc.callback)
			}))
			if tc.callback != nil && !errors.Is(err, tc.callback) ||
				tc.closeErr != nil && !errors.Is(err, tc.closeErr) ||
				tc.callback == nil && tc.closeErr == nil && err != nil {
				t.Fatalf("result: %v", err)
			}
		})
	}
}

func TestRunServiceIndependentFailuresDuringStop(t *testing.T) {
	for _, stage := range []string{"acquire", "renew", "settle", "close"} {
		t.Run(stage, func(t *testing.T) {
			marker := errors.New(stage + " failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			route := &routeReceiver{
				receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
					return []*azservicebus.ReceivedMessage{{
						MessageID: "one",
					}}, nil
				},
				complete: func(context.Context) error {
					cancel()
					if stage == "settle" {
						return marker
					}
					return nil
				},
				close: func(context.Context) error {
					if stage == "close" {
						return marker
					}
					return nil
				}}
			receiver := &renewalReceiver{
				routeReceiver: *route,
				renew:         func(context.Context, *azservicebus.ReceivedMessage) error { cancel(); return marker },
			}
			c, _ := NewConsumer(func() (Receiver, error) {
				if stage == "acquire" {
					cancel()
					return nil, marker
				}
				return receiver, nil
			}, ConsumerConfig{
				Source: "orders",
			})
			opts := []broker.HandlerOption{}
			if stage == "renew" {
				opts = append(opts, broker.WithRequirements(broker.KeepAliveRequirement{
					Interval: time.Millisecond,
				}))
			}
			err := c.RunService(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
				if stage == "renew" {
					<-work.Done()
				}
				return broker.Handled{}, nil
			}, opts...))
			if !errors.Is(err, marker) {
				t.Fatalf("lost %s cause: %v", stage, err)
			}
		})
	}
}

func TestRunServiceGraceExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &routeReceiver{
		receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
			return []*azservicebus.ReceivedMessage{{
				MessageID: "one",
			}}, nil
		},
	}
	c, _ := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{
		Source: "orders",
		Shutdown: broker.ShutdownPolicy{
			GracePeriod: time.Millisecond,
		},
	})
	err := c.RunService(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
		cancel()
		<-work.Done()
		return nil, work.Err()
	}))
	if !errors.Is(err, ErrGracePeriodExceeded) {
		t.Fatalf("grace expiry lost: %v", err)
	}
}

// callbackContext signals the first context check after the callback returns.
type callbackContext struct {
	context.Context
	returned atomic.Bool
	checked  chan struct{}
	once     sync.Once
}

func (c *callbackContext) Err() error {
	err := c.Context.Err()
	if c.returned.Load() {
		c.once.Do(func() { close(c.checked) })
	}
	return err
}

func TestRunServiceIndependentCallbackCancellationBeforeStop(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var deliveryContext *callbackContext
	receiver := &renewalReceiver{
		routeReceiver: routeReceiver{
			receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				return []*azservicebus.ReceivedMessage{
					{
						MessageID: "one",
					},
				}, nil
			},
		},
		renew: func(ctx context.Context, _ *azservicebus.ReceivedMessage) error {
			close(started)
			<-ctx.Done()
			return nil
		},
	}
	c, err := NewConsumer(func() (Receiver, error) { return receiver, nil }, ConsumerConfig{
		Source: "orders",
		Shutdown: broker.ShutdownPolicy{
			Mode: broker.ShutdownCancel,
		},
		Observer: &Observer{
			Begin: func(ctx context.Context, _ DeliveryInfo) context.Context {
				deliveryContext = &callbackContext{
					Context: ctx,
					checked: make(chan struct{}),
				}
				return deliveryContext
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- c.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			<-started
			deliveryContext.returned.Store(true)
			return nil, context.Canceled
		}, broker.WithRequirements(broker.KeepAliveRequirement{
			Interval: time.Millisecond,
		})))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("renewal did not start")
	}
	select {
	case <-deliveryContext.checked:
	case <-time.After(time.Second):
		t.Fatal("callback context was not captured before renewal joined")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("independent callback error lost: %v", err)
	}
}

func TestRunServiceGracefulDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := false
	receiver := &routeReceiver{
		receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
			return []*azservicebus.ReceivedMessage{
				{
					MessageID: "one",
				},
			}, nil
		},
		complete: func(context.Context) error { completed = true; return nil },
	}
	consumer, err := NewConsumer(func() (Receiver, error) { return receiver, nil }, ConsumerConfig{
		Source: "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.RunService(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
		cancel()
		if work.Err() != nil {
			t.Error("graceful stop canceled callback")
		}
		return broker.Handled{}, nil
	}))
	if err != nil || !completed {
		t.Fatalf("result=%v complete=%v", err, completed)
	}
}

func TestRunServiceCanceledNativeDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &routeReceiver{
		receive: func(poll context.Context) ([]*azservicebus.ReceivedMessage, error) {
			cancel()
			var dialer net.Dialer
			conn, err := dialer.DialContext(poll, "tcp", "localhost:5672")
			if conn != nil {
				if closeErr := conn.Close(); closeErr != nil {
					return nil, closeErr
				}
			}
			return nil, err
		},
	}
	c, err := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{
		Source: "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = c.RunService(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Fatal("callback called")
		return nil, nil
	}))
	if err != nil {
		t.Fatalf("canceled native dial: %v", err)
	}
}
