package azuresb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestRenewalRequirements(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		intervals            []time.Duration
		destructive, invalid bool
	}{
		{name: "positive", intervals: []time.Duration{time.Millisecond}},
		{name: "equal", intervals: []time.Duration{time.Millisecond, time.Millisecond}},
		{name: "zero", intervals: []time.Duration{0}, invalid: true},
		{name: "negative", intervals: []time.Duration{-1}, invalid: true},
		{name: "conflict", intervals: []time.Duration{1, 2}, invalid: true},
		{name: "reverse-conflict", intervals: []time.Duration{2, 1}, invalid: true},
		{name: "receive-and-delete", intervals: []time.Duration{1}, destructive: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var acquired bool
			config := ConsumerConfig{Source: "orders"}
			if tc.destructive {
				config.ReceiveMode = azservicebus.ReceiveModeReceiveAndDelete
			}
			c, err := NewConsumer(func() (Receiver, error) { acquired = true; return nil, errors.New("unexpected acquisition") }, config)
			if err != nil {
				t.Fatal(err)
			}
			var req []broker.Requirement
			for _, interval := range tc.intervals {
				req = append(req, broker.KeepAliveRequirement{Interval: interval})
			}
			err = c.Validate(req...)
			if (err != nil) != tc.invalid {
				t.Fatalf("validation=%v invalid=%v", err, tc.invalid)
			}
			if tc.invalid {
				_ = c.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
					t.Fatal("unexpected callback")
					return nil, nil
				}, broker.WithRequirements(req...)))
				if acquired {
					t.Fatal("acquired invalid receiver")
				}
			}
		})
	}
}

func TestRenewalJoinNormalReturnAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "return", true: "panic"}[panics], func(t *testing.T) {
			started, returned, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := errors.New("callback panic")
			var completed atomic.Int32
			r := &renewalReceiver{routeReceiver: routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
			}, complete: func(context.Context) error { completed.Add(1); cancel(); return nil }, close: func(context.Context) error {
				select {
				case <-finished:
				default:
					t.Error("closed before renewal joined")
				}
				return nil
			}}, renew: func(ctx context.Context, _ *azservicebus.ReceivedMessage) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("unbounded renewal")
				}
				close(started)
				<-release
				defer close(finished)
				if ctx.Err() != nil {
					t.Error("normal unwind canceled admitted renewal")
				}
				return nil
			}}
			c, _ := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{Source: "orders"})
			result := make(chan error, 1)
			recovered := make(chan any, 1)
			go func() {
				defer func() { recovered <- recover() }()
				result <- c.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
					<-started
					close(returned)
					if panics {
						panic(marker)
					}
					return broker.Handled{}, nil
				}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
			}()
			select {
			case <-returned:
			case err := <-result:
				t.Fatalf("callback not admitted: %v", err)
			case <-time.After(time.Second):
				t.Fatal("renewal not started")
			}
			select {
			case <-recovered:
				t.Fatal("Run returned before renewal")
			case <-time.After(5 * time.Millisecond):
			}
			if completed.Load() != 0 {
				t.Fatal("terminal before renewal")
			}
			close(release)
			select {
			case p := <-recovered:
				if panics && p != marker {
					t.Fatalf("panic=%v", p)
				}
			case <-time.After(time.Second):
				t.Fatal("Run did not join")
			}
			if completed.Load() != map[bool]int32{false: 1, true: 0}[panics] {
				t.Fatal("wrong terminal count")
			}
		})
	}
}

func TestRenewalFailureAndForcedCancellation(t *testing.T) {
	for _, forced := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			name := map[bool]string{false: "native", true: "forced"}[forced] + map[bool]string{false: "-single", true: "-joined"}[mixed]
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				nativeErr, callbackErr, cleanupErr := errors.New("native lock loss"), errors.New("callback failure"), errors.New("cleanup failure")
				var terminals atomic.Int32
				r := &renewalReceiver{routeReceiver: routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
					return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
				}, complete: func(context.Context) error { terminals.Add(1); return nil },
					abandon: func(context.Context) error { terminals.Add(1); return nil },
					close:   func(context.Context) error { return cleanupErr }},
					renew: func(work context.Context, _ *azservicebus.ReceivedMessage) error {
						if !forced {
							return nativeErr
						}
						cancel()
						<-work.Done()
						if mixed {
							return errors.Join(work.Err(), nativeErr)
						}
						return work.Err()
					}}
				c, _ := NewConsumer(func() (Receiver, error) { return r, nil },
					ConsumerConfig{Source: "orders", Shutdown: broker.ShutdownPolicy{Mode: broker.ShutdownCancel}})
				err := c.Run(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
					select {
					case <-work.Done():
					case <-time.After(time.Second):
						t.Error("callback not canceled")
					}
					return broker.RetryAfter{}, callbackErr
				}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.RedeliveryRequirement{})))
				if !errors.Is(err, callbackErr) || !errors.Is(err, cleanupErr) || ((!forced || mixed) && !errors.Is(err, nativeErr)) ||
					terminals.Load() != 0 {
					t.Fatalf("error=%v terminal=%d", err, terminals.Load())
				}
			})
		}
	}
}

func TestRenewalGraceSnapshotAndObserverIsolation(t *testing.T) {
	for _, observerPanics := range []bool{false, true} {
		t.Run(map[bool]string{false: "serialized", true: "panic"}[observerPanics], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			original := time.Now()
			native := &azservicebus.ReceivedMessage{MessageID: "one", LockedUntil: &original}
			var calls, active, hookActive atomic.Int32
			renewed := make(chan struct{})
			observerErr := errors.New("observer failed")
			r := &renewalReceiver{routeReceiver: routeReceiver{receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
				return []*azservicebus.ReceivedMessage{native}, nil
			}, complete: func(context.Context) error {
				if active.Load() != 0 {
					t.Error("terminal overlaps renewal")
				}
				return nil
			}}, renew: func(work context.Context, msg *azservicebus.ReceivedMessage) error {
				if active.Add(1) != 1 {
					t.Error("overlapping renewal")
				}
				defer active.Add(-1)
				if work.Err() != nil {
					t.Error("graceful cancellation reached renewal")
				}
				updated := original.Add(time.Hour)
				msg.LockedUntil = &updated
				if calls.Add(1) == 3 {
					close(renewed)
				}
				return nil
			}}
			c, _ := NewConsumer(func() (Receiver, error) { return r, nil },
				ConsumerConfig{Source: "orders", Observer: &Observer{Record: func(_ context.Context, o Observation) {
					if hookActive.Add(1) != 1 {
						t.Error("concurrent observer")
					}
					defer hookActive.Add(-1)
					if o.Operation == "renew" && observerPanics {
						panic(observerErr)
					}
				}}})
			err := c.Run(ctx, broker.NewHandler(func(work context.Context, d broker.Delivery) (broker.Disposition, error) {
				cancel()
				select {
				case <-renewed:
				case <-time.After(time.Second):
					t.Error("graceful renewal stopped")
				}
				if work.Err() != nil {
					t.Error("graceful callback canceled")
				}
				if !d.(MetadataReader).Metadata().LockedUntil.Equal(original) {
					t.Error("snapshot mutated")
				}
				return broker.Handled{}, nil
			}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond},
				broker.KeepAliveRequirement{Interval: time.Millisecond})))
			if calls.Load() < 3 || !errors.Is(err, context.Canceled) {
				t.Fatalf("renewals=%d error=%v", calls.Load(), err)
			}
			if observerPanics && !errors.Is(err, observerErr) {
				t.Fatalf("lost observer panic: %v", err)
			}
		})
	}
}

func TestRenewalFailureSuppressesSuccessfulDispositions(t *testing.T) {
	for _, disposition := range []broker.Disposition{broker.Handled{}, broker.RetryAfter{}} {
		for _, policy := range []broker.ShutdownPolicy{{}, {Mode: broker.ShutdownCancel}, {GracePeriod: time.Millisecond}} {
			ctx, cancel := context.WithCancel(context.Background())
			marker := errors.New("native renewal failed")
			terminals := 0
			r := &renewalReceiver{routeReceiver: routeReceiver{
				receive: func(context.Context) ([]*azservicebus.ReceivedMessage, error) {
					return []*azservicebus.ReceivedMessage{{MessageID: "one"}}, nil
				},
				complete: func(context.Context) error { terminals++; return nil },
				abandon:  func(context.Context) error { terminals++; return nil },
			}, renew: func(work context.Context, _ *azservicebus.ReceivedMessage) error {
				if policy.Mode == broker.ShutdownCancel || policy.GracePeriod > 0 {
					cancel()
					<-work.Done()
					return work.Err()
				}
				return marker
			}}
			c, _ := NewConsumer(func() (Receiver, error) { return r, nil }, ConsumerConfig{Source: "orders", Shutdown: policy})
			err := c.Run(ctx, broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
				select {
				case <-work.Done():
				case <-time.After(time.Second):
					t.Error("callback not canceled")
				}
				return disposition, nil
			}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.RedeliveryRequirement{})))
			cancel()
			expected := marker
			if policy.Mode == broker.ShutdownCancel || policy.GracePeriod > 0 {
				expected = context.Canceled
			}
			if !errors.Is(err, expected) || terminals != 0 {
				t.Fatalf("error=%v terminals=%d", err, terminals)
			}
		}
	}
}

type renewalReceiver struct {
	routeReceiver
	renew func(context.Context, *azservicebus.ReceivedMessage) error
}

func (r *renewalReceiver) RenewMessageLock(
	ctx context.Context, msg *azservicebus.ReceivedMessage, _ *azservicebus.RenewMessageLockOptions,
) error {
	return r.renew(ctx, msg)
}
