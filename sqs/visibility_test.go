package sqs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestSDKRetryVisibilityExactDelayAndCause(t *testing.T) {
	for _, delay := range []time.Duration{0, time.Second, 12 * time.Hour} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := errors.New("retry diagnostic cause")
			var callbacks, changes, deletes atomic.Int32
			var observed []adapter.Observation
			client, url := newWire(t, func(_ context.Context, operation string, input request) (int, any) {
				switch operation {
				case "ReceiveMessage":
					return 200, map[string]any{"Messages": []any{native("{}", nil)}}
				case "ChangeMessageVisibility":
					changes.Add(1)
					if input["VisibilityTimeout"] != float64(delay/time.Second) || input["ReceiptHandle"] != "private-receipt" {
						t.Errorf("incorrect visibility request: %+v", input)
					}
					cancel()
					return 200, request{}
				case "DeleteMessage":
					deletes.Add(1)
					cancel()
					return 200, request{}
				default:
					t.Errorf("unexpected operation: %s", operation)
					return 500, request{}
				}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) { observed = append(observed, o) }}
			})
			handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				callbacks.Add(1)
				return broker.RetryAfter{Delay: delay, Cause: marker}, nil
			}, broker.WithRequirements(broker.RedeliveryRequirement{}))
			err := consumer.Run(ctx, handler)
			if !errors.Is(err, context.Canceled) || callbacks.Load() != 1 || changes.Load() != 1 || deletes.Load() != 0 {
				t.Fatalf("route err=%v callbacks=%d changes=%d deletes=%d", err, callbacks.Load(), changes.Load(), deletes.Load())
			}
			for _, operation := range []string{"process", "retry"} {
				found := false
				for _, o := range observed {
					if o.Operation == operation {
						found = true
						if !errors.Is(o.Cause, marker) {
							t.Errorf("%s lost retry cause", operation)
						}
					}
				}
				if !found {
					t.Errorf("missing %s observation: %+v", operation, observed)
				}
			}
		})
	}
}

func TestSDKVisibilityValidationBeforeEffects(t *testing.T) {
	var calls atomic.Int32
	client, url := newWire(t, func(context.Context, string, request) (int, any) { calls.Add(1); return 200, request{} })
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = time.Second })
	for _, requirements := range [][]broker.Requirement{
		{broker.RedeliveryRequirement{}, broker.AttemptsRequirement{}, broker.KeepAliveRequirement{Interval: time.Millisecond}},
		{broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.KeepAliveRequirement{Interval: time.Millisecond}},
	} {
		if err := consumer.Validate(requirements...); err != nil {
			t.Errorf("valid requirements rejected: %v", err)
		}
	}
	for _, requirements := range [][]broker.Requirement{
		{broker.KeepAliveRequirement{}}, {broker.KeepAliveRequirement{Interval: -time.Second}},
		{broker.KeepAliveRequirement{Interval: time.Second}},
		{broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.KeepAliveRequirement{Interval: 2 * time.Millisecond}},
		{broker.KeepAliveRequirement{Interval: 2 * time.Millisecond}, broker.KeepAliveRequirement{Interval: time.Millisecond}},
		{broker.KeepAliveRequirement{}, broker.KeepAliveRequirement{Interval: time.Millisecond}},
		{broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.KeepAliveRequirement{}},
	} {
		if err := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("callback admitted")
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirements...))); err == nil {
			t.Error("invalid requirements accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("validation made remote requests")
	}
}

func TestSDKInvalidRetryNeverChangesVisibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		delay    time.Duration
		declared bool
	}{
		{"fraction",
			time.Millisecond,
			true},
		{"negative",
			-time.Second,
			true},
		{"overflow",
			12*time.Hour + time.Second,
			true},
		{"undeclared",
			time.Second,
			false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var terminals atomic.Int32
			client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
				if operation != "ReceiveMessage" {
					terminals.Add(1)
				}
				return 200, map[string]any{"Messages": []any{native("{}", nil)}}
			})
			consumer := newConsumer(t, client, url, nil)
			var requirements []broker.Requirement
			if tc.declared {
				requirements = append(requirements, broker.RedeliveryRequirement{})
			}
			err := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				return broker.RetryAfter{Delay: tc.delay}, nil
			}, broker.WithRequirements(requirements...)))
			if err == nil || terminals.Load() != 0 {
				t.Fatalf("invalid retry err=%v terminals=%d", err, terminals.Load())
			}
		})
	}
}

func awaitStarted(t *testing.T, entered <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("run ended before operation: %v", err)
	case <-time.After(testWait):
		t.Fatal("operation did not start")
	}
}

func TestSDKNormalReturnJoinsActiveRenewalWithoutCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	callbackReturned := make(chan struct{})
	var renewals, deletes atomic.Int32
	client, url := newWire(t, func(requestCtx context.Context, operation string, input request) (int, any) {
		switch operation {
		case "ReceiveMessage":
			return 200, map[string]any{"Messages": []any{native("{}", nil)}}
		case "ChangeMessageVisibility":
			if renewals.Add(1) == 1 {
				close(entered)
			}
			if input["VisibilityTimeout"] != float64(1) {
				t.Errorf("renewal visibility %v", input)
			}
			select {
			case <-release:
			case <-requestCtx.Done():
				t.Error("normal callback return canceled admitted renewal")
			}
			return 200, request{}
		case "DeleteMessage":
			deletes.Add(1)
			cancel()
			return 200, request{}
		default:
			return 500, request{}
		}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = time.Second })
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			<-entered
			close(callbackReturned)
			return broker.Handled{}, nil
		},
			broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond},
				broker.KeepAliveRequirement{Interval: time.Millisecond})))
	}()
	awaitStarted(t, entered, done)
	await(t, callbackReturned)
	if deletes.Load() != 0 {
		t.Fatal("delete overlapped active renewal")
	}
	select {
	case err := <-done:
		t.Fatalf("run failed to join renewal: %v", err)
	default:
	}
	release <- struct{}{}
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if renewals.Load() != 1 || deletes.Load() != 1 {
		t.Fatalf("renewals=%d deletes=%d", renewals.Load(), deletes.Load())
	}
}

func TestSDKRenewalFailureCancelsCallbackAndPreservesCauses(t *testing.T) {
	callbackCause := errors.New("independent callback failure")
	var terminals atomic.Int32
	var mu sync.Mutex
	var observations []adapter.Observation
	client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			return 400, request{"__type": "ReceiptHandleIsInvalid", "message": "sensitive receipt"}
		}
		if operation == "DeleteMessage" {
			terminals.Add(1)
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.VisibilityTimeout = time.Second
		c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
			mu.Lock()
			defer mu.Unlock()
			observations = append(observations, o)
		}}
	})
	callbackJoined := make(chan struct{})
	err := consumer.Run(context.Background(), broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
		defer close(callbackJoined)
		<-ctx.Done()
		return broker.Handled{}, callbackCause
	}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
	var apiError smithy.APIError
	if !errors.Is(err, callbackCause) || !errors.As(err, &apiError) || apiError.ErrorCode() != "ReceiptHandleIsInvalid" {
		t.Fatalf("joined causes missing: %v", err)
	}
	await(t, callbackJoined)
	if terminals.Load() != 0 {
		t.Fatal("failed renewal allowed terminal operation")
	}
	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, o := range observations {
		if o.Operation == "renew" {
			found = true
			if !errors.As(o.Err, &apiError) {
				t.Fatal("observer lost native cause")
			}
		}
	}
	if !found {
		t.Fatal("renewal outcome not observed")
	}
}

func TestSDKForcedShutdownCancelsRenewalAndSuppressesRetry(t *testing.T) {
	for _, policy := range []broker.ShutdownPolicy{{Mode: broker.ShutdownCancel}, {GracePeriod: time.Millisecond}} {
		t.Run(policy.GracePeriod.String(), func(t *testing.T) { testForcedRenewalShutdown(t, policy) })
	}
}
func testForcedRenewalShutdown(t *testing.T, policy broker.ShutdownPolicy) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	entered, exited := make(chan struct{}), make(chan struct{})
	var changes, deletes atomic.Int32
	client, url := newWire(t, func(requestCtx context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			changes.Add(1)
			close(entered)
			<-requestCtx.Done()
			close(exited)
			return 200, request{}
		}
		if operation == "DeleteMessage" {
			deletes.Add(1)
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.VisibilityTimeout = time.Second
		c.Shutdown = policy
	})
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, broker.NewHandler(func(cbCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
			<-cbCtx.Done()
			return broker.RetryAfter{}, nil
		}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond}, broker.RedeliveryRequirement{})))
	}()
	awaitStarted(t, entered, done)
	stop := errors.New("owner shutdown")
	cancel(stop)
	if err := await(t, done); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	await(t, exited)
	if changes.Load() != 1 || deletes.Load() != 0 {
		t.Fatalf("forced shutdown admitted terminal changes=%d deletes=%d", changes.Load(), deletes.Load())
	}
}

func TestSDKRenewalGracefulStopContinuesUntilCallbackDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan struct{})
	var renewals, deletes atomic.Int32
	client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
		switch operation {
		case "ChangeMessageVisibility":
			switch renewals.Add(1) {
			case 1:
				cancel()
			case 2:
				close(second)
			}
			return 200, request{}
		case "DeleteMessage":
			deletes.Add(1)
			return 200, request{}
		default:
			return 200, map[string]any{"Messages": []any{native("{}", nil)}}
		}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = time.Second })
	err := consumer.Run(ctx, broker.NewHandler(func(cbCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
		await(t, second)
		if cbCtx.Err() != nil {
			t.Error("graceful callback canceled")
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
	if !errors.Is(err, context.Canceled) || renewals.Load() < 2 || deletes.Load() != 1 {
		t.Fatalf("renewals=%d deletes=%d err=%v", renewals.Load(), deletes.Load(), err)
	}
}

func TestSDKRenewalMixedCancellationFailureIsRetained(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	marker := errors.New("independent SDK middleware failure")
	client, url := newWire(t, func(requestCtx context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			close(entered)
			<-requestCtx.Done()
			return 200, request{}
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	client = awssqs.New(client.Options(), func(o *awssqs.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("JoinedFailure",
				func(ctx context.Context,
					input middleware.InitializeInput,
					next middleware.InitializeHandler) (middleware.InitializeOutput,
					middleware.Metadata,
					error) {
					output, metadata, err := next.HandleInitialize(ctx, input)
					if errors.Is(err, context.Canceled) {
						err = errors.Join(err, marker)
					}
					return output, metadata, err
				}), middleware.Before)
		})
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.VisibilityTimeout = time.Second
		c.Shutdown.Mode = broker.ShutdownCancel
	})
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, broker.NewHandler(func(cbCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
			<-cbCtx.Done()
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
	}()
	awaitStarted(t, entered, done)
	cancel()
	if err := await(t, done); !errors.Is(err, marker) || !errors.Is(err, context.Canceled) {
		t.Fatalf("independent failure suppressed: %v", err)
	}
}

func TestSDKRetryFailureNeverChoosesOppositeAction(t *testing.T) {
	var callbacks, changes, deletes atomic.Int32
	marker := errors.New("requested retry reason")
	var outcome adapter.Observation
	client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			changes.Add(1)
			return 500, request{"__type": "InternalError", "message": "unknown native result"}
		}
		if operation == "DeleteMessage" {
			deletes.Add(1)
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t,
		client,
		url,
		func(c *adapter.ConsumerConfig) {
			c.ResilientReceive = true
			c.Observer = &adapter.Observer{Record: func(_ context.Context,
				o adapter.Observation) {
				if o.Operation == adapter.OperationRetry {
					outcome = o
				}
			}}
		})
	err := consumer.Run(context.Background(),
		broker.NewHandler(func(context.Context,
			broker.Delivery) (broker.Disposition,
			error) {
			callbacks.Add(1)
			return broker.RetryAfter{Delay: time.Second,
					Cause: marker},
				nil
		},
			broker.WithRequirements(broker.RedeliveryRequirement{})))
	var nativeError smithy.APIError
	if !errors.As(err,
		&nativeError) || nativeError.ErrorCode() != "InternalError" || callbacks.Load() != 1 || changes.Load() != 1 || deletes.Load() != 0 {
		t.Fatalf("err=%v callbacks=%d changes=%d deletes=%d",
			err,
			callbacks.Load(),
			changes.Load(),
			deletes.Load())
	}
	if outcome.Outcome != adapter.OutcomeUnknown || !errors.Is(outcome.Cause,
		marker) || !errors.As(outcome.Err,
		&nativeError) {
		t.Fatalf("retry evidence %+v",
			outcome)
	}
}

func TestSDKRenewalStartsBeforeDecodeAndObserverPanicCannotChangeDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	renewed := make(chan struct{})
	observerCause := errors.New("renewal observer panic")
	var renewals, deletes atomic.Int32
	client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			renewals.Add(1)
			return 200, request{}
		}
		if operation == "DeleteMessage" {
			deletes.Add(1)
			cancel()
			return 200, request{}
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.VisibilityTimeout = time.Second
		c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
			if o.Operation == adapter.OperationRenew {
				close(renewed)
				panic(observerCause)
			}
		}}
	})
	decoder := func([]byte) (string, error) {
		select {
		case <-renewed:
			return "decoded", nil
		case <-time.After(testWait):
			return "", errors.New("renewal did not run during decoding")
		}
	}
	err := consumer.Run(ctx, broker.NewTypedHandler(decoder, func(context.Context, string) error { return nil },
		broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond})))
	if !errors.Is(err, observerCause) || !errors.Is(err, context.Canceled) || renewals.Load() < 1 || deletes.Load() != 1 {
		t.Fatalf("renewals=%d deletes=%d err=%v", renewals.Load(), deletes.Load(), err)
	}
}

func TestSDKPanicUnwindingJoinsRenewalBeforeOuterRecovery(t *testing.T) {
	for _, stage := range []string{"decoder", "callback"} {
		t.Run(stage, func(t *testing.T) {
			entered, release, panicking := make(chan struct{}), make(chan struct{}), make(chan struct{})
			joined := make(chan struct{}, 1)
			var renewals, deletes atomic.Int32
			var abortRenewal atomic.Bool
			nextCtx, stopNext := context.WithCancel(context.Background())
			defer stopNext()
			client, url := newWire(t, func(requestCtx context.Context, operation string, _ request) (int, any) {
				switch operation {
				case "ChangeMessageVisibility":
					if renewals.Add(1) == 1 {
						close(entered)
					}
					select {
					case <-release:
					case <-requestCtx.Done():
						t.Error("panic unwinding canceled admitted renewal")
					}
					if abortRenewal.Load() {
						return 400, request{"__type": "ReceiptHandleIsInvalid", "message": "cleanup regression worker"}
					}
					return 200, request{}
				case "DeleteMessage":
					deletes.Add(1)
					stopNext()
					return 200, request{}
				default:
					return 200, map[string]any{"Messages": []any{native("{}", nil)}}
				}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.VisibilityTimeout = time.Second
				c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
					if o.Operation == adapter.OperationRenew {
						joined <- struct{}{}
					}
				}}
			})
			marker := errors.New(stage + " panic identity")
			panicNow := func() { <-entered; close(panicking); panic(marker) }
			decoder := func([]byte) (string, error) {
				if stage == "decoder" {
					panicNow()
				}
				return "decoded", nil
			}
			handler := broker.NewTypedHandler(decoder, func(context.Context, string) error { panicNow(); return nil },
				broker.WithRequirements(broker.KeepAliveRequirement{Interval: time.Millisecond}))
			recovered := make(chan any, 1)
			go func() { defer func() { recovered <- recover() }(); _ = consumer.Run(context.Background(), handler) }()
			await(t, entered)
			await(t, panicking)
			var panicValue any
			early := false
			select {
			case panicValue = <-recovered:
				early = true
				abortRenewal.Store(true)
				t.Error("outer recovery completed while admitted renewal was still running")
			case <-time.After(50 * time.Millisecond):
			}
			if deletes.Load() != 0 {
				t.Error("panic issued terminal operation")
			}
			close(release)
			await(t, joined)
			if !early {
				panicValue = await(t, recovered)
			}
			if panicValue != marker {
				t.Fatalf("panic identity changed: %v", panicValue)
			}
			var nextCallbacks int
			err := consumer.Run(nextCtx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				nextCallbacks++
				return broker.Handled{}, nil
			}))
			if !errors.Is(err, context.Canceled) || nextCallbacks != 1 || deletes.Load() != 1 || renewals.Load() != 1 {
				t.Fatalf("next run contaminated: err=%v callbacks=%d deletes=%d renewals=%d", err, nextCallbacks, deletes.Load(), renewals.Load())
			}
		})
	}
}
