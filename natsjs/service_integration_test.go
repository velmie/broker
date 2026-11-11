package natsjs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func TestRunServiceParentStopAndPrefetch(t *testing.T) {
	for _, mode := range []string{
		"pre_canceled", "idle", "prefetched", "deadline", "custom", "custom_grace_cause", "pre_canceled_grace_cause",
	} {
		t.Run(
			mode,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.BatchSize = 3
				parent, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				cause := context.Canceled
				if mode == "custom" {
					cause = errors.New("service stopped by upstream")
				}
				if mode == "custom_grace_cause" || mode == "pre_canceled_grace_cause" {
					config.Shutdown.Mode = broker.ShutdownCancel
					cause = natsjs.ErrGracePeriodExceeded
				}
				if mode == "deadline" {
					var deadlineCancel context.CancelFunc
					parent, deadlineCancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
					defer deadlineCancel()
					cause = context.DeadlineExceeded
				}
				if mode == "pre_canceled" || mode == "pre_canceled_grace_cause" {
					cancel(cause)
				}
				if mode == "prefetched" {
					for range config.BatchSize {
						if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
							t.Fatal(err)
						}
					}
				}
				var stopCause error
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if event.Operation == natsjs.OperationFetch && mode != "deadline" {
						cancel(cause)
					}
					if event.Operation == natsjs.OperationStop && event.Outcome == natsjs.OutcomeStarted {
						stopCause = event.Cause
					}
				}
				consumer, err := natsjs.NewConsumer(nc, config)
				if err != nil {
					t.Fatal(err)
				}
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						t.Error("callback admitted after stop")
						return broker.Handled{}, nil
					},
				)
				if err = consumer.RunService(parent, handler); err != nil {
					t.Fatal(err)
				}
				if !errors.Is(stopCause, cause) {
					t.Fatalf("stop cause=%v want=%v", stopCause, cause)
				}
				if mode == "prefetched" {
					assertPending(t, js, config, config.BatchSize)
				}
			},
		)
	}
}

func TestRunServiceGracefulDrainAndRenewal(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, renewal, release := make(chan context.Context, 1), make(chan struct{}, 1), make(chan struct{})
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeConfirmed {
			select {
			case renewal <- struct{}{}:
			default:
			}
		}
	}
	handler := broker.NewHandler(
		func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
			entered <- work
			<-release
			return broker.Handled{}, nil
		},
		broker.WithKeepAlive(40*time.Millisecond),
	)
	result := startServiceConsumer(t, nc, js, config, parent, handler)
	work := receive(t, entered)
	cancel()
	receive(t, renewal)
	if work.Err() != nil {
		t.Fatal("graceful stop canceled active work")
	}
	close(release)
	if err := receive(t, result); err != nil {
		t.Fatal(err)
	}
	assertPending(t, js, config, 0)
}

func TestRunServiceForcedStopCallbackOutcomes(t *testing.T) {
	for _, mode := range []string{"nil", "direct", "wrapped", "joined", "grace"} {
		t.Run(
			mode,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				if mode == "grace" {
					config.Shutdown.GracePeriod = 25 * time.Millisecond
				} else {
					config.Shutdown.Mode = broker.ShutdownCancel
				}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered := make(chan struct{})
				independent := errors.New("business effect failed")
				handler := broker.NewHandler(
					func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
						close(entered)
						<-work.Done()
						switch mode {
						case "direct":
							return nil, work.Err()
						case "wrapped":
							return nil, fmt.Errorf("incomplete effect: %w", work.Err())
						case "joined":
							return nil, errors.Join(independent, work.Err())
						default:
							return broker.RetryAfter{
								Cause: independent,
							}, nil
						}
					},
					broker.WithRequirements(broker.RedeliveryRequirement{}),
				)
				result := startServiceConsumer(t, nc, js, config, parent, handler)
				receive(t, entered)
				cancel()
				err := receive(t, result)
				switch mode {
				case "nil", "direct":
					if err != nil {
						t.Fatalf("cooperative stop: %v", err)
					}
				case "wrapped":
					if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "incomplete effect") {
						t.Fatalf("wrapped callback: %v", err)
					}
				case "joined":
					if !errors.Is(err, independent) || !errors.Is(err, context.Canceled) {
						t.Fatalf("joined callback: %v", err)
					}
				case "grace":
					if !errors.Is(err, natsjs.ErrGracePeriodExceeded) {
						t.Fatalf("grace expiry: %v", err)
					}
				}
				assertPending(t, js, config, 1)
			},
		)
	}
}

func TestRunServiceIndependentCallbackAndCleanupFailures(t *testing.T) {
	_, js, config := provision(t, time.Second)
	nc, err := nats.Connect(serverURL, nats.NoReconnect(), nats.FlusherTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("actual callback failure")
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			nc.Close()
			cancel()
			return nil, failure
		},
	)
	err = receive(t, startServiceConsumer(t, nc, js, config, parent, handler))
	for _, cause := range []error{failure, nats.ErrConnectionClosed} {
		if !errors.Is(err, cause) || !strings.Contains(err.Error(), cause.Error()) {
			t.Errorf("missing %v in %v", cause, err)
		}
	}
	assertPending(t, js, config, 1)
}

func TestRunServiceAdmittedSettlement(t *testing.T) {
	for _, disposition := range terminalResults() {
		t.Run(
			disposition.name,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.Shutdown.Mode = broker.ShutdownCancel
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				confirmed := false
				config.Observer.Record = func(work context.Context, event natsjs.Observation) {
					if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
						cancel()
						receive(t, work.Done())
					}
					if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeConfirmed {
						confirmed = true
					}
				}
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						return disposition.value, nil
					},
					broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}),
				)
				err := receive(t, startServiceConsumer(t, nc, js, config, parent, handler))
				if err != nil || !confirmed {
					t.Fatalf("result=%v confirmed=%v", err, confirmed)
				}
				if disposition.name != "retry" {
					assertPending(t, js, config, 0)
				}
			},
		)
	}
}

func TestRunServiceSettlementTimeoutDuringStop(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config.OperationTimeout = 200 * time.Millisecond
	config.Shutdown.Mode = broker.ShutdownCancel
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	admitted, send := make(chan struct{}), make(chan struct{})
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeStarted {
			close(admitted)
			<-send
		}
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil })
	result := startServiceConsumer(t, nc, js, config, parent, handler)
	receive(t, admitted)
	resume := pauseServer(t)
	cancel()
	close(send)
	err := receive(t, result)
	resume()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost independent ACK deadline: %v", err)
	}
	awaitReadback(
		t,
		func() error {
			info, readErr := js.ConsumerInfo(config.Stream, config.Consumer)
			if readErr != nil {
				return readErr
			}
			if info.NumAckPending != 0 {
				return fmt.Errorf("pending=%d", info.NumAckPending)
			}
			return nil
		},
	)
}

func TestRunServiceStartupFailureConcurrentStop(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	if err := js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationStartup && event.Outcome == natsjs.OutcomeFailed {
			cancel()
		}
	}
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Fatal("missing consumer processed a delivery")
			return broker.Handled{}, nil
		},
	)
	err = consumer.RunService(parent, handler)
	if !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("lost native startup failure: %v", err)
	}
}

func TestRunServiceRealCoordinatedConsumers(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	peer := config
	peer.Consumer = "service-peer"
	if _, err := js.AddConsumer(
		peer.Stream,
		&nats.ConsumerConfig{
			Durable:       peer.Consumer,
			FilterSubject: peer.Subject,
			AckPolicy:     nats.AckExplicitPolicy,
		},
	); err != nil {
		t.Fatal(err)
	}
	peerEntered := make(chan struct{})
	failure := errors.New("callback failed")
	coordinator := broker.NewCoordinator()
	failed := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			receive(t, peerEntered)
			return nil, failure
		},
	)
	graceful := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			close(peerEntered)
			return broker.Handled{}, nil
		},
	)
	if err := coordinator.Add("failed", func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, config) }, failed); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("peer", func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, peer) }, graceful); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	err := coordinator.RunService(context.Background())
	var registration *broker.RegistrationError
	if !errors.Is(err, failure) || errors.Is(err, context.Canceled) || !errors.As(err, &registration) || registration.Name != "failed" {
		t.Fatalf("result=%v", err)
	}
	assertPending(t, js, peer, 0)
}

func TestRunServiceObserverPanicTextInSlog(t *testing.T) {
	cause := errors.New("observer error value")
	for _, value := range []any{cause, "observer string value"} {
		t.Run(
			fmt.Sprint(value),
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.Observer.Begin = func(context.Context, natsjs.DeliveryInfo) context.Context { panic(value) }
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						return nil, errors.New("callback stopped")
					},
				)
				err := receive(t, startServiceConsumer(t, nc, js, config, context.Background(), handler))
				var recovered *natsjs.ObserverPanicError
				if !errors.As(err, &recovered) || value == cause && !errors.Is(err, cause) {
					t.Fatalf("observer panic: %v", err)
				}
				var output bytes.Buffer
				slog.New(slog.NewTextHandler(&output, nil)).Error("run failed", "error", err)
				if !strings.Contains(output.String(), fmt.Sprint(value)) {
					t.Fatalf("panic value missing: %s", output.String())
				}
			},
		)
	}
}

//nolint:gocritic // Each test owns its configuration value.
func startServiceConsumer(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext, config natsjs.ConsumerConfig,
	ctx context.Context, handler broker.Handler,
) <-chan error {
	t.Helper()
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- consumer.RunService(ctx, handler) }()
	return result
}

func TestRunServiceIndependentCallbackDeadlineAndContextCancellation(t *testing.T) {
	for _, mode := range []string{"callback_deadline", "observer_canceled"} {
		t.Run(
			mode,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.Shutdown.Mode = broker.ShutdownCancel
				if mode == "observer_canceled" {
					config.Observer.Begin = func(ctx context.Context, _ natsjs.DeliveryInfo) context.Context {
						child, cancel := context.WithCancel(ctx)
						cancel()
						return child
					}
				}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ready, release := make(chan struct{}), make(chan struct{})
				handler := broker.NewHandler(
					func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
						if mode == "observer_canceled" {
							close(ready)
							<-release
							return nil, work.Err()
						}
						local, finish := context.WithTimeout(work, 25*time.Millisecond)
						defer finish()
						<-local.Done()
						close(ready)
						<-release
						return nil, local.Err()
					},
				)
				result := startServiceConsumer(t, nc, js, config, parent, handler)
				receive(t, ready)
				cancel()
				close(release)
				err := receive(t, result)
				want := context.DeadlineExceeded
				if mode == "observer_canceled" {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("lost independent callback context: %v", err)
				}
				assertPending(t, js, config, 1)
			},
		)
	}
}

func TestRunServiceStartupRequestStopAndTimeout(t *testing.T) {
	for _, mode := range []string{"parent_stop", "operation_timeout"} {
		t.Run(
			mode,
			func(t *testing.T) {
				nc, _, config := provision(t, time.Second)
				config.APIPrefix = "service-start"
				config.OperationTimeout = 150 * time.Millisecond
				admin := reconnectConnection(t)
				requested := make(chan struct{}, 1)
				subscription, err := admin.Subscribe(
					config.APIPrefix+".CONSUMER.INFO.>",
					func(*nats.Msg) {
						requested <- struct{}{}
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(
					func() {
						if cleanupErr := subscription.Unsubscribe(); cleanupErr != nil {
							t.Error(cleanupErr)
						}
					},
				)
				if err = admin.Flush(); err != nil {
					t.Fatal(err)
				}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if mode == "operation_timeout" && event.Operation == natsjs.OperationStartup && event.Outcome == natsjs.OutcomeFailed {
						cancel()
					}
				}
				consumer, err := natsjs.NewConsumer(nc, config)
				if err != nil {
					t.Fatal(err)
				}
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						t.Error("startup did not finish")
						return broker.Handled{}, nil
					},
				)
				result := make(chan error, 1)
				go func() { result <- consumer.RunService(parent, handler) }()
				receive(t, requested)
				if mode == "parent_stop" {
					cancel()
				}
				err = receive(t, result)
				if mode == "parent_stop" && err != nil {
					t.Fatalf("startup stop: %v", err)
				}
				if mode == "operation_timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("startup deadline: %v", err)
				}
			},
		)
	}
}

func TestRunServiceRenewalFailureDuringStop(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config.OperationTimeout = 200 * time.Millisecond
	config.Shutdown.Mode = broker.ShutdownCancel
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	admitted, send := make(chan struct{}), make(chan struct{})
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeStarted {
			close(admitted)
			<-send
		}
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeUnknown {
			cancel()
		}
	}
	handler := broker.NewHandler(
		func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
			<-work.Done()
			return nil, work.Err()
		},
		broker.WithKeepAlive(25*time.Millisecond),
	)
	result := startServiceConsumer(t, nc, js, config, parent, handler)
	receive(t, admitted)
	resume := pauseServer(t)
	close(send)
	err := receive(t, result)
	resume()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost renewal deadline: %v", err)
	}
	assertPending(t, js, config, 1)
}

func TestRunServiceStopDuringPullRecovery(t *testing.T) {
	nc := reconnectConnection(t)
	js, config := provisionTopology(
		t,
		nc,
		nats.MemoryStorage,
		&nats.ConsumerConfig{
			AckWait:    time.Second,
			MaxWaiting: 1,
		},
	)
	held, err := js.PullSubscribe(config.Subject, config.Consumer, nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(
		func() {
			if cleanupErr := held.Unsubscribe(); cleanupErr != nil {
				t.Error(cleanupErr)
			}
		},
	)
	startHeldPull(t, held)
	waitPull(t, js, config)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == natsjs.OutcomeInterrupted {
			cancel()
		}
	}
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		},
	)
	if err = consumer.RunService(parent, handler); err != nil {
		t.Fatalf("stop during recovery: %v", err)
	}
}

func TestRunServiceRecoveryRealGenerations(t *testing.T) {
	for _, stopDuringBackoff := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("stop_during_backoff_%v", stopDuringBackoff),
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.OperationTimeout = 150 * time.Millisecond
				if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
						cancel()
					}
				}
				resume := pauseServer(t)
				var logs bytes.Buffer
				logger := slog.New(
					slog.NewTextHandler(
						&logs,
						&slog.HandlerOptions{
							ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
								if attr.Key == "error" {
									resume()
									if stopDuringBackoff {
										cancel()
									}
								}
								return attr
							},
						},
					),
				)
				generations, callbacks := 0, 0
				factory := broker.WithConsumerRecovery(
					func() (broker.Consumer, error) {
						generations++
						return natsjs.NewConsumer(nc, config)
					},
					broker.RecoveryConfig{
						Source:      config.Stream,
						MaxRestarts: 1,
						Retryable:   func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
						Logger:      logger,
					},
				)
				consumer, err := factory()
				if err != nil {
					t.Fatal(err)
				}
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						callbacks++
						return broker.Handled{}, nil
					},
				)
				err = consumer.(broker.ServiceConsumer).RunService(parent, handler)
				if stopDuringBackoff {
					if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || generations != 1 || callbacks != 0 {
						t.Fatalf("interrupted recovery: err=%v generations=%d callbacks=%d", err, generations, callbacks)
					}
				} else if err != nil || generations != 2 || callbacks != 1 {
					t.Fatalf("recovered: err=%v generations=%d callbacks=%d", err, generations, callbacks)
				}
				if !strings.Contains(logs.String(), context.DeadlineExceeded.Error()) {
					t.Fatalf("restart log lost original cause: %s", logs.String())
				}
			},
		)
	}
}

func TestRunServiceForcedStopSuppressesAllDispositions(t *testing.T) {
	for _, disposition := range terminalResults() {
		t.Run(
			disposition.name,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.Shutdown.Mode = broker.ShutdownCancel
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered := make(chan struct{})
				started := false
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
						started = true
					}
				}
				handler := broker.NewHandler(
					func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
						close(entered)
						<-work.Done()
						return disposition.value, nil
					},
					broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}),
				)
				result := startServiceConsumer(t, nc, js, config, parent, handler)
				receive(t, entered)
				cancel()
				if err := receive(t, result); err != nil || started {
					t.Fatalf("stop=%v terminal started=%v", err, started)
				}
				assertPending(t, js, config, 1)
			},
		)
	}
}

func TestRunServiceFailedTerminalRetainsDispositionCause(t *testing.T) {
	for _, mode := range []string{"retry", "terminate", "reject"} {
		t.Run(
			mode,
			func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				config.OperationTimeout = 200 * time.Millisecond
				config.Shutdown.Mode = broker.ShutdownCancel
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				admitted, send := make(chan struct{}), make(chan struct{})
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
						close(admitted)
						<-send
					}
				}
				cause := errors.New("selected delivery policy")
				handler := broker.NewHandler(
					func(context.Context, broker.Delivery) (broker.Disposition, error) {
						if mode == "retry" {
							return broker.RetryAfter{
								Delay: time.Second,
								Cause: cause,
							}, nil
						}
						if mode == "reject" {
							return broker.Reject{Cause: cause}, nil
						}
						return natsjs.Terminate{
							Cause: cause,
						}, nil
					},
					broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}, broker.RejectRequirement{}),
				)
				result := startServiceConsumer(t, nc, js, config, parent, handler)
				receive(t, admitted)
				resume := pauseServer(t)
				cancel()
				close(send)
				err := receive(t, result)
				resume()
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) {
					t.Fatalf("terminal failure lost its causes: %v", err)
				}
				if mode == "terminate" || mode == "reject" {
					awaitReadback(
						t,
						func() error {
							info, readErr := js.ConsumerInfo(config.Stream, config.Consumer)
							if readErr != nil {
								return readErr
							}
							if info.NumAckPending != 0 {
								return fmt.Errorf("pending=%d", info.NumAckPending)
							}
							return nil
						},
					)
				} else {
					assertPending(t, js, config, 1)
				}
			},
		)
	}
}

func TestRunServiceKeepsTypedHandlerCancellationStage(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config.Shutdown.Mode = broker.ShutdownCancel
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	handler := broker.NewTypedHandler(
		broker.DecodeJSON[struct{}],
		func(work context.Context, _ struct{}) error {
			close(entered)
			<-work.Done()
			return work.Err()
		},
	)
	result := startServiceConsumer(t, nc, js, config, parent, handler)
	receive(t, entered)
	cancel()
	err := receive(t, result)
	var stage *broker.StageError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &stage) || stage.Stage != broker.StageHandle {
		t.Fatalf("typed callback stage was lost: %v", err)
	}
	assertPending(t, js, config, 1)
}

func TestRunServiceGraceSentinelAsParentCauseDuringCallback(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config.Shutdown.Mode = broker.ShutdownCancel
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	entered := make(chan struct{})
	handler := broker.NewHandler(
		func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
			close(entered)
			<-work.Done()
			return nil, work.Err()
		},
	)
	result := startServiceConsumer(t, nc, js, config, parent, handler)
	receive(t, entered)
	cancel(natsjs.ErrGracePeriodExceeded)
	if err := receive(t, result); err != nil {
		t.Fatalf("parent cause was mistaken for the consumer's grace expiry: %v", err)
	}
	assertPending(t, js, config, 1)
}

func TestRunServiceFetchFailureConcurrentStop(t *testing.T) {
	_, js, config := provision(t, time.Second)
	config.FetchTimeout = 3 * time.Second
	worker, err := nats.Connect(serverURL, nats.NoReconnect(), nats.FlusherTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == natsjs.OutcomeFailed {
			cancel()
		}
	}
	consumer, err := natsjs.NewConsumer(worker, config)
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		},
	)
	result := make(chan error, 1)
	go func() { result <- consumer.RunService(parent, handler) }()
	waitPull(t, js, config)
	worker.Close()
	err = receive(t, result)
	if !errors.Is(err, nats.ErrConnectionClosed) || !strings.Contains(err.Error(), "fetch") {
		t.Fatalf("lost native fetch failure: %v", err)
	}
}
