package natsjs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestGracefulShutdownRetainsProcessingValuesAndRenewal(t *testing.T) {
	nc, js, config := provision(t, 150*time.Millisecond)
	config.Observer.Begin = func(ctx context.Context, _ natsjs.DeliveryInfo) context.Context {
		return context.WithValue(ctx, correlationKey{}, "delivery-span")
	}
	parent, stop := context.WithCancel(context.WithValue(context.Background(), requestKey{}, "request"))
	defer stop()
	entered, release := make(chan context.Context, 1), make(chan struct{})
	renewed := make(chan struct{}, 16)
	var terminalContext bool
	config.Observer.Record = func(ctx context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeConfirmed {
			renewed <- struct{}{}
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			terminalContext = ctx.Value(correlationKey{}) == "delivery-span"
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(ctx context.Context, _ struct{}) error {
		entered <- ctx
		<-release
		return nil
	}, broker.WithKeepAlive(25*time.Millisecond))
	result := startConsumer(t, nc, js, config, parent, h, `{}`)
	work := receive(t, entered)
	stop()
	for range 8 {
		receive(t, renewed)
	}
	if work.Err() != nil || work.Value(requestKey{}) != "request" {
		t.Fatalf("graceful context: %v", work.Err())
	}
	close(release)
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !terminalContext {
		t.Error("terminal observation lost delivery correlation")
	}
	assertPending(t, js, config, 0)
	if nc.IsClosed() {
		t.Error("consumer closed borrowed connection")
	}
}

func TestForcedStopSuppressesEveryUnadmittedDisposition(t *testing.T) {
	for _, mode := range []string{"cancel", "grace_expiry"} {
		for _, disposition := range terminalResults() {
			t.Run(mode+"_"+disposition.name, func(t *testing.T) {
				nc, js, config := provision(t, time.Second)
				if mode == "cancel" {
					config.Shutdown.Mode = broker.ShutdownCancel
				} else {
					config.Shutdown.GracePeriod = 20 * time.Millisecond
				}
				ctx, stop := context.WithCancel(context.Background())
				defer stop()
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var executed bool
				config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
					if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
						executed = true
					}
				}
				h := broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
					close(entered)
					<-work.Done()
					close(canceled)
					<-release // Deliberately ignore cancellation until released.
					return disposition.value, nil
				}, broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}))
				result := startConsumer(t, nc, js, config, ctx, h, `{}`)
				receive(t, entered)
				stop()
				receive(t, canceled)
				select {
				case err := <-result:
					t.Fatalf("Run returned before callback joined: %v", err)
				default:
				}
				close(release)
				err := receive(t, result)
				if !errors.Is(err, context.Canceled) || executed {
					t.Fatalf("stop result=%v executed=%v", err, executed)
				}
				if mode == "grace_expiry" && !errors.Is(err, natsjs.ErrGracePeriodExceeded) {
					t.Fatalf("grace cause lost: %v", err)
				}
				assertPending(t, js, config, 1)
			})
		}
	}
}

func TestAdmittedTerminalOperationSurvivesLaterForcedStop(t *testing.T) {
	for _, disposition := range terminalResults() {
		t.Run(disposition.name, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			config.Shutdown.Mode = broker.ShutdownCancel
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			var confirmations int
			config.Observer.Record = func(work context.Context, event natsjs.Observation) {
				if !isTerminal(event.Operation) {
					return
				}
				if event.Outcome == natsjs.OutcomeStarted {
					stop()
					receive(t, work.Done()) // Stop has won after terminal admission.
				}
				if event.Outcome == natsjs.OutcomeConfirmed {
					confirmations++
				}
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return disposition.value, nil },
				broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}))
			result := startConsumer(t, nc, js, config, ctx, h, `{}`)
			if err := receive(t, result); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if confirmations != 1 {
				t.Fatalf("confirmations=%d", confirmations)
			}
			if disposition.name != "retry" {
				assertPending(t, js, config, 0)
			}
		})
	}
}

func TestIdleAndPrefetchedShutdownDoNotAdmitCallbacks(t *testing.T) {
	for _, mode := range []string{"already_canceled", "idle", "prefetched"} {
		t.Run(mode, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			config.BatchSize = 3
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			if mode == "already_canceled" {
				stop()
			}
			if mode == "prefetched" {
				for range config.BatchSize {
					if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
				}
			}
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationFetch {
					stop()
				}
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("callback admitted after stop")
				return broker.Handled{}, nil
			})
			c, err := natsjs.NewConsumer(nc, config)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- c.Run(ctx, h) }()
			if err = receive(t, result); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "prefetched" {
				assertPending(t, js, config, config.BatchSize)
			}
		})
	}
}

func TestIdlePollTimeoutContinuesUntilMessageArrives(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	idle := make(chan struct{}, 16)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == natsjs.OutcomeIdle {
			idle <- struct{}{}
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error { called++; return nil })
	result := make(chan error, 1)
	go func() { result <- c.Run(ctx, h) }()
	receive(t, idle)
	receive(t, idle)
	if _, err = js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err = receive(t, result); !errors.Is(err, context.Canceled) || called != 1 {
		t.Fatalf("result=%v calls=%d", err, called)
	}
}

func TestRunDeadlineRequestsGracefulDrainWithoutProcessingDeadline(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(work context.Context, _ struct{}) error {
		entered <- work
		<-release
		return nil
	})
	result := startConsumer(t, nc, js, config, parent, h, `{}`)
	work := receive(t, entered)
	receive(t, parent.Done())
	if _, exists := work.Deadline(); exists || work.Err() != nil {
		t.Error("run deadline leaked into graceful processing")
	}
	close(release)
	if err := receive(t, result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	assertPending(t, js, config, 0)
}

func TestCoordinatorAppliesEachConsumersShutdownPolicy(t *testing.T) {
	nc, js, graceful := provision(t, time.Second)
	cancelConfig := graceful
	cancelConfig.Consumer = "cancel-worker"
	cancelConfig.Shutdown.Mode = broker.ShutdownCancel
	remote := &nats.ConsumerConfig{Durable: cancelConfig.Consumer, FilterSubject: graceful.Subject, AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer(graceful.Stream, remote); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	graceEntered, cancelEntered, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var gracefulContext context.Context
	coordinator := broker.NewCoordinator()
	graceHandler := broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
		gracefulContext = work
		close(graceEntered)
		<-release
		return broker.Handled{}, nil
	})
	cancelHandler := broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
		close(cancelEntered)
		<-work.Done()
		close(canceled)
		return broker.Handled{}, nil
	})
	graceFactory := func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, graceful) }
	if err := coordinator.Add("graceful", graceFactory, graceHandler); err != nil {
		t.Fatal(err)
	}
	cancelFactory := func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, cancelConfig) }
	if err := coordinator.Add("cancel", cancelFactory, cancelHandler); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(graceful.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	receive(t, graceEntered)
	receive(t, cancelEntered)
	stop()
	receive(t, canceled)
	if gracefulContext.Err() != nil {
		t.Fatal("cancel policy leaked to graceful peer")
	}
	close(release)
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertPending(t, js, graceful, 0)
	assertPending(t, js, cancelConfig, 1)
}

type requestKey struct{}
type correlationKey struct{}

//nolint:gocritic // Test configuration is an independent value for each run.
func startConsumer(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext, config natsjs.ConsumerConfig,
	ctx context.Context, handler broker.Handler, body string,
) <-chan error {
	t.Helper()
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subject, []byte(body)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- c.Run(ctx, handler) }()
	return result
}

//nolint:gocritic // Inspect the same immutable binding used by the test.
func assertPending(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig, expected int) {
	t.Helper()
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != expected {
		t.Fatalf("pending=%d, want %d", info.NumAckPending, expected)
	}
}

func terminalResults() []struct {
	name  string
	value broker.Disposition
} {
	return []struct {
		name  string
		value broker.Disposition
	}{
		{"handled", broker.Handled{}}, {"retry", broker.RetryAfter{Delay: time.Second}}, {"terminate", natsjs.Terminate{}},
	}
}

func isTerminal(operation string) bool {
	return operation == natsjs.OperationAck || operation == natsjs.OperationNak || operation == natsjs.OperationTerm
}

func pauseServer(t *testing.T) func() {
	t.Helper()
	if output, err := docker("pause", containerID); err != nil {
		t.Fatalf("pause NATS: %v: %s", err, output)
	}
	var once sync.Once
	resume := func() {
		once.Do(func() {
			if output, err := docker("unpause", containerID); err != nil {
				t.Errorf("resume NATS: %v: %s", err, output)
			}
		})
	}
	t.Cleanup(resume)
	return resume
}

func awaitReadback(t *testing.T, check func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := check()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Errorf("readback: %w", err))
		case <-ticker.C:
		}
	}
}
