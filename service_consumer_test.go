package broker_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestCoordinatorRunServicePreflightAndStop(t *testing.T) {
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.Handled{}, nil
		},
	)
	t.Run(
		"unsupported",
		func(t *testing.T) {
			for _, wrap := range []bool{false, true} {
				coordinator := broker.NewCoordinator()
				started := make(chan struct{}, 1)
				good := func() (broker.Consumer, error) {
					return serviceTestConsumer{
						run: func(context.Context) error {
							started <- struct{}{}
							return nil
						},
					}, nil
				}
				bad := broker.ConsumerFactory(
					func() (broker.Consumer, error) {
						return consumer{
							validate: func() error { return nil },
							run: func(context.Context) error {
								started <- struct{}{}
								return nil
							},
						}, nil
					},
				)
				if wrap {
					bad = broker.WithConsumerRecovery(
						bad,
						broker.RecoveryConfig{
							Source: "legacy",
						},
					)
				}
				if err := coordinator.Add("good", good, handler); err != nil {
					t.Fatal(err)
				}
				if err := coordinator.Add("legacy", bad, handler); err != nil {
					t.Fatal(err)
				}
				err := coordinator.RunService(context.Background())
				var registration *broker.RegistrationError
				if !errors.As(err, &registration) || registration.Name != "legacy" || registration.Operation != "validate" ||
					!strings.Contains(err.Error(), "RunService") {
					t.Fatalf("unsupported wrapped=%v: %v", wrap, err)
				}
				select {
				case <-started:
					t.Fatal("consumer started before preflight completed")
				default:
				}
			}
		},
	)
	t.Run(
		"empty and pre-canceled",
		func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(errors.New("upstream stopped"))
			if err := broker.NewCoordinator().RunService(ctx); err != nil {
				t.Fatal(err)
			}
		},
	)
	t.Run(
		"failed run joins clean peer",
		func(t *testing.T) {
			coordinator := broker.NewCoordinator()
			failure := errors.New("independent failure")
			ready := make(chan struct{})
			joined := make(chan struct{})
			failed := serviceTestConsumer{
				run: func(context.Context) error {
					waitForSignal(t, ready)
					return failure
				},
			}
			peer := serviceTestConsumer{
				run: func(ctx context.Context) error {
					close(ready)
					<-ctx.Done()
					close(joined)
					return nil
				},
			}
			if err := coordinator.Add("failed", func() (broker.Consumer, error) { return failed, nil }, handler); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Add("peer", func() (broker.Consumer, error) { return peer, nil }, handler); err != nil {
				t.Fatal(err)
			}
			err := coordinator.RunService(context.Background())
			var registration *broker.RegistrationError
			if !errors.Is(err, failure) || errors.Is(err, context.Canceled) || !errors.As(err, &registration) || registration.Name != "failed" {
				t.Fatalf("result: %v", err)
			}
			select {
			case <-joined:
			default:
				t.Fatal("returned before peer joined")
			}
		},
	)
}

func TestServiceRecoveryCapabilityAndGenerations(t *testing.T) {
	failure := errors.New("first generation failed")
	handler := recoveryHandler(t)
	t.Run(
		"recovered clean stop",
		func(t *testing.T) {
			generation := 0
			var output bytes.Buffer
			factory := broker.WithConsumerRecovery(
				func() (broker.Consumer, error) {
					generation++
					if generation == 1 {
						return serviceTestConsumer{
							run: func(context.Context) error { return failure },
						}, nil
					}
					return serviceTestConsumer{
						run: func(context.Context) error { return nil },
					}, nil
				},
				broker.RecoveryConfig{
					Source:      "orders",
					MaxRestarts: 1,
					Retryable:   func(error) bool { return true },
					Logger:      slog.New(slog.NewTextHandler(&output, nil)),
				},
			)
			consumer, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			service, ok := consumer.(broker.ServiceConsumer)
			if !ok {
				t.Fatal("capability was lost")
			}
			err = service.RunService(context.Background(), handler)
			if err != nil || generation != 2 || !strings.Contains(output.String(), failure.Error()) {
				t.Fatalf("result=%v generations=%d log=%s", err, generation, output.String())
			}
		},
	)
	t.Run(
		"stop during backoff preserves failure",
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logged := make(chan struct{})
			factory := broker.WithConsumerRecovery(
				func() (broker.Consumer, error) {
					return serviceTestConsumer{
						run: func(context.Context) error { return failure },
					}, nil
				},
				broker.RecoveryConfig{
					Source:      "orders",
					MaxRestarts: 1,
					Backoff:     time.Hour,
					Retryable:   func(error) bool { return true },
					Logger: slog.New(
						recoverySignalLogger{
							Handler: slog.NewTextHandler(&bytes.Buffer{}, nil),
							logged:  logged,
						},
					),
				},
			)
			consumer, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- consumer.(broker.ServiceConsumer).RunService(ctx, handler) }()
			waitRecoverySignal(t, logged)
			cancel()
			err = waitRecoveryResult(t, result)
			if !errors.Is(err, failure) || errors.Is(err, context.Canceled) {
				t.Fatalf("result=%v", err)
			}
		},
	)
	t.Run(
		"replacement must support service contract",
		func(t *testing.T) {
			generation := 0
			factory := broker.WithConsumerRecovery(
				func() (broker.Consumer, error) {
					generation++
					if generation == 1 {
						return serviceTestConsumer{
							run: func(context.Context) error { return failure },
						}, nil
					}
					return recoveryConsumer{
						run: func(context.Context, broker.Handler) error {
							t.Error("unsupported replacement started")
							return nil
						},
					}, nil
				},
				broker.RecoveryConfig{
					Source:      "orders",
					MaxRestarts: 1,
					Retryable:   func(error) bool { return true },
					Logger:      slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
				},
			)
			consumer, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			err = consumer.(broker.ServiceConsumer).RunService(context.Background(), handler)
			if !errors.Is(err, failure) || !strings.Contains(err.Error(), "RunService") {
				t.Fatalf("result=%v", err)
			}
		},
	)
}

func TestRecoveredPanicCauseInSlog(t *testing.T) {
	cause := errors.New("actual handler panic")
	for _, value := range []any{cause, "actual string panic"} {
		var output bytes.Buffer
		handler := broker.NewHandler(
			func(context.Context, broker.Delivery) (broker.Disposition, error) {
				panic(value)
			},
		).WithMiddleware(broker.RecoverPanics())
		_, err := handler.Handle(context.Background(), replyDelivery{})
		slog.New(slog.NewTextHandler(&output, nil)).Error("processing failed", "error", err)
		if !strings.Contains(output.String(), "actual") {
			t.Fatalf("panic value missing in log %s", output.String())
		}
		if value == cause && !errors.Is(err, cause) {
			t.Fatalf("panic error identity lost: %v", err)
		}
	}
}

type serviceTestConsumer struct {
	run func(context.Context) error
}

func (c serviceTestConsumer) Validate(...broker.Requirement) error            { return nil }
func (c serviceTestConsumer) Run(ctx context.Context, _ broker.Handler) error { return c.run(ctx) }
func (c serviceTestConsumer) RunService(ctx context.Context, _ broker.Handler) error {
	return c.run(ctx)
}

func TestCoordinatorRunServiceFiniteSuccessDoesNotStopPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peerReady, finiteDone, peerChecked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finite := serviceTestConsumer{
		run: func(context.Context) error {
			waitForSignal(t, peerReady)
			close(finiteDone)
			return nil
		},
	}
	peer := serviceTestConsumer{
		run: func(work context.Context) error {
			close(peerReady)
			waitForSignal(t, finiteDone)
			if work.Err() != nil {
				t.Error("finite success stopped peer")
			}
			close(peerChecked)
			<-work.Done()
			return nil
		},
	}
	handler := broker.NewHandler(
		func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.Handled{}, nil
		},
	)
	coordinator := broker.NewCoordinator()
	if err := coordinator.Add("finite", func() (broker.Consumer, error) { return finite, nil }, handler); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("peer", func() (broker.Consumer, error) { return peer, nil }, handler); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- coordinator.RunService(ctx) }()
	waitForSignal(t, peerChecked)
	cancel()
	if err := waitRecoveryResult(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestServiceRecoveryRetainsTerminalGenerationFailures(t *testing.T) {
	first := errors.New("first generation failed")
	second := errors.New("second generation failed")
	generation := 0
	factory := broker.WithConsumerRecovery(
		func() (broker.Consumer, error) {
			generation++
			cause := first
			if generation == 2 {
				cause = second
			}
			return serviceTestConsumer{
				run: func(context.Context) error { return cause },
			}, nil
		},
		broker.RecoveryConfig{
			Source:      "orders",
			MaxRestarts: 1,
			Retryable:   func(error) bool { return true },
			Logger:      slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		},
	)
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.(broker.ServiceConsumer).RunService(context.Background(), recoveryHandler(t))
	if !errors.Is(err, first) || !errors.Is(err, second) || generation != 2 {
		t.Fatalf("result=%v generations=%d", err, generation)
	}
}
