package broker_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestConsumerRecoveryFiniteSuccessNeverRestarts(t *testing.T) {
	factories, runs := 0, 0
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return recoveryConsumer{run: func(context.Context, broker.Handler) error { runs++; return nil }}, nil
	}, broker.RecoveryConfig{Source: "orders", MaxRestarts: 0})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Fatal("wrapper invoked application callback")
		return broker.Handled{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if factories != 1 || runs != 1 {
		t.Fatalf("factories=%d runs=%d", factories, runs)
	}
}

func TestConsumerRecoveryFiniteReturnRetainsConcurrentCancellation(t *testing.T) {
	canceled := errors.New("application shutdown")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(canceled)
	factories := 0
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return recoveryConsumer{run: func(context.Context, broker.Handler) error {
			cancel(canceled)
			return nil
		}}, nil
	}, broker.RecoveryConfig{Source: "orders", MaxRestarts: 1,
		Retryable: func(error) bool { t.Fatal("successful run was classified"); return true },
	})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(ctx, recoveryHandler(t)); !errors.Is(err, canceled) || factories != 1 {
		t.Fatalf("shutdown cause lost or successful run restarted: err=%v factories=%d", err, factories)
	}
}

func TestConsumerRecoveryRejectsConfigurationBeforeBinding(t *testing.T) {
	for _, config := range []broker.RecoveryConfig{
		{Source: ""}, {Source: "orders", MaxRestarts: -1}, {Source: "orders", Backoff: -time.Second},
	} {
		calls := 0
		factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) { calls++; return recoveryConsumer{}, nil }, config)
		if _, err := factory(); err == nil || calls != 0 {
			t.Fatalf("invalid config err=%v calls=%d", err, calls)
		}
	}
}

func TestConsumerRecoveryCoordinatorPreflightBarrier(t *testing.T) {
	invalid := errors.New("missing renewal capability")
	var output bytes.Buffer
	config := broker.RecoveryConfig{Source: "orders", MaxRestarts: 2, Logger: slog.New(newProcessingCapture(&output)),
		Retryable: func(error) bool { t.Fatal("preflight failure classified"); return true }}
	factories, validations, runs := 0, 0, 0
	first := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return recoveryConsumer{validate: func(requirements ...broker.Requirement) error {
			validations++
			if len(requirements) != 1 {
				t.Fatal("requirements lost")
			}
			return nil
		}, run: func(context.Context, broker.Handler) error { runs++; return nil }}, nil
	}, config)
	coordinator := broker.NewCoordinator()
	handler := recoveryHandler(t)
	if err := coordinator.Add("orders", first, handler); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Add("unsupported", func() (broker.Consumer, error) {
		return recoveryConsumer{validate: func(...broker.Requirement) error { return invalid }}, nil
	}, handler); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Run(context.Background()); !errors.Is(err, invalid) {
		t.Fatalf("validation cause lost: %v", err)
	}
	if factories != 1 || validations != 1 || runs != 0 || output.Len() != 0 {
		t.Fatalf("factories=%d validations=%d runs=%d log=%s", factories, validations, runs, output.String())
	}
}

func TestConsumerRecoveryWaitsForJoinedGenerationAndRevalidates(t *testing.T) {
	failed := errors.New("native startup failed")
	entered, release, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var output bytes.Buffer
	factories, classifications := 0, 0
	var validations []int
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := recoveryHandler(t)
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		generation := factories
		if generation == 2 {
			select {
			case <-joined:
			default:
				t.Error("replacement created before old Run joined")
			}
		}
		return recoveryConsumer{validate: func(requirements ...broker.Requirement) error {
			validations = append(validations, generation)
			if !reflect.DeepEqual(requirements, handler.Requirements()) {
				t.Error("generation lost capabilities")
			}
			return nil
		}, run: func(received context.Context, got broker.Handler) error {
			if received != ctx || !reflect.DeepEqual(got.Requirements(), handler.Requirements()) {
				t.Error("Run context or handler changed")
			}
			if generation == 1 {
				close(entered)
				<-release
				close(joined)
				return failed
			}
			return nil
		}}, nil
	}, broker.RecoveryConfig{Source: "orders", MaxRestarts: 1, Logger: slog.New(newProcessingCapture(&output)),
		Retryable: func(err error) bool {
			classifications++
			if err != failed {
				t.Error("classifier lost original error")
			}
			return true
		}})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Validate(handler.Requirements()...); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx, handler) }()
	waitRecoverySignal(t, entered)
	close(release)
	if err = waitRecoveryResult(t, done); err != nil {
		t.Fatalf("successful replacement retained old failure: %v", err)
	}
	if factories != 2 || classifications != 1 || !reflect.DeepEqual(validations, []int{1, 1, 2}) {
		t.Fatalf("factories=%d classifications=%d validations=%v", factories, classifications, validations)
	}
}

func TestConsumerRecoveryFailureBoundariesRetainOriginalCauses(t *testing.T) {
	cleanup := errors.New("native cleanup failure")
	first := errors.Join(errors.New("first native failure"), cleanup)
	second := errors.New("second failure")
	for _, test := range []struct {
		name                                   string
		stage                                  string
		max                                    int
		retry                                  bool
		factories, runs, classifications, logs int
	}{
		{name: "initial factory", stage: "initial factory", max: 2, retry: true, factories: 1},
		{name: "initial validation", stage: "initial validation", max: 2, retry: true, factories: 1},
		{name: "replacement factory", stage: "replacement factory", max: 2, retry: true,
			factories: 2, runs: 1, classifications: 1, logs: 1},
		{name: "replacement validation", stage: "replacement validation", max: 2, retry: true,
			factories: 2, runs: 1, classifications: 1, logs: 1},
		{name: "unclassified run", stage: "run", max: 2, factories: 1, runs: 1, classifications: 1},
		{name: "exhausted runs", stage: "run", max: 1, retry: true, factories: 2, runs: 2, classifications: 1, logs: 1},
		{name: "no restarts", stage: "run", max: 0, retry: true, factories: 1, runs: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			capture := newProcessingCapture(&output)
			factories, runs, classifications := 0, 0, 0
			factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
				factories++
				generation := factories
				if test.stage == "initial factory" {
					return nil, first
				}
				if test.stage == "replacement factory" && generation == 2 {
					return nil, second
				}
				return recoveryConsumer{validate: func(...broker.Requirement) error {
					if test.stage == "initial validation" {
						return first
					}
					if test.stage == "replacement validation" && generation == 2 {
						return second
					}
					return nil
				}, run: func(context.Context, broker.Handler) error {
					runs++
					if generation == 1 {
						return first
					}
					return second
				}}, nil
			}, broker.RecoveryConfig{Source: "orders", MaxRestarts: test.max, Logger: slog.New(capture),
				Retryable: func(error) bool { classifications++; return test.retry }})
			consumer, err := factory()
			if err == nil {
				err = consumer.Run(context.Background(), recoveryHandler(t))
			}
			if !errors.Is(err, first) || !errors.Is(err, cleanup) {
				t.Fatalf("original failure or cleanup cause lost: %v", err)
			}
			if test.factories == 2 && !errors.Is(err, second) {
				t.Fatalf("replacement failure lost: %v", err)
			}
			if factories != test.factories || runs != test.runs || classifications != test.classifications || len(capture.records) != test.logs {
				t.Fatalf("factories=%d runs=%d classifications=%d records=%d", factories, runs, classifications, len(capture.records))
			}
			if !strings.Contains(err.Error(), "orders") {
				t.Fatalf("source missing from failure: %v", err)
			}
		})
	}
}

func TestConsumerRecoveryCancellationDuringBackoffCreatesNoReplacement(t *testing.T) {
	original := errors.New("run failure")
	canceled := errors.New("application shutdown")
	logged := make(chan struct{})
	var output bytes.Buffer
	capture := newProcessingCapture(&output)
	logger := slog.New(recoverySignalLogger{Handler: capture, logged: logged})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(canceled)
	factories := 0
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return recoveryConsumer{run: func(context.Context, broker.Handler) error { return original }}, nil
	}, broker.RecoveryConfig{Source: "orders", MaxRestarts: 1, Backoff: time.Hour,
		Retryable: func(error) bool { return true }, Logger: logger})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx, recoveryHandler(t)) }()
	waitRecoverySignal(t, logged)
	cancel(canceled)
	err = waitRecoveryResult(t, done)
	if !errors.Is(err, original) || !errors.Is(err, canceled) || factories != 1 || len(capture.records) != 1 {
		t.Fatalf("err=%v factories=%d records=%d", err, factories, len(capture.records))
	}
}

func TestConsumerRecoveryLogsOriginalFailureWithApplicationRedaction(t *testing.T) {
	cause := &processingFailure{field: "account_id", entity: "order-7", secret: "private-cause"}
	var output bytes.Buffer
	capture := newProcessingCapture(&output)
	factories := 0
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		generation := factories
		return recoveryConsumer{run: func(context.Context, broker.Handler) error {
			if generation == 1 {
				return cause
			}
			return nil
		}}, nil
	}, broker.RecoveryConfig{Source: "nats://example.test/orders?token=private-query#private-fragment", MaxRestarts: 1,
		Retryable: func(error) bool { return true }, Logger: slog.New(capture)})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(context.Background(), recoveryHandler(t)); err != nil {
		t.Fatal(err)
	}
	if len(capture.records) != 1 {
		t.Fatalf("records=%d", len(capture.records))
	}
	attrs := processingAttrs(&capture.records[0])
	if attrs["error"].Any() != cause || attrs["operation"].String() != "recover" || attrs["stage"].String() != "run" ||
		attrs["outcome"].String() != "restart_scheduled" || attrs["next_generation"].Int64() != attrs["generation"].Int64()+1 ||
		attrs["backoff"].Kind() != slog.KindDuration {
		t.Fatalf("restart context lost: %+v", attrs)
	}
	for _, value := range []string{"private-cause", "private-query", "private-fragment"} {
		if strings.Contains(output.String(), value) {
			t.Fatalf("unsafe diagnostic: %s", output.String())
		}
	}
	for _, value := range []string{"account_id", "order-7", "example.test", "dependency rejected field"} {
		if !strings.Contains(output.String(), value) {
			t.Fatalf("diagnostic detail missing %q: %s", value, output.String())
		}
	}
}

func recoveryHandler(t *testing.T) broker.Handler {
	t.Helper()
	return broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("recovery wrapper invoked callback directly")
		return broker.Handled{}, nil
	}, broker.WithKeepAlive(time.Second))
}
func waitRecoverySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery boundary")
	}
}
func waitRecoveryResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery result")
	}
	return nil
}

type recoverySignalLogger struct {
	slog.Handler
	logged chan struct{}
}

//nolint:gocritic // slog.Handler requires a value record.
func (h recoverySignalLogger) Handle(ctx context.Context, record slog.Record) error {
	err := h.Handler.Handle(ctx, record)
	close(h.logged)
	return err
}

type recoveryConsumer struct {
	validate func(...broker.Requirement) error
	run      func(context.Context, broker.Handler) error
}

func (c recoveryConsumer) Validate(requirements ...broker.Requirement) error {
	if c.validate != nil {
		return c.validate(requirements...)
	}
	return nil
}
func (c recoveryConsumer) Run(ctx context.Context, handler broker.Handler) error {
	return c.run(ctx, handler)
}
