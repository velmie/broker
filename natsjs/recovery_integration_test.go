package natsjs_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestRecoveryStartupUsesFreshConsumer(t *testing.T) {
	nc, js, config := provision(t, testTimeout)
	config.OperationTimeout = 200 * time.Millisecond
	if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	resume := pauseServer(t)
	calls, factories, records := 0, 0, 0
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == "error" {
			records++
			cause, ok := attr.Value.Any().(error)
			var native *natsjs.OperationError
			if !ok || !errors.As(cause, &native) || native.Operation != natsjs.OperationStartup || native.Source == "" || !retryableStartup(cause) {
				t.Error("original startup cause missing")
			}
			resume()
		}
		return attr
	}}))
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return natsjs.NewConsumer(nc, config)
	}, broker.RecoveryConfig{
		Source: config.Stream, MaxRestarts: 2, Backoff: 10 * time.Millisecond, Retryable: retryableStartup, Logger: logger,
	})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		return broker.Handled{}, nil
	})
	err = consumer.Run(ctx, h)
	if !errors.Is(err, context.Canceled) || calls != 1 || factories != 2 || records != 1 {
		t.Fatalf("result=%v calls=%d factories=%d records=%d", err, calls, factories, records)
	}
	assertPending(t, js, config, 0)
}

func retryableStartup(err error) bool {
	if multiple, ok := err.(interface{ Unwrap() []error }); ok {
		children := multiple.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryableStartup(child) {
				return false
			}
		}
		return true
	}
	if native, ok := err.(*natsjs.OperationError); ok {
		return native.Operation == natsjs.OperationStartup && startupTimeoutCause(native.Cause)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return retryableStartup(cause)
	}
	return false
}

func TestRecoveryUnknownAcknowledgmentDoesNotRestart(t *testing.T) {
	nc, js, config := provision(t, testTimeout)
	config.OperationTimeout = 200 * time.Millisecond
	if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	admitted, send := make(chan struct{}), make(chan struct{})
	calls, factories, unknown := 0, 0, 0
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeStarted {
			close(admitted)
			<-send
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeUnknown {
			unknown++
		}
	}
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return natsjs.NewConsumer(nc, config)
	}, broker.RecoveryConfig{
		Source: config.Stream, MaxRestarts: 2, Retryable: retryableStartup, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		return broker.Handled{}, nil
	})
	result := make(chan error, 1)
	go func() { result <- consumer.Run(context.Background(), h) }()
	receive(t, admitted)
	resume := pauseServer(t)
	close(send)
	err = receive(t, result)
	resume()
	var native *natsjs.OperationError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &native) || native.Operation != natsjs.OperationAck ||
		calls != 1 || factories != 1 || unknown != 1 {
		t.Fatalf("result=%v calls=%d factories=%d unknown=%d", err, calls, factories, unknown)
	}
}

func TestRecoveryStopDuringBackoffCreatesNoReplacement(t *testing.T) {
	nc, _, config := provision(t, testTimeout)
	config.OperationTimeout = 200 * time.Millisecond
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	resume := pauseServer(t)
	factories, calls := 0, 0
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == "error" {
			stop()
		}
		return attr
	}}))
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return natsjs.NewConsumer(nc, config)
	}, broker.RecoveryConfig{
		Source: config.Stream, MaxRestarts: 2, Backoff: time.Hour, Retryable: retryableStartup, Logger: logger,
	})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		return broker.Handled{}, nil
	})
	result := make(chan error, 1)
	go func() { result <- consumer.Run(ctx, h) }()
	err = receive(t, result)
	resume()
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || factories != 1 || calls != 0 {
		t.Fatalf("result=%v factories=%d callbacks=%d", err, factories, calls)
	}
}

func TestRecoveryRetainsCallbackAndCleanupCausesWithoutRestart(t *testing.T) {
	_, js, config := provision(t, testTimeout)
	nc, err := nats.Connect(serverURL, nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if _, err = js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("application failure")
	factories, calls := 0, 0
	factory := broker.WithConsumerRecovery(func() (broker.Consumer, error) {
		factories++
		return natsjs.NewConsumer(nc, config)
	}, broker.RecoveryConfig{
		Source: config.Stream, MaxRestarts: 2, Retryable: retryableStartup, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	consumer, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		nc.Close()
		return nil, cause
	})
	result := make(chan error, 1)
	go func() { result <- consumer.Run(context.Background(), h) }()
	err = receive(t, result)
	if !errors.Is(err, cause) || !errors.Is(err, nats.ErrConnectionClosed) || calls != 1 || factories != 1 {
		t.Fatalf("result=%v factories=%d callbacks=%d", err, factories, calls)
	}
	assertPending(t, js, config, 1)
}

func startupTimeoutCause(err error) bool {
	if branches, ok := err.(interface{ Unwrap() []error }); ok {
		children := branches.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !startupTimeoutCause(child) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return startupTimeoutCause(cause)
	}
	return err == context.DeadlineExceeded || err == nats.ErrTimeout
}
