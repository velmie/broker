package natsjs_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestCallbackCleanupAndObserverFailuresRetainIndependentCauses(t *testing.T) {
	_, js, config := provision(t, time.Second)
	nc, err := nats.Connect(serverURL, nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	primary, instrumentation := errors.New("application failed"), errors.New("recording failed")
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationProcess && event.Outcome == natsjs.OutcomeFailed {
			panic(instrumentation)
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error {
		nc.Close() // Simulate connection loss. The adapter must retain cleanup failure.
		return primary
	})
	err = receive(t, startConsumer(t, nc, js, config, context.Background(), h, `{}`))
	for _, cause := range []error{primary, instrumentation, nats.ErrConnectionClosed} {
		if !errors.Is(err, cause) {
			t.Errorf("lost %v in %v", cause, err)
		}
	}
	var observerPanic *natsjs.ObserverPanicError
	if !errors.As(err, &observerPanic) || observerPanic.Operation != natsjs.OperationProcess {
		t.Fatalf("observer evidence: %v", err)
	}
	assertPending(t, js, config, 1)
}

func TestProcessingLoggerRetainsOriginalCauseBeforeStructuredRedaction(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	cause := &fieldFailure{field: "sku", entity: "order-7", privateValue: "secret-value\nforged-record"}
	var log bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == "error" {
			original, ok := attr.Value.Any().(error)
			var failure *fieldFailure
			if !ok || !errors.As(original, &failure) {
				t.Error("logger did not receive original error object")
				return slog.String("error", "invalid diagnostic")
			}
			return slog.Group("error", slog.String("reason", "invalid inventory reference"),
				slog.String("field", failure.field), slog.String("entity", failure.entity))
		}
		return attr
	}}))
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationProcess && event.Outcome == natsjs.OutcomeFailed {
			if !errors.Is(event.Err, cause) {
				t.Error("original processing cause lost")
			}
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error { return cause }).WithMiddleware(
		broker.LogProcessing(logger, broker.ProcessingLogConfig{}),
	)
	err := receive(t, startConsumer(t, nc, js, config, context.Background(), h, `{}`))
	if !errors.Is(err, cause) {
		t.Fatal(err)
	}
	serialized := log.String()
	for _, expected := range []string{"sku", "order-7", "invalid inventory reference", config.Stream, "process"} {
		if !strings.Contains(serialized, expected) {
			t.Errorf("missing %q in diagnostic %s", expected, serialized)
		}
	}
	if strings.Contains(serialized, "secret-value") || strings.Contains(serialized, "forged-record") {
		t.Fatalf("sensitive input serialized: %s", serialized)
	}
}

func TestObserverPanicDoesNotChangeSuccessfulDisposition(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	cause := errors.New("observer unavailable")
	var records int
	config.Observer.Begin = func(context.Context, natsjs.DeliveryInfo) context.Context { panic(cause) }
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		records++
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error { stop(); return nil })
	err := receive(t, startConsumer(t, nc, js, config, ctx, h, `{}`))
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertPending(t, js, config, 0)
	// Startup and fetch records precede Begin. Later calls are disabled so a
	// repeatedly panicking observer cannot grow an unbounded error collection.
	if records != 2 {
		t.Fatalf("records after observer failure: %d, want 2", records)
	}
}

func TestRecoveredProcessingPanicRetainsCauseWithoutSettlement(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	cause := &fieldFailure{field: "sku", entity: "order-7", privateValue: "secret-value"}
	var callbacks, terminals int
	var observed error
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationProcess && event.Outcome == natsjs.OutcomeFailed {
			observed = event.Err
		}
		if event.Operation == natsjs.OperationAck || event.Operation == natsjs.OperationNak || event.Operation == natsjs.OperationTerm {
			terminals++
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error {
		callbacks++
		panic(cause)
	}, broker.WithRedelivery(time.Second, func(error) bool {
		t.Error("panic entered automatic application retry classification")
		return true
	})).WithMiddleware(broker.RecoverPanics())
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := receive(t, startConsumer(t, nc, js, config, ctx, h, `{}`))
	for _, failure := range []error{err, observed} {
		var recovered *broker.PanicError
		if !errors.Is(failure, cause) || !errors.As(failure, &recovered) || len(recovered.Stack) == 0 {
			t.Fatalf("lost panic evidence: %v", failure)
		}
	}
	if callbacks != 1 || terminals != 0 {
		t.Fatalf("callbacks=%d terminal operations=%d", callbacks, terminals)
	}
	assertPending(t, js, config, 1)
}

type fieldFailure struct{ field, entity, privateValue string }

func (e *fieldFailure) Error() string {
	return e.field + " for " + e.entity + ": invalid reference " + e.privateValue
}
