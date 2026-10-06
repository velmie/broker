package examples_test

import (
	"context"
	"errors"
	"testing"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/broker/otelbroker"
)

func TestIdempotencyReplayRetainsProcessingSpansAndRequirements(t *testing.T) {
	provider, recorder := recording(t)
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	effects, replays := 0, 0
	middleware, err := idempotency.Middleware(idempo.NewEngine(store),
		idempotency.WithRequireKey(true),
		idempotency.WithOnReplay(func(ctx context.Context, _ broker.Delivery, _ *idempo.Response) {
			replays++
			if !trace.SpanContextFromContext(ctx).IsValid() {
				t.Error("replay observation lost its processing context")
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	var recordedError error
	handler := broker.NewTypedHandler(broker.DecodeJSON[struct{ Amount int }],
		func(ctx context.Context, _ struct{ Amount int }) error {
			effects++
			if !trace.SpanContextFromContext(ctx).IsValid() {
				t.Error("business callback lost its processing context")
			}
			return nil
		}, broker.WithRequirements(broker.RedeliveryRequirement{})).WithMiddleware(
		otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("idempotency")),
			otelbroker.WithErrorRecorder(func(_ context.Context, _ trace.Span, cause error) { recordedError = cause })),
		broker.RecoverPanics(), middleware)
	if requirements := handler.Requirements(); len(requirements) != 1 {
		t.Fatalf("middleware lost native redelivery requirement: %v", requirements)
	}
	input := delivery{message: broker.Message{ID: "stable-command", Body: []byte(`{"Amount":42}`)}}
	for range 2 {
		result, handleErr := handler.Handle(context.Background(), input)
		if handleErr != nil {
			t.Fatal(handleErr)
		}
		if _, ok := result.(broker.Handled); !ok {
			t.Fatalf("expected handled processing, got %T", result)
		}
	}
	input.message.Body = []byte(`{"Amount":43}`)
	if _, handleErr := handler.Handle(context.Background(), input); !errors.Is(handleErr, idempo.ErrKeyConflict) {
		t.Fatalf("changed command must conflict: %v", handleErr)
	}
	if effects != 1 || replays != 1 || !errors.Is(recordedError, idempo.ErrKeyConflict) {
		t.Fatalf("effects=%d replays=%d observed=%v", effects, replays, recordedError)
	}
	spans := recorder.Ended()
	if len(spans) != 3 || spans[2].Status().Code != codes.Error {
		t.Fatalf("each delivery needs an ended processing span, including replay and conflict: %v", spans)
	}
	for _, span := range spans {
		if !span.SpanContext().IsValid() || span.SpanKind() != trace.SpanKindConsumer {
			t.Fatal("invalid processing span")
		}
	}
}

type delivery struct{ message broker.Message }

func (d delivery) Message() broker.Message { return d.message }
func (delivery) Source() string            { return "orders" }

func recording(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return provider, recorder
}
