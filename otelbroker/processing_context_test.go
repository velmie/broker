package otelbroker_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
)

func TestProcessingContextIsDeliveryLocal(t *testing.T) {
	provider, recorder := recording(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const count = 24
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			local := otelbroker.CaptureProcessing(ctx)
			if otelbroker.ProcessingContext(local) != local {
				t.Error("unexpected context before processing")
			}
			var spanID trace.SpanID
			h := broker.NewHandler(func(call context.Context, _ broker.Delivery) (broker.Disposition, error) {
				spanID = trace.SpanContextFromContext(call).SpanID()
				if call.Done() != ctx.Done() {
					t.Error("lost cancellation ancestry")
				}
				return broker.Handled{}, nil
			}).WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("test")),
				otelbroker.WithProcessingAttributes(func(context.Context, broker.Delivery) []attribute.KeyValue {
					return []attribute.KeyValue{attribute.String("traffic.class", "internal")}
				})))
			if _, err := h.Handle(local, delivery{}); err != nil {
				t.Error(err)
			}
			if captured := trace.SpanContextFromContext(otelbroker.ProcessingContext(local)).SpanID(); captured != spanID || !captured.IsValid() {
				t.Errorf("captured span %s, want %s", captured, spanID)
			}
		})
	}
	wg.Wait()
	if len(recorder.Ended()) != count {
		t.Fatalf("spans=%d", len(recorder.Ended()))
	}
	for _, span := range recorder.Ended() {
		if attr(span, "traffic.class") != "internal" {
			t.Errorf("missing selected attributes: %v", span.Attributes())
		}
	}
}

type spanLogHandler struct {
	span trace.SpanContext
}

func (h *spanLogHandler) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // slog.Handler requires a value record.
func (h *spanLogHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.span = trace.SpanContextFromContext(ctx)
	return nil
}
func (h *spanLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *spanLogHandler) WithGroup(string) slog.Handler      { return h }

func TestCaptureProcessingCorrelatesLogRecord(t *testing.T) {
	provider, recorder := recording(t)
	local := otelbroker.CaptureProcessing(context.Background())
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, nil
	}).WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("test"))))
	if _, err := handler.Handle(local, delivery{}); err != nil {
		t.Fatal(err)
	}
	logs := &spanLogHandler{}
	slog.New(logs).InfoContext(otelbroker.ProcessingContext(local), "settled")
	span := recorder.Ended()[0].SpanContext()
	if !logs.span.IsValid() || logs.span.TraceID() != span.TraceID() || logs.span.SpanID() != span.SpanID() {
		t.Fatalf("log span=%v, processing span=%v", logs.span, span)
	}
}
