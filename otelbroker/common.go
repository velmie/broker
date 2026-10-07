package otelbroker

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
)

const (
	// ScopeName identifies this instrumentation.
	ScopeName = "github.com/velmie/broker/otelbroker"
	// Version is the instrumentation version.
	Version = "1.0.0"
)

// Option configures tracing. Supplied callbacks and interfaces must be non-nil,
// concurrency-safe and nonpanicking. Custom names, attributes and errors are
// application-owned redaction boundaries.
type Option func(*options)

// WithPropagator selects propagation instead of the global propagator.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagator = p }
}

// WithTracer takes precedence over the local span and global providers.
func WithTracer(t trace.Tracer) Option { return func(o *options) { o.tracer = t } }

// WithSpanStartOptions snapshots options applied after the instrumentation defaults.
func WithSpanStartOptions(values ...trace.SpanStartOption) Option {
	snapshot := append([]trace.SpanStartOption(nil), values...)
	return func(o *options) { o.start = snapshot }
}

// WithSpanNameFormatter selects a name from operation, destination and read-only message data.
func WithSpanNameFormatter(f func(string, string, broker.Message) string) Option {
	return func(o *options) { o.name = f }
}

// WithErrorRecorder receives the original error, including requested retry causes.
// It must select safe diagnostic fields before exporting application error data.
// The default records only the error type. Failure status uses fixed text.
func WithErrorRecorder(f func(context.Context, trace.Span, error)) Option {
	return func(o *options) { o.record = f }
}

type options struct {
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer
	start      []trace.SpanStartOption
	name       func(string, string, broker.Message) string
	record     func(context.Context, trace.Span, error)
}

func configure(values []Option) options {
	o := options{
		name: func(operation, destination string, _ broker.Message) string { return destination + " " + operation },
		record: func(_ context.Context, span trace.Span, err error) {
			span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", fmt.Sprintf("%T", err))))
		},
	}
	for _, value := range values {
		value(&o)
	}
	return o
}

func (o *options) provider(ctx context.Context) trace.Tracer {
	if o.tracer != nil {
		return o.tracer
	}
	span := trace.SpanFromContext(ctx)
	provider := otel.GetTracerProvider()
	if sc := span.SpanContext(); sc.IsValid() && !sc.IsRemote() {
		provider = span.TracerProvider()
	}
	return provider.Tracer(ScopeName, trace.WithInstrumentationVersion(Version), trace.WithSchemaURL(semconv.SchemaURL))
}

func (o *options) propagation() propagation.TextMapPropagator {
	if o.propagator != nil {
		return o.propagator
	}
	return otel.GetTextMapPropagator()
}

func (o *options) startOptions(system, destination, operation string, kind trace.SpanKind) []trace.SpanStartOption {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(system), semconv.MessagingDestinationName(destination),
		semconv.MessagingOperationName(operation), semconv.MessagingOperationTypeKey.String(operation),
	}
	result := []trace.SpanStartOption{trace.WithSpanKind(kind), trace.WithAttributes(attrs...)}
	return append(result, o.start...)
}

func (o *options) failure(ctx context.Context, span trace.Span, err error) {
	span.SetStatus(codes.Error, "operation failed")
	span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	o.record(ctx, span, err)
}
