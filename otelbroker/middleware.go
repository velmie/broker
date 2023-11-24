package otelbroker

import (
	"github.com/velmie/broker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// PublisherMiddleware creates a middleware for broker.Publisher that integrates tracing.
func PublisherMiddleware(option ...Option) broker.PublisherMiddleware {
	opts := defaultOptions()
	opts.spanNameFormatter = spanNameFormatter("publish")
	opts.apply(option)
	return func(next broker.Publisher) broker.Publisher {
		return broker.PublisherFunc(func(topic string, msg *broker.Message) error {
			tracer := opts.tracer
			if span := trace.SpanFromContext(msg.Context()); span.SpanContext().IsValid() {
				tracer = newTracer(span.TracerProvider())
			} else {
				tracer = newTracer(otel.GetTracerProvider())
			}

			kind := trace.WithSpanKind(trace.SpanKindProducer)
			attrs := append(commonAttributes(topic, msg), semconv.MessagingOperationPublish)
			sopts := append(
				[]trace.SpanStartOption{kind, trace.WithAttributes(attrs...)},
				opts.spanStartOptions...,
			)

			ctx, span := tracer.Start(msg.Context(), opts.spanNameFormatter(topic, msg), sopts...)
			defer span.End()

			if msg.Header == nil {
				msg.Header = make(broker.Header)
			}
			opts.propagator.Inject(ctx, propagation.MapCarrier(msg.Header))
			err := next.Publish(topic, msg)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			return err
		})
	}
}

// ConsumerMiddleware creates a middleware for broker.Handler that integrates tracing.
func ConsumerMiddleware(option ...Option) broker.Middleware {
	opts := defaultOptions()
	opts.spanNameFormatter = spanNameFormatter("receive")
	opts.apply(option)
	return func(next broker.Handler) broker.Handler {
		return func(event broker.Event) error {
			msg := event.Message()
			topic := event.Topic()

			tracer := opts.tracer
			if span := trace.SpanFromContext(msg.Context()); span.SpanContext().IsValid() {
				tracer = newTracer(span.TracerProvider())
			} else {
				tracer = newTracer(otel.GetTracerProvider())
			}
			if msg.Header == nil {
				msg.Header = make(broker.Header)
			}
			ctx := opts.propagator.Extract(msg.Context(), propagation.MapCarrier(msg.Header))

			kind := trace.WithSpanKind(trace.SpanKindConsumer)
			attrs := append(commonAttributes(topic, msg), semconv.MessagingOperationReceive)
			sopts := append(
				[]trace.SpanStartOption{kind, trace.WithAttributes(attrs...)},
				opts.spanStartOptions...,
			)

			ctx, span := tracer.Start(ctx, opts.spanNameFormatter(topic, msg), sopts...)
			defer span.End()

			msg.SetContext(ctx)

			err := next(event)

			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			return err
		}
	}
}

// Option is a functional option type for configuring propagation
type Option func(opts *options)

// WithPropagator returns an Option that sets a custom propagator
// for text map propagation of trace context.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(opts *options) {
		opts.propagator = p
	}
}

// WithTracer returns an Option that sets a custom tracer
// for the trace context.
func WithTracer(t trace.Tracer) Option {
	return func(opts *options) {
		opts.tracer = t
	}
}

// WithSpanStartOptions returns an Option that sets custom
// SpanStartOptions for starting new spans.
func WithSpanStartOptions(startOpts ...trace.SpanStartOption) Option {
	return func(opts *options) {
		opts.spanStartOptions = startOpts
	}
}

// WithSpanNameFormatter returns an Option that sets a custom
// function for formatting span names. The formatter function
// can be used to dynamically generate span names based on the
// context, such as the message topic or other properties of the
// broker message.
func WithSpanNameFormatter(formatter func(topic string, msg *broker.Message) string) Option {
	return func(opts *options) {
		opts.spanNameFormatter = formatter
	}
}

// options holds configuration for trace context propagation.
// It includes a propagator which is responsible for the actual
// mechanism of injecting and extracting trace information.
type options struct {
	propagator        propagation.TextMapPropagator
	tracer            trace.Tracer
	spanStartOptions  []trace.SpanStartOption
	spanNameFormatter func(topic string, msg *broker.Message) string
}

// apply applies the given functional options to the options instance.
// This method modifies the options struct based on the provided Option
// functions and then returns the modified options.
func (o *options) apply(opts []Option) *options {
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// defaultOptions creates and returns an options instance with default settings.
// It sets the OpenTelemetry TextMapPropagator as the default propagator.
func defaultOptions() *options {
	return &options{
		propagator: otel.GetTextMapPropagator(),
	}
}

func spanNameFormatter(operation string) func(topic string, msg *broker.Message) string {
	return func(topic string, msg *broker.Message) string {
		return topic + " " + operation
	}
}
