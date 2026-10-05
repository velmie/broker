package otelbroker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
)

// TracePublishing creates a producer send span ending when Publish returns.
// Success has only the underlying publisher guarantee. Injection changes detached
// propagator-owned headers; it never changes caller storage or retains context.
func TracePublishing(system, destination string, values ...Option) broker.PublisherMiddleware {
	o := configure(values)
	return func(next broker.Publisher) broker.Publisher {
		return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
			tracer := o.provider(ctx)
			start := o.startOptions(system, destination, "send", trace.SpanKindProducer)
			ctx, span := tracer.Start(ctx, o.name("send", destination, message), start...)
			complete := false
			defer func() {
				if !complete {
					span.SetStatus(codes.Error, "operation panicked")
				}
				span.End()
			}()
			propagator := o.propagation()
			carrier := newCarrier(nil, propagator.Fields())
			propagator.Inject(ctx, carrier)
			message.Headers = carrier.inject(message.Headers)
			err := next.Publish(ctx, message)
			if err != nil {
				o.failure(ctx, span, err)
			}
			complete = true
			return err
		})
	}
}

// TraceProcessing creates a consumer process span, preserving requirements and
// callback results. A valid ambient span stays the parent; extracted message
// context becomes a link. Without an ambient span, the message context is parent.
// Retry is recorded as intent, never as confirmed settlement. Place RecoverPanics
// inside this middleware to observe a recovered error. Unrecovered panics propagate
// without their values being recorded automatically by the SDK.
func TraceProcessing(system string, values ...Option) broker.HandlerMiddleware {
	o := configure(values)
	return broker.HandlerMiddleware{Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
		return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			message := delivery.Message()
			tracer := o.provider(ctx)
			propagator := o.propagation()
			// Clear only the span for extraction, preserving values, cancellation and deadlines.
			extracted := propagator.Extract(trace.ContextWithSpanContext(ctx, trace.SpanContext{}), newCarrier(message.Headers, propagator.Fields()))
			remote := trace.SpanContextFromContext(extracted)
			start := o.startOptions(system, delivery.Source(), "process", trace.SpanKindConsumer)
			ambient := trace.SpanFromContext(ctx)
			if ambient.SpanContext().IsValid() {
				ctx = trace.ContextWithSpan(extracted, ambient)
				if remote.IsValid() {
					start = append(start, trace.WithLinks(trace.Link{SpanContext: remote}))
				}
			} else {
				ctx = extracted
			}
			ctx, span := tracer.Start(ctx, o.name("process", delivery.Source(), message), start...)
			complete := false
			defer func() {
				if !complete {
					span.SetStatus(codes.Error, "operation panicked")
				}
				span.End()
			}()
			result, err := next(ctx, delivery)
			outcome := "returned"
			if err != nil {
				outcome = "failed"
				o.failure(ctx, span, err)
			} else {
				switch value := result.(type) {
				case broker.Handled:
					outcome = "handled"
				case broker.RetryAfter:
					outcome = "retry_requested"
					if value.Cause != nil {
						o.record(ctx, span, value.Cause)
					}
				}
			}
			span.SetAttributes(attribute.String("broker.processing.outcome", outcome))
			complete = true
			return result, err
		}
	}}
}
