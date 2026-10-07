package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker/natsjs/v3"
)

// The example controls source names and records no raw application error text.
// Each delivery owns its span through context. No shared span registry is needed.
func newObserver(tracer trace.Tracer) natsjs.Observer {
	return natsjs.Observer{
		Begin: func(ctx context.Context, info natsjs.DeliveryInfo) context.Context {
			ctx, _ = tracer.Start(ctx, "delivery", trace.WithAttributes(
				semconv.MessagingSystemKey.String("nats"), semconv.MessagingDestinationName(info.Source),
			))
			return ctx
		},
		Record: func(ctx context.Context, event natsjs.Observation) {
			if event.Outcome == natsjs.OutcomeStarted {
				return
			}
			switch event.Operation {
			case natsjs.OperationRenew, natsjs.OperationAck, natsjs.OperationNak, natsjs.OperationTerm, natsjs.OperationDisposition:
			default:
				return // TraceProcessing owns the processing span.
			}
			now := time.Now()
			requested := event.Outcome != natsjs.OutcomeSuppressed
			attrs := []attribute.KeyValue{
				semconv.MessagingSystemKey.String("nats"), semconv.MessagingDestinationName(event.Source),
				semconv.MessagingOperationName(event.Operation),
				attribute.String("broker.transport.outcome", event.Outcome), attribute.Bool("broker.transport.requested", requested),
			}
			kind := trace.SpanKindInternal
			if requested {
				kind = trace.SpanKindClient
				operation := semconv.MessagingOperationTypeSettle
				if event.Operation == natsjs.OperationRenew {
					operation = semconv.MessagingOperationTypeKey.String("renew")
				}
				attrs = append(attrs, operation)
			}
			_, span := tracer.Start(ctx, event.Operation, trace.WithSpanKind(kind), trace.WithAttributes(attrs...),
				trace.WithTimestamp(now.Add(-event.Duration)))
			if event.Err != nil {
				span.SetStatus(codes.Error, "transport operation failed")
				span.SetAttributes(semconv.ErrorTypeKey.String(fmt.Sprintf("%T", event.Err)))
			}
			if event.Cause != nil {
				span.SetAttributes(attribute.String("broker.transport.cause_type", fmt.Sprintf("%T", event.Cause)))
			}
			span.End(trace.WithTimestamp(now))
			if event.Operation != natsjs.OperationRenew {
				delivery := trace.SpanFromContext(ctx)
				if event.Err != nil {
					delivery.SetStatus(codes.Error, "delivery did not complete")
				}
				delivery.End(trace.WithTimestamp(now))
			}
		},
	}
}
