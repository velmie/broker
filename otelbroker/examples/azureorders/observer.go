package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	adapter "github.com/velmie/broker/azuresb"
)

type deliveryObserver struct {
	tracer trace.Tracer
	active trace.Span
}

func (o *deliveryObserver) hooks() *adapter.Observer {
	return &adapter.Observer{
		Begin: func(ctx context.Context, _ adapter.DeliveryInfo) context.Context {
			ctx, o.active = o.tracer.Start(ctx, "delivery", trace.WithAttributes(attribute.String("messaging.system", "servicebus")))
			return ctx
		},
		Record: func(ctx context.Context, event adapter.Observation) {
			if event.Operation == adapter.OperationProcess {
				if event.Outcome == adapter.OutcomeFailed {
					o.finish(event.Err)
				}
				return
			}
			recordNative(ctx, o.tracer, &event)
			switch event.Operation {
			case adapter.OperationComplete, adapter.OperationAbandon, adapter.OperationDisposition:
				o.finish(event.Err)
			}
		},
	}
}

// Run also ends a remaining delivery after joined ReceiveAndDelete or cancellation.
func (o *deliveryObserver) finish(err error) {
	if o.active == nil {
		return
	}
	if err != nil {
		o.active.SetStatus(codes.Error, "delivery did not complete")
	}
	o.active.End()
	o.active = nil
}
func recordNative(ctx context.Context, tracer trace.Tracer, event *adapter.Observation) {
	now := time.Now()
	requested := event.Outcome != adapter.OutcomeSuppressed && event.Operation != adapter.OperationDisposition &&
		event.Operation != adapter.OperationMetadata
	kind := trace.SpanKindInternal
	if requested {
		kind = trace.SpanKindClient
	}
	_, span := tracer.Start(ctx, event.Operation, trace.WithSpanKind(kind), trace.WithTimestamp(now.Add(-event.Duration)),
		trace.WithAttributes(
			attribute.String("messaging.system", "servicebus"), attribute.String("messaging.operation.name", event.Operation),
			attribute.String("broker.transport.outcome", event.Outcome), attribute.Bool("broker.transport.requested", requested)))
	if event.Err != nil {
		span.SetStatus(codes.Error, "native operation failed")
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", event.Err)))
	}
	span.End(trace.WithTimestamp(now))
}
