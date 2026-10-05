package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

type deliveryObserver struct {
	tracer trace.Tracer
	active trace.Span
}

func (o *deliveryObserver) hooks() *sqs.Observer {
	return &sqs.Observer{
		Begin: func(ctx context.Context, _ sqs.DeliveryInfo) context.Context {
			ctx, o.active = o.tracer.Start(ctx, "delivery", trace.WithAttributes(attribute.String("messaging.system", "aws_sqs")))
			return ctx
		},
		Record: func(ctx context.Context, event sqs.Observation) {
			if event.Operation == sqs.OperationProcess {
				if event.Outcome == sqs.OutcomeFailed {
					o.finish(event.Err)
				}
				return
			}
			recordNative(ctx, o.tracer, "aws_sqs", event.Operation, event.Outcome, event.Duration, event.Err)
			switch event.Operation {
			case sqs.OperationDelete, sqs.OperationRetry, sqs.OperationDisposition:
				o.finish(event.Err)
			}
		},
	}
}

// Run calls finish after all native users join, covering canceled admission too.
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

func snsObserver(tracer trace.Tracer) *sns.Observer {
	return &sns.Observer{Record: func(ctx context.Context, event sns.Observation) {
		recordNative(ctx, tracer, "aws_sns", event.Operation, event.Outcome, event.Duration, event.Err)
	}}
}

func recordNative(ctx context.Context, tracer trace.Tracer, system, operation, outcome string, duration time.Duration, err error) {
	now := time.Now()
	requested := outcome != sqs.OutcomeSuppressed && operation != sqs.OperationDisposition && operation != sqs.OperationMetadata
	kind := trace.SpanKindInternal
	if requested {
		kind = trace.SpanKindClient
	}
	_, span := tracer.Start(ctx, operation, trace.WithSpanKind(kind), trace.WithTimestamp(now.Add(-duration)), trace.WithAttributes(
		attribute.String("messaging.system", system), attribute.String("messaging.operation.name", operation),
		attribute.String("broker.transport.outcome", outcome), attribute.Bool("broker.transport.requested", requested)))
	if err != nil {
		span.SetStatus(codes.Error, "native operation failed")
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	}
	span.End(trace.WithTimestamp(now))
}
