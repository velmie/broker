package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

type finiteConsumer struct {
	native      *sqs.Consumer
	observation *deliveryObserver
	accepted    bool
	stop        context.CancelFunc
}

func (c *finiteConsumer) Validate(requirements ...broker.Requirement) error {
	return c.native.Validate(requirements...)
}
func (c *finiteConsumer) Run(ctx context.Context, handler broker.Handler) (result error) {
	owner := ctx
	ctx, c.stop = context.WithCancel(ctx)
	defer c.stop()
	defer func() { c.observation.finish(result) }()
	result = c.native.Run(ctx, handler)
	if c.accepted && owner.Err() == nil && onlyCancellation(result) {
		return nil
	}
	return result
}

type forwardedDelivery struct {
	original broker.Delivery
	message  broker.Message
}

func (d *forwardedDelivery) Message() broker.Message { return d.message }
func (d *forwardedDelivery) Source() string          { return d.original.Source() }
func (d *forwardedDelivery) Attempt() broker.Attempt {
	return d.original.(broker.AttemptReader).Attempt()
}
func (d *forwardedDelivery) Metadata() sqs.Metadata {
	return d.original.(sqs.MetadataReader).Metadata()
}

func consume(ctx context.Context, queues *nativeSQS.Client, queue, topic string, logger *slog.Logger, tracer trace.Tracer,
	effect func(context.Context, broker.Delivery, order) error) error {
	handler := orderHandler(logger, tracer, effect)
	if topic != "" {
		handler = decodeEnvelope(topic, handler)
	}
	factory := func() (broker.Consumer, error) {
		observation := &deliveryObserver{tracer: tracer}
		finite := &finiteConsumer{observation: observation}
		hooks := observation.hooks()
		record := hooks.Record
		hooks.Record = func(ctx context.Context, event sqs.Observation) {
			record(ctx, event)
			if event.Operation == sqs.OperationDelete && event.Outcome == sqs.OutcomeAccepted {
				finite.accepted = true
				logger.InfoContext(ctx, "SQS deletion accepted", "source", sourceName)
				finite.stop()
			}
		}
		native, err := sqs.NewConsumer(queues, sqs.ConsumerConfig{QueueURL: queue, Source: sourceName,
			WaitTimeSeconds: 1, ReceiveTimeout: operationBudget, OperationTimeout: operationBudget,
			Shutdown: broker.ShutdownPolicy{GracePeriod: time.Second}, Observer: hooks})
		if err != nil {
			return nil, err
		}
		finite.native = native
		return finite, nil
	}
	recovered := broker.WithConsumerRecovery(factory, broker.RecoveryConfig{Source: sourceName, MaxRestarts: 2,
		Backoff: recoveryBackoff, Retryable: retryReceiveThrottle, Logger: logger})
	consumer, err := recovered()
	if err != nil {
		return err
	}
	return consumer.Run(ctx, handler)
}

func orderHandler(logger *slog.Logger, tracer trace.Tracer, effect func(context.Context, broker.Delivery, order) error) broker.Handler {
	return broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(ctx context.Context, delivery broker.Delivery, value order) (broker.Disposition,
			error) {
			if delivery.Message().ID != exampleID || value.Amount != exampleAmount {
				return nil, errors.New("unexpected order")
			}
			attempt := delivery.(broker.AttemptReader).Attempt()
			metadata := delivery.(sqs.MetadataReader).Metadata()
			if !attempt.Known || metadata.MessageID == "" {
				return nil, errors.New("native delivery metadata is required")
			}
			// Retained metadata is detached and carries no receipt authority.
			logger.InfoContext(ctx, "native delivery received", "source", sourceName, "attempt", attempt.Number)
			if err := effect(ctx, delivery, value); err != nil {
				return nil, err
			}
			logger.InfoContext(ctx, "order effect completed", "source", sourceName)
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{})).WithMiddleware(
		otelbroker.TraceProcessing("aws_sqs", otelbroker.WithTracer(tracer), otelbroker.WithPropagator(propagation.TraceContext{})),
		broker.LogProcessing(logger, broker.ProcessingLogConfig{}), broker.RecoverPanics(), broker.SkipOwnMessages("order-worker"))
}

func decodeEnvelope(topic string, next broker.Handler) broker.Handler {
	return broker.NewHandler(func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		message, err := sns.DecodeNotification(delivery.Message().Body, topic)
		if err != nil {
			return nil, &broker.DecodeError{Cause: err}
		}
		return next.Handle(ctx, &forwardedDelivery{original: delivery, message: message})
	}, broker.WithRequirements(next.Requirements()...))
}

func onlyCancellation(err error) bool {
	if err == nil || err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !onlyCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		return cause != nil && onlyCancellation(cause)
	}
	return false
}
