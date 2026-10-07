package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
	"github.com/velmie/broker/otelbroker"
)

const (
	logicalIDField     = "ID"
	nativeIDField      = "MessageID"
	amountField        = "amount"
	deliveryCountField = "DeliveryCount"
)

type finiteConsumer struct {
	native      *adapter.Consumer
	observation *deliveryObserver
	accepted    bool
	stop        context.CancelFunc
}

func (c *finiteConsumer) Validate(requirements ...broker.Requirement) error {
	return c.native.Validate(requirements...)
}
func (c *finiteConsumer) Run(owner context.Context, handler broker.Handler) (result error) {
	ctx, stop := context.WithCancel(owner)
	c.stop = stop
	defer stop()
	defer func() { c.observation.finish(result) }()
	result = c.native.Run(ctx, handler)
	if c.accepted && owner.Err() == nil && onlyCancellation(result) {
		return nil
	}
	return result
}

func consume(ctx context.Context, newReceiver func() (adapter.Receiver, error), settings *options, logger *slog.Logger,
	tracer trace.Tracer, effect func(context.Context, broker.Delivery, order) error) error {
	entered := false
	typed := orderHandler(settings, logger, tracer, effect)
	// The latch precedes decoding and all processing middleware, and outlives generations.
	handler := broker.NewHandler(func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		entered = true
		return typed.Handle(ctx, delivery)
	}, broker.WithRequirements(typed.Requirements()...))
	factory := func() (broker.Consumer, error) {
		observation := &deliveryObserver{tracer: tracer}
		finite := &finiteConsumer{observation: observation}
		hooks := observation.hooks()
		record := hooks.Record
		hooks.Record = func(ctx context.Context, event adapter.Observation) {
			record(ctx, event)
			success := event.Operation == adapter.OperationComplete && event.Outcome == adapter.OutcomeAccepted
			if settings.receiveMode == receiveDeleteMode {
				success = event.Operation == adapter.OperationProcess && event.Outcome == adapter.OutcomeSucceeded
			}
			if success {
				finite.accepted = true
				if settings.receiveMode == peekLockMode {
					logger.InfoContext(ctx, "Azure completion accepted", "source", sourceName)
				} else {
					logger.InfoContext(ctx, "ReceiveAndDelete processing finished; receipt preceded callback and has no acknowledgment guarantee",
						"source", sourceName)
				}
				finite.stop()
			}
		}
		consumer, err := adapter.NewConsumer(newReceiver, adapter.ConsumerConfig{Source: sourceName, ReceiveMode: settings.mode(),
			ReceiveTimeout: receiveBudget, OperationTimeout: operationBudget, CloseTimeout: closeBudget,
			Shutdown: broker.ShutdownPolicy{GracePeriod: time.Second}, Observer: hooks})
		if err != nil {
			return nil, err
		}
		finite.native = consumer
		return finite, nil
	}
	recovered := broker.WithConsumerRecovery(factory, broker.RecoveryConfig{Source: sourceName, MaxRestarts: 2,
		Backoff: recoveryBackoff, Logger: logger,
		Retryable: func(err error) bool { return retryStartup(ctx, entered, err) }})
	consumer, err := recovered()
	if err != nil {
		return err
	}
	return consumer.Run(ctx, handler)
}

func orderHandler(settings *options, logger *slog.Logger, tracer trace.Tracer,
	effect func(context.Context, broker.Delivery, order) error) broker.Handler {
	return broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(ctx context.Context, delivery broker.Delivery, value order) (broker.Disposition, error) {
			if delivery.Message().ID != settings.id {
				return nil, &adapter.FieldError{Field: logicalIDField, Cause: errors.New("unexpected logical ID")}
			}
			if value.Amount != exampleAmount {
				return nil, &adapter.FieldError{Field: amountField, Cause: errors.New("unexpected amount")}
			}
			metadata := delivery.(adapter.MetadataReader).Metadata()
			attempt := delivery.(broker.AttemptReader).Attempt()
			if metadata.MessageID != settings.id {
				return nil, &adapter.FieldError{Field: nativeIDField, Cause: errors.New("unexpected native ID")}
			}
			if !attempt.Known {
				return nil, &adapter.FieldError{Field: deliveryCountField, Cause: errors.New("native attempt required")}
			}
			logger.InfoContext(ctx, "native delivery received", "source", sourceName, "attempt", attempt.Number)
			if err := effect(ctx, delivery, value); err != nil {
				return nil, err
			}
			logger.InfoContext(ctx, "order effect completed", "source", sourceName)
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{})).WithMiddleware(
		otelbroker.TraceProcessing("servicebus", otelbroker.WithTracer(tracer), otelbroker.WithPropagator(propagation.TraceContext{})),
		broker.LogProcessing(logger, broker.ProcessingLogConfig{}), broker.RecoverPanics(), broker.SkipOwnMessages("order-worker"))
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
	if cause := errors.Unwrap(err); cause != nil {
		return onlyCancellation(cause)
	}
	return false
}
