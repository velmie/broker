// Package otelnatsjs instruments native JetStream consumer observations.
package otelnatsjs

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/velmie/broker"
	native "github.com/velmie/broker/natsjs/v4"
	"github.com/velmie/broker/otelbroker"
)

const (
	settlementCounterName = "messaging.broker.settlement.attempts"
	outcomeAttribute      = "broker.processing.outcome"
	outcomeHandled        = "handled"
	outcomeRetry          = "retry_requested"
	outcomeReject         = "reject_requested"
	outcomeFailed         = "failed"
)

// NewObserver records completed settlements and native failures. It owns no
// consumer, exporter or shutdown. Meter and logger must be non-nil. The caller
// owns the supplied instrumentation. NATS server and SDK errors are trusted
// dependency diagnostics and are passed unchanged to the logger.
func NewObserver(meter metric.Meter, logger *slog.Logger) (native.Observer, error) {
	counter, err := meter.Int64Counter(
		settlementCounterName,
		metric.WithUnit("{attempt}"),
		metric.WithDescription("Completed NATS settlement attempts"),
	)
	if err != nil {
		return native.Observer{}, fmt.Errorf("create settlement attempt counter: %w", err)
	}
	return native.Observer{
		Begin: func(ctx context.Context, _ native.DeliveryInfo) context.Context {
			return otelbroker.CaptureProcessing(ctx)
		},
		Record: func(ctx context.Context, event native.Observation) {
			ctx = otelbroker.ProcessingContext(ctx)
			switch event.Operation {
			case native.OperationStartup:
				if event.Outcome == native.OutcomeSucceeded && event.Err == nil {
					logger.InfoContext(
						ctx,
						"consumer ready",
						slog.String("operation", event.Operation),
						slog.String("source", event.Source),
						slog.String("outcome", "ready"),
					)
				}
			case native.OperationAck, native.OperationNak, native.OperationTerm:
				if event.Outcome == native.OutcomeStarted || event.Outcome == native.OutcomeSuppressed {
					return
				}
				outcome := outcomeFailed
				if event.Err == nil && event.Outcome == native.OutcomeConfirmed {
					switch event.Disposition.(type) {
					case broker.Handled:
						outcome = outcomeHandled
					case broker.Reject, native.Terminate:
						outcome = outcomeReject
					case broker.RetryAfter:
						outcome = outcomeRetry
					}
				}
				counter.Add(
					ctx,
					1,
					metric.WithAttributes(
						semconv.MessagingSystemKey.String("nats"),
						semconv.MessagingConsumerGroupName(event.Metadata.Consumer),
						attribute.String(outcomeAttribute, outcome),
					),
				)
				if event.Err != nil {
					logger.ErrorContext(
						ctx,
						"consumer settlement failed",
						slog.String("operation", event.Operation),
						slog.String("source", event.Source),
						slog.String("outcome", event.Outcome),
						slog.Any("error", event.Err),
					)
				}
			case native.OperationFetch:
				if event.Outcome == native.OutcomeInterrupted {
					logger.WarnContext(
						ctx,
						"consumer receive interrupted",
						slog.String("operation", event.Operation),
						slog.String("source", event.Source),
						slog.String("outcome", event.Outcome),
						slog.Any("error", event.Err),
					)
				} else if event.Outcome == native.OutcomeFailed && event.Err != nil {
					logger.ErrorContext(
						ctx,
						"consumer receive failed",
						slog.String("operation", event.Operation),
						slog.String("source", event.Source),
						slog.String("outcome", event.Outcome),
						slog.Any("error", event.Err),
					)
				}
			}
		},
	}, nil
}
