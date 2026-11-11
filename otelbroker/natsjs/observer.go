// Package natsjs instruments native JetStream consumer observations.
package natsjs

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/velmie/broker"
	native "github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/otelbroker"
)

// ObserverConfig selects the completed delivery-attempt counter.
type ObserverConfig struct {
	AttemptCounterName        string
	AttemptCounterDescription string
}

// NewObserver records completed settlements and native failures. It owns no
// consumer, exporter or shutdown. The caller owns the supplied instrumentation.
func NewObserver(meter metric.Meter, logger *slog.Logger, cfg ObserverConfig) (native.Observer, error) {
	counter, err := meter.Int64Counter(cfg.AttemptCounterName,
		metric.WithUnit("{attempt}"), metric.WithDescription(cfg.AttemptCounterDescription))
	if err != nil {
		return native.Observer{}, fmt.Errorf("create attempt counter: %w", err)
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
					logger.InfoContext(ctx, "consumer ready", slog.String("operation", event.Operation),
						slog.String("source", event.Source), slog.String("stage", "subscribe"), slog.String("outcome", "ready"))
				}
			case native.OperationAck, native.OperationNak, native.OperationTerm, native.OperationDisposition:
				if event.Outcome == native.OutcomeStarted || event.Outcome == native.OutcomeSuppressed {
					return
				}
				outcome := "failed"
				if event.Err == nil && event.Outcome == native.OutcomeConfirmed {
					switch event.Disposition.(type) {
					case broker.Handled:
						outcome = "success"
					case broker.Reject, native.Terminate:
						outcome = "rejected"
					}
				}
				counter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
				if event.Err != nil {
					logger.ErrorContext(ctx, "consumer settlement failed", slog.String("operation", event.Operation),
						slog.String("source", event.Source), slog.String("stage", event.Operation),
						slog.String("outcome", event.Outcome), slog.Any("error", event.Err))
				}
			case native.OperationFetch:
				if event.Outcome == native.OutcomeInterrupted {
					logger.WarnContext(ctx, "consumer receive interrupted", slog.String("operation", event.Operation),
						slog.String("source", event.Source), slog.String("stage", event.Operation),
						slog.String("outcome", event.Outcome), slog.Any("error", event.Err))
				} else if event.Outcome == native.OutcomeFailed && event.Err != nil {
					logger.ErrorContext(ctx, "consumer receive failed", slog.String("operation", event.Operation),
						slog.String("source", event.Source), slog.String("stage", event.Operation),
						slog.String("outcome", event.Outcome), slog.Any("error", event.Err))
				}
			}
		},
	}, nil
}
