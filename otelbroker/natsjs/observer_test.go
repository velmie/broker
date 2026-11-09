package natsjs_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	otelnatsjs "github.com/velmie/broker/otelbroker/natsjs"
)

func TestObserverCountsCompletedAttemptsAndLogsNativeFailures(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	var output bytes.Buffer
	observer, err := otelnatsjs.NewObserver(provider.Meter("test"), slog.New(slog.NewJSONHandler(&output, nil)), otelnatsjs.ObserverConfig{
		AttemptCounterName: "test.attempts", AttemptCounterDescription: "delivery attempts",
	})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("native settlement failed")
	ctx := observer.Begin(context.Background(), natsjs.DeliveryInfo{})
	for _, event := range []natsjs.Observation{
		{Source: "orders/durable", Operation: natsjs.OperationStartup, Outcome: natsjs.OutcomeSucceeded},
		{Operation: natsjs.OperationProcess, Outcome: natsjs.OutcomeSucceeded},
		{Operation: natsjs.OperationAck, Outcome: natsjs.OutcomeStarted},
		{Operation: natsjs.OperationTerm, Outcome: natsjs.OutcomeSuppressed, Err: cause},
		{Operation: natsjs.OperationAck, Outcome: natsjs.OutcomeConfirmed, Disposition: broker.Handled{}},
		{Operation: natsjs.OperationTerm, Outcome: natsjs.OutcomeConfirmed, Disposition: broker.Reject{}},
		{Operation: natsjs.OperationNak, Outcome: natsjs.OutcomeConfirmed, Disposition: broker.RetryAfter{}},
		{Source: "orders/durable", Operation: natsjs.OperationTerm, Outcome: natsjs.OutcomeUnknown, Err: cause},
		{Source: "orders/durable", Operation: natsjs.OperationFetch, Outcome: natsjs.OutcomeInterrupted, Err: cause},
		{Source: "orders/durable", Operation: natsjs.OperationFetch, Outcome: natsjs.OutcomeFailed, Err: cause},
		{Operation: natsjs.OperationRun, Outcome: natsjs.OutcomeFailed, Err: cause},
	} {
		observer.Record(ctx, event)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, point := range metrics.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64]).DataPoints {
		value, _ := point.Attributes.Value("outcome")
		got[value.AsString()] = point.Value
	}
	if got["success"] != 1 || got["rejected"] != 1 || got["failed"] != 2 {
		t.Fatalf("outcomes=%v", got)
	}
	if strings.Count(output.String(), "native settlement failed") != 3 || !strings.Contains(output.String(), "orders/durable") ||
		!strings.Contains(output.String(), `"stage":"fetch"`) || !strings.Contains(output.String(), `"stage":"term"`) ||
		!strings.Contains(output.String(), `"outcome":"ready"`) {
		t.Fatalf("diagnostics=%s", output.String())
	}
}
