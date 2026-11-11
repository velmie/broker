package otelnatsjs_test

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
	"github.com/velmie/broker/natsjs/v4"
	"github.com/velmie/broker/otelbroker/otelnatsjs"
)

func TestObserverCountsCompletedAttemptsAndLogsNativeFailures(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		if shutdownErr := provider.Shutdown(context.Background()); shutdownErr != nil {
			t.Error(shutdownErr)
		}
	}()
	var output bytes.Buffer
	observer, err := otelnatsjs.NewObserver(provider.Meter("test"), slog.New(slog.NewJSONHandler(&output, nil)))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("native settlement failed")
	ctx := observer.Begin(context.Background(), natsjs.DeliveryInfo{})
	for _, event := range []natsjs.Observation{
		{
			Source:    "orders/durable",
			Operation: natsjs.OperationStartup,
			Outcome:   natsjs.OutcomeSucceeded,
		},
		{
			Operation: natsjs.OperationProcess,
			Outcome:   natsjs.OutcomeSucceeded,
		},
		{
			Operation: natsjs.OperationDisposition,
			Outcome:   natsjs.OutcomeFailed,
			Err:       cause,
		},
		{
			Operation: natsjs.OperationAck,
			Outcome:   natsjs.OutcomeStarted,
		},
		{
			Operation: natsjs.OperationTerm,
			Outcome:   natsjs.OutcomeSuppressed,
			Err:       cause,
		},
		{
			Operation:   natsjs.OperationAck,
			Outcome:     natsjs.OutcomeConfirmed,
			Disposition: broker.Handled{},
		},
		{
			Operation:   natsjs.OperationTerm,
			Outcome:     natsjs.OutcomeConfirmed,
			Disposition: broker.Reject{},
		},
		{
			Operation:   natsjs.OperationNak,
			Outcome:     natsjs.OutcomeConfirmed,
			Disposition: broker.RetryAfter{},
		},
		{
			Source:    "orders/durable",
			Operation: natsjs.OperationTerm,
			Outcome:   natsjs.OutcomeUnknown,
			Err:       cause,
		},
		{
			Source:    "orders/durable",
			Operation: natsjs.OperationFetch,
			Outcome:   natsjs.OutcomeInterrupted,
			Err:       cause,
		},
		{
			Source:    "orders/durable",
			Operation: natsjs.OperationFetch,
			Outcome:   natsjs.OutcomeFailed,
			Err:       cause,
		},
		{
			Operation: natsjs.OperationRun,
			Outcome:   natsjs.OutcomeFailed,
			Err:       cause,
		},
	} {
		event.Metadata.Consumer = "durable"
		observer.Record(ctx, event)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	instrument := metrics.ScopeMetrics[0].Metrics[0]
	if instrument.Name != "messaging.broker.settlement.attempts" || instrument.Unit != "{attempt}" {
		t.Fatalf("instrument name=%s unit=%s", instrument.Name, instrument.Unit)
	}
	got := map[string]int64{}
	for _, point := range instrument.Data.(metricdata.Sum[int64]).DataPoints {
		value, _ := point.Attributes.Value("broker.processing.outcome")
		system, _ := point.Attributes.Value("messaging.system")
		group, _ := point.Attributes.Value("messaging.consumer.group.name")
		if system.AsString() != "nats" || group.AsString() != "durable" || point.Attributes.Len() != 3 {
			t.Fatalf("metric attributes: %v", point.Attributes)
		}
		got[value.AsString()] = point.Value
	}
	if len(got) != 4 || got["handled"] != 1 || got["reject_requested"] != 1 ||
		got["retry_requested"] != 1 || got["failed"] != 1 {
		t.Fatalf("outcomes=%v", got)
	}
	if strings.Count(output.String(), "native settlement failed") != 3 || !strings.Contains(output.String(), "orders/durable") ||
		strings.Contains(output.String(), `"stage":`) ||
		!strings.Contains(output.String(), `"outcome":"ready"`) {
		t.Fatalf("diagnostics=%s", output.String())
	}
}
