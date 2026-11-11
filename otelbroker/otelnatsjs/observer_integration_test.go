package otelnatsjs_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
	"github.com/velmie/broker/otelbroker/otelnatsjs"
)

//nolint:gocyclo // One end-to-end flow verifies retry, settlement and exported metrics.
func TestObserverRealConsumerSettlementOutcomes(t *testing.T) {
	address := os.Getenv("NATS_TEST_URL")
	if address == "" {
		t.Skip("NATS_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := nats.Connect(address, nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	name := "OBSERVATION_TEST"
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{"observed.orders"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		if cleanupErr := js.DeleteStream(cleanupCtx, name); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	_, err = js.CreateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable:       "worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second,
		MaxDeliver:    5,
		FilterSubject: "observed.orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		if shutdownErr := provider.Shutdown(context.Background()); shutdownErr != nil {
			t.Error(shutdownErr)
		}
	}()
	observer, err := otelnatsjs.NewObserver(provider.Meter("integration"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	record := observer.Record
	observer.Record = func(ctx context.Context, event natsjs.Observation) {
		record(ctx, event)
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			cancel()
		}
	}
	consumer, err := natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
		Stream:   name,
		Consumer: "worker",
		Subjects: []string{"observed.orders"},
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		attempts++
		if attempts == 1 {
			return broker.RetryAfter{}, nil
		}
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.RedeliveryRequirement{}))
	if _, err = js.Publish(ctx, "observed.orders", []byte("order")); err != nil {
		t.Fatal(err)
	}
	if err = consumer.RunService(ctx, handler); err != nil {
		t.Fatal(err)
	}
	var result metricdata.ResourceMetrics
	if err = reader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, scope := range result.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name != "messaging.broker.settlement.attempts" {
				continue
			}
			for _, point := range instrument.Data.(metricdata.Sum[int64]).DataPoints {
				outcome, _ := point.Attributes.Value("broker.processing.outcome")
				group, _ := point.Attributes.Value("messaging.consumer.group.name")
				if group.AsString() != "worker" {
					t.Fatalf("unexpected consumer group: %v", point.Attributes)
				}
				got[outcome.AsString()] += point.Value
			}
		}
	}
	if attempts != 2 || got["retry_requested"] != 1 || got["handled"] != 1 || len(got) != 2 {
		t.Fatalf("attempts=%d outcomes=%v", attempts, got)
	}
}
