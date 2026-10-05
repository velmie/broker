package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/broker/natsjs/v3"
)

func TestInjectedUnknownAckRetainsMarkerForRealRedelivery(t *testing.T) {
	endpoint := natsEndpoint(t)
	control, js, config := provisionTest(t, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), routeBudget)
	defer cancel()
	connection, err := nats.Connect(endpoint, nats.FlusherTimeout(operationBudget), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	store := memory.New()
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	engine := idempo.NewEngine(store, idempo.WithLockTTL(lockTTL), idempo.WithResultTTL(resultTTL))
	var effects, replays, unknown, opposite atomic.Int32
	middleware, err := idempotency.Middleware(engine, idempotency.WithUseSourceInKey(false), idempotency.WithKeyPrefix(keyPrefix),
		idempotency.WithOnReplay(func(context.Context, broker.Delivery, *idempo.Response) { replays.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewTypedHandler(broker.DecodeJSON[order], func(context.Context, order) error {
		effects.Add(1)
		return nil
	}).WithMiddleware(middleware)
	config.Observer = injectedAckFailure(connection, &unknown, &opposite)
	if err = publish(ctx, control, config.Subject); err != nil {
		t.Fatal(err)
	}
	consumer, err := natsjs.NewConsumer(connection, config)
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.Run(ctx, handler)
	assertUnknownAck(t, err, effects.Load(), unknown.Load(), opposite.Load())
	if err = verifyMarker(ctx, store); err != nil {
		t.Fatal(err)
	}
	// Read server state independently. The returned ACK error alone proves no state.
	info, err := js.ConsumerInfo(config.Stream, config.Consumer, nats.Context(ctx))
	if err != nil || info.NumAckPending != 1 {
		t.Fatalf("native pending readback: %+v %v", info, err)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	if err = secondRun(ctx, control, &config, handler, logger); err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 1 || replays.Load() != 1 {
		t.Fatal("unknown ACK replay reran business effect")
	}
	if err = verifyMarker(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err = verifyEmpty(ctx, js, &config); err != nil {
		t.Fatal(err)
	}
}

func assertUnknownAck(t *testing.T, err error, effects, unknown, opposite int32) {
	t.Helper()
	if !errors.Is(err, nats.ErrConnectionClosed) || effects != 1 || unknown != 1 || opposite != 0 {
		t.Fatalf("injected unknown ACK evidence: effects=%d unknown=%d opposite=%d err=%v", effects, unknown, opposite, err)
	}
}
func provisionTest(t *testing.T, endpoint string) (*nats.Conn, nats.JetStreamContext, natsjs.ConsumerConfig) {
	t.Helper()
	control, err := nats.Connect(endpoint, nats.FlusherTimeout(operationBudget), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(control.Close)
	js, err := control.JetStream(nats.MaxWait(operationBudget))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
	defer cancel()
	config, cleanup, err := provision(ctx, js)
	t.Cleanup(func() {
		if cleanErr := cleanup(); cleanErr != nil {
			t.Error(cleanErr)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return control, js, config
}

func injectedAckFailure(connection *nats.Conn, unknown, opposite *atomic.Int32) natsjs.Observer {
	return natsjs.Observer{Record: func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeStarted {
			// Inject connection closure after terminal admission. This is not a server outage.
			connection.Close()
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeUnknown {
			unknown.Add(1)
		}
		if (event.Operation == natsjs.OperationNak || event.Operation == natsjs.OperationTerm) && event.Outcome == natsjs.OutcomeStarted {
			opposite.Add(1)
		}
	}}
}
