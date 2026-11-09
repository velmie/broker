package natsjs_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v3"
)

//nolint:gocyclo // This test checks the full lifecycle of one provisioned topology.
func TestTopologyRealServer(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn, jetstream.WithDefaultTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cfg := natsjs.WorkQueueStreamConfig{
		Name:            "TOPOLOGY_TEST",
		Subject:         "topology.test.>",
		MaxMessageBytes: 1024,
		DuplicateWindow: time.Minute,
		MaxMessages:     100,
		MaxBytes:        10240,
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- natsjs.EnsureWorkQueueStream(ctx, js, cfg) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	consumer := natsjs.DurableConsumerConfig{
		Stream:        cfg.Name,
		Durable:       "test",
		Subject:       "topology.test.create",
		AckWait:       time.Second,
		MaxDeliveries: 5,
		MaxAckPending: 1,
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, consumer); err != nil {
		t.Fatal(err)
	}
	if err = natsjs.CheckStreams(ctx, js, cfg.Name); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	updated := stream.CachedInfo().Config
	updated.MaxMsgs = 42
	updated.MaxBytes = 20480
	if _, err = js.UpdateStream(ctx, updated); err != nil {
		t.Fatal(err)
	}
	if err = natsjs.EnsureWorkQueueStream(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}
	stream, err = js.Stream(ctx, cfg.Name)
	if err != nil || stream.CachedInfo().Config.MaxMsgs != 42 {
		t.Fatalf("operator limits changed: %v", err)
	}
	actual, err := js.Consumer(ctx, cfg.Name, consumer.Durable)
	if err != nil {
		t.Fatal(err)
	}
	consumerCfg := actual.CachedInfo().Config
	consumerCfg.MaxAckPending = 7
	if _, err = js.UpdateConsumer(ctx, cfg.Name, consumerCfg); err != nil {
		t.Fatal(err)
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, consumer); err != nil {
		t.Fatal(err)
	}
	actual, err = js.Consumer(ctx, cfg.Name, consumer.Durable)
	if err != nil || actual.CachedInfo().Config.MaxAckPending != 7 {
		t.Fatalf("operator ack limit changed: %v", err)
	}
	cfg.Subject = "topology.other.>"
	var op *natsjs.OperationError
	if err := natsjs.EnsureWorkQueueStream(ctx, js, cfg); !errors.As(err, &op) || !strings.Contains(err.Error(), "subject") {
		t.Fatalf("expected identified incompatible field: %v", err)
	}
	missing := natsjs.RetainedStreamConfig{
		Stream:          "MISSING",
		Subject:         "missing.event",
		MaxMessageBytes: 1024,
	}
	if err := natsjs.RequireRetainedStream(ctx, js, missing); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("expected missing external stream: %v", err)
	}
}

func TestLimitsAndRetainedProfiles(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn, jetstream.WithDefaultTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	limits := natsjs.LimitsStreamConfig{
		Name:            "LIMITS_TEST",
		Subjects:        []string{"retained.a.>", "retained.b.>"},
		DuplicateWindow: time.Minute,
	}
	if err = natsjs.EnsureLimitsStream(ctx, js, limits); err != nil {
		t.Fatal(err)
	}
	if err = natsjs.EnsureLimitsStream(ctx, js, limits); err != nil {
		t.Fatal(err)
	}
	limits.Subjects = []string{"retained.a.>"}
	if err = natsjs.EnsureLimitsStream(ctx, js, limits); err == nil || !strings.Contains(err.Error(), "subjects") {
		t.Fatalf("expected incompatible subjects: %v", err)
	}
	external := natsjs.RetainedStreamConfig{
		Stream:          "EXTERNAL_TEST",
		Subject:         "external.event.created",
		MaxMessageBytes: 1024,
	}
	_, err = js.CreateStream(
		ctx,
		jetstream.StreamConfig{
			Name:      external.Stream,
			Subjects:  []string{"external.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			Discard:   jetstream.DiscardNew,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := natsjs.RequireRetainedStream(ctx, js, external); err != nil {
		t.Fatal(err)
	}
	external.Subject = "other.event"
	if err := natsjs.RequireRetainedStream(ctx, js, external); err == nil || !strings.Contains(err.Error(), "subject") {
		t.Fatalf("expected uncovered subject: %v", err)
	}
}

func TestTopologyConfigRejectsInvalidSubjectWithoutIO(t *testing.T) {
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "STREAM",
		Durable:       "durable",
		Subject:       "invalid\u2003subject",
		AckWait:       time.Second,
		MaxDeliveries: 1,
		MaxAckPending: 1,
	}
	if err := natsjs.EnsureDurableConsumer(context.Background(), nil, cfg); err == nil || !strings.Contains(err.Error(), "subject") {
		t.Fatalf("invalid subject was accepted: %v", err)
	}
}
