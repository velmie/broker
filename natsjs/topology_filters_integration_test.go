package natsjs_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v4"
)

func TestEnsureDurableConsumerFilterSet(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "FILTER_SET",
		Subjects:  []string{"events.*"},
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "FILTER_SET",
		Durable:       "worker",
		Subjects:      []string{"events.created", "events.paid"},
		AckWait:       time.Second,
		MaxDeliveries: -1,
		MaxAckPending: 10,
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}
	consumer, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	if err != nil {
		t.Fatal(err)
	}
	actual := consumer.CachedInfo().Config
	if actual.FilterSubject != "" || !slices.Equal(actual.FilterSubjects, cfg.Subjects) {
		t.Fatalf("unexpected native filters: %+v", actual)
	}
}

func TestDurableFilterValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		subjects []string
		field    string
	}{
		{
			name:  "omitted",
			field: "subjects",
		},
		{
			name:     "empty",
			subjects: []string{},
			field:    "subjects",
		},
		{
			name:     "empty element",
			subjects: []string{"events.created", ""},
			field:    "subjects[1]",
		},
		{
			name:     "duplicate",
			subjects: []string{"events.created", "events.created"},
			field:    "subjects[1]",
		},
		{
			name:     "invalid",
			subjects: []string{"events.created", "events.>.*"},
			field:    "subjects[1]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := natsjs.DurableConsumerConfig{
				Stream:        "FILTER_SET",
				Durable:       "worker",
				Subjects:      tc.subjects,
				AckWait:       time.Second,
				MaxDeliveries: -1,
				MaxAckPending: 1,
			}
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("invalid filters accepted or field missing: %v", err)
			}
		})
	}
}

//nolint:gocyclo // One scenario verifies progress and remote topology across multiple drift attempts.
func TestEnsureDurableFilterSetPreservesProgressAndRejectsDrift(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "FILTER_PROGRESS",
		Subjects: []string{"progress.*"},
		Discard:  jetstream.DiscardOld,
		MaxAge:   time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "FILTER_PROGRESS",
		Durable:       "worker",
		Subjects:      []string{"progress.created", "progress.paid"},
		AckWait:       time.Second,
		MaxDeliveries: -1,
		MaxAckPending: 10,
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(ctx, "progress.created", []byte("first")); err != nil {
		t.Fatal(err)
	}
	consumer, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for message := range batch.Messages() {
		if err = message.DoubleAck(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = batch.Error(); err != nil {
		t.Fatal(err)
	}
	before, err := consumer.Info(ctx)
	if err != nil || before.AckFloor.Stream != 1 {
		t.Fatalf("ack before ensure: %+v %v", before, err)
	}
	cfg.Subjects = []string{"progress.paid", "progress.created"}
	if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err != nil {
		t.Fatalf("reordered filters: %v", err)
	}
	for _, subjects := range [][]string{
		{"progress.created"},
		{"progress.created", "progress.paid", "progress.other"},
		{"progress.created", "progress.other"},
		{"progress.created", "other.outside"},
		{"progress.created", "progress.>"},
	} {
		cfg.Subjects = subjects
		if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err == nil {
			t.Fatalf("accepted filters %v", subjects)
		}
	}
	missing := cfg
	missing.Durable = "uncovered"
	missing.Subjects = []string{"progress.created", "other.outside"}
	if err = natsjs.EnsureDurableConsumer(ctx, js, missing); err == nil {
		t.Fatal("uncovered filter accepted")
	}
	if _, err = js.Consumer(ctx, cfg.Stream, missing.Durable); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("uncovered consumer was created: %v", err)
	}
	afterConsumer, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	if err != nil {
		t.Fatal(err)
	}
	after := afterConsumer.CachedInfo()
	if after.AckFloor.Stream != before.AckFloor.Stream || !slices.Equal(after.Config.FilterSubjects, before.Config.FilterSubjects) {
		t.Fatalf("consumer modified: before=%+v after=%+v", before, after)
	}
}
