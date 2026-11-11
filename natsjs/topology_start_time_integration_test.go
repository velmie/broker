package natsjs_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v3"
)

func TestDurableConsumerStartsAtConfiguredTime(t *testing.T) {
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
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "START_TIME_TEST",
		Subjects: []string{"start.time"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstAck, err := js.Publish(ctx, "start.time", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	secondAck, err := js.Publish(ctx, "start.time", []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.GetMsg(ctx, firstAck.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	second, err := stream.GetMsg(ctx, secondAck.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if second.Time.Sub(first.Time) < 2*time.Nanosecond {
		t.Fatalf("server timestamps are not sufficiently distinct: %v and %v", first.Time, second.Time)
	}
	start := first.Time.Add(second.Time.Sub(first.Time) / 2)
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "START_TIME_TEST",
		Durable:       "worker",
		Subject:       "start.time",
		AckWait:       time.Second,
		MaxDeliveries: 3,
		MaxAckPending: 1,
		StartTime:     start,
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err != nil {
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
	var bodies []string
	for message := range batch.Messages() {
		bodies = append(bodies, string(message.Data()))
	}
	if err = batch.Error(); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 || bodies[0] != "after" {
		t.Fatalf("delivered %v, want only after", bodies)
	}
}

//nolint:gocyclo // The integration scenario checks the acknowledged position before and after verification.
func TestEnsureDurableConsumerPreservesExistingDeliverNew(t *testing.T) {
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
		Name:     "EXISTING_START_TEST",
		Subjects: []string{"existing.start"},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := js.CreateConsumer(ctx, "EXISTING_START_TEST", jetstream.ConsumerConfig{
		Name:          "worker",
		Durable:       "worker",
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second,
		MaxDeliver:    3,
		FilterSubject: "existing.start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(ctx, "existing.start", []byte("first")); err != nil {
		t.Fatal(err)
	}
	batch, err := created.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var first jetstream.Msg
	for message := range batch.Messages() {
		first = message
	}
	if err = batch.Error(); err != nil {
		t.Fatal(err)
	}
	if first == nil || string(first.Data()) != "first" {
		t.Fatal("first delivery missing")
	}
	if err = first.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "EXISTING_START_TEST",
		Durable:       "worker",
		Subject:       "existing.start",
		AckWait:       time.Second,
		MaxDeliveries: 3,
		MaxAckPending: 1,
		StartTime:     time.Now().Add(-time.Hour),
	}
	if err = natsjs.EnsureDurableConsumer(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}
	existing, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	if err != nil {
		t.Fatal(err)
	}
	if existing.CachedInfo().Config.DeliverPolicy != jetstream.DeliverNewPolicy {
		t.Fatal("existing delivery policy was modified")
	}
	if _, err = js.Publish(ctx, "existing.start", []byte("second")); err != nil {
		t.Fatal(err)
	}
	batch, err = existing.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for message := range batch.Messages() {
		bodies = append(bodies, string(message.Data()))
	}
	if err = batch.Error(); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 || bodies[0] != "second" {
		t.Fatalf("resumed deliveries = %v, want only second", bodies)
	}
}
