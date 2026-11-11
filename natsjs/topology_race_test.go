package natsjs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v3"
)

type racingStreamCreates struct {
	jetstream.JetStream
	arrivals      sync.WaitGroup
	lookups       atomic.Int32
	failedCreates atomic.Int32
}

func (r *racingStreamCreates) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if r.lookups.Add(1) <= 2 {
		r.arrivals.Done()
		r.arrivals.Wait()
		return nil, jetstream.ErrStreamNotFound
	}
	return r.JetStream.Stream(ctx, name)
}

//nolint:gocritic // JetStream requires configuration by value.
func (r *racingStreamCreates) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	stream, err := r.JetStream.CreateStream(ctx, cfg)
	if err != nil {
		r.failedCreates.Add(1)
	}
	return stream, err
}

type racingConsumerCreates struct {
	jetstream.JetStream
	arrivals      sync.WaitGroup
	lookups       atomic.Int32
	failedCreates atomic.Int32
}

func (r *racingConsumerCreates) Consumer(ctx context.Context, stream, name string) (jetstream.Consumer, error) {
	if r.lookups.Add(1) <= 2 {
		r.arrivals.Done()
		r.arrivals.Wait()
		return nil, jetstream.ErrConsumerNotFound
	}
	return r.JetStream.Consumer(ctx, stream, name)
}

//nolint:gocritic // JetStream requires configuration by value.
func (r *racingConsumerCreates) CreateConsumer(
	ctx context.Context,
	stream string,
	cfg jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	consumer, err := r.JetStream.CreateConsumer(ctx, stream, cfg)
	if err != nil {
		r.failedCreates.Add(1)
	}
	return consumer, err
}

func TestConcurrentDifferentTopologyCreatesUseFallback(t *testing.T) {
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	streams := &racingStreamCreates{
		JetStream: js,
	}
	streams.arrivals.Add(2)
	results := make(chan error, 2)
	for _, subject := range []string{"race.one.>", "race.two.>"} {
		go func() {
			results <- natsjs.EnsureLimitsStream(t.Context(), streams, natsjs.LimitsStreamConfig{
				Name:            "RACE_STREAM",
				Subjects:        []string{subject},
				DuplicateWindow: time.Minute,
			})
		}()
	}
	first, second := <-results, <-results
	if (first == nil) == (second == nil) || streams.failedCreates.Load() != 1 || streams.lookups.Load() != 3 {
		t.Fatalf("stream creates: errors=(%v,%v) failed=%d lookups=%d", first, second, streams.failedCreates.Load(), streams.lookups.Load())
	}
	_, err = js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     "RACE_CONSUMERS",
		Subjects: []string{"consume.>"},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumers := &racingConsumerCreates{
		JetStream: js,
	}
	consumers.arrivals.Add(2)
	for _, subject := range []string{"consume.one", "consume.two"} {
		go func() {
			results <- natsjs.EnsureDurableConsumer(t.Context(), consumers, natsjs.DurableConsumerConfig{
				Stream:        "RACE_CONSUMERS",
				Durable:       "worker",
				Subject:       subject,
				AckWait:       time.Second,
				MaxDeliveries: 2,
				MaxAckPending: 1,
			})
		}()
	}
	first, second = <-results, <-results
	var op *natsjs.OperationError
	if first == nil {
		first = second
	}
	if first == nil || !errors.As(first, &op) || op.Operation != "consumer_verify" ||
		consumers.failedCreates.Load() != 1 || consumers.lookups.Load() != 3 {
		t.Fatalf("consumer creates: error=%v failed=%d lookups=%d", first, consumers.failedCreates.Load(), consumers.lookups.Load())
	}
}
