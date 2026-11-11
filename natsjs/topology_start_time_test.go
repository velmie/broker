package natsjs_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v3"
)

type capturingConsumerCreation struct {
	jetstream.JetStream
	stream  jetstream.Stream
	created jetstream.ConsumerConfig
}

func (c *capturingConsumerCreation) Stream(context.Context, string) (jetstream.Stream, error) {
	return c.stream, nil
}

func (c *capturingConsumerCreation) Consumer(context.Context, string, string) (jetstream.Consumer, error) {
	return nil, jetstream.ErrConsumerNotFound
}

//nolint:gocritic // JetStream requires configuration by value.
func (c *capturingConsumerCreation) CreateConsumer(
	_ context.Context,
	_ string,
	cfg jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	c.created = cfg
	return &staticConsumer{
		info: jetstream.ConsumerInfo{
			Config: cfg,
		},
	}, nil
}

func TestEnsureDurableConsumerCreateStartTime(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		start  time.Time
		policy jetstream.DeliverPolicy
	}{
		{
			name:   "zero means all",
			policy: jetstream.DeliverAllPolicy,
		},
		{
			name:   "start time",
			start:  start,
			policy: jetstream.DeliverByStartTimePolicy,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := &capturingConsumerCreation{
				stream: &staticStream{
					info: jetstream.StreamInfo{
						Config: jetstream.StreamConfig{
							Name:     "ORDERS",
							Subjects: []string{"orders.>"},
						},
					},
				},
			}
			cfg := natsjs.DurableConsumerConfig{
				Stream:        "ORDERS",
				Durable:       "worker",
				Subject:       "orders.created",
				AckWait:       time.Second,
				MaxDeliveries: 3,
				MaxAckPending: 1,
				StartTime:     tc.start,
			}
			if err := natsjs.EnsureDurableConsumer(context.Background(), js, cfg); err != nil {
				t.Fatal(err)
			}
			if js.created.DeliverPolicy != tc.policy {
				t.Fatalf("delivery policy %v, want %v", js.created.DeliverPolicy, tc.policy)
			}
			if tc.start.IsZero() && js.created.OptStartTime != nil ||
				!tc.start.IsZero() && (js.created.OptStartTime == nil || !js.created.OptStartTime.Equal(tc.start)) {
				t.Fatalf("start time %v, want %v", js.created.OptStartTime, tc.start)
			}
		})
	}
}

func TestEnsureDurableConsumerAcceptsExistingStartPositions(t *testing.T) {
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "ORDERS",
		Durable:       "worker",
		Subject:       "orders.created",
		AckWait:       time.Second,
		MaxDeliveries: 3,
		MaxAckPending: 1,
		StartTime:     time.Now(),
	}
	for _, policy := range []jetstream.DeliverPolicy{
		jetstream.DeliverNewPolicy,
		jetstream.DeliverByStartTimePolicy,
		jetstream.DeliverByStartSequencePolicy,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			actual := jetstream.ConsumerConfig{
				Name:          "worker",
				Durable:       "worker",
				DeliverPolicy: policy,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       time.Second,
				MaxDeliver:    3,
				FilterSubject: "orders.created",
			}
			switch policy {
			case jetstream.DeliverByStartTimePolicy:
				otherTime := cfg.StartTime.Add(-time.Hour)
				actual.OptStartTime = &otherTime
			case jetstream.DeliverByStartSequencePolicy:
				actual.OptStartSeq = 42
			}
			js := &staticTopology{
				stream: &staticStream{
					info: jetstream.StreamInfo{
						Config: jetstream.StreamConfig{
							Name:     "ORDERS",
							Subjects: []string{"orders.>"},
						},
					},
				},
				consumer: &staticConsumer{
					info: jetstream.ConsumerInfo{
						Config: actual,
					},
				},
			}
			if err := natsjs.EnsureDurableConsumer(context.Background(), js, cfg); err != nil {
				t.Fatalf("existing start policy %v rejected: %v", policy, err)
			}
		})
	}
}
