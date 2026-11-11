package natsjs_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v4"
)

type staticConsumer struct {
	jetstream.Consumer
	info jetstream.ConsumerInfo
}

func (c *staticConsumer) CachedInfo() *jetstream.ConsumerInfo { return &c.info }

type staticTopology struct {
	jetstream.JetStream
	stream   jetstream.Stream
	consumer jetstream.Consumer
}

func (s *staticTopology) Stream(context.Context, string) (jetstream.Stream, error) {
	return s.stream, nil
}

func (s *staticTopology) Consumer(context.Context, string, string) (jetstream.Consumer, error) {
	return s.consumer, nil
}

func TestDurableConsumerDriftRejected(t *testing.T) {
	want := natsjs.DurableConsumerConfig{
		Stream:        "ORDERS",
		Durable:       "worker",
		Subjects:      []string{"orders.created"},
		AckWait:       time.Minute,
		MaxDeliveries: 5,
		MaxAckPending: 1,
	}
	base := jetstream.ConsumerConfig{
		Name:          "worker",
		Durable:       "worker",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		MaxDeliver:    5,
		FilterSubject: "orders.created",
	}
	for _, tc := range []struct {
		field  string
		change func(*jetstream.ConsumerConfig)
	}{
		{
			field:  "ack_wait",
			change: func(c *jetstream.ConsumerConfig) { c.AckWait = time.Second },
		},
		{
			field:  "max_deliveries",
			change: func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 2 },
		},
		{
			field:  "push_delivery",
			change: func(c *jetstream.ConsumerConfig) { c.DeliverSubject = "push" },
		},
		{
			field:  "subjects",
			change: func(c *jetstream.ConsumerConfig) { c.FilterSubject = "orders.other" },
		},
	} {
		t.Run(tc.field, func(t *testing.T) {
			actual := base
			tc.change(&actual)
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
			err := natsjs.EnsureDurableConsumer(context.Background(), js, want)
			if err == nil || !strings.Contains(err.Error(), "incompatible "+tc.field+": have ") ||
				!strings.Contains(err.Error(), ", want ") {
				t.Fatalf("missing drift and values: %v", err)
			}
		})
	}
}

func TestStreamRetentionDriftRejected(t *testing.T) {
	for _, retention := range []jetstream.RetentionPolicy{jetstream.WorkQueuePolicy, jetstream.LimitsPolicy} {
		for _, tc := range []struct {
			field  string
			change func(*jetstream.StreamConfig)
		}{
			{
				field:  "discard",
				change: func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld },
			},
			{
				field:  "max_age",
				change: func(c *jetstream.StreamConfig) { c.MaxAge = time.Second },
			},
			{
				field:  "max_message_bytes",
				change: func(c *jetstream.StreamConfig) { c.MaxMsgSize = 50 },
			},
		} {
			t.Run(retention.String()+"/"+tc.field, func(t *testing.T) {
				actual := jetstream.StreamConfig{
					Name:       "ORDERS",
					Subjects:   []string{"orders.>"},
					Storage:    jetstream.FileStorage,
					Retention:  retention,
					Discard:    jetstream.DiscardNew,
					MaxMsgSize: -1,
					Duplicates: time.Minute,
				}
				tc.change(&actual)
				js := &staticTopology{
					stream: &staticStream{
						info: jetstream.StreamInfo{
							Config: actual,
						},
					},
				}
				var err error
				if retention == jetstream.WorkQueuePolicy {
					err = natsjs.EnsureWorkQueueStream(context.Background(), js, natsjs.WorkQueueStreamConfig{
						Name:            "ORDERS",
						Subject:         "orders.>",
						MaxMessageBytes: 100,
						DuplicateWindow: time.Minute,
						MaxMessages:     10,
						MaxBytes:        1000,
					})
				} else {
					err = natsjs.RequireRetainedStream(context.Background(), js, natsjs.RetainedStreamConfig{
						Stream:          "ORDERS",
						Subject:         "orders.created",
						MaxMessageBytes: 100,
					})
				}
				if err == nil || !strings.Contains(err.Error(), "incompatible "+tc.field+": have ") ||
					!strings.Contains(err.Error(), ", want ") {
					t.Fatalf("missing drift and values: %v", err)
				}
			})
		}
	}
}

func TestDurableConsumerEquivalentNativeFilters(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected []string
		scalar   string
		multiple []string
		valid    bool
	}{
		{
			name:     "scalar",
			expected: []string{"orders.created"},
			scalar:   "orders.created",
			valid:    true,
		},
		{
			name:     "singleton list",
			expected: []string{"orders.created"},
			multiple: []string{"orders.created"},
			valid:    true,
		},
		{
			name:     "reordered",
			expected: []string{"orders.created", "orders.paid"},
			multiple: []string{"orders.paid", "orders.created"},
			valid:    true,
		},
		{
			name:     "unfiltered",
			expected: []string{"orders.created"},
		},
		{
			name:     "extra",
			expected: []string{"orders.created"},
			multiple: []string{"orders.created", "orders.paid"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := natsjs.DurableConsumerConfig{
				Stream:        "ORDERS",
				Durable:       "worker",
				Subjects:      slices.Clone(tc.expected),
				AckWait:       time.Second,
				MaxDeliveries: -1,
				MaxAckPending: 1,
			}
			actual := jetstream.ConsumerConfig{
				Name:           "worker",
				Durable:        "worker",
				AckPolicy:      jetstream.AckExplicitPolicy,
				AckWait:        time.Second,
				MaxDeliver:     -1,
				FilterSubject:  tc.scalar,
				FilterSubjects: slices.Clone(tc.multiple),
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
			err := natsjs.EnsureDurableConsumer(context.Background(), js, cfg)
			if (err == nil) != tc.valid {
				t.Fatalf("verification: %v", err)
			}
			if !slices.Equal(cfg.Subjects, tc.expected) || !slices.Equal(actual.FilterSubjects, tc.multiple) {
				t.Fatal("verification changed input order")
			}
		})
	}
}
