package natsjs_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v4"
)

func TestDurableConsumerDeliveryLimitExact(t *testing.T) {
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
		Name:     "DELIVERY_LIMIT",
		Subjects: []string{"delivery.limit"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		have int
		want int
	}{
		{
			name: "equal",
			have: 5,
			want: 5,
		},
		{
			name: "larger",
			have: 100,
			want: 5,
		},
		{
			name: "smaller",
			have: 2,
			want: 5,
		},
		{
			name: "unlimited existing",
			have: -1,
			want: 5,
		},
		{
			name: "unlimited requested",
			have: 5,
			want: -1,
		},
		{
			name: "both unlimited",
			have: -1,
			want: -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := natsjs.DurableConsumerConfig{
				Stream:        "DELIVERY_LIMIT",
				Durable:       strings.ReplaceAll(tc.name, " ", "_"),
				Subjects:      []string{"delivery.limit"},
				AckWait:       time.Second,
				MaxDeliveries: tc.want,
				MaxAckPending: 1,
			}
			_, err := js.CreateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
				Durable:       cfg.Durable,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       2 * time.Second,
				MaxDeliver:    tc.have,
				FilterSubject: cfg.Subjects[0],
			})
			if err != nil {
				t.Fatal(err)
			}
			err = natsjs.EnsureDurableConsumer(ctx, js, cfg)
			if tc.have == tc.want {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			want := fmt.Sprintf("incompatible max_deliveries: have %d, want %d", tc.have, tc.want)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected %q, got %v", want, err)
			}
		})
	}
}
