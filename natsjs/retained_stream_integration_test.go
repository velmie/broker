package natsjs_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v4"
)

func TestRetainedStreamWithWildcardConsumer(t *testing.T) {
	profile := natsjs.RetainedStreamConfig{
		Stream:          "RETAINED_WILDCARD",
		MaxMessageBytes: 1024,
	}
	if _, present := reflect.TypeOf(profile).FieldByName("Subject"); present {
		t.Fatal("retained stream profile must not have a subject field")
	}
	if err := profile.Validate(); err != nil {
		t.Fatalf("subject-free profile: %v", err)
	}
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:      profile.Stream,
		Subjects:  []string{"payments.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardNew,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := js.DeleteStream(context.Background(), profile.Stream); cleanupErr != nil {
			t.Errorf("delete stream: %v", cleanupErr)
		}
	})
	if err = natsjs.RequireRetainedStream(t.Context(), js, profile); err != nil {
		t.Fatalf("require retained stream: %v", err)
	}
	consumer := natsjs.DurableConsumerConfig{
		Stream:        profile.Stream,
		Durable:       "payment-worker",
		Subjects:      []string{"payments.payment.*"},
		AckWait:       time.Second,
		MaxDeliveries: 1,
		MaxAckPending: 1,
	}
	if err = natsjs.EnsureDurableConsumer(t.Context(), js, consumer); err != nil {
		t.Fatalf("ensure wildcard consumer: %v", err)
	}
	actual, err := js.Consumer(t.Context(), profile.Stream, consumer.Durable)
	if err != nil || actual.CachedInfo().Config.FilterSubject != consumer.Subjects[0] {
		t.Fatalf("consumer filter: %v", err)
	}
	consumer.Subjects = []string{"other.*"}
	err = natsjs.EnsureDurableConsumer(t.Context(), js, consumer)
	var op *natsjs.OperationError
	if !errors.As(err, &op) || op.Operation != "stream_verify" || !strings.Contains(err.Error(), "subjects") {
		t.Fatalf("uncovered filter must fail stream verification: %v", err)
	}
}
