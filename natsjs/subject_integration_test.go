package natsjs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestWildcardGuardUsesReceivedSubjectBeforeDecoding(t *testing.T) {
	nc, js, config := provisionWildcard(t)
	own := config.Stream + ".local.updated"
	foreign := config.Stream + ".remote.updated"
	for _, item := range []struct{ subject, body string }{
		{own, "deliberately invalid JSON"}, {foreign, `{"ID":"order-1"}`},
	} {
		if _, err := js.Publish(item.subject, []byte(item.body)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	var subjects, sources []string
	var callbacks, acks int
	guard := broker.HandlerMiddleware{Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
		return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			subject := delivery.(natsjs.SubjectReader).Subject()
			subjects = append(subjects, subject)
			sources = append(sources, delivery.Source())
			if subject == own {
				return broker.Handled{}, nil
			}
			return next(ctx, delivery)
		}
	}}
	handler := broker.NewTypedHandler(broker.DecodeJSON[struct{ ID string }],
		func(_ context.Context, order struct{ ID string }) error {
			callbacks++
			if order.ID != "order-1" {
				t.Errorf("decoded order=%+v", order)
			}
			return nil
		}).WithMiddleware(guard)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			acks++
			if acks == 2 {
				stop()
			}
		}
	}
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(subjects, []string{own, foreign}) || callbacks != 1 || acks != 2 {
		t.Fatalf("subjects=%v callbacks=%d acks=%d", subjects, callbacks, acks)
	}
	wantSource := config.Stream + "/" + config.Consumer
	if !reflect.DeepEqual(sources, []string{wantSource, wantSource}) {
		t.Fatalf("source identity changed: %v", sources)
	}
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Durable != config.Consumer || info.AckFloor.Stream != 2 || info.NumAckPending != 0 {
		t.Fatalf("durable progress: %+v", info)
	}
	if _, err = js.Publish(config.Stream+".later.updated", nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(subjects, []string{own, foreign}) {
		t.Fatal("retained subject changed after callback")
	}
}

func provisionWildcard(t *testing.T) (*nats.Conn, nats.JetStreamContext, natsjs.ConsumerConfig) {
	t.Helper()
	nc, js, config := provision(t, time.Second)
	config.Subject = config.Stream + ".*.updated"
	if _, err := js.UpdateStream(&nats.StreamConfig{
		Name: config.Stream, Subjects: []string{config.Subject}, Storage: nats.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
		t.Fatal(err)
	}
	if _, err := js.AddConsumer(config.Stream, &nats.ConsumerConfig{
		Durable: config.Consumer, FilterSubject: config.Subject, AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	return nc, js, config
}
