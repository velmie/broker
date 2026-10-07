package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestInstancePolicySkipsOwnMessageAndCompletesBothDeliveries(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	var calls, acknowledgments int
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			acknowledgments++
			if acknowledgments == 2 {
				stop()
			}
		}
	}
	h := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		calls++
		if delivery.Message().ID != "other" {
			t.Error("own message reached business")
		}
		return broker.Handled{}, nil
	}).WithMiddleware(broker.SkipOwnMessages("worker-1"))
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ instance, id string }{{"worker-1", "own"}, {"worker-2", "other"}} {
		publisher := broker.WrapPublisher(pub, broker.StampInstanceID(item.instance), broker.ValidateIDHeader("Command-Id"))
		if err = publisher.Publish(ctx, broker.Message{ID: item.id, Headers: []broker.Header{
			{Name: "Command-Id", Value: []byte(item.id)},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	validated := broker.WrapPublisher(pub, broker.ValidateIDHeader("Command-Id"))
	if err = validated.Publish(ctx, broker.Message{ID: "conflicting", Headers: []broker.Header{
		{Name: "Command-Id", Value: []byte("another")},
	}}); err == nil {
		t.Fatal("conflicting identity reached transport")
	}
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Run(ctx, h); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 1 || acknowledgments != 2 {
		t.Fatalf("business calls=%d acknowledgments=%d", calls, acknowledgments)
	}
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || info.NumAckPending != 0 || info.AckFloor.Stream != 2 {
		t.Fatalf("completion: %+v, %v", info, err)
	}
	stream, err := js.StreamInfo(config.Stream)
	if err != nil || stream.State.Msgs != 2 {
		t.Fatalf("invalid identity published: %+v, %v", stream, err)
	}
}
