package natsjs_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestPublicationAndDeliveryPreserveRepresentableHeaders(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	input := broker.Message{ID: "logical-id", Body: []byte("body"), Headers: []broker.Header{
		{Name: "X-Mixed", Value: []byte("first")}, {Name: "X-Mixed", Value: []byte("second")},
		{Name: "x-mixed", Value: []byte("case alias")}, {Name: "Binary", Value: []byte{0xff, 0x00, 'a'}},
	}}
	want := broker.Message{ID: input.ID, Body: bytes.Clone(input.Body), Headers: []broker.Header{
		{Name: "Binary", Value: []byte{0xff, 0x00, 'a'}}, {Name: nats.MsgIdHdr, Value: []byte(input.ID)},
		{Name: "X-Mixed", Value: []byte("first")}, {Name: "X-Mixed", Value: []byte("second")}, {Name: "x-mixed", Value: []byte("case alias")},
	}}
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	if err = pub.Publish(ctx, input); err != nil {
		t.Fatal(err)
	}
	if len(input.Headers) != 4 {
		t.Fatal("publisher mutated header list")
	}
	input.Body[0] = '!'
	input.Headers[0].Value[0] = '!'
	var retained broker.Message
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	h := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		retained = delivery.Message()
		return broker.Handled{}, nil
	})
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Run(ctx, h); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retained, want) {
		t.Fatalf("received %#v, want %#v", retained, want)
	}
	if _, err = js.Publish(config.Subject, []byte("later")); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retained, want) {
		t.Fatal("retained message changed after callback and later publish")
	}
}

func TestPublisherRejectsLossyHeadersAndPreservesCause(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []broker.Message{
		{Headers: []broker.Header{{Name: "X", Value: []byte(" value")}}},
		{Headers: []broker.Header{{Name: "X", Value: []byte("value\t")}}},
		{Headers: []broker.Header{{Name: "X", Value: []byte("value\r\nInjected: yes")}}},
		{Headers: []broker.Header{{Name: "Bad:Name", Value: []byte("value")}}},
		{ID: "one", Headers: []broker.Header{{Name: nats.MsgIdHdr, Value: []byte("two")}}},
		{Headers: []broker.Header{{Name: "nats-msg-id", Value: []byte("one")}}},
		{Headers: []broker.Header{{Name: nats.MsgIdHdr, Value: []byte("one")}, {Name: nats.MsgIdHdr, Value: []byte("one")}}},
		{ID: "line\nbreak"},
	} {
		if publishErr := pub.Publish(context.Background(), message); publishErr == nil {
			t.Fatal("lossy header publication accepted")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller stopped publication")
	cancel(cause)
	if publishErr := pub.Publish(ctx, broker.Message{}); !errors.Is(publishErr, cause) {
		t.Fatalf("publish cancellation cause: %v", publishErr)
	}
	info, err := js.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("invalid input was published: %+v, %v", info, err)
	}
}

func TestAmbiguousIdentityAndCorrelationNeverReachBusiness(t *testing.T) {
	for _, key := range []string{nats.MsgIdHdr, "Correlation-Id"} {
		t.Run(key, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			input := &nats.Msg{Subject: config.Subject, Data: []byte(`{}`), Header: nats.Header{key: []string{"one", "two"}}}
			if _, err := js.PublishMsg(input); err != nil {
				t.Fatal(err)
			}
			c, err := natsjs.NewConsumer(nc, config)
			if err != nil {
				t.Fatal(err)
			}
			h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error {
				t.Error("ambiguous metadata reached business")
				return nil
			}, broker.WithCorrelationIDBinding())
			if err = c.Run(context.Background(), h); err == nil {
				t.Fatal("ambiguous metadata accepted")
			}
			assertPending(t, js, config, 1)
		})
	}
}
