package natsjs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/natsjs/v3/examples/orders"
)

func TestTypedReplyIsStoredBeforeRequestAcknowledgment(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	replyStream := config.Stream + "_replies"
	replySubject := replyStream + ".created"
	replies := provisionReplyPublisher(t, nc, js, replyStream, replySubject)
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	var effects, replyCalls, ackStarts int
	observed := broker.WrapPublisher(replies, func(next broker.Publisher) broker.Publisher {
		return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
			replyCalls++
			if effects != 1 {
				t.Error("reply preceded business effect")
			}
			return next.Publish(ctx, message)
		})
	})
	h := orders.NewReplyHandler(func(_ context.Context, command orders.CreateOrderCommand) error {
		want := orders.CreateOrderCommand{ID: "order-1", SKU: "book-1", Quantity: 2}
		if command != want {
			t.Errorf("command mapping: %+v", command)
		}
		effects++
		return nil
	}, observed)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation != natsjs.OperationAck {
			return
		}
		if event.Outcome == natsjs.OutcomeStarted {
			ackStarts++
			assertStoredOrderReply(t, js, replyStream, replySubject)
		}
		if event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	coordinator := broker.NewCoordinator()
	if err := coordinator.Add("create-order", func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, config) }, h); err != nil {
		t.Fatal(err)
	}
	commands, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	publish := broker.NewTypedPublisher(broker.EncoderFunc(json.Marshal), commands,
		func(context.Context, orders.CreateOrderDTO) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: "command-1", Headers: []broker.Header{
				{Name: "Reply-To", Value: []byte("untrusted.destination")},
			}}, nil
		})
	input := orders.CreateOrderDTO{OrderID: "order-1", ProductCode: "book-1", Count: 2}
	for range 2 {
		if err = publish(ctx, input); err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("stable publication identity: %+v, %v", info, err)
	}
	if err = coordinator.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if effects != 1 || replyCalls != 1 || ackStarts != 1 {
		t.Fatalf("effects=%d replies=%d ACKs=%d", effects, replyCalls, ackStarts)
	}
	consumer, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || consumer.NumAckPending != 0 || consumer.AckFloor.Stream != 1 {
		t.Fatalf("request completion: %+v, %v", consumer, err)
	}
}

func TestFailedReplyPreservesCauseAndLeavesRequestPending(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	// A separate closed publisher connection leaves the receiving connection live.
	replyConn, err := nats.Connect(serverURL, nats.FlusherTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replyConn.Close)
	replies, err := natsjs.NewPublisher(replyConn, natsjs.PublisherConfig{Subject: "approved.replies"})
	if err != nil {
		t.Fatal(err)
	}
	replyConn.Close()
	var effects, terminalCalls int
	var observed error
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationProcess && event.Outcome == natsjs.OutcomeFailed {
			observed = event.Err
		}
		if event.Operation == natsjs.OperationAck || event.Operation == natsjs.OperationNak || event.Operation == natsjs.OperationTerm {
			terminalCalls++
		}
	}
	h := orders.NewReplyHandler(func(context.Context, orders.CreateOrderCommand) error {
		effects++
		return nil
	}, replies)
	_, err = js.PublishMsg(&nats.Msg{Subject: config.Subject, Header: nats.Header{nats.MsgIdHdr: []string{"command-1"}},
		Data: []byte(`{"order_id":"order-1","product_code":"book-1","count":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err = c.Run(ctx, h)
	for _, failure := range []error{err, observed} {
		var stage *broker.StageError
		if !errors.Is(failure, nats.ErrConnectionClosed) || !errors.As(failure, &stage) || stage.Stage != broker.StagePublish {
			t.Fatalf("reply diagnostic lost stage/cause: %v", failure)
		}
		if !strings.Contains(failure.Error(), "approved.replies") {
			t.Errorf("reply diagnostic lost affected destination: %v", failure)
		}
	}
	if effects != 1 || terminalCalls != 0 {
		t.Fatalf("effects=%d terminal calls=%d", effects, terminalCalls)
	}
	assertPending(t, js, config, 1)
}

func TestReplyWithoutRequestIdentityNeverRunsBusiness(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	replies := broker.PublisherFunc(func(context.Context, broker.Message) error {
		t.Error("missing request identity reached publication")
		return nil
	})
	h := orders.NewReplyHandler(func(context.Context, orders.CreateOrderCommand) error {
		t.Error("missing request identity reached business")
		return nil
	}, replies)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := receive(t, startConsumer(t, nc, js, config, ctx, h, `{"order_id":"order-1","product_code":"book-1","count":2}`))
	var stage *broker.StageError
	if !errors.As(err, &stage) || stage.Stage != broker.StageResolve || !strings.Contains(err.Error(), "Message.ID") {
		t.Fatalf("missing request identity accepted or field lost: %v", err)
	}
	assertPending(t, js, config, 1)
}

func provisionReplyPublisher(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext, stream, subject string,
) *natsjs.Publisher {
	t.Helper()
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: stream, Subjects: []string{subject}, Storage: nats.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := js.DeleteStream(stream); err != nil {
			t.Error(err)
		}
	})
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func assertStoredOrderReply(t *testing.T, js nats.JetStreamContext, stream, subject string) {
	t.Helper()
	reply, err := js.GetLastMsg(stream, subject)
	if err != nil {
		t.Errorf("ACK before stored reply: %v", err)
		return
	}
	if string(reply.Data) != `{"order_id":"order-1"}` || reply.Header.Get(nats.MsgIdHdr) != "command-1.created" ||
		reply.Header.Get("Correlation-Id") != "command-1" {
		t.Errorf("reply payload/identity/correlation: %+v", reply)
	}
}
