package natsjs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestPublisherNativeAcknowledgment(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	message := broker.Message{ID: "event", Body: []byte("body")}
	first, err := publisher.PublishWithAck(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := publisher.PublishWithAck(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if first.Stream != config.Stream || first.Sequence != 1 || first.Duplicate || duplicate.Stream != first.Stream ||
		duplicate.Sequence != first.Sequence || !duplicate.Duplicate {
		t.Fatalf("acks first=%+v duplicate=%+v", first, duplicate)
	}
	message.ID = "rejected"
	message.Headers = []broker.Header{{Name: nats.ExpectedStreamHdr, Value: []byte("another-stream")}}
	rejected, err := publisher.PublishWithAck(context.Background(), message)
	var api *nats.APIError
	var operation *natsjs.OperationError
	if rejected != nil || !errors.As(err, &api) || !errors.As(err, &operation) ||
		operation.Operation != "publish" || operation.Source != config.Subject {
		t.Fatalf("native rejection ack=%+v err=%v", rejected, err)
	}
	info, err := js.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("rejected publication stored: %+v %v", info, err)
	}
}

func TestPublisherConcurrentAcknowledgmentsStayPerCall(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		body string
		ack  *nats.PubAck
		err  error
	}
	const calls = 8
	results := make(chan result, calls)
	for index := range calls {
		go func() {
			body := fmt.Sprintf("event-%d", index)
			ack, publishErr := publisher.PublishWithAck(context.Background(), broker.Message{ID: body, Body: []byte(body)})
			results <- result{body, ack, publishErr}
		}()
	}
	sequences := make(map[uint64]bool)
	for range calls {
		item := receive(t, results)
		if item.err != nil {
			t.Fatal(item.err)
		}
		if item.ack.Stream != config.Stream || sequences[item.ack.Sequence] {
			t.Fatalf("ACK shared between calls: %+v", item.ack)
		}
		sequences[item.ack.Sequence] = true
		stored, getErr := js.GetMsg(config.Stream, item.ack.Sequence)
		if getErr != nil || string(stored.Data) != item.body {
			t.Fatalf("per-call association: %+v %v", stored, getErr)
		}
	}
}

func TestEncodeMessageNativeOptionsAndDetachedOwnership(t *testing.T) {
	_, js, config := provision(t, time.Second)
	input := broker.Message{ID: "original", Body: []byte("body"), Headers: []broker.Header{{Name: "X-Tag", Value: []byte("value")}}}
	message, err := natsjs.EncodeMessage(config.Subject, input)
	if err != nil {
		t.Fatal(err)
	}
	input.Body[0] = 'B'
	input.Headers[0].Value[0] = 'V'
	if string(message.Data) != "body" || message.Header.Get("X-Tag") != "value" {
		t.Fatal("native message aliases caller storage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ack, err := js.PublishMsg(message, nats.Context(ctx), nats.ExpectStream(config.Stream),
		nats.ExpectLastSequence(0), nats.MsgId("native-id"))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Stream != config.Stream || ack.Sequence != 1 || message.Header.Get(nats.MsgIdHdr) != "native-id" {
		t.Fatalf("native controls: %+v", ack)
	}
	if input.ID != "original" || len(input.Headers) != 1 || input.Headers[0].Name != "X-Tag" || string(input.Headers[0].Value) != "Value" {
		t.Fatalf("SDK options mutated caller input: %+v", input)
	}
	message.Data[0] = 'x'
	if string(input.Body) != "Body" {
		t.Fatal("caller body aliases native storage")
	}
	for _, option := range []nats.PubOpt{nats.ExpectStream("wrong-stream"), nats.ExpectLastSequence(99)} {
		native, encodeErr := natsjs.EncodeMessage(config.Subject, broker.Message{Body: []byte("rejected")})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		_, publishErr := js.PublishMsg(native, nats.Context(ctx), option)
		var api *nats.APIError
		if !errors.As(publishErr, &api) {
			t.Fatalf("native option rejection cause: %v", publishErr)
		}
	}
	conflict, err := natsjs.EncodeMessage(config.Subject, broker.Message{Body: []byte("conflict")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.PublishMsg(conflict, nats.Context(ctx), nats.AckWait(time.Second)); !errors.Is(err, nats.ErrContextAndTimeout) {
		t.Fatalf("native context/timeout conflict: %v", err)
	}
	info, err := js.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("rejected controls stored: %+v %v", info, err)
	}
}

func TestPublisherTimeoutCanFollowSuccessfulStorage(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	info, err := js.StreamInfo(config.Stream)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.NoAck = true
	if _, err = js.UpdateStream(&info.Config); err != nil {
		t.Fatal(err)
	}
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject, OperationTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := publisher.PublishWithAck(context.Background(), broker.Message{ID: "stored-without-ack", Body: []byte("event")})
	if ack != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("uncertain ACK=%+v err=%v", ack, err)
	}
	info, err = js.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("timed-out publication storage: %+v %v", info, err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeMessageRetainsValidationCause(t *testing.T) {
	for _, test := range []struct {
		name, subject, field string
		message              broker.Message
	}{
		{name: "subject", subject: "orders.*", field: "Subject"},
		{name: "header", subject: "orders", field: "Headers.Value", message: broker.Message{
			Headers: []broker.Header{{Name: "X-Token", Value: []byte("private\r\nvalue")}},
		}},
		{name: "identity", subject: "orders", field: "Nats-Msg-Id", message: broker.Message{
			ID: "one", Headers: []broker.Header{{Name: nats.MsgIdHdr, Value: []byte("two")}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			message, err := natsjs.EncodeMessage(test.subject, test.message)
			var field *broker.FieldError
			var operation *natsjs.OperationError
			if message != nil || !errors.As(err, &field) || field.Field != test.field || !errors.Is(err, field.Cause) {
				t.Fatalf("validation cause: %v", err)
			}
			if !errors.As(err, &operation) || operation.Operation != "encode" {
				t.Fatalf("encode operation: %v", err)
			}
			encoded, marshalErr := json.Marshal(broker.DescribeError(err, natsjs.ErrorFields))
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			for _, want := range []string{
				`"field":"` + test.field + `"`, `"operation":"encode"`, `"source":"` + test.subject + `"`,
			} {
				if !strings.Contains(string(encoded), want) {
					t.Fatalf("missing diagnostic %s: %s", want, encoded)
				}
			}
			if strings.Contains(string(encoded), "private") {
				t.Fatalf("rejected header leaked into diagnostics: %s", encoded)
			}
		})
	}
}
