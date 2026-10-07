package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

const (
	iteratorEmptyWait    = 30 * time.Millisecond
	iteratorMessageCount = 2
)

func runIterator(ctx context.Context, connection *nats.Conn) (result error) {
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		return err
	}
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	if sessionErr := runIteratorSession(ctx, js, stream, false); sessionErr != nil {
		return sessionErr
	}
	return runIteratorSession(ctx, js, stream, true)
}

func runIteratorSession(ctx context.Context, js nats.JetStreamContext, stream string, queue bool) (result error) {
	name, group := "iterator_reader", ""
	if queue {
		name, group = "queue_iterator", "readers"
	}
	subject := stream + "." + name
	remote := &nats.ConsumerConfig{Durable: name, DeliverSubject: nats.NewInbox(), DeliverGroup: group, FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy, AckWait: time.Minute}
	if _, err := js.AddConsumer(stream, remote, nats.Context(ctx)); err != nil {
		return operationError(subject, "create_consumer", err)
	}
	var subscription *nats.Subscription
	var err error
	if queue {
		subscription, err = js.QueueSubscribeSync(subject, group, nats.Bind(stream, name), nats.ManualAck())
	} else {
		subscription, err = js.SubscribeSync(subject, nats.Bind(stream, name), nats.ManualAck())
	}
	if err != nil {
		return operationError(subject, "subscribe", err)
	}
	defer func() {
		if closeErr := subscription.Unsubscribe(); closeErr != nil {
			result = errors.Join(result, operationError(subject, "unsubscribe", closeErr))
		}
	}()
	for value := 1; value <= iteratorMessageCount; value++ {
		body, encodeErr := json.Marshal(iteratorCommand{Value: value})
		if encodeErr != nil {
			return operationError(subject, "encode", encodeErr)
		}
		if _, err = js.Publish(subject, body, nats.Context(ctx)); err != nil {
			return operationError(subject, "publish", err)
		}
	}
	effects := 0
	handler := broker.NewTypedHandler(broker.DecodeJSON[iteratorCommand], func(_ context.Context, command iteratorCommand) error {
		effects++
		if command.Value != effects {
			return errors.New("iterator callback order changed")
		}
		return nil
	})
	// These calls own reception synchronously. There is no caller-created receive
	// goroutine or background callback, and only the selected message is settled.
	first, err := subscription.NextMsg(operationTimeout)
	if err != nil {
		return operationError(subject, "next", err)
	}
	if processingErr := handleIterator(ctx, first, handler); processingErr != nil {
		return processingErr
	}
	info, err := js.ConsumerInfo(stream, name, nats.Context(ctx))
	if err != nil {
		return operationError(subject, "read_consumer", err)
	}
	if effects != 1 || !oneIteratorMessagePending(info) {
		return errors.New("one iterator step did not leave the next message pending")
	}
	second, err := subscription.NextMsgWithContext(ctx)
	if err != nil {
		return operationError(subject, "next", err)
	}
	if processingErr := handleIterator(ctx, second, handler); processingErr != nil {
		return processingErr
	}
	if effects != iteratorMessageCount {
		return errors.New("iterator did not process exactly two messages")
	}
	return checkIteratorEmpty(ctx, subscription, subject)
}

func checkIteratorEmpty(ctx context.Context, subscription *nats.Subscription, subject string) error {
	if _, err := subscription.NextMsg(iteratorEmptyWait); !errors.Is(err, nats.ErrTimeout) {
		return operationError(subject, "empty_next", errors.Join(errors.New("expected native timeout from empty iterator"), err))
	}
	emptyCtx, stopEmpty := context.WithTimeout(ctx, iteratorEmptyWait)
	_, err := subscription.NextMsgWithContext(emptyCtx)
	stopEmpty()
	if !errors.Is(err, context.DeadlineExceeded) {
		return operationError(subject, "empty_context", errors.Join(errors.New("expected deadline from empty iterator"), err))
	}
	canceled, stopCanceled := context.WithCancel(ctx)
	stopCanceled()
	if _, err = subscription.NextMsgWithContext(canceled); !errors.Is(err, context.Canceled) {
		return operationError(subject, "canceled_next", errors.Join(errors.New("expected cancellation from empty iterator"), err))
	}
	return nil
}

func handleIterator(ctx context.Context, native *nats.Msg, handler broker.Handler) error {
	if err := context.Cause(ctx); err != nil {
		return operationError(native.Subject, "process", err)
	}
	data := broker.Message{ID: native.Header.Get(nats.MsgIdHdr), Body: bytes.Clone(native.Data)}
	for name, values := range native.Header {
		for _, value := range values {
			data.Headers = append(data.Headers, broker.Header{Name: name, Value: []byte(value)})
		}
	}
	disposition, err := handler.Handle(ctx, &iteratorDelivery{message: data, source: native.Subject})
	if err != nil {
		return operationError(native.Subject, "process", err)
	}
	if _, ok := disposition.(broker.Handled); !ok {
		return operationError(native.Subject, "disposition", fmt.Errorf("iterator requires Handled, got %T", disposition))
	}
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
	defer cancel()
	if err = native.AckSync(nats.Context(ackCtx)); err != nil {
		return operationError(native.Subject, "ack", err)
	}
	return nil
}

type iteratorCommand struct {
	Value int `json:"value"`
}
type iteratorDelivery struct {
	message broker.Message
	source  string
}

func (d *iteratorDelivery) Message() broker.Message { return d.message }
func (d *iteratorDelivery) Source() string          { return d.source }

func oneIteratorMessagePending(info *nats.ConsumerInfo) bool {
	return info.NumAckPending == 1 && info.NumPending == 0 || info.NumAckPending == 0 && info.NumPending == 1
}
