package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
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
	return runIteratorSession(ctx, js, stream)
}

func runIteratorSession(ctx context.Context, js nats.JetStreamContext, stream string) (result error) {
	const name = "iterator_reader"
	subject := stream + "." + name
	remote := &nats.ConsumerConfig{Durable: name, FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy, AckWait: time.Minute}
	if _, err := js.AddConsumer(stream, remote, nats.Context(ctx)); err != nil {
		return operationError(subject, "create_consumer", err)
	}
	subscription, err := js.PullSubscribe(subject, "", nats.Bind(stream, name))
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
	processor, err := natsjs.NewProcessor(handler, stream+"/"+name)
	if err != nil {
		return err
	}
	first, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		return operationError(subject, "next", err)
	}
	if processingErr := handleIterator(ctx, first[0], processor); processingErr != nil {
		return processingErr
	}
	info, err := js.ConsumerInfo(stream, name, nats.Context(ctx))
	if err != nil {
		return operationError(subject, "read_consumer", err)
	}
	if effects != 1 || !oneIteratorMessagePending(info) {
		return errors.New("one iterator step did not leave the next message pending")
	}
	second, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		return operationError(subject, "next", err)
	}
	if processingErr := handleIterator(ctx, second[0], processor); processingErr != nil {
		return processingErr
	}
	if effects != iteratorMessageCount {
		return errors.New("iterator did not process exactly two messages")
	}
	return checkIteratorEmpty(ctx, subscription, subject)
}

func checkIteratorEmpty(ctx context.Context, subscription *nats.Subscription, subject string) error {
	if _, err := subscription.Fetch(1, nats.MaxWait(iteratorEmptyWait)); !errors.Is(err, nats.ErrTimeout) {
		return operationError(subject, "empty_next", errors.Join(errors.New("expected native timeout from empty iterator"), err))
	}
	emptyCtx, stopEmpty := context.WithTimeout(ctx, iteratorEmptyWait)
	_, err := subscription.Fetch(1, nats.Context(emptyCtx))
	stopEmpty()
	if !errors.Is(err, context.DeadlineExceeded) {
		return operationError(subject, "empty_context", errors.Join(errors.New("expected deadline from empty iterator"), err))
	}
	canceled, stopCanceled := context.WithCancel(ctx)
	stopCanceled()
	if _, err = subscription.Fetch(1, nats.Context(canceled)); !errors.Is(err, context.Canceled) {
		return operationError(subject, "canceled_next", errors.Join(errors.New("expected cancellation from empty iterator"), err))
	}
	return nil
}

// The application owns this explicit-ack iterator. Processing and the chosen
// native acknowledgment stay separate, so a processing failure leaves it pending.
func handleIterator(ctx context.Context, native *nats.Msg, processor *natsjs.Processor) error {
	if err := processor.Process(ctx, native); err != nil {
		return err
	}
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
	defer cancel()
	return operationError(native.Subject, "ack", native.AckSync(nats.Context(ackCtx)))
}

type iteratorCommand struct {
	Value int `json:"value"`
}

func oneIteratorMessagePending(info *nats.ConsumerInfo) bool {
	return info.NumAckPending == 1 && info.NumPending == 0 || info.NumAckPending == 0 && info.NumPending == 1
}
