package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

const (
	pushFirstWorker  = "first"
	pushWaveCount    = 2
	pushSecondWorker = "second"
)

func runPush(ctx context.Context, connection *nats.Conn) (result error) {
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		return err
	}
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	if sessionErr := runPushSession(ctx, js, stream, false); sessionErr != nil {
		return sessionErr
	}
	return runPushSession(ctx, js, stream, true)
}

func runPushSession(ctx context.Context, js nats.JetStreamContext, stream string, queue bool) (result error) {
	name, group := "push_worker", ""
	if queue {
		name, group = "queue_push", "workers"
	}
	subject := stream + "." + name
	firstID, secondID := name+"-first-wave", name+"-second-wave"
	remote := &nats.ConsumerConfig{Durable: name, DeliverSubject: nats.NewInbox(), DeliverGroup: group, FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy, AckWait: time.Minute}
	if _, err := js.AddConsumer(stream, remote, nats.Context(ctx)); err != nil {
		return operationError(subject, "create_consumer", err)
	}
	progress := &pushProgress{effects: make(map[string]string), signal: make(chan struct{}, 1)}
	first, err := subscribePush(ctx, js, stream, remote, pushFirstWorker, progress)
	if err != nil {
		return err
	}
	subscriptions := []pushSubscription{first}
	defer func() {
		for _, owned := range subscriptions {
			result = errors.Join(result, drainPush(owned.subscription, owned.closed))
		}
		progress.mu.Lock()
		result = errors.Join(result, errors.Join(progress.failures...))
		progress.mu.Unlock()
	}()
	if publishErr := publishPush(ctx, js, subject, firstID); publishErr != nil {
		return publishErr
	}
	if waitErr := progress.wait(ctx, 1); waitErr != nil {
		return waitErr
	}
	if queue {
		// Two members share the durable. Explicit handoff establishes participation
		// without requiring a fairness guarantee from the native queue group.
		second, subscribeErr := subscribePush(ctx, js, stream, remote, pushSecondWorker, progress)
		if subscribeErr != nil {
			return subscribeErr
		}
		subscriptions = []pushSubscription{second}
		if drainErr := drainPush(first.subscription, first.closed); drainErr != nil {
			return drainErr
		}
	}
	if publishErr := publishPush(ctx, js, subject, secondID); publishErr != nil {
		return publishErr
	}
	if waitErr := progress.wait(ctx, pushWaveCount); waitErr != nil {
		return waitErr
	}
	progress.mu.Lock()
	defer progress.mu.Unlock()
	expectedSecond := pushFirstWorker
	if queue {
		expectedSecond = pushSecondWorker
	}
	if len(progress.effects) != 2 || progress.effects[firstID] != pushFirstWorker || progress.effects[secondID] != expectedSecond {
		return errors.New("push effects did not cover the two disjoint waves")
	}
	return nil
}

func subscribePush(ctx context.Context, js nats.JetStreamContext, stream string, remote *nats.ConsumerConfig,
	worker string, progress *pushProgress) (pushSubscription, error) {
	handler := broker.NewTypedHandler(broker.DecodeJSON[pushCommand], func(_ context.Context, command pushCommand) error {
		progress.mu.Lock()
		defer progress.mu.Unlock()
		if _, duplicate := progress.effects[command.ID]; duplicate {
			return errors.New("push effect received a duplicate logical ID")
		}
		progress.effects[command.ID] = worker
		return nil
	})
	callback := func(message *nats.Msg) {
		callbackErr := handlePush(ctx, message, handler)
		progress.mu.Lock()
		progress.completed++
		if callbackErr != nil {
			progress.failures = append(progress.failures, callbackErr)
		}
		progress.mu.Unlock()
		select {
		case progress.signal <- struct{}{}:
		default:
		}
	}
	var subscription *nats.Subscription
	var err error
	if remote.DeliverGroup != "" {
		subscription, err = js.QueueSubscribe(remote.FilterSubject, remote.DeliverGroup, callback,
			nats.Bind(stream, remote.Durable), nats.ManualAck())
	} else {
		subscription, err = js.Subscribe(remote.FilterSubject, callback, nats.Bind(stream, remote.Durable), nats.ManualAck())
	}
	if err != nil {
		return pushSubscription{}, operationError(remote.FilterSubject, "subscribe", err)
	}
	closed := make(chan struct{})
	subscription.SetClosedHandler(func(string) { close(closed) })
	return pushSubscription{subscription: subscription, closed: closed}, nil
}

func publishPush(ctx context.Context, js nats.JetStreamContext, subject, id string) error {
	body, err := json.Marshal(pushCommand{ID: id})
	if err != nil {
		return err
	}
	_, err = js.PublishMsg(&nats.Msg{Subject: subject, Data: body, Header: nats.Header{nats.MsgIdHdr: []string{id}}}, nats.Context(ctx))
	return operationError(subject, "publish", err)
}

func (p *pushProgress) wait(ctx context.Context, count int) error {
	for {
		p.mu.Lock()
		seen := p.completed
		callbackErrors := errors.Join(p.failures...)
		p.failures = nil
		p.mu.Unlock()
		if callbackErrors != nil {
			return callbackErrors
		}
		if seen >= count {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-p.signal:
		}
	}
}

func handlePush(ctx context.Context, native *nats.Msg, handler broker.Handler) error {
	if err := context.Cause(ctx); err != nil {
		return operationError(native.Subject, "process", err)
	}
	data := broker.Message{ID: native.Header.Get(nats.MsgIdHdr), Body: bytes.Clone(native.Data)}
	for name, values := range native.Header {
		for _, value := range values {
			data.Headers = append(data.Headers, broker.Header{Name: name, Value: []byte(value)})
		}
	}
	disposition, err := handler.Handle(ctx, &pushDelivery{message: data, source: native.Subject})
	if err != nil {
		return operationError(native.Subject, "process", err)
	}
	if _, ok := disposition.(broker.Handled); !ok {
		return operationError(native.Subject, "disposition", fmt.Errorf("push requires Handled, got %T", disposition))
	}
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
	defer cancel()
	if err = native.AckSync(nats.Context(ackCtx)); err != nil {
		return operationError(native.Subject, "ack", err)
	}
	return nil
}

// Drain starts SDK shutdown asynchronously. The closed hook was installed before
// publication and stop, so its completion joins every admitted callback.
func drainPush(subscription *nats.Subscription, closed <-chan struct{}) error {
	err := subscription.Drain()
	if err != nil && subscription.IsValid() {
		err = errors.Join(err, subscription.Unsubscribe())
	}
	<-closed
	if err != nil {
		return operationError(subscription.Subject, "drain", err)
	}
	return nil
}

type pushCommand struct {
	ID string `json:"id"`
}
type pushSubscription struct {
	subscription *nats.Subscription
	closed       chan struct{}
}
type pushDelivery struct {
	message broker.Message
	source  string
}

func (d *pushDelivery) Message() broker.Message { return d.message }
func (d *pushDelivery) Source() string          { return d.source }

// pushProgress belongs to this fixed two-wave demonstration. It records business
// effects separately from checked-ACK completion and never blocks an SDK callback.
type pushProgress struct {
	mu        sync.Mutex
	effects   map[string]string
	failures  []error
	completed int
	signal    chan struct{}
}
