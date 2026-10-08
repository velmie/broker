package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
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
	if sessionErr := runPushSession(ctx, connection, js, stream, false); sessionErr != nil {
		return sessionErr
	}
	return runPushSession(ctx, connection, js, stream, true)
}

func runPushSession(ctx context.Context, connection *nats.Conn, js nats.JetStreamContext, stream string, queue bool) (result error) {
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
	first, err := startPush(ctx, connection, stream, remote, pushFirstWorker, progress)
	if err != nil {
		return err
	}
	runs := []*pushRun{first}
	defer func() {
		for _, owned := range runs {
			result = errors.Join(result, owned.stop())
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
		if stopErr := first.stop(); stopErr != nil {
			return stopErr
		}
		second, subscribeErr := startPush(ctx, connection, stream, remote, pushSecondWorker, progress)
		if subscribeErr != nil {
			return subscribeErr
		}
		runs = []*pushRun{second}
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

func startPush(ctx context.Context, connection *nats.Conn, stream string, remote *nats.ConsumerConfig,
	worker string, progress *pushProgress) (*pushRun, error) {
	handler := broker.NewTypedHandler(broker.DecodeJSON[pushCommand], func(_ context.Context, command pushCommand) error {
		progress.mu.Lock()
		defer progress.mu.Unlock()
		if _, duplicate := progress.effects[command.ID]; duplicate {
			return errors.New("push effect received a duplicate logical ID")
		}
		progress.effects[command.ID] = worker
		return nil
	})
	consumer, err := natsjs.NewPushConsumer(connection, natsjs.ConsumerConfig{
		Stream: stream, Consumer: remote.Durable, Subject: remote.FilterSubject,
		Observer: natsjs.Observer{Record: func(_ context.Context, event natsjs.Observation) {
			if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
				progress.mu.Lock()
				progress.completed++
				progress.mu.Unlock()
				progress.notify()
			}
		}},
	})
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	owned := &pushRun{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(owned.done)
		owned.err = consumer.Run(runCtx, handler)
		if owned.err != nil && !onlyCancellation(owned.err) {
			progress.mu.Lock()
			progress.failures = append(progress.failures, owned.err)
			progress.mu.Unlock()
			progress.notify()
		}
	}()
	return owned, nil
}

func (p *pushProgress) notify() {
	select {
	case p.signal <- struct{}{}:
	default:
	}
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

type pushCommand struct {
	ID string `json:"id"`
}

// The application cancels and joins Run; the consumer owns receive and settlement.
type pushRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (s *pushRun) stop() error {
	s.cancel()
	<-s.done
	if onlyCancellation(s.err) {
		return nil
	}
	return s.err
}

// pushProgress belongs to this fixed two-wave demonstration. It records business
// effects separately from checked-ACK completion and never blocks the runtime observer.
type pushProgress struct {
	mu        sync.Mutex
	effects   map[string]string
	failures  []error
	completed int
	signal    chan struct{}
}
