package main

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	asyncMaxPending    = 2
	asyncRetryAttempts = 2
	asyncStallBudget   = 20 * time.Millisecond
	asyncRetryWait     = 30 * time.Millisecond
	asyncAckTimeout    = 50 * time.Millisecond
)

func runAsyncPublish(ctx context.Context, conn *nats.Conn) (result error) {
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(asyncMaxPending), nats.PublishAsyncTimeout(operationTimeout))
	if err != nil {
		return operationError("async-publish", "JetStream", err)
	}
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	defer js.CleanupPublisher()
	if publishErr := publishExpectations(ctx, js, stream); publishErr != nil {
		return publishErr
	}
	if retryErr := publishNoResponderRetry(ctx, js); retryErr != nil {
		return retryErr
	}
	return publishPendingLifecycle(ctx, conn)
}

func publishExpectations(ctx context.Context, js nats.JetStreamContext, stream string) error {
	subject := stream + ".events"
	first, err := js.PublishAsync(subject, []byte("first"), nats.MsgId("first"), nats.ExpectStream(stream), nats.ExpectLastSequence(0))
	if err != nil {
		return operationError(stream, "publish-first", err)
	}
	ack, err := awaitPublish(ctx, first)
	if err != nil {
		return operationError(stream, "ack-first", err)
	}
	if ack.Stream != stream || ack.Sequence != 1 {
		return operationError(stream, "ack-first", errors.New("unexpected stream or sequence"))
	}
	second,
		err := js.PublishAsync(subject,
		[]byte("second"),
		nats.ExpectLastMsgId("first"),
		nats.ExpectLastSequence(1),
		nats.ExpectLastSequencePerSubject(1))
	if err != nil {
		return operationError(stream, "publish-next", err)
	}
	if _, err = awaitPublish(ctx, second); err != nil {
		return operationError(stream, "ack-next", err)
	}
	rejected, err := js.PublishAsync(subject, []byte("rejected"), nats.ExpectStream("missing-stream"))
	if err != nil {
		return operationError(stream, "publish-expectation", err)
	}
	_, err = awaitPublish(ctx, rejected)
	var api *nats.APIError
	if !errors.As(err, &api) || api.ErrorCode != 10060 {
		return operationError(stream, "expectation-rejection", errors.Join(errors.New("expected stream mismatch"), err))
	}
	// Completion includes rejected futures, so every future was inspected first.
	select {
	case <-js.PublishAsyncComplete():
		return nil
	case <-ctx.Done():
		return operationError(stream, "complete", context.Cause(ctx))
	}
}

func publishNoResponderRetry(ctx context.Context, js nats.JetStreamContext) error {
	started := time.Now()
	future, err := js.PublishAsync(nats.NewInbox(), []byte("no-stream"), nats.RetryAttempts(asyncRetryAttempts),
		nats.RetryWait(asyncRetryWait))
	if err != nil {
		return operationError("async-publish", "retry-publish", err)
	}
	_, err = awaitPublish(ctx, future)
	if !errors.Is(err, nats.ErrNoResponders) {
		return operationError("async-publish", "retry-result", errors.Join(errors.New("expected no-stream response"), err))
	}
	if time.Since(started) < asyncRetryWait {
		return operationError("async-publish", "retry-wait", errors.New("native retry delay not observed"))
	}
	return nil
}

func publishPendingLifecycle(ctx context.Context, conn *nats.Conn) (result error) {
	subject := nats.NewInbox()
	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		return operationError("async-publish", "hold-subscribe", err)
	}
	defer func() {
		result = errors.Join(result, operationError("async-publish", "hold-unsubscribe", sub.Unsubscribe()))
	}()
	if err = conn.FlushWithContext(ctx); err != nil {
		return operationError("async-publish", "hold-flush", err)
	}
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(1))
	if err != nil {
		return operationError("async-publish", "pending-JetStream", err)
	}
	defer js.CleanupPublisher()
	pending, err := js.PublishAsync(subject, []byte("pending"))
	if err != nil {
		return operationError("async-publish", "pending-publish", err)
	}
	if _, err = sub.NextMsgWithContext(ctx); err != nil {
		return operationError("async-publish", "pending-receive", err)
	}
	_, err = js.PublishAsync(subject, []byte("stalled"), nats.StallWait(asyncStallBudget))
	if !errors.Is(err, nats.ErrTooManyStalledMsgs) {
		return operationError("async-publish", "stall", errors.Join(errors.New("expected pending backpressure"), err))
	}
	js.CleanupPublisher()
	_, err = awaitPublish(ctx, pending)
	if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
		return operationError("async-publish", "pending-cleanup", errors.Join(errors.New("expected publisher closed"), err))
	}
	if js.PublishAsyncPending() != 0 {
		return operationError("async-publish", "pending-count", errors.New("pending futures remain after cleanup"))
	}
	return publishNativeTimeout(ctx, conn, subject)
}

func publishNativeTimeout(ctx context.Context, conn *nats.Conn, subject string) error {
	js, err := conn.JetStream(nats.PublishAsyncTimeout(asyncAckTimeout))
	if err != nil {
		return operationError("async-publish", "timeout-JetStream", err)
	}
	defer js.CleanupPublisher()
	future, err := js.PublishAsync(subject, []byte("timeout"))
	if err != nil {
		return operationError("async-publish", "timeout-publish", err)
	}
	_, err = awaitPublish(ctx, future)
	if !errors.Is(err, nats.ErrAsyncPublishTimeout) {
		return operationError("async-publish", "timeout-result", errors.Join(errors.New("expected async acknowledgement timeout"), err))
	}
	return nil
}

func awaitPublish(ctx context.Context, future nats.PubAckFuture) (*nats.PubAck, error) {
	select {
	case ack := <-future.Ok():
		return ack, nil
	case err := <-future.Err():
		return nil, err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
