# Native publication results

Use `Publisher.PublishWithAck` when a bound publication needs the JetStream
storage acknowledgment. It preserves `Publish`'s identity checks and operation
budget. `Stream`, `Sequence` and `Duplicate` come from the native `PubAck`.
They describe storage acceptance, not downstream processing.

## Keep typed values and middleware

`NewTypedPublisherWithResult` performs metadata resolution and encoding, applies
ordinary publisher middleware and returns the operation's result to that call:

```go
package messaging

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

type Order struct {
	ID       string
	Quantity int
}

func OrdersWithAck(publisher *natsjs.Publisher, middleware ...broker.PublisherMiddleware) func(context.Context, Order) (*nats.PubAck, error) {
	return broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		publisher.PublishWithAck,
		func(_ context.Context, order Order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: order.ID}, nil
		}, middleware...)
}
```

Pass `otelbroker.TracePublishing` as ordinary middleware when tracing is needed.
Each call retains its own result. A result can accompany an error from middleware
that runs after publication, so an error does not imply that nothing was stored.

See [typed publication and middleware ownership](../../docs/publishing.md) for
the shared result contract, concurrency and skipped or repeated publications.

## Choose native options

Use `EncodeMessage` to obtain a validated, detached SDK message. The application
then owns its native operation, options, deadline and result:

```go
func PublishExpected(ctx context.Context, js nats.JetStreamContext, subject, stream string,
	message broker.Message) (*nats.PubAck, error) {
	request, err := natsjs.EncodeMessage(subject, message)
	if err != nil {
		return nil, err
	}
	ack, err := js.PublishMsg(request, nats.Context(ctx), nats.ExpectStream(stream))
	if err != nil {
		return ack, &natsjs.OperationError{Source: subject, Operation: "publish", Cause: err}
	}
	return ack, nil
}
```

This function can also be the publication callback passed to
`NewTypedPublisherWithResult`. The adapter owns mapping; the caller chooses the
SDK operation without copying header or identity conversion.

Native options follow SDK precedence: `MsgId` may replace the mapped logical ID,
and expectation options may replace corresponding headers. Supply each control
once and choose one source for identity. Mutating the returned native message
never changes the Broker message. Combining native `Context` and nonzero
`AckWait` returns `nats.ErrContextAndTimeout` before sending. Provide an explicit
operation budget through the context or native timeout.

The bound `PublishWithAck` path accepts no native options. It uses the caller's
context capped by `PublisherConfig.OperationTimeout` and preserves logical ID
consistency. Native expectation headers remain available on that path.

A server rejection retains the native cause through `errors.Is` and `errors.As`.
Use `broker.DescribeError` with `natsjs.ErrorFields` for safe native codes and
operation details. A timeout or cancellation can occur after storage. Keep IDs
stable when the application chooses to retry an uncertain publication.

## Retain asynchronous results

Use a dedicated application-owned JetStream context for a publication session.
Set its `PublishAsyncMaxPending` and `PublishAsyncTimeout`, and choose a bounded
`StallWait` for enqueue backpressure. Convert each Broker message with
`EncodeMessage`, then call `PublishMsgAsync`. Its `PubAckFuture` can be the result
of `NewTypedPublisherWithResult` just like `*nats.PubAck`.

An async callback checks its supplied context before enqueueing. Native async
publication does not accept `Context` or `AckWait` options. Cancellation while
waiting for a future stops that wait; it does not retract publication or resolve
the future. The native message belongs to that future until completion and
should remain read-only. Encoding detaches the original Broker data.

Inspect every future's `Ok()` or `Err()` channel. `PublishAsyncComplete` and an
empty pending count include failures. They do not establish that every message
was stored. A timeout can also occur after storage. An OTel publication span
around this callback covers enqueueing, while future acknowledgment remains a
separate operation.

Stop and join callers that enqueue before cleaning up their session. Call
`CleanupPublisher` only on the JetStream context owned by that session, then
inspect outstanding futures. Cleanup reports `ErrJetStreamPublisherClosed` for
pending futures and releases its reply subscription. It does not close the
borrowed connection. Keep unrelated publishers in separate JetStream contexts.
The [async example](../cmd/native/publishing.go) demonstrates success, rejection,
backpressure, native timeout and pending-future cleanup through this mapping.
