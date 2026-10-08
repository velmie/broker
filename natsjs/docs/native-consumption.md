# Native processing and ownership

Choose who owns delivery operations before starting a consumer. Native metadata
access does not transfer acknowledgment authority.

| API | Receive resource | Renewal and settlement | Connection and topology |
| --- | --- | --- | --- |
| `NewConsumer` / `NewPullConsumer` | Acquired and closed by `Run` | Broker | Application |
| `NewPushConsumer` | Bound sync or queue subscription acquired and closed by `Run` | Broker | Application |
| `NewProcessor` | Application's native subscription or iterator | Application | Application |

## Use Broker's runtime with native push configuration

Provision a named push consumer through the SDK. Set `DeliverSubject`,
`AckExplicitPolicy` and one `FilterSubject`. Set `DeliverGroup` for competing
queue members. Bind its stream, name and filter with `NewPushConsumer`.

`Run` reads and validates the existing configuration, then uses its delivery
subject, queue group, flow control and heartbeat settings. It never creates or
deletes the native consumer. Callbacks are sequential for each run. Normal
Broker dispositions, requirements, observers and shutdown policies apply.
Cancel the run's context and wait for `Run` before closing the borrowed connection.

The [push example](../cmd/native/push.go) demonstrates both push modes. The
[adapter guide](../README.md#delivery-decisions) defines settlement guarantees.

## Keep native receive and settlement in the application

Use `NewProcessor(handler, source)` when the application already owns a native
loop or needs to select native operations itself. The processor detaches message
data and metadata, invokes ordinary handlers and middleware, and requires
`Handled` on success. It performs no ACK, NAK, TERM, renewal or resource cleanup.

For an explicit-ack pull subscription, a single application-owned step can be:

```go
package messaging

import (
	"context"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v3"
)

func Next(ctx context.Context, subscription *nats.Subscription, processor *natsjs.Processor) error {
	// Fetch controls when this caller requests the next delivery.
	messages, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		return err
	}
	if err := processor.Process(ctx, messages[0]); err != nil {
		return err
	}
	// Only this caller owns the native acknowledgment.
	return messages[0].AckSync(nats.Context(ctx))
}
```

Create the processor once with a stable source, normally `stream/consumer`.
Keep the native message read-only during `Process`. The application supplies
context deadlines, owns the subscription and joins its receive work before
closing it. A failed ACK can leave the outcome unknown even after successful
processing. Do not infer safe retry from cancellation or timeout.

The constructor accepts `AttemptsRequirement`. It rejects redelivery,
keepalive, termination and unknown requirements before any callback, because it
does not manage those operations. Use a consumer when Broker should implement
these requirements. `Process` also rejects unsupported dispositions instead of
silently turning them into acknowledgment.

The [iterator example](../cmd/native/iterator.go) creates a real pull consumer,
processes each selected message and performs its native ACK. Message mapping
and requirement validation stay in the library.
