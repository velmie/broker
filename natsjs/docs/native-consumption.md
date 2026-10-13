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

## Select a native delivery policy

The same processor accepts messages from native push, pull and ordered
subscriptions. Configure those subscriptions through the SDK before receiving.
Their policies determine which operations the caller can perform:

| Native policy | Caller responsibility |
| --- | --- |
| AckExplicit | Confirm each successful effect or choose a native redelivery operation |
| AckNone or ordered | Handle processing failures without assuming acknowledgment-driven redelivery |
| AckAll | Confirm only a contiguous sequence of successful effects |
| Multiple filters | Check creation succeeds and preserve the native filters when binding |
| BackOff | Distinguish timeout redelivery from an explicit delayed NAK |

For AckAll, `ProcessBatch` executes handlers in order and stops at the first
error. It performs no settlement. A caller processing a contiguous batch may
confirm its last message only after the batch and all preceding deliveries in
that consumer have succeeded:

```go
if err := processor.ProcessBatch(ctx, messages); err != nil {
	return err // Stop this receive/settlement session without a cumulative ACK.
}
if len(messages) > 0 {
	return messages[len(messages)-1].AckSync(nats.Context(ctx))
}
return nil
```

A later cumulative ACK can also confirm an earlier failed delivery. Do not
continue this session after a failed batch or use this pattern with gaps in
processing. Previously completed business effects are not undone.

Multiple filters require server support. If the SDK returns an unsupported
configuration error, do not start receiving from a partially created consumer.
Bind successful native configuration without replacing it with a single filter.
The [policy tests](../processor_policy_integration_test.go) exercise both outcomes
against the server versions selected by CI.

BackOff applies to an unconfirmed delivery's timeout. `NakWithDelay` sets the
explicit NAK delay independently. The processor neither renews nor settles the
message. Long callbacks therefore require a caller-owned renewal policy if the
selected native mode supports it. Broker-managed consumers retain their own
validation and settlement guarantees.

The [policy examples](../cmd/native/policies.go) combine native configuration
with `Processor`, including AckNone, AckAll, ordered delivery and BackOff.
