# Azure Service Bus adapter

`github.com/velmie/broker/azuresb` connects Broker to application-configured Azure
SDK senders and receivers. It publishes to queues or topics and consumes queues
or subscriptions sequentially. Dependencies are listed in [go.mod](go.mod).

The application owns credentials, endpoints, SDK retry policy and topology.
The adapter does not provision entities or discover their settings.

## Bind native resources

Create the SDK client in application wiring. Bind a sender with
`client.NewSender(entityName, nil)`, then pass it to
`azuresb.NewPublisher(sender, config)`. The publisher borrows the sender and
supports concurrent calls when that sender does. `Publish` keeps caller storage
read-only and waits for native send acceptance. A failed request can leave
acceptance unknown.

A consumer receives a factory that creates a fresh, exclusive receiver per run.
Keep the SDK mode and adapter declaration consistent:

```go
package messaging

import (
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	"github.com/velmie/broker/azuresb"
)

func Orders(client *azservicebus.Client, queue string) broker.ConsumerFactory {
	mode := azservicebus.ReceiveModePeekLock
	newReceiver := func() (azuresb.Receiver, error) {
		return client.NewReceiverForQueue(queue, &azservicebus.ReceiverOptions{
			ReceiveMode: mode,
		})
	}
	return func() (broker.Consumer, error) {
		return azuresb.NewConsumer(newReceiver, azuresb.ConsumerConfig{
			Source: "orders", ReceiveMode: mode,
		})
	}
}
```

Register the factory with a [Coordinator](../docs/coordinator.md). Construction
and validation acquire no receiver. Successful acquisition transfers cleanup to
`Run`; a failing factory must clean up its own partial acquisition. Repeated runs
and recovery require a fresh receiver.

For a subscription, call `NewReceiverForSubscription(topic, subscription, options)`
in the receiver factory. The SDK does not expose its mode for validation. A
ReceiveAndDelete receiver declared as PeekLock could remove messages before
processing, so derive both settings from the same value.

`Run` closes its receiver once after callbacks and native operations join.
The caller then closes borrowed senders and the SDK client, preserving cleanup
errors. Explicitly close owned links: the SDK client's close operation does not
report every underlying link failure. The [orders command](cmd/orders/main.go)
shows the ownership and cleanup order.

## Delivery decisions

| Mode and result | Native action |
| --- | --- |
| PeekLock, `broker.Handled{}` | `CompleteMessage` after successful processing |
| PeekLock, declared `broker.RetryAfter{Delay: 0}` | Immediate `AbandonMessage` |
| ReceiveAndDelete, `broker.Handled{}` | No later settlement; receipt already removed the delivery |
| Error or unsupported disposition | No new terminal operation |

Declare `RedeliveryRequirement` for abandonment. Nonzero delays are unsupported.
ReceiveAndDelete rejects redelivery and keepalive requirements before acquisition.
It cannot restore a message after a callback failure. Attempt metadata is
supported in both modes.

`WithKeepAlive(interval)` requests PeekLock renewal before decoding and during
processing. The interval is a positive cadence, not a new lock duration. Choose
it and the operation budget to fit inside the entity's lock duration with room
for network delay. Without renewal, processing must finish within the lock.

Normal callback return stops renewal scheduling and joins an active renewal
without canceling it. Renewal failure cancels processing and suppresses completion
or abandonment. Graceful shutdown keeps renewal active while admitted work
finishes. Forced cancellation suppresses terminal operations not yet started;
active operations join before receiver closure. A failed completion does not
trigger abandonment or local replay. Renewed locks can still be lost.

| Configuration | Contract |
| --- | --- |
| `Source` | Stable diagnostic name: 1–128 ASCII letters, digits, `.`, `_`, `-` or `/` |
| `ReceiveMode` | Must match the receiver factory's native mode |
| `ReceiveTimeout` | Receive budget; zero selects thirty seconds |
| `OperationTimeout` | Send, renewal or terminal-operation budget; zero selects thirty seconds |
| `CloseTimeout` | Receiver cleanup budget; zero selects thirty seconds |
| `Shutdown` | [Core shutdown policy](../docs/coordinator.md#shutdown) |

Negative budgets are invalid. Cancellation is cooperative. Do not use connection
strings, personal data or request-controlled values as diagnostic sources.

## Dead-letter sources

Set `ReceiverOptions.SubQueue` in the owned receiver factory:
`SubQueueDeadLetter` selects the ordinary dead-letter subqueue, and
`SubQueueTransfer` selects the forwarding source's transfer dead-letter subqueue.
Use the same PeekLock mode in the adapter configuration.

The usual typed handler can process and complete these deliveries. Completion
removes the message from that subqueue. Repair and resubmission need a separate
application policy. Selecting a subqueue does not introduce a Broker dead-letter
disposition.

`DeadLetterReason` and `DeadLetterErrorDescription` remain available in mapped
headers and `Metadata().ApplicationProperties`. Treat their values as untrusted
application data.

## Message mapping

Publication requires a nonempty UTF-8 `Message.ID`. It becomes native `MessageID`
and the canonical string property `id`. Receipt takes logical identity from
native `MessageID`; an optional `id` property must be a matching string. Aliases
and conflicts fail before processing. Reuse the same logical ID when deliberately
retrying an uncertain send. Service deduplication depends on entity configuration.

Bodies are arbitrary bytes. Outgoing header names must be nonempty, unique UTF-8
text; values must be UTF-8 and become string application properties. To carry
binary metadata, explicitly encode it as Base64 under an application-defined
header convention. The adapter does not infer or decode that convention.

Received scalar properties become byte-valued headers: strings and symbols use
UTF-8, numbers and booleans use text, times use UTC RFC3339Nano, UUIDs use string
form, byte slices are copied and null becomes nil. Unsupported properties retain
their field, cause and available `ValueType` in a `FieldError`.

`MetadataReader` returns a detached snapshot with native scalar types, message
ID, delivery count, enqueue time, sequence and initial lock deadline. Its
`LockedUntil` does not track later renewal. No lock token or settlement method is
exposed. `AttemptReader` uses the SDK's already one-based `DeliveryCount`; zero
means unknown.

## Observations and errors

Observer hooks retain source, operation, outcome, duration and original error or
retry cause. Processing success and accepted native completion are separate
outcomes. An unknown native result does not prove the service made no change.
Hooks must return promptly and support concurrent callers when shared. Hook
failures remain visible without replaying settlement.

Causes may be raw AMQP errors or `*azservicebus.Error`. The latter exposes a code
but not its private inner cause through `Unwrap`. The logging adapter must retain
useful codes, fields and independent cleanup causes while redacting connection
strings and application values. See the [command logger](cmd/orders/diagnostics.go)
and [core diagnostics guide](../docs/error-handling.md#structured-diagnostics).

## Run the example

Prepare the [development workspace](../docs/development.md#local-workspace).
Use dedicated existing entities without unrelated messages or competing
consumers. Supply a send/receive connection string through the environment.
From `azuresb`, select one route:

```sh
export AZURE_SERVICEBUS_CONNECTION_STRING='<connection string from your secret manager>'
go run ./cmd/orders -queue '<dedicated queue>' -id order-example-001
```

```sh
go run ./cmd/orders -topic '<dedicated topic>' -subscription '<dedicated subscription>' -id order-example-002
```

The default PeekLock route requests abandonment once, processes redelivery and
finishes after accepted completion. Add `-receive-mode receive-and-delete` only
for a dedicated route where removal before processing is intended. The command
does not create or delete topology.

To exercise renewal on an entity configured with a sixty-second lock, use
`-work-duration 65s -renew-every 5s` in PeekLock mode. Choose corresponding values
for other lock durations. Logs distinguish the effect, renewal and completion.
See [emulator testing](../docs/development.md#azure-emulator-tests) for runtime
fixture requirements.
