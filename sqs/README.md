# Amazon SQS adapter

`github.com/velmie/broker/sqs` publishes to and consumes from a bound SQS queue.
It borrows an application-configured AWS SDK for Go v2 client. The application
owns credentials, region, endpoint, HTTP transport and SDK retries. Dependencies
are listed in [go.mod](go.mod).

## Bind a queue

Resolve the queue URL explicitly with the native SDK, then bind a consumer
without starting a receive request:

```go
package messaging

import (
	native "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

func Orders(client *native.Client, queueURL string) broker.ConsumerFactory {
	return func() (broker.Consumer, error) {
		return sqs.NewConsumer(client, sqs.ConsumerConfig{
			QueueURL: queueURL,
			Source:   "orders",
		})
	}
}
```

Register it with a [Coordinator](../docs/coordinator.md). Bind publication through
`NewPublisher(client, PublisherConfig{QueueURL: queueURL, Source: "orders"})`.
The adapter does not look up, create or delete queues. See
[AWS client configuration](../docs/aws-sdk.md).

`Source` is a diagnostic label of 1–128 ASCII letters, digits, `.`, `_`, `-` or
`/`. Use a stable application name without credentials or personal data.

## Delivery and configuration

Callbacks run sequentially. `Handled` requests `DeleteMessage` after successful
processing. `RetryAfter`, with a declared `RedeliveryRequirement`, changes
visibility using a whole-second delay from zero through twelve hours. Errors or
invalid dispositions end the run without a terminal operation.

`WithKeepAlive(interval)` renews visibility before decoding and during processing.
The interval must be positive and shorter than `VisibilityTimeout`. Normal return
stops renewal scheduling and joins an active call without canceling it. Renewal
failure cancels processing and suppresses deletion or redelivery. Prefetched
messages waiting for callback admission have no renewal.

| Consumer option | Contract |
| --- | --- |
| `BatchSize` | 1–10; zero selects one |
| `WaitTimeSeconds` | 0–20; zero leaves the queue's polling setting in effect |
| `VisibilityTimeout` | Positive whole seconds, at most twelve hours; zero selects thirty seconds |
| `ReceiveTimeout` | Must exceed the effective polling wait; default adds thirty seconds, assuming a twenty-second wait when omitted |
| `OperationTimeout` | Delete, retry and renewal budget; zero selects thirty seconds |
| `ResilientReceive` | Opt-in transient receive retries after one, two and four seconds |
| `Shutdown` | [Core shutdown policy](../docs/coordinator.md#shutdown) |

`ResilientReceive` resets after a successful receive and stops on the fourth
consecutive transient failure or an immediate terminal failure. It does not
retry processing or settlement. SDK retries remain inside each native call.

Canceling a run stops receiving and admission. Graceful shutdown lets admitted
work finish. Forced cancellation suppresses terminal operations not yet started;
active native operations and renewal join before return. A delete error can leave
the outcome unknown and does not trigger another disposition or callback replay.

## Message mapping

Publication waits for `SendMessage` acceptance. It supports concurrent callers
and treats input storage as read-only. `OperationTimeout` also bounds sends.
Acceptance does not establish downstream processing, and failed requests can have
unknown outcomes.

Bodies must be nonempty valid SQS text. Headers become String attributes for
representable text and Binary attributes for other nonempty bytes. The adapter
validates native names, at most ten final attributes and a combined body/attribute
limit of 1 MiB. The queue's configured limits also apply. Duplicate names, empty
values, reserved `id` case aliases and identity conflicts fail locally.

A nonempty `Message.ID` becomes the canonical String attribute `id`. Received
logical identity comes only from that attribute, never from native `MessageId`.
String and Number attributes retain text bytes; Binary values retain raw bytes.
Native maps do not preserve cross-attribute ordering.

For a FIFO queue, set `FIFO: true`, supply `MessageGroupID` and keep a stable
nonempty logical ID for deduplication. Group and deduplication IDs accept 1–128
printable ASCII characters without spaces. A configured group also passes
through for standard queues. These settings do not promise exactly-once effects.

`MetadataReader` returns detached native attributes and the service message ID,
without the receipt handle. `AttemptReader` uses `ApproximateReceiveCount`:
missing means unknown; malformed or zero values fail with their original cause.

## Observations

Optional `Observer.Begin` and `Observer.Record` hooks expose delivery context and
native outcomes separately from processing. Calls are serialized within a run;
shared hooks must support concurrent runs and publications. Hooks must return
promptly. A hook panic remains visible without replaying native operations.

`OperationError` and `FieldError` preserve source, operation or field and original
causes. The logging adapter owns redaction of SDK and application data. See
[diagnostics](../docs/error-handling.md#structured-diagnostics).

## Run the example

Prepare the [development workspace](../docs/development.md#local-workspace) and
start a disposable ElasticMQ service at a loopback HTTP endpoint. From `sqs`:

```sh
go run ./cmd/orders -endpoint http://127.0.0.1:9324
```

The command uses synthetic credentials, creates a temporary queue, publishes an
order, requests one redelivery, renews visibility during processing and confirms
deletion before cleanup. Its output distinguishes the effect from accepted
redelivery, renewal and deletion.
