# NATS JetStream adapter

`github.com/velmie/broker/natsjs/v3` connects Broker publishers and handlers to
NATS JetStream. It publishes to a bound subject and consumes an existing durable
pull consumer with explicit acknowledgments. Dependencies are listed in
[go.mod](go.mod).

The application owns the NATS connection and remote topology. Configure a
positive `nats.FlusherTimeout` when connecting, keep the connection open while
consumers run, and close it after they join.

## Bind a source

This function returns a locally bound consumer factory for a provisioned source:

```go
package messaging

import (
	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func Orders(conn *nats.Conn, stream, consumer, subject string) broker.ConsumerFactory {
	return func() (broker.Consumer, error) {
		return natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
			Stream: stream, Consumer: consumer, Subject: subject,
		})
	}
}
```

Register the factory and a handler with a [Coordinator](../docs/coordinator.md).
`NewConsumer` validates locally without acquiring a subscription. `Run` checks
remote configuration, attaches its local pull subscription and releases it after
all owned work joins. It does not create, update or delete streams or durable
consumers. See [consumer progress and ownership](docs/consumers.md).

The remote consumer must use explicit acknowledgments, exactly one
`FilterSubject` matching `Subject`, and no native `BackOff`. Keep its configuration
stable during a run. Push, ordered and other native policies are demonstrated in
[caller-owned SDK sessions](cmd/native/README.md).

| Configuration | Meaning |
| --- | --- |
| `Stream`, `Consumer`, `Subject` | Required binding; diagnostic source is `stream/consumer` |
| `Domain` or `APIPrefix` | Optional JetStream management/fetch namespace; mutually exclusive |
| `BatchSize` | Maximum fetched batch; zero selects one |
| `FetchTimeout` | Fetch budget; zero selects one second |
| `OperationTimeout` | Startup, renewal and terminal-operation budget; zero selects two seconds |
| `Shutdown` | [Core shutdown policy](../docs/coordinator.md#shutdown) |
| `Observer` | Delivery context and native-operation observations |

An API prefix may have one trailing dot. Namespace selection affects management,
binding and fetching, not publication subjects. Invalid or wrong namespaces fail
before processing. Namespace names must be valid for the native subject rules.

Callbacks run sequentially. Prefetched messages waiting for admission have no
renewal and remain pending if shutdown stops admission. Keep batches small when
callbacks may be slow. Operation budgets cooperate with the connection's write
budget and cannot interrupt unresponsive callbacks or hooks.

## Delivery decisions

| Handler result or requirement | Native behavior |
| --- | --- |
| `broker.Handled{}` | `AckSync` waits for an acknowledgment response |
| `broker.RetryAfter{Delay: delay}` with `RedeliveryRequirement` | Requests native delayed NAK; zero requests immediate redelivery |
| `natsjs.Terminate{}` with `natsjs.TerminationRequirement` | Requests native TERM; does not publish to a dead-letter destination |
| `broker.AttemptsRequirement{}` | Exposes one-based transport delivery attempts |
| `broker.WithKeepAlive(interval)` | Sends progress acknowledgments while admitted processing runs |

Use concrete value forms. Unknown or nil dispositions, undeclared capabilities,
invalid delays and retries beyond a finite `MaxDeliver` fail without settlement.
Equal requirements are idempotent; conflicting renewal intervals are invalid.
Renewal cadence must be positive and shorter than remote `AckWait`.

Renewal begins before decoding. A normal callback return stops scheduling and
joins any active renewal without canceling it. A renewal failure cancels
processing and suppresses terminal settlement. Graceful shutdown keeps renewal
active while admitted work finishes. Forced cancellation suppresses terminal
operations that have not started; admitted operations retain their own budgets
and join before return.

A confirmed protocol response does not establish exclusive processing or
exactly-once effects. A failed terminal call can leave the outcome unknown. The
adapter neither applies an opposite disposition nor replays the callback.

## Message mapping

`NewPublisher(conn, PublisherConfig{Subject: subject})` binds a destination and
waits for a JetStream publication response. Calls may run concurrently while
caller-owned message data remains read-only.

A nonempty logical ID maps to `Nats-Msg-Id`. A supplied ID header must be unique,
valid UTF-8 and consistent with `Message.ID`; reserved case aliases are rejected.
Other representable headers retain case and duplicate values. Values containing
CR, LF or surrounding whitespace are rejected. Across different header names,
native map ordering is not retained.

`MetadataReader` exposes a detached native snapshot without acknowledgment
methods. Attempt counts describe transport deliveries, not business executions.

## Observations and errors

`Observer.Begin` can attach a delivery context while preserving cancellation.
`Observer.Record` receives source, operation, outcome, duration, disposition and
original `Err` or retry `Cause`. Processing and later acknowledgment are distinct
records. Renewal can overlap processing, so hooks must support concurrency and
return promptly. A panic disables later hooks for that run; already entered
hooks still join and their failures remain visible.

`OperationError` and `FieldError` preserve original causes. Pass errors to your
logging adapter and use `broker.DescribeError` with `natsjs.ErrorFields` to retain
native codes and safe operation context. Application errors still need
application-aware redaction. See [structured diagnostics](../docs/error-handling.md#structured-diagnostics).

## Run the example

Prepare the [development workspace](../docs/development.md#local-workspace) and
start a disposable NATS server with JetStream. From `natsjs`:

```sh
go run ./cmd/orders -url nats://127.0.0.1:4222
```

The command creates a temporary stream and consumer, publishes an order,
processes it, publishes a receipt and confirms acknowledgment before cleanup.
Output includes `created order`, `reply confirmed` and `ack confirmed`.
`-domain` and `-api-prefix` select a configured JetStream namespace.

See [the application mapping](examples/orders/application.go) for typed DTOs and
commands, and [native sessions](cmd/native/README.md) for SDK-specific features.
