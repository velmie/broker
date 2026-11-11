# NATS JetStream adapter

`github.com/velmie/broker/natsjs/v4` connects Broker publishers and handlers to
NATS JetStream. It requires NATS server 2.10.0 or newer. It publishes to a bound
subject and consumes existing named pull or push consumers with explicit acknowledgments. Dependencies are listed in
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
	"github.com/velmie/broker/natsjs/v4"
)

func Orders(conn *nats.Conn, stream, consumer, subject string) broker.ConsumerFactory {
	return func() (broker.Consumer, error) {
		return natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
			Stream:   stream,
			Consumer: consumer,
			Subjects: []string{subject},
		})
	}
}
```

Register the factory and a handler with a [Coordinator](../docs/coordinator.md).
`NewConsumer` requires a durable. Use `NewPullConsumer` for a named durable or
ephemeral consumer, including [one consumer per instance](docs/consumers.md#receive-a-copy-in-each-instance).
`NewPushConsumer` binds a push consumer and uses its native queue group, if any.
All three constructors validate locally without acquiring a subscription. `Run` checks
remote configuration, attaches its local subscription and releases it after
all owned work joins. It does not create, update or delete streams or remote
consumers. Recognized pull receive interruptions are retried on the same local
subscription. See [consumer progress and recovery](docs/consumers.md).

The remote consumer must use explicit acknowledgments, the same exact set of
filters as `Subjects`, and no native `BackOff`. Keep its configuration
stable during a run. See [native processing and ownership](docs/native-consumption.md)
when the application should own receive and settlement. Ordered and other native
policies are demonstrated in [native sessions](cmd/native/README.md).

| Configuration | Meaning |
| --- | --- |
| `Stream`, `Consumer`, `Subjects` | Required binding; diagnostic source is `stream/consumer` |
| `Domain` or `APIPrefix` | Optional JetStream management/fetch namespace; mutually exclusive |
| `BatchSize` | Maximum pull batch; zero selects one; push requires one |
| `FetchTimeout` | Receive wait budget; zero selects one second |
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

## Select multiple subjects

`Subjects` is a nonempty set of unique, valid NATS subject filters. Use a
one-element slice for a single filter. Order has no meaning. Provisioning checks
that every filter is covered by a stream subject. An existing durable must have
exactly the requested filters. A mismatch fails without changing the consumer or
its acknowledgement progress. NATS rejects overlapping filters according to its
own rules.

For a stream containing `events.*`, provision and bind one durable for two events:

```go
package messaging

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v4"
)

func BindEvents(ctx context.Context, js jetstream.JetStream, conn *nats.Conn) (*natsjs.Consumer, error) {
	filters := []string{"events.created", "events.paid"}
	if err := natsjs.EnsureDurableConsumer(ctx, js, natsjs.DurableConsumerConfig{
		Stream:        "EVENTS",
		Durable:       "events-processor",
		Subjects:      filters,
		AckWait:       time.Minute,
		MaxDeliveries: -1,
		MaxAckPending: 100,
	}); err != nil {
		return nil, err
	}
	return natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
		Stream:   "EVENTS",
		Consumer: "events-processor",
		Subjects: filters,
	})
}
```

The stream can be externally managed and need not use Broker's retained-stream
profile. Filters must be covered by its directly bound subjects. Stream retention
and limits determine how long messages remain available. A change in filter
membership requires coordinated operator reconfiguration of the durable. Deploying
a changed configuration alone fails on drift and does not reset progress.

For multiple filters, the selected NATS SDK sends consumer creation to
`$JS.API.CONSUMER.CREATE.<stream>.<consumer>` without a filter suffix. A domain or
custom API prefix changes the prefix. Authorization tied to a single filter's
API-subject suffix does not constrain this request. Operators must authorize the
named consumer appropriately. The adapter does not change credentials.

From `natsjs`, run the [filter example](cmd/filters/main.go) against a disposable JetStream server:

```sh
go run ./cmd/filters -url nats://127.0.0.1:4222
```

The command publishes two selected events and one excluded event, confirms both
acknowledgements, and removes its temporary stream.

## Shutdown results

`RunService` returns `nil` when the parent stops by cancellation, deadline or a
custom cause and the consumer's work and cleanup succeed. The parent cause is
available through `context.Cause(parent)` and the stop observation. A configured
grace period is separate: expiration returns an error matching
`ErrGracePeriodExceeded`. An independent callback, renewal, startup, receive,
settlement, cleanup or observer failure remains an error, including a timeout
concurrent with shutdown.

An immediately canceled callback cooperates when it returns exactly its canceled
delivery context's `ctx.Err()` after the adapter canceled that context. Wrapped
or joined callback errors remain failures. This check applies to the final error
returned by the Broker handler, after middleware. `NewTypedHandler` wraps errors
from its business callback in a `StageHandle` error, so that wrapped cancellation
remains a failure. `NewTypedDeliveryHandler` leaves callback errors unwrapped.
An unadmitted terminal operation is
suppressed and the delivery remains pending. An admitted operation completes
under its own budget. A retry or termination cause is not itself a run failure,
but an actual failed terminal operation retains both causes.

`Run` retains its existing cancellation result. Do not discard a non-nil
`RunService` result merely because it matches `context.Canceled`. See the
[coordinator result table](../docs/coordinator.md#shutdown).

## Delivery decisions

| Handler result or requirement | Native behavior |
| --- | --- |
| `broker.Handled{}` | `AckSync` waits for an acknowledgment response |
| `broker.RetryAfter{Delay: delay}` with `RedeliveryRequirement` | Requests native delayed NAK; zero requests immediate redelivery |
| `broker.Reject{}` with `broker.RejectRequirement` | Requests native TERM; does not publish to a dead-letter destination |
| `natsjs.Terminate{}` with `natsjs.TerminationRequirement` | Requests native TERM; does not publish to a dead-letter destination |
| `broker.AttemptsRequirement{}` | Exposes one-based transport delivery attempts |
| `broker.WithKeepAlive(interval)` | Sends progress acknowledgments while admitted processing runs |

Use concrete value forms. Unknown or nil dispositions, undeclared capabilities,
invalid delays and retries beyond a finite `MaxDeliver` fail without settlement.
Prefer `broker.Reject` when the provider-neutral decision meets the handler's needs.
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
caller-owned message data remains read-only. `PublishWithAck` also returns the
native acknowledgment. For SDK options and detached message conversion, see
[native publication](docs/native-publishing.md).

A nonempty logical ID maps to `Nats-Msg-Id`. A supplied ID header must be unique,
valid UTF-8 and consistent with `Message.ID`; reserved case aliases are rejected.
Other representable headers retain case and duplicate values. Values containing
CR, LF or surrounding whitespace are rejected. Across different header names,
native map ordering is not retained.

`MetadataReader` exposes a detached native snapshot without acknowledgment
methods. Attempt counts describe transport deliveries, not business executions.
`SubjectReader` exposes the actual received subject. Use it for a
[wildcard guard before decoding](docs/consumers.md#filter-by-the-received-subject).
`Delivery.Source()` identifies the bound stream and consumer, so it remains the
same across all subjects matched by its filters.

## Observations and errors

`Observer.Begin` can attach a delivery context while preserving cancellation.
`Observer.Record` receives source, operation, outcome, duration, disposition and
original `Err` or retry `Cause`. Processing and later acknowledgment are distinct
records. A fetch with `OutcomeInterrupted` records a pull receive or recovery
lookup attempt selected for retry and its original error; it does not mean `Run`
failed. Renewal can overlap processing, so hooks must support concurrency and
return promptly. A panic disables later hooks for that run; already entered hooks
still join and their failures remain visible.

`OperationError` and `FieldError` preserve original causes. Pass original errors
to `slog` to include their operation and cause text. See
[structured diagnostics](../docs/error-handling.md#structured-diagnostics).

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

## Configured connection and topology profiles

`ConnectionConfig` loads a single NATS endpoint with optional username and
password and verified TLS. `Validate` checks the configuration without opening
resources. `OpenConnection` returns a native connection. Its context governs
connection attempts and must remain alive until the connection is closed.
Close the connection after its users have stopped. The configuration defaults
are intended for a configuration loader, not applied by `OpenConnection`.
Other authentication or connection profiles can use the native NATS client.
The connection ignores server-advertised endpoints on reconnect. The suggested
three reconnects at 250 ms are a fail-fast policy: an outage longer than about a
second can leave the connection permanently closed.

`EnsureLimitsStream`, `EnsureWorkQueueStream` and `EnsureDurableConsumer` create
missing resources and verify existing ones without modifying them.
`RequireRetainedStream` verifies an external stream without creating it, and
`CheckStreams` checks availability only. They borrow the JetStream client and
honor its timeout and the supplied context. Work-queue `MaxMessages` and
`MaxBytes`, and durable `MaxAckPending`, apply when creating a resource. They do
not limit operator adjustments to existing compatible resources.
NATS server 2.10.0 or newer is required.

`DurableConsumerConfig.StartTime` sets the earliest message time only when a
durable is created. Its zero value starts from the beginning of the stream.
An existing durable retains its position and resumes from its acknowledgement
floor, regardless of its original start policy or the supplied `StartTime`.

For an externally owned retained stream, a new consumer can start at the
beginning of a business-defined time window:

```go
package messaging

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/velmie/broker/natsjs/v4"
)

func EnsureEventsConsumer(ctx context.Context, js jetstream.JetStream, businessWindow time.Duration) error {
	if err := natsjs.RequireRetainedStream(ctx, js, natsjs.RetainedStreamConfig{
		Stream:          "EVENTS",
		Subject:         "events.created",
		MaxMessageBytes: 1024 * 1024,
	}); err != nil {
		return err
	}
	return natsjs.EnsureDurableConsumer(ctx, js, natsjs.DurableConsumerConfig{
		Stream:        "EVENTS",
		Durable:       "events-processor",
		Subjects:      []string{"events.created"},
		AckWait:       time.Minute,
		MaxDeliveries: 5,
		MaxAckPending: 10,
		StartTime:     time.Now().Add(-businessWindow),
	})
}
```

For a service that owns its stream and durable consumer, connect and provision
the required topology before binding a Broker consumer:

```go
package messaging

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/velmie/broker/natsjs/v4"
)

func ConnectOrders(ctx context.Context, cfg natsjs.ConnectionConfig, logger *slog.Logger) (*nats.Conn, error) {
	conn, err := natsjs.OpenConnection(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(conn)
	if err == nil {
		err = natsjs.EnsureWorkQueueStream(ctx, js, natsjs.WorkQueueStreamConfig{
			Name:            "ORDERS",
			Subject:         "orders.>",
			MaxMessageBytes: 1024 * 1024,
			DuplicateWindow: 2 * time.Minute,
			MaxMessages:     10000,
			MaxBytes:        1024 * 1024 * 1024,
		})
	}
	if err == nil {
		err = natsjs.EnsureDurableConsumer(ctx, js, natsjs.DurableConsumerConfig{
			Stream:        "ORDERS",
			Durable:       "processor",
			Subjects:      []string{"orders.created"},
			AckWait:       time.Minute,
			MaxDeliveries: 5,
			MaxAckPending: 10,
		})
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}
```

Pass the returned connection to `Orders` above and close it after all consumers
have stopped. Set connection fields explicitly or use a configuration loader to
apply the tagged defaults.
