# Migration guide

## Upgrade NATS v3.3.1 to v4

The NATS adapter requires NATS server 2.10.0 or newer. Upgrade every server
that can host or serve the JetStream consumer before adopting v4. Change imports
from `github.com/velmie/broker/natsjs/v3` to
`github.com/velmie/broker/natsjs/v4` and update dependent modules together.

Replace `Subject: "events.created"` with
`Subjects: []string{"events.created"}` in `ConsumerConfig`. There is no implicit
filter and no singular selector.
Keep the existing stream and durable names. Reusing a compatible durable preserves
its acknowledgement position. Multiple filters must match the remote set exactly;
coordinate membership changes with the operator before deploying them. See the
[filter and permission guidance](../natsjs/README.md#select-multiple-subjects).

Since v3.3.1, the adapter also provides:

- `ConnectionConfig` and `OpenConnection` for configured, verified connections.
  Connection and topology lifetime remain application-owned.
- Create-or-verify topology profiles, `RequireRetainedStream` and availability-only
  `CheckStreams`. Verification fails on incompatible topology without updating it.
- `DurableConsumerConfig.StartTime` for a newly created consumer. Existing
  consumers retain their position and delivery policy.
- Provider-neutral `broker.Reject` settlement through native TERM, alongside the
  adapter's native termination API.
- Optional native settlement instrumentation in the `otelnatsjs` module.

Review the [adapter guide](../natsjs/README.md) when adopting these additions.

### Upgrade topology configuration from v4 prereleases

The topology profiles were not present in v3.3.1. If an earlier v4 prerelease
uses `DurableConsumerConfig.Subject`, replace it with `Subjects` and update
serialized `subject` configuration to a `subjects` list.

If a prerelease uses `RetainedStreamConfig.Subject`, remove that field.
`RequireRetainedStream` verifies the stream's retention and size settings, while
`EnsureDurableConsumer` checks that the stream covers every consumer filter,
including wildcard filters.

## Upgrade from the subscriber API

The typed API uses explicit contexts, destination-bound publishers and consumers
that own native delivery operations. Upgrade application wiring, processing
policies and message metadata together.

This guide covers the transition from core v0 to core v1 and compatible adapter
modules, including the `natsjs/v3` import path. Earlier documentation remains at
its [published revision](https://github.com/velmie/broker/tree/61bf31d38dc270d914217cf7b00e05bbc64a5367/docs).
Use [local workspace setup](development.md#local-workspace) to verify a source
checkout.

## Update wiring

Update the core and every Broker adapter or integration used by the application
as one dependency change. Go selects one version of the root module for a build.
Include idempotency middleware when present. Refer to each module's `go.mod` for
its dependency contract.

| Earlier API | Typed API |
| --- | --- |
| Context stored in `Message` | Explicit context on `Publish`, `Handle` and `Run` |
| Destination passed with publication | Destination-bound adapter publisher |
| `CreateHandler` | `NewTypedHandler` |
| Handler needing event metadata or ACK | `NewTypedDeliveryHandler`, returned disposition and declared requirements |
| `CreateReplyHandler` | `NewReplyHandler` with an approved `ReplyResolver` |
| Subscription coordinator and individual unsubscribe calls | Consumer factories, `Coordinator.Run` and context cancellation |
| Application type for a plain delivery | `NewDelivery(message, source)` |

Construct native SDK clients in application wiring. Keep deployed queue names,
durable names, start positions and formats unless their migration is separately
planned. Provision topology explicitly, bind publishers to destinations and
consumer factories to existing sources. Factories acquire no receive resources.
See [adapter ownership](backends.md) and [coordinator lifecycle](coordinator.md).

## Preserve message contracts

Replace `map[string]string` headers with ordered `[]broker.Header`. Preserve
bytes and duplicates until a specific convention validates them. Do not collapse
all headers into a string map.

Single-text core helpers now reject duplicate aliases and invalid UTF-8.
Correlation binding requires `WithCorrelationIDBinding()` and calls its setter
once after decoding. Absence and an explicitly empty value remain distinct.

The earlier `Created-At` helper used a signed decimal Unix timestamp in seconds.
Preserve it with `strconv.FormatInt(created.Unix(), 10)` when existing consumers
expect that format. It is neither milliseconds nor RFC3339. Neither message API
adds the timestamp automatically. The old getter discarded parse errors; choose
an explicit application policy for invalid or overflowing input.

The [metadata migration example](../metadata_migration_test.go) preserves the
wire conventions, distinguishes absence from zero and retains `strconv.NumError`.
It deliberately requires canonical spelling for selected text headers. From the
root module:

```sh
go test -run '^ExampleNewReplyHandler_metadataMigration$' -v
```

Keep logical message IDs stable across uncertain publication retries. For SQS,
received Binary attributes now contain raw bytes; the earlier adapter represented
them as Base64 text. Update any decoding or republishing convention accordingly.
Native service IDs and receipt tokens do not replace logical identity.

## Make reply routing explicit

Use `NewTypedHandler` for one-way commands. `NewReplyHandler` requires an approved
publisher and stable nonempty response ID before business work. If replies are
optional, select the appropriate handler in application wiring.

Do not trust an incoming `Reply-To` as an arbitrary destination. Map an approved
route label to a bound publisher. To preserve the earlier reply convention, set
these independent values explicitly:

| Response value | Source |
| --- | --- |
| `Message.ID` | Stable response ID under the application's identity rule |
| `Reply-Message-Id` | Request `Message.ID` |
| `Correlation-Id` | Validated nonempty request correlation value |

The [request/reply guide](request-reply.md) uses request ID as correlation for its
own example. Preserve the existing application's convention during migration.
Effects, response publication and request acknowledgment remain separate steps.

## Rebuild processing policies

`WithMiddleware(a, b)` now means `a(b(handler))`, reversing the earlier wrapping
loop. The first argument is outermost, and a later call wraps the existing value.
`WrapPublisher` uses the same order. Preserve declared requirements and copy any
caller storage that custom middleware changes.

A non-nil handler error stops the consumer and suppresses its disposition. Use
`WithRedelivery` for explicitly classified `StageHandle` failures or return a
supported `RetryAfter` with its requirement. Decode, metadata, resolution and
publication failures do not enter the built-in business-error classifier.
Check native delay precision and mode restrictions. A classifier must account
for partial business effects.

Use `RecoverPanics`, `LogProcessing` and adapter observers at their owning
boundaries. Supply original errors to the logger so their cause text is visible.
See [error handling](error-handling.md).

`WithConsumerRecovery` replaces a run only after all its work joins. Classify the
complete cause tree and bound replacements. Do not restart blindly after
uncertain settlement, processing or cleanup failures.

## Service shutdown results

`LogProcessing` omits cooperative cancellation when the handler returns exactly
its context's non-nil `Err()` value. Wrapped cancellation errors remain logged.
The `Level` and `Attributes` callbacks are not invoked for omitted records.
OpenTelemetry processing spans still record cooperative cancellation as an error.
Non-nil retry and rejection causes now also set span error status. OpenTelemetry
header extraction combines case aliases and preserves all values for multi-value
propagators. Single-value extraction uses the first valid value, including when
the same header occurs more than once.

Core v1.2, NATS v3.3, SQS v1.2 and Azure Service Bus v1.2 support `RunService`. For groups
consisting entirely of supported consumers, replace `Coordinator.Run(ctx)` with `RunService(ctx)` and
return any non-nil error directly. Successful parent cancellation, deadline or
custom cancellation cause returns `nil` after work and cleanup join. Do not
discard errors using `errors.Is(err, context.Canceled)`: an error may contain
independent failures. Existing `Run` behavior is unchanged.

The NATS, SQS and Azure managed consumers support this contract. Recovery wrappers
preserve the capability when their initial consumer supports it and validate
replacement generations. See the [result table](coordinator.md#shutdown),
[NATS shutdown details](../natsjs/README.md#shutdown-results),
[SQS shutdown details](../sqs/README.md#shutdown-results) and
[Azure shutdown details](../azuresb/readme.md#shutdown-results).

`PanicError.Error`, `natsjs.ObserverPanicError.Error`, `sqs.ObserverPanicError.Error` and
`azuresb.ObserverPanicError.Error` include the panic value and error cause text. Ordinary error logging therefore includes that text.
Error identity and the separately available processing stack are unchanged.

## NATS pull receive recovery

In `natsjs/v3` v3.2, recognized pull receive interruptions are retried within the
same `Run`. Applications can remove wrappers that restart a run solely for those
receive interruptions. Observers can distinguish `OutcomeInterrupted` attempts
from terminal fetch failures. Other errors still require an application-owned
recovery decision. See [pull receive recovery](../natsjs/docs/consumers.md#recover-pull-receives).

## Retained idempotency records

The earlier idempotency API used `Event.Topic()` in both the key and default
fingerprint, and defaulted to fail-open. The typed API uses `Delivery.Source()`
and defaults to `CommitFailClosedKeepLock`.

Handle the error returned by `idempotency.Middleware` and attach its descriptor
with `WithMiddleware`. Remove `WithAckOnReplay`: replay returns `Handled`, leaving
native settlement to the consumer. Replace `WithUseTopicInKey` with
`WithUseSourceInKey`. Custom fingerprint, replay and commit-error callbacks now
receive explicit context and delivery arguments.

Retained records match only while their namespace and fingerprint remain the
same. This example preserves an old `billing:` prefix and `orders`
topic when the new native source name differs and no fingerprint headers were
selected:

```go
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
)

func Deduplicate(engine *idempo.Engine) (broker.HandlerMiddleware, error) {
	return idempotency.Middleware(engine,
		idempotency.WithRequireKey(true),
		idempotency.WithUseSourceInKey(false),
		idempotency.WithKeyPrefix("billing:orders:"),
		idempotency.WithFingerprintFunc(func(_ context.Context, delivery broker.Delivery) (idempo.Fingerprint, error) {
			bodyHash := sha256.Sum256(delivery.Message().Body)
			return idempo.Fingerprint{
				Operation: "consume",
				Target:    "orders",
				BodyHash:  hex.EncodeToString(bodyHash[:]),
			}, nil
		}),
	)
}
```

For an existing custom fingerprint, preserve its operation, target and hashes.
The default selected-header hash sorts exact names, contributes
`strconv.Quote(name) + "=" + strconv.Quote(value) + "\n"` for each, then hashes
with SHA-256 and hex-encodes the result. Missing and empty values contribute the
same empty value. A nil header collection produces an empty hash; a non-nil empty
collection hashes the selected empty values. Preserve that distinction when
matching retained records. Ambiguous duplicate headers cannot represent the old
single-value map and are rejected.

The engine lease and native delivery renewal are independent. See
[idempotency limits](../idempotency/README.md#delivery-limits).

## Azure module requirements

Azure adapter v1.1.0 requires Go 1.26 or later because its HTTP/2 dependency
includes security fixes in `golang.org/x/net` v0.60.0. Upgrade the Go toolchain
before updating this adapter and use a supported patch release. The core and
other adapter toolchain requirements remain in their own module manifests.
See the [Azure dependency manifest](../azuresb/go.mod).

## Verify the application

For an existing named ephemeral JetStream pull consumer, use
`natsjs.NewPullConsumer` with its current stream, name and filter set. Leave
its native lifetime and delivery policy intact. `NewConsumer` retains its
durable-only contract. Both bindings preserve `Source()` as `stream/consumer`,
and neither changes remote topology. See [per-instance delivery](../natsjs/docs/consumers.md#receive-a-copy-in-each-instance).

For wildcard guards, read the actual subject through `natsjs.SubjectReader`.
Keep `Delivery.Source()` for its existing source identity. Adding subject access
does not change Source-based idempotency keys, fingerprints or tracing destinations.

For existing explicit-ack push or queue consumers, `natsjs.NewPushConsumer` binds
their native configuration to the same handler and shutdown contract. If the
application owns a native iterator or callback and settlement, use
`natsjs.NewProcessor` for mapping and handler execution. It never acknowledges or
renews messages. Keep settlement under one owner. See [native processing](../natsjs/docs/native-consumption.md).

For publication result inspection, use `NewTypedPublisherWithResult` with a
result-returning operation, such as `natsjs.Publisher.PublishWithAck`. Existing
error-only publishers need no change. Keep native options at the adapter boundary
through [native publication](../natsjs/docs/native-publishing.md).

SQS and SNS publishers expose `PublishWithResult` for native results and
`BuildRequest` for per-call SDK controls. Azure's `EncodeMessage` connects Broker
metadata and bytes to SDK send, batch and scheduling operations. Use the common
typed result function when that native operation returns a result, such as
Service Bus scheduling sequence numbers.

For application-owned SQS or Azure receivers, use the adapter's `NewProcessor`
to retain Broker handlers and middleware. The application then owns renewal,
settlement and shutdown. In Azure, preserve the original received SDK object for
completion or deferral, and renew session locks through the session receiver.
See the [native access and ownership table](backends.md#native-composition).

These entry points are additive. Existing core v1 and adapter interfaces, source
identities, wire formats and managed settlement behavior need no migration to
keep working. Adopt a native path at the application wiring boundary when its
ownership contract matches the desired operation.

Check stored keys and fingerprints against retained records, and wire metadata
against existing producers and consumers. Exercise native publication,
redelivery, reply failure, uncertain settlement and shutdown on the selected
transport. Verify diagnostic causes and redaction through the actual configured
logger. Local emulators cover their supported protocol paths; deployment
credentials, service configuration and cloud behavior need their own checks.
