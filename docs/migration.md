# Upgrade from the subscriber API

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
Refer to each module's `go.mod` for its dependency contract.

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
boundaries. Supply original errors to the logger. Preserve
structured causes and useful fields through application-aware redaction. See [error handling](error-handling.md).

`WithConsumerRecovery` replaces a run only after all its work joins. Classify the
complete cause tree and bound replacements. Do not restart blindly after
uncertain settlement, processing or cleanup failures.

## Verify the application

Check stored keys and fingerprints against retained records, and wire metadata
against existing producers and consumers. Exercise native publication,
redelivery, reply failure, uncertain settlement and shutdown on the selected
transport. Verify diagnostic causes and redaction through the actual configured
logger. Local emulators cover their supported protocol paths; deployment
credentials, service configuration and cloud behavior need their own checks.
