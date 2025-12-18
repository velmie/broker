# Middleware

"broker" uses a simple functional middleware model:

```go
type Middleware func(broker.Handler) broker.Handler
```

Middleware runs around your handler and can add logging, tracing, idempotency, panic recovery, and other cross cutting behavior.

## Built-in middleware (core module)

- `broker.PanicRecoveryMiddleware()` converts panics into errors, so your error handler can handle them.
- `broker.LoggingMiddleware(logger, opts...)` logs handler duration and outcome. Options can include headers and body (use carefully).
- `broker.LoopbackPreventionMiddleware(instanceID, logger?)` skips messages that were published by the same instance. It relies on the "Instance-Id" header.

Publisher side helper:

- `broker.PublishWithInstanceID(instanceID)` is a `broker.PublisherMiddleware` that sets the "Instance-Id" header on every published message.

## Loopback prevention use case

Loopback prevention is useful when the same process both publishes and subscribes to the same topic (or to a topology where its own published messages can come back to it). Without a guard you can get:

- accidental processing of your own "output" events
- feedback loops (a consumer publishes an event that triggers itself again)
- double work in multi-instance setups where each instance should only handle events from other instances

Typical examples:

- an instance publishes an "entity.updated" event after writing to a database, but it also subscribes to "entity.updated" to maintain a local cache. You want each instance to react to updates from other instances, not to its own writes.
- a service bridges messages between topics and should not re-process messages it just forwarded.

To enable it, make sure the publisher sets "Instance-Id" and the consumer checks it:

```go
pub = broker.PublishWithInstanceID(instanceID)(pub)

handler := broker.CreateHandler(
	broker.DecoderFunc(json.Unmarshal),
	consumeFn,
	broker.LoopbackPreventionMiddleware(instanceID, logger),
)
```

## Add-on middleware modules

- `otelbroker.ConsumerMiddleware(...)` and `otelbroker.PublisherMiddleware(...)` propagate OpenTelemetry context and create spans.
- `idempotency.Middleware(engine, ...)` makes consumers idempotent by skipping replays (see [idempotency/README.md](../idempotency/README.md)).

## Example: consumer middleware chain

```go
mw := []broker.Middleware{
	broker.PanicRecoveryMiddleware(),
	broker.LoggingMiddleware(logger),
	otelbroker.ConsumerMiddleware(),
	broker.LoopbackPreventionMiddleware(instanceID, logger),
	idempotency.Middleware(engine),
}

handler := broker.CreateHandler(
	broker.DecoderFunc(json.Unmarshal),
	consumeFn,
	mw...,
)
```

Note: middlewares are applied in the order you pass them to `CreateHandler`, but the last middleware becomes the outermost wrapper. If ordering matters, write a small test to lock it down.

## Logging options

`LoggingMiddleware` is intentionally flexible so you can keep logs safe and useful:

```go
mw := broker.LoggingMiddleware(
	logger,
	broker.WithLogError(true),
	broker.WithLogHeader(true),
	broker.WithLogBodyOnError(true),
	broker.WithLogHeaderFunc(func(e broker.Event) string {
		// redact secrets, drop noisy headers, etc.
		return "redacted"
	}),
)
```

Prefer logging body only on errors, and consider truncation/redaction in `WithLogBodyFunc`.

## Example: publisher middleware

```go
pub = broker.PublishWithInstanceID(instanceID)(pub)
pub = otelbroker.PublisherMiddleware()(pub)
```

If you use loopback prevention, make sure the publisher sets "Instance-Id" (either manually or via `PublishWithInstanceID`).
