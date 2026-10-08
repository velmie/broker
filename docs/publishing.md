# Typed publication and native results

A `Publisher` sends a `Message` to a bound destination. `NewTypedPublisher`
resolves application-owned metadata, encodes a value and calls that publisher.
It returns an error. The [quickstart](../readme.md#publish-and-handle-a-message) demonstrates this
path without a transport service.

Use `NewTypedPublisherWithResult` when the caller needs the publication
operation's result. Supply an encoder, a function taking a context and Message
and returning `(R, error)`, a metadata function, and optional
`PublisherMiddleware`. The returned typed function returns `(R, error)` to each
caller. `R` remains the SDK or application's type; Broker defines no combined
transport result.

Metadata, encoding and publication failures retain their stage and original
cause. A result may accompany an error from middleware or instrumentation after
publication. Inspect both. Success has the supplied operation's guarantee. For
example, returning an asynchronous future proves enqueueing, while the future
reports the later acknowledgment.

## Middleware ownership

Result-aware typed publication constructs middleware wrappers for each call.
Keep shared middleware state in the supplied closure. The constructor copies
the middleware slice, and concurrent calls retain independent results. Codecs,
metadata functions, the publication function and shared closures must support
the application's concurrency. Input message data remains read-only.

Middleware must call the next publisher synchronously and exactly once for
success. Skipping with an error preserves that error. Skipping with success
returns a `PublisherMiddleware` field error. A second call is rejected before
another publication. Swallowing a publication error does not turn failure into
success. Middleware that batches asynchronously or retries through repeated
calls to the next publisher does not satisfy this result contract.

Use ordinary publishing middleware, including tracing, when these rules match
its behavior. See [NATS acknowledgment and futures](../natsjs/docs/native-publishing.md),
[SQS request and result access](../sqs/docs/native.md),
[SNS native publication](../sns/docs/native.md), and
[Service Bus scheduling](../azuresb/docs/native.md) for SDK composition.
