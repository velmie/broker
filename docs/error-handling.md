# Error handling

In "broker", your handler returns an error and the backend decides what to do with the message. Most backends implement a common pattern:

- handler returns `nil` and AutoAck is enabled: acknowledge the message
- handler returns an error: do not acknowledge, optionally NAK/abandon, then call `ErrorHandler` if configured

You attach error behavior via `SubscribeOption`:

```go
_, err := sub.Subscribe(
	"topic",
	handler,
	broker.WithErrorHandler(myErrorHandler),
)
```

## Built-in error handlers

Core helpers:

- `LogErrorHandler(logger)` logs the error with the topic
- `DelayErrorHandler(d, logger?)` sleeps before returning (useful to slow down hot loops)
- `CombineErrorHandlers(...)` runs multiple handlers sequentially
- `WithDefaultErrorHandler(sub, logger)` sets a default policy: log, wait 5s, then resubscribe

Example:

```go
eh := broker.CombineErrorHandlers(
	broker.LogErrorHandler(logger),
	broker.DelayErrorHandler(2*time.Second, logger),
	broker.ResubscribeErrorHandler(sub, broker.ResubscribeWithLogger(logger)),
)

_, _ = sub.Subscribe("topic", handler, broker.WithErrorHandler(eh))
```

## ResubscribeErrorHandler

`ResubscribeErrorHandler` calls `sub.Unsubscribe()` and then attempts to subscribe again using the original handler and options. It is intended for backends where resubscribing is a meaningful recovery action (connection drop, consumer invalidation, etc).

Notes:

- it waits for `sub.Done()` before re-subscribing
- it stops retrying on `broker.AlreadySubscribed`

## AutoAck and manual ack

When you disable AutoAck with `broker.DisableAutoAck()`, your handler must call `event.Ack()`. If you add idempotency and plan to skip replays, make sure replays get acknowledged too (see [idempotency/README.md](../idempotency/README.md)).
