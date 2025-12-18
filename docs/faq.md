# FAQ

## What should I use for Message.ID

Use a stable identifier for the business operation. Don't generate a new random ID per retry. Many transports and middlewares rely on stable IDs for dedupe or idempotency.

## Why do I see duplicates

Common causes:

- your producer sets a new `Message.ID` every time
- you disabled AutoAck and forgot to call `event.Ack()`
- your handler has side effects but is not idempotent and the broker redelivers on timeouts

Use `idempotency.Middleware(engine, ...)` for consumer dedupe when your transport does not provide enough guarantees.

## Loopback prevention does not work

`LoopbackPreventionMiddleware` checks the "Instance-Id" header. Make sure your publisher sets it, for example:

```go
pub = broker.PublishWithInstanceID(instanceID)(pub)
```

## Which middleware order is correct

The last middleware passed to `CreateHandler` becomes the outermost wrapper. If ordering matters, write a test that asserts the behavior.

## Request reply is timing out

Make sure:

- the request has `Header["Reply-To"]` set
- the requester is subscribed to the reply topic before publishing
- you correlate replies using `Reply-Message-Id` or `Correlation-Id`

