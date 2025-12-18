# Broker

"broker" is a small Go library that standardizes publish/subscribe message handling across different transports.
You write business logic once, then plug in a backend like NATS JetStream, AWS SQS/SNS, or Azure Service Bus.

Each module declares its supported Go version in its `go.mod`.

The core package focuses on:

- Minimal interfaces (`Publisher`, `Subscriber`, `Event`) that keep your handlers transport-agnostic
- Helpers for typed handlers (`CreateHandler`, `CreateReplyHandler`)
- A middleware model (`Middleware`) for cross-cutting concerns (logging, tracing, idempotency, panic recovery)

## Documentation

- [Docs index](docs/README.md)
- [Getting started](docs/getting-started.md)
- [Middleware](docs/middleware.md)
- [Coordinator](docs/coordinator.md)
- [Request reply](docs/request-reply.md)

## Installation

Core:

```bash
go get github.com/velmie/broker
```

Backends and add-ons are separate modules:

- NATS JetStream: `go get github.com/velmie/broker/natsjs/v2` (see [natsjs/README.md](natsjs/README.md))
- AWS SQS: `go get github.com/velmie/broker/sqs`
- AWS SNS: `go get github.com/velmie/broker/sns`
- Azure Service Bus: `go get github.com/velmie/broker/azuresb`
- OpenTelemetry middleware: `go get github.com/velmie/broker/otelbroker`
- Idempotency middleware: `go get github.com/velmie/broker/idempotency` (see [idempotency/README.md](idempotency/README.md))

## Quick Start

### Publish

```go
msg := broker.NewMessageWithContext(ctx)
msg.ID = "order-123" // stable ID matters for dedupe/idempotency in many backends
msg.Body = []byte(`{"orderId":"123"}`)
msg.Header.SetCorrelationID("corr-1")

if err := pub.Publish("orders.created", msg); err != nil {
	// handle error
}
```

### Subscribe (typed handler + middleware)

```go
type OrderCreated struct {
	OrderID string `json:"orderId"`
}

handler := broker.CreateHandler(
	broker.DecoderFunc(json.Unmarshal),
	func(ctx context.Context, m OrderCreated) error {
		// business logic
		return nil
	},
	broker.PanicRecoveryMiddleware(),
)

_, err := sub.Subscribe("orders.created", handler)
if err != nil {
	// handle error
}
```

## Acknowledgment and retries

- By default, subscriptions use AutoAck. If the handler returns `nil`, the message is acknowledged.
- Disable it with `broker.DisableAutoAck()` and call `event.Ack()` yourself.
- For retries/resubscribe flows, attach an error handler, for example `broker.WithDefaultErrorHandler(sub, logger)`.

## Examples

Runnable examples live in `_examples/`.

## Package layout

- Core interfaces and helpers: repo root (`package broker`)
- Transport backends: `natsjs/`, `sqs/`, `sns/`, `azuresb/`
- Add-ons: `otelbroker/` (tracing middleware), `idempotency/` (idempotent consumer middleware)
