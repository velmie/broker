# Broker docs

Start here if you want to understand the library in under a minute:

- [Getting Started](getting-started.md)
- [Headers](headers.md)
- [Middleware](middleware.md)
- [Error Handling](error-handling.md)
- [Subscription Coordinator](coordinator.md)
- [Request Reply](request-reply.md)
- [Backends](backends.md)
- [FAQ](faq.md)

## Modules

This repository is multi-module. Each backend or add-on has its own `go.mod`.

- Core interfaces and helpers: `github.com/velmie/broker` (repo root)
- NATS JetStream backend: `github.com/velmie/broker/natsjs/v2` (see [natsjs/README.md](../natsjs/README.md))
- AWS SQS backend: `github.com/velmie/broker/sqs`
- AWS SNS backend: `github.com/velmie/broker/sns`
- Azure Service Bus backend: `github.com/velmie/broker/azuresb`
- OpenTelemetry middleware: `github.com/velmie/broker/otelbroker`
- Idempotency middleware: `github.com/velmie/broker/idempotency` (see [idempotency/README.md](../idempotency/README.md))

## Examples

Runnable examples live in `../_examples/`.

If you are looking for end-to-end service wiring, also check how this library is used in a real project: `wallet-business-users2/internal/broker` (local path in this workspace).
