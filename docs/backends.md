# Backends

This repo contains multiple transport modules. Your application code should depend on `broker.Publisher` and `broker.Subscriber`, then choose a backend module for production wiring.

## NATS JetStream

- Module: `github.com/velmie/broker/natsjs/v2`
- Best for: low latency pub/sub, consumer groups, request/reply style workflows
- Message ID: written to NATS header `Nats-Msg-Id` (JetStream de-dup)
- Tests: integration tests use Docker (`ory/dockertest`)

See [natsjs/README.md](../natsjs/README.md).

## AWS SQS

- Module: `github.com/velmie/broker/sqs`
- Best for: durable queues, simple at-least-once processing
- Ack: `event.Ack()` deletes the message
- Message ID: stored via `Header["id"]` in message attributes (publisher calls `broker.SetIDHeader`)

## AWS SNS

- Module: `github.com/velmie/broker/sns`
- Best for: fan-out pub/sub (often combined with SQS subscriptions)
- FIFO topics: `MessageDeduplicationId` is set to `Message.ID`

## Azure Service Bus

- Module: `github.com/velmie/broker/azuresb`
- Best for: Azure native queues/topics with lock and delivery semantics
- Ack: `event.Ack()` completes the message (or enable AutoAck)
- Message ID: mapped to Service Bus MessageID

See [azuresb/readme.md](../azuresb/readme.md).

## Add-ons (not transports)

- OpenTelemetry: `github.com/velmie/broker/otelbroker`
- Idempotent consumers: `github.com/velmie/broker/idempotency`
