# Idempotency with NATS

This command shows a stored completion marker surviving NATS redelivery without
repeating the business effect. It creates temporary JetStream topology and
removes it after confirming acknowledgment and an empty consumer.

Prepare the [development workspace](../../docs/development.md#local-workspace)
and start a disposable NATS server with JetStream. From `idempotency/examples`:

```sh
go run ./cmd/orders -url nats://127.0.0.1:4222
```

The command commits one marker, suppresses the first acknowledgment and lets
native redelivery replay the marker. Expected events include
`completion marker committed`, `stored completion replayed`,
`native acknowledgment confirmed` and `native consumer empty`.

The in-memory store loses markers on exit. Select a shared durable store and
verify its atomicity and cleanup contract for deployment.

Run the integration tests from the same directory:

```sh
go test -race -count=1 ./...
```

Tests own a temporary NATS container. To use an existing disposable server, set
`NATS_TEST_URL`. They also cover an uncertain acknowledgment after a connection
closes, one business effect and replay from the retained marker.
