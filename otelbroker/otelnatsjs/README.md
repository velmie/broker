# OpenTelemetry NATS JetStream observer

`github.com/velmie/broker/otelbroker/otelnatsjs` connects native NATS JetStream
consumer observations to OpenTelemetry metrics and structured logs. It borrows
the meter and logger. The application owns the consumer, meter provider,
exporters and their shutdown.

Attach the observer when configuring the consumer, and trace its handler to
correlate settlement logs with the processing span:

```go
package instrumentation

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
	"github.com/velmie/broker/otelbroker"
	"github.com/velmie/broker/otelbroker/otelnatsjs"
)

func Configure(
	config *natsjs.ConsumerConfig,
	handler broker.Handler,
	meter metric.Meter,
	logger *slog.Logger,
) (broker.Handler, error) {
	observer, err := otelnatsjs.NewObserver(meter, logger)
	if err != nil {
		return broker.Handler{}, err
	}
	config.Observer = observer
	return handler.WithMiddleware(otelbroker.TraceProcessing("nats")), nil
}
```

`NewObserver` requires a meter and logger. A meter provider and a logger
handler configured for the application's telemetry pipeline are required for
production. Consumer configuration and lifecycle remain with the application.

## Metric

The counter `messaging.broker.settlement.attempts` has unit `{attempt}`. It
counts a completed native ACK, NAK or TERM request once, including a request
whose result is unknown or failed. Started and suppressed operations, processing,
fetching and renewal are not counted. A confirmed settlement records:

| `broker.processing.outcome` | Meaning |
| --- | --- |
| `handled` | ACK of a handled delivery was confirmed |
| `retry_requested` | NAK of a requested retry was confirmed |
| `reject_requested` | TERM of a rejection or native termination was confirmed |
| `failed` | Settlement failed, has an unknown result, or has no recognized confirmed disposition |

The remaining attributes are `messaging.system` (`nats`) and
`messaging.consumer.group.name` (the native durable or consumer name). Use stable,
operationally controlled consumer names. A confirmed retry is not a failed
settlement, and none of these counts proves exactly-once processing.

The observer logs startup, interrupted or failed fetches, and settlement failures.
Original errors reach the supplied structured logger. A processing context is
captured for later native observations without changing the context used for
native operations.

## Integration test

Run the module checks with `NATS_TEST_URL` set to a dedicated disposable NATS
server with JetStream enabled to exercise real consumer settlement. The test
creates and deletes its own stream. Without the variable, that integration test
skips and the deterministic unit tests still run.
