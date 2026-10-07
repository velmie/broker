# Broker

Broker is a messaging abstraction for Go. It separates application logic from
transport APIs through typed publishers, handlers, middleware and coordinated
shutdown.

Adapters connect these contracts to a messaging system and handle delivery,
acknowledgments and renewal. Use a supplied adapter or implement one for another
transport.

The core uses only the standard library. Transport adapters, OpenTelemetry and
idempotency are separate modules, so applications can choose what they need.

## Publish and handle a message

This example publishes and handles an order in memory, so it needs no running
message broker.

Save this as `main.go` in a separate directory within a Go module that depends on
Broker:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/velmie/broker"
)

type Order struct {
	ID       string `json:"id"`
	Quantity int    `json:"quantity"`
}

func main() {
	ctx := context.Background()
	var message broker.Message

	// PublisherFunc lets a function serve as a publisher. This one stores the message.
	destination := broker.PublisherFunc(func(_ context.Context, msg broker.Message) error {
		message = msg
		return nil
	})
	// The typed publisher encodes orders and attaches an application-owned ID.
	publish := broker.NewTypedPublisher(broker.EncoderFunc(json.Marshal), destination,
		func(_ context.Context, order Order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: order.ID}, nil
		})
	if err := publish(ctx, Order{ID: "order-1", Quantity: 2}); err != nil {
		log.Fatal(err)
	}

	// The handler decodes JSON before calling the application function.
	handler := broker.NewTypedHandler(broker.DecodeJSON[Order],
		func(_ context.Context, order Order) error {
			fmt.Printf("created order %s: %d items\n", order.ID, order.Quantity)
			return nil
		})
	// NewDelivery supplies the message and source for a direct handler call.
	result, err := handler.Handle(ctx, broker.NewDelivery(message, "orders"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("result: %T\n", result)
}
```

Run it from that directory:

```sh
go run .
```

```text
created order order-1: 2 items
result: broker.Handled
```

`NewTypedPublisher` attaches your message ID and headers, encodes the value and
calls the publisher. `NewTypedHandler` decodes each delivery and calls your
function. A nil error returns `Handled`, asking the adapter to acknowledge or
complete the delivery. The in-memory example stops at that request.

Try replacing `message.Body` with JSON where `quantity` is a string before calling
`Handle`. Decoding fails before the application function runs. Domain validation,
such as requiring a positive quantity, belongs in the application function.

## Wire an application

Create the native SDK client and provision the queue, stream or subscription in
your application. Bind a publisher to a destination and a consumer to a source.
Register a consumer factory and its handler with `Coordinator`, then call `Run(ctx)`.
Cancel the context to stop receiving, wait for `Run` to return, and only then
close the SDK client. The [order wiring](natsjs/cmd/orders/main.go) shows this
lifecycle end to end.

Add policies where the application needs them:

- `NewReplyHandler` publishes a response before requesting acknowledgment. Its
  resolver selects an approved destination and a stable response ID.
- `WithRedelivery` requests transport redelivery for business errors selected by
  your classifier. Choose errors that are safe to retry after partial work.
- `Handler.WithMiddleware` and `WrapPublisher` compose processing and publication
  wrappers. In `a, b` order, `a` is outermost.
- [Idempotency](idempotency/README.md) skips repeated processing after a recorded
  success. [OpenTelemetry](otelbroker/README.md) traces processing and publication.

An unhandled error stops the consumer after its active work joins. Successful
processing and a confirmed acknowledgment are separate outcomes. Messages can
be redelivered, so application effects must tolerate duplicates.

## Diagnostics

`LogProcessing` records callback failures through `slog`. Use `DescribeError` at
your logger's serialization boundary to retain error types, causes, stages and
fields without arbitrary error text. Application and adapter describers add safe
details for their own errors. Your logging adapter owns redaction of
application-specific data. See the [configured logger](natsjs/cmd/native/diagnostics.go).

## Modules

| Module | Use it for |
| --- | --- |
| `github.com/velmie/broker` | Typed handlers, publishers, replies, middleware and coordinated shutdown |
| [`natsjs/v3`](natsjs/README.md) | NATS JetStream publication and durable pull consumers |
| [`sqs`](sqs/README.md) | Amazon SQS publication and consumption |
| [`sns`](sns/README.md) | Amazon SNS publication and notification decoding |
| [`azuresb`](azuresb/readme.md) | Azure Service Bus queues and subscriptions |
| [`otelbroker`](otelbroker/README.md) | OpenTelemetry processing and publication spans |
| [`idempotency`](idempotency/README.md) | Completion markers and replay handling with `idempo` |

Module paths below the root use the `github.com/velmie/broker/` prefix. Each guide
covers its dependencies, configuration, delivery guarantees and runnable examples.
Continue with the [documentation index](docs/README.md), or use the
[migration guide](docs/migration.md) for an existing application.
