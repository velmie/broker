# Connect an application

Broker separates application processing from message transport. A publisher is
bound to a destination. A consumer receives deliveries from a source and passes
them to a handler. The handler returns a delivery decision, which the adapter
translates into its native acknowledgment or completion operation.

The [README example](../readme.md#publish-and-handle-a-message) runs this flow in
memory. For a messaging system, use the core and the appropriate
[adapter module](backends.md). Their manifests specify compatible dependencies.

## Define the application handler

Keep the business function independent of the transport. Wrap an
application-supplied `save` function:

```go
package orders

import (
	"context"
	"errors"

	"github.com/velmie/broker"
)

type Order struct {
	ID       string `json:"id"`
	Quantity int    `json:"quantity"`
}

func NewHandler(save func(context.Context, Order) error) broker.Handler {
	return broker.NewTypedHandler(broker.DecodeJSON[Order],
		func(ctx context.Context, order Order) error {
			if order.Quantity <= 0 {
				return errors.New("quantity must be positive")
			}
			return save(ctx, order)
		})
}
```

Decoding happens before the callback. A nil callback error requests `Handled`.
An error stops the consumer unless an explicit policy maps it to a supported
disposition. Add [redelivery](middleware.md) only for failures that are safe to
retry after any partial application work.

Use `NewTypedDeliveryHandler` when the callback needs the source, headers or an
explicit disposition. For a direct call, construct a delivery with
`broker.NewDelivery(message, "orders")`. It shares the message's storage and has
no transport-specific capabilities.

## Bind and run

1. Create the native SDK client and provision the required queue, stream or
   subscription. The application owns credentials, topology and client lifetime.
2. Bind an adapter publisher to a destination. Wrap it with `NewTypedPublisher`
   to encode application values and attach application-owned metadata.
3. Bind a consumer locally through a `ConsumerFactory`. Register the factory and
   handler with a [Coordinator](coordinator.md), then call `Run(ctx)`.
4. Cancel the run context to request shutdown. Wait for `Run` to return before
   closing clients or telemetry providers.

The runnable [NATS orders application](../natsjs/README.md#run-the-example)
shows the complete wiring. Other adapter guides provide equivalent examples.

Consumers own delivery acknowledgment and renewal. A successful callback,
successful publication and confirmed acknowledgment are separate outcomes.
Keep [logical message IDs](message-identity.md) stable across retries, and make
application effects tolerate duplicates.
