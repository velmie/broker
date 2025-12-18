# Getting Started

"broker" standardizes publish/subscribe message handling behind small interfaces. Your application code depends on `broker.Publisher` and `broker.Subscriber`, while the transport specific code lives in a backend module.

## Core concepts

- `Publisher.Publish(topic, msg)` sends a `*broker.Message`
- `Subscriber.Subscribe(topic, handler, opts...)` registers a `broker.Handler`
- `broker.Event` carries `Topic()`, `Message()`, and `Ack()`
- `broker.Message` carries `ID`, `Header`, `Body`, and a `context.Context`

## Publish

```go
msg := broker.NewMessageWithContext(ctx)
msg.ID = "order-123"
msg.Body = []byte(`{"orderId":"123"}`)
msg.Header.SetCorrelationID("corr-1")

if err := pub.Publish("orders.created", msg); err != nil {
	// handle error
}
```

## Subscribe with typed handlers

`CreateHandler` decodes `Message.Body` into a typed value and passes it to your function.

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
)

_, err := sub.Subscribe("orders.created", handler)
if err != nil {
	// handle error
}
```

## Acknowledgment and retries

- Default behavior is AutoAck: if the handler returns `nil`, the message is acknowledged.
- Disable it with `broker.DisableAutoAck()` and call `event.Ack()` yourself.
- Attach an error handler to log, delay, and resubscribe (for example `broker.WithDefaultErrorHandler(sub, logger)`).

## Choosing a backend

Pick a module that matches your transport:

- NATS JetStream: [natsjs/README.md](../natsjs/README.md)
- AWS SQS/SNS: `../sqs/`, `../sns/`
- Azure Service Bus: `../azuresb/`
