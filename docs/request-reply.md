# Request Reply

The core package supports a simple request reply pattern via `CreateReplyHandler`.

## How it works

- A request message includes a reply topic in `Header["Reply-To"]`.
- The consumer processes the request and publishes a response to that topic.
- The response includes `Header["Reply-Message-Id"]` set to the request message ID.
- `Correlation-Id` is propagated when present.

## Consumer side

```go
type Request struct {
	Name string `json:"name"`
}

type Response struct {
	Greeting string `json:"greeting"`
}

handler := broker.CreateReplyHandler(
	broker.DecoderFunc(json.Unmarshal),
	broker.EncoderFunc(json.Marshal),
	pub,
	func(ctx context.Context, req Request) (Response, error) {
		return Response{Greeting: "Hello " + req.Name}, nil
	},
	broker.PanicRecoveryMiddleware(),
)

_, err := sub.Subscribe("greeting.request", handler)
if err != nil {
	// handle error
}
```

If "Reply-To" is missing, the handler processes the request and returns without publishing a response.

## Requester side

The requester chooses a reply topic, subscribes to it, publishes a request, then waits for the response.

```go
replyTopic := "greeting.reply." + instanceID
reqID := "req-123"

replyCh := make(chan *broker.Message, 1)
_, _ = sub.Subscribe(replyTopic, func(e broker.Event) error {
	if e.Message().Header.GetReplyMessageID() != reqID {
		return nil
	}
	replyCh <- e.Message()
	return nil
})

req := broker.NewMessageWithContext(ctx)
req.ID = reqID
req.Header.SetReplyTo(replyTopic)
req.Body = []byte(`{"name":"Gopher"}`)

_ = pub.Publish("greeting.request", req)

select {
case resp := <-replyCh:
	_ = resp // decode and use
case <-ctx.Done():
	// timeout
}
```

This pattern is transport dependent. Some backends make it easy (for example NATS), while others require dedicated reply topics or queues.

