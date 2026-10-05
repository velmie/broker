# Frequently asked questions

## Does Broker hide every transport difference?

It shares application-facing publication and processing contracts. Native
configuration, capabilities, metadata and delivery guarantees remain explicit.
See the [adapter comparison](backends.md).

## Must a handler implement an interface?

No. `NewTypedHandler` accepts an ordinary `func(context.Context, T) error`.
Use `NewTypedDeliveryHandler` for metadata or explicit dispositions. Use
`NewDelivery(message, source)` when calling a handler directly with application
data. Native consumers construct their own deliveries automatically.

## When is a delivery acknowledged?

The adapter acts on the returned disposition after processing. `Handled` requests
successful transport progress. It does not prove that a later acknowledgment
succeeded. In Azure ReceiveAndDelete mode, removal happens during receipt and
there is no later completion request.

## Does returning an error retry the message?

A non-nil error stops the run without applying its disposition. Use
`WithRedelivery` or an explicit supported `RetryAfter` for an intentional native
redelivery policy. Server redelivery after expiry or restart is a separate event.

## Who supplies message IDs?

The application supplies stable logical IDs. Broker does not generate them or
promise exactly-once business execution. See [identity](message-identity.md) and
[idempotency](../idempotency/README.md).

## How do middleware and shutdown compose?

The first middleware argument is outermost. Cancel the coordinator's context to
request shutdown, wait for `Run` to join, then close shared clients. Consumer
shutdown policies control how admitted callbacks receive cancellation. See
[middleware](middleware.md) and [coordinator](coordinator.md).

## Where should transport diagnostics go?

Use the existing structured logger. `LogProcessing` records handler outcomes;
adapter observers report native operations. Preserve original causes and
affected fields through application-aware redaction at serialization time. See
[error handling](error-handling.md).
