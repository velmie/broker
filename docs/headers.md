# Headers

`broker.Header` is a `map[string]string` attached to every `broker.Message`. It is the main place for transport independent metadata like correlation, reply routing, and loopback prevention.

## Standard keys

Core helpers live in `header.go`:

- `Correlation-Id`: trace or correlate a flow across services.
- `Instance-Id`: identifies the producer instance, used for loopback prevention.
- `Reply-To`: topic or queue where a response should be published.
- `Reply-Message-Id`: request message ID attached to a response.
- `Created-At`: Unix timestamp as string.

Use the typed helpers instead of raw strings:

```go
msg.Header.SetCorrelationID("corr-1")
msg.Header.SetInstanceID(instanceID)
msg.Header.SetReplyTo("reply.topic")
msg.Header.SetReplyMessageID("req-123")
msg.Header.SetCreatedAt(time.Now().Unix())
```

## Message ID and the "id" header

`broker.Message.ID` should be a stable, deterministic identifier for the business operation. Several backends use it for dedupe or routing.

`broker.SetIDHeader(msg)` copies `Message.ID` into `Header["id"]` when missing. Some backends rely on this to preserve IDs across transports (for example SQS message attributes).

## Correlation ID into typed payloads

If your decoded message type implements:

```go
type CorrelationIDAware interface {
	SetCorrelationID(id string)
}
```

`CreateHandler` sets the correlation ID from `Header["Correlation-Id"]` automatically.

## Security note

Headers often get persisted or logged by brokers and middleware. Don't put secrets in headers. When using `LoggingMiddleware`, prefer logging body only on errors and redact headers via `WithLogHeaderFunc`.

