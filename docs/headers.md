# Message headers

`Message.Headers` is an ordered `[]Header`. Each header contains a name and a
`[]byte` value. The core preserves case, duplicates and binary data. Adapters
document restrictions imposed by their native representation.

This function constructs application metadata without depending on a transport:

```go
package orders

import "github.com/velmie/broker"

func Metadata(id, correlationID string) broker.MessageMetadata {
	return broker.MessageMetadata{
		ID: id,
		Headers: []broker.Header{
			{Name: "Content-Type", Value: []byte("application/json")},
			{Name: "Correlation-Id", Value: []byte(correlationID)},
		},
	}
}
```

Headers carry application conventions, not universal transport configuration.
Broker does not automatically add timestamps, correlation IDs or reply routes.

## Text conventions

Core helpers that read a single text header match its name case-insensitively.
Duplicate values, including differently cased aliases, and invalid UTF-8 fail
before the operation they protect.

| Helper | Convention |
| --- | --- |
| `WithCorrelationIDBinding` | After decoding, passes an optional `Correlation-Id` to a DTO implementing `SetCorrelationID(string)` |
| `StampInstanceID` | Replaces `Instance-Id` on a detached copy of publication headers |
| `SkipOwnMessages` | Requests `Handled` without calling the next handler when `Instance-Id` matches the configured instance |
| `ValidateIDHeader(name)` | Checks that an optional text header agrees with `Message.ID`; does not add or require it |

These values are sender-controlled. Correlation and loop prevention do not
establish identity or authorization. Tracing and idempotency have their own
documented rules for selecting headers.

## Ownership

Received message data is detached from the native delivery and may be retained.
Publishing treats the message and all its slices as read-only until the call
returns. A middleware that changes headers must copy the storage it changes.
`NewDelivery` wraps existing application data without copying it, so avoid
concurrent mutation while a handler uses that delivery.

See [message identity](message-identity.md) for transport ID mapping and
[migration](migration.md#preserve-message-contracts) for existing wire formats.
