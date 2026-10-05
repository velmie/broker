# Compose middleware

Processing middleware wraps a `HandlerFunc`. Publication middleware wraps a
destination-bound `Publisher`. Both receive the explicit context and preserve
the next operation's result unless their documented policy changes it.

`handler.WithMiddleware(a, b)` produces `a(b(handler))`. The first argument is
outermost. `WrapPublisher(publisher, a, b)` uses the same order. A later call wraps
the already composed value.

## Processing policies

Place logging around panic recovery and the typed handler:

```go
package orders

import (
	"context"
	"log/slog"
	"time"

	"github.com/velmie/broker"
)

type Order struct {
	ID string `json:"id"`
}

func NewHandler(
	process func(context.Context, Order) error,
	retryable func(error) bool,
	logger *slog.Logger,
) broker.Handler {
	handler := broker.NewTypedHandler(broker.DecodeJSON[Order], process,
		// Request native redelivery only for business failures accepted by this policy.
		broker.WithRedelivery(time.Second, retryable),
	)
	return handler.WithMiddleware(
		broker.LogProcessing(logger, broker.ProcessingLogConfig{}),
		broker.RecoverPanics(),
	)
}
```

The logger's handler must redact application-specific errors while retaining
useful causes and fields. See [diagnostics](error-handling.md#structured-diagnostics).

| Helper | Behavior |
| --- | --- |
| `WithRedelivery` | Maps classified `StageHandle` failures to `RetryAfter`; never calls the business function again itself |
| `WithKeepAlive` | Declares adapter-owned renewal before decoding starts |
| `RecoverPanics` | Converts processing panics into errors that retain the panic evidence |
| `LogProcessing` | Records failed processing and requested retries; successful callbacks are opt-in |
| `SkipOwnMessages` | Skips the next callback for the configured `Instance-Id` |

Options such as `WithRedelivery` and `WithKeepAlive` belong at handler
construction. Middleware can also declare requirements. Consumers validate the
whole requirement set before processing, including transport-specific limits.
Pass the documented concrete value forms, such as `Handled{}` and
`KeepAliveRequirement{...}`.

## Publication policies

Use `WrapPublisher` before passing the publisher to `NewTypedPublisher`.
`StampInstanceID` adds loop-prevention metadata, and `ValidateIDHeader` checks an
application ID convention. Wrappers must copy caller-owned storage before
changing it. Header rules are described in [message headers](headers.md).

[OpenTelemetry](../otelbroker/README.md) supplies processing and publication
wrappers. [Idempotency](../idempotency/README.md) supplies completion-marker
middleware. Place wrappers deliberately: an outer logger or span observes only
failures returned by the wrappers inside it.
