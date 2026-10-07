# Errors and diagnostics

A non-nil handler error suppresses its disposition and stops the consumer run.
Broker preserves original causes through wrapping and joining. Inspect them with
`errors.Is` and `errors.As`, and keep independent cleanup failures visible.

## Processing stages

| Failure | Context |
| --- | --- |
| Decoder or nil pointer DTO | `DecodeError` |
| Metadata binding | `StageMetadata` |
| Ordinary typed business callback | `StageHandle` |
| Reply destination or response identity | `StageResolve` |
| Encoding | `StageEncode` |
| Publication | `StagePublish` |

`NewTypedDeliveryHandler` leaves callback errors unclassified. If such a callback
deliberately participates in `WithRedelivery`, it must return an appropriate
`StageError` itself. A stage identifies what failed; it does not establish retry
safety.

`WithRedelivery` selects explicitly staged business errors and requests native
redelivery. Decode, reply-resolution and publication failures do not enter its
classifier. The classifier must account for partial effects, including any
publication performed inside the business function. `RetryAfter.Cause` retains
the reason even when the returned error is nil.

## Structured diagnostics

`LogProcessing` passes original errors to `slog`, together with processing stage,
source and duration. Successful callbacks are silent by default. It records
processing outcomes; adapter observers report later acknowledgment, renewal or
publication outcomes.

Use `DescribeError` at the logger's serialization boundary. For example:

```go
package logging

import (
	"io"
	"log/slog"

	"github.com/velmie/broker"
)

func NewLogger(output io.Writer, describe ...func(error) map[string]any) *slog.Logger {
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if err, ok := attr.Value.Any().(error); ok {
				return slog.Any(attr.Key, broker.DescribeError(err, describe...))
			}
			return attr
		},
	}))
}
```

The projector retains error types, cause relationships and known Broker or
standard-library fields, including JSON field/type failures. It bounds depth,
node count and scalar values. It omits arbitrary error text, bodies, headers,
addresses and panic values.

Supply application describers for approved business details and adapter
describers for native codes. Each describer inspects one original node. The first
non-nil field map wins; traversal of its causes continues. For NATS, pass
`natsjs.ErrorFields` after any application-specific describer. The
[native command logger](../natsjs/cmd/native/diagnostics.go) shows this composition.

Bounds prevent oversized records, not disclosure of sensitive values. The
logging adapter still owns redaction of custom fields, other attributes and
application-specific errors. Preserve the affected operation, field or safe
entity identifier. Log a failure once at the boundary that can explain its
outcome, with severity appropriate to that outcome. A `slog` handler must report
its own sink failures because the logger does not return them.

## Consumer recovery

`WithConsumerRecovery` replaces a failed consumer run only when the supplied
classifier accepts its complete error tree. `MaxRestarts` bounds replacements;
`Backoff` separates them. The previous run must join before a fresh factory is
called. Factory and validation failures are terminal.

Classify only failures for which another receiving generation is safe. Do not
restart blindly after business failures, uncertain settlement or joined cleanup
errors. Recovery does not replay callbacks itself, but the server may redeliver
messages. Scheduled replacements retain their original errors in the configured
logger. Terminal failure preserves all failed generation causes.
