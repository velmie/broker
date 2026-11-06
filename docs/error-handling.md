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

Pass the original error to a standard `slog` handler. For example:

```go
package logging

import (
	"io"
	"log/slog"

)

func LogFailure(output io.Writer, err error) {
	logger := slog.New(slog.NewJSONHandler(output, nil))
	logger.Error("consumer failed", slog.Any("error", err))
}
```

The error text identifies the operation and includes its causes. Log a failure
once at the boundary that can explain its
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
For `RunService`, stopping during backoff retains previous failures without adding
the parent stop cause. A successful later generation returns nil and leaves the
earlier failures in the restart log. See [coordinator shutdown](coordinator.md#shutdown).
