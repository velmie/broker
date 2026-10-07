# Coordinate consumers

`Coordinator` starts a group of consumers, requests peer shutdown when one run
fails, and waits for every run to finish. It is single-use.

This wiring function accepts an application-configured factory and handler:

```go
package wiring

import (
	"context"

	"github.com/velmie/broker"
)

func Run(ctx context.Context, factory broker.ConsumerFactory, handler broker.Handler) error {
	coordinator := broker.NewCoordinator()
	if err := coordinator.Add("orders", factory, handler); err != nil {
		return err
	}
	return coordinator.Run(ctx)
}
```

A factory binds a source locally and returns a consumer without opening receive
resources or starting workers. Registration names must be unique and use letters,
digits, dots, dashes or underscores. Native SDK clients belong to application
wiring and must remain open until all consumers using them return.

## Startup

`Run` creates and validates every registered consumer before starting any run.
Factory or local capability errors therefore prevent processing across the group.
Each consumer checks its remote binding inside its own `Run`. There is no shared
remote-readiness barrier: one source may already be processing when another
reports a remote startup failure.

A failed run cancels its peers. A successful finite source leaves peers running.
The coordinator joins all results and preserves independent causes through
`errors.Join`. `RegistrationError` identifies the registration and operation.

## Shutdown

Cancel the run context and wait for `Run` to return. Each consumer's
`ShutdownPolicy` controls its admitted callback:

| Policy | Behavior |
| --- | --- |
| Zero value / `ShutdownGraceful` | Stop admitting work and let the active callback finish |
| Graceful with a positive `GracePeriod` | Request callback cancellation when the grace period expires |
| `ShutdownCancel` | Request callback cancellation immediately; requires a zero grace period |

Cancellation is cooperative. A grace period cannot terminate an unresponsive
callback, SDK call or instrumentation hook. Renewal and cleanup still join before
the consumer returns. Close borrowed clients and flush telemetry afterward.

For explicitly classified transient run failures, wrap the factory with
`WithConsumerRecovery`. Replacements are sequential and start only after the
previous run has joined. See [recovery boundaries](error-handling.md#consumer-recovery).
