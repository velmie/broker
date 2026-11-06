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
	return coordinator.RunService(ctx)
}
```

A factory binds a source locally and returns a consumer without opening receive
resources or starting workers. Registration names must be unique and use letters,
digits, dots, dashes or underscores. Native SDK clients belong to application
wiring and must remain open until all consumers using them return.

## Startup

`RunService` creates and validates every registered consumer and checks that each
implements `broker.ServiceConsumer` before starting any run. Unsupported
registrations return a `RegistrationError` with operation `validate`.
Factory or local capability errors therefore prevent processing across the group.
Each consumer checks its remote binding inside its own run. There is no shared
remote-readiness barrier: one source may already be processing when another
reports a remote startup failure.

A failed run cancels its peers. A successful finite source leaves peers running.
The coordinator joins all results and preserves independent causes through
`errors.Join`. `RegistrationError` identifies the registration and operation.

| Managed adapter | `RunService` support |
| --- | --- |
| NATS JetStream | Yes |
| SQS | Not yet; use `Run` |
| Azure Service Bus | Not yet; use `Run` |
| SNS | Publisher only |

Custom consumer factories can opt in by returning a consumer that implements
`ServiceConsumer`. A mixed group requires every registration to implement it.
Existing `Run` remains available and retains its cancellation result.

## Shutdown

Cancel the run context and wait for `RunService` to return. Each consumer's
`ShutdownPolicy` controls its admitted callback:

| Policy | Behavior |
| --- | --- |
| Zero value / `ShutdownGraceful` | Stop admitting work and let the active callback finish |
| Graceful with a positive `GracePeriod` | Request callback cancellation when the grace period expires |
| `ShutdownCancel` | Request callback cancellation immediately; requires a zero grace period |

Cancellation is cooperative. A grace period cannot terminate an unresponsive
callback, SDK call or instrumentation hook. Renewal and cleanup still join before
the consumer returns. Close borrowed clients and flush telemetry afterward.

| Event | `RunService` result |
| --- | --- |
| Finite success or empty coordinator | `nil` |
| Parent canceled, deadline reached, or custom cancellation cause, with successful work and cleanup | `nil` |
| Immediate stop suppresses an unadmitted terminal operation | `nil` when the callback cooperates; delivery remains pending |
| Grace period expires | An error matching the adapter's grace-expiry cause |
| Callback, startup, receive, renewal, settlement, cleanup or observer fails independently | An error retaining its original causes |
| One registration fails while peers stop cleanly | The failed registration's error |

An independent operation timeout remains a failure, even when the parent stops.
The parent cause remains available from `context.Cause(ctx)` and adapter stop
observations. Do not discard a non-nil result because it matches
`context.Canceled`: it may also contain an independent failure.

For NATS, a callback returning exactly its canceled delivery context's `ctx.Err()`
after adapter-initiated cancellation is cooperative. Wrapped and joined callback
errors are failures. An application that needs to distinguish business outcomes
from cancellation should classify them at the callback boundary. The comparison
uses the final Broker handler error after middleware. `NewTypedHandler` wraps its
business callback errors; `NewTypedDeliveryHandler` leaves them unwrapped.

For explicitly classified transient run failures, wrap the factory with
`WithConsumerRecovery`. Replacements are sequential and start only after the
previous run has joined. See [recovery boundaries](error-handling.md#consumer-recovery).
The wrapper exposes `RunService` only when its initial consumer supports it, and
checks each replacement before starting it. Stopping during recovery retains
previous failed generations in the returned error. A later successful generation
returns `nil`; previously recovered failures remain in the restart log.
