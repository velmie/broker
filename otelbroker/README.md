# OpenTelemetry middleware

`github.com/velmie/broker/otelbroker` traces Broker processing and publication.
It uses the OpenTelemetry API without initializing a provider, exporter, worker
or metrics pipeline. The core and transport adapters have no OpenTelemetry
dependency. API dependencies are declared in [go.mod](go.mod), and the selected
semantic conventions in [common.go](common.go).

## Wrap handlers and publishers

Add tracing to existing application wiring:

```go
package instrumentation

import (
	"go.opentelemetry.io/otel/propagation"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
)

func Trace(
	handler broker.Handler,
	publisher broker.Publisher,
	system, destination string,
) (broker.Handler, broker.Publisher) {
	// Select the wire propagation convention explicitly.
	propagator := otelbroker.WithPropagator(propagation.TraceContext{})
	handler = handler.WithMiddleware(
		otelbroker.TraceProcessing(system, propagator),
		broker.RecoverPanics(),
	)
	publisher = broker.WrapPublisher(publisher,
		otelbroker.TracePublishing(system, destination, propagator),
	)
	return handler, publisher
}
```

Supply the actual messaging system and the publisher's bound destination.
Processing derives its destination from `Delivery.Source()`. Use stable approved
names, because they become span names and attributes. This example selects W3C
TraceContext and does not propagate baggage.

`TraceProcessing` creates a consumer `process` span and preserves requirements,
dispositions and returned errors. `TracePublishing` creates a producer `send`
span, injects context into detached headers and covers the underlying publisher's
confirmation contract. Neither proves subsequent processing or acknowledgment.
Place `RecoverPanics` inside tracing to observe recovered failures.

## Context and propagation

At each invocation, `WithTracer` takes precedence. Otherwise a valid local parent
supplies its provider, including an unsampled parent; without one, the current
global provider is used. Selection happens before remote metadata extraction.
With an ambient span, processing is its child and links to extracted message
context. Without an ambient span, valid extracted context becomes its parent.
Context values and cancellation remain intact.

Propagation reads selected names case-insensitively only when there is one valid
text value. Duplicates, aliases and invalid text are ignored by tracing without
rejecting application processing. Injection replaces the selected propagator's
fields while preserving unrelated ordered and binary headers. Caller data stays
unchanged.

The propagator owns format and size limits. Application wiring owns trust policy
and whether to carry baggage. Propagation is correlation, not authorization.

## Attributes and errors

`WithSpanStartOptions` and `WithSpanNameFormatter` customize spans. The formatter
receives the operation, destination and read-only message. For per-message
attributes, put application middleware inside tracing and use
`trace.SpanFromContext(ctx).SetAttributes(...)` with approved values.

`WithErrorRecorder` receives the original error and active span. The default
records type and failure status without raw error text, bodies or header values.
Custom recorders must preserve useful safe field or entity details while
redacting application-specific data. Logical and correlation IDs require an
explicit export decision. Callbacks must support concurrency and must not panic.

## Observe native completion

Processing ends before the adapter settles a delivery. Connect adapter
`Observer.Begin` and `Observer.Record` hooks when native-operation spans are
needed. The [traced examples](examples/README.md) show delivery spans with separate
ACK, delete, renewal or completion observations. Successful processing can coexist
with an unknown native outcome. Avoid instrumenting the same operation twice.

The application owns providers, processors and exporters. Join consumer work,
then flush and shut down telemetry under a separate bounded context. Preserve
export and shutdown failures.

Runnable commands and cross-module tests live in [examples](examples/README.md).
