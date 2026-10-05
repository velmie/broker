# Idempotency middleware

`github.com/velmie/broker/idempotency` connects Broker handlers to an
application-owned [`idempo` engine](https://github.com/velmie/idempo). It stores a
completion marker after successful processing and skips that processing when
the same key and fingerprint return. The consumer still owns acknowledgment and
redelivery. The middleware depends on Broker core and idempo. Dependencies are
declared in [go.mod](go.mod).

## Wrap a handler

Create the engine with a store, lock lease and result retention suitable for the
application. The middleware borrows the engine and never closes its store.
Handle middleware configuration errors before consumption starts:

```go
package orders

import (
	"context"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
)

type Order struct {
	Amount int `json:"amount"`
}

func NewHandler(engine *idempo.Engine, save func(context.Context, Order) error) (broker.Handler, error) {
	deduplicate, err := idempotency.Middleware(engine,
		idempotency.WithRequireKey(true),
		idempotency.WithKeyPrefix("billing:"),
		idempotency.WithCommitTimeout(2*time.Second),
	)
	if err != nil {
		return broker.Handler{}, err
	}
	return broker.NewTypedHandler(broker.DecodeJSON[Order], save).
		WithMiddleware(broker.RecoverPanics(), deduplicate), nil
}
```

The first middleware is outermost. Put logs or tracing outside idempotency to
observe replays and acquisition failures. Outer `RecoverPanics` converts panic
evidence after the middleware has attempted cleanup. Native requirements are
preserved.

## Processing and settlement

Only concrete `broker.Handled{}` with no error commits an empty `idempo.Response`.
The marker stores no application response, body or headers. A replay requests
`Handled` without calling the next handler. An optional replay hook receives an
independent copy of the stored response.

Other dispositions, nil results and handler errors release the lease without
committing. Unlock failures join the original error and suppress settlement.
A processing panic also releases the lease and propagates its evidence, together
with any cleanup failure.

Once stored, a marker survives an uncertain native ACK, Delete or Complete.
A later delivery can replay it and request settlement again. Native operations
are not retried by this middleware.

Concurrent duplicates return `idempo.ErrInProgress` by default. Engine options
can enable bounded waiting. If a waiting entry disappears with
`idempo.ErrKeyNotFound`, the middleware attempts acquisition once more. Conflicts
and store failures preserve their original causes.

## Keys and fingerprints

The default stored key is `KeyPrefix + Delivery.Source() + ":" + key`. The message
key comes from trimmed `Message.ID`, then the exact `Idempotency-Key` text header.
A missing key passes through unless required. The default validator allows at
most 255 bytes in the resolved message key.

Choose a trusted, stable and unambiguous namespace for each independently keyed
service, operation or tenant. Validate keys or separate stores where concatenated
namespaces could collide. Key selection does not authenticate the sender.

The default fingerprint contains operation `consume`, source, a SHA-256 body hash
and an optional hash of selected text headers. Include all inputs that change
processing meaning. Selected headers need one exact-case UTF-8 value; duplicates
and case aliases fail. Other ordered or binary headers remain unchanged.

| Option | Purpose |
| --- | --- |
| `WithHeaderName` | Fallback header; an empty name disables fallback |
| `WithKeyPrefix`, `WithUseSourceInKey` | Stored namespace; source inclusion defaults to true |
| `WithRequireKey`, `WithKeyValidator` | Missing-key policy and trimmed message-key validation |
| `WithFingerprintHeaders` | Selected header names, trimmed, deduplicated and sorted |
| `WithFingerprintFunc` | Application-owned fingerprint and input validation |
| `WithCommitTimeout` | Positive budget for each Commit and Unlock call; default five seconds |
| `WithCommitErrorMode`, `WithCommitErrorHandler` | Commit-failure policy and observation |
| `WithOnReplay` | Callback with context, delivery and copied stored response |

`Middleware` validates configuration and copies selected header names.
`NewConfig` applies defaults without validation. Shared callbacks must support
concurrency. For retained records, preserve the
[existing namespace and fingerprint](../docs/migration.md#retained-idempotency-records).

## Commit failures

A commit error can occur after the business effect has succeeded and can leave
marker persistence uncertain.

| Mode | Behavior |
| --- | --- |
| `CommitFailClosedKeepLock` (default) | Return the error and leave the lease or accepted marker untouched |
| `CommitFailClosedUnlock` | Return the error and release the key; another execution can repeat the effect |
| `CommitFailOpen` | Require a commit-error hook, report the cause and release the key; request `Handled` only if cleanup succeeds |

Commit and cleanup use separate bounded contexts that preserve parent values
without inheriting cancellation. Stores must honor context. The budgets cannot
terminate unresponsive stores or callbacks.

`idempotency.Error` retains operation, field and original cause. The processing
boundary supplies the source. Pass original errors to the logging adapter, which
must redact keys, credentials and application values while retaining useful
context. See [diagnostics](../docs/error-handling.md#structured-diagnostics).

Hooks are synchronous and must not wait for their own consumer to stop. A replay
hook panic leaves the marker intact. A commit-error hook panic preserves the
lease or marker and propagates both panic evidence and the commit failure.

## Delivery limits

The engine does not renew its lock lease. Broker keepalive renews the broker
delivery only. Set `LockTTL` above the maximum processing time, including commit
margin, and `ResultTTL` to cover the promised retry window. Expiry permits another
owner or execution.

Effects, marker persistence and settlement are separate steps. A crash after an
effect but before its marker can repeat that effect. Use business idempotency or
transactional coordination where arbitrary database effects must be protected.

## Run the example

Prepare the [development workspace](../docs/development.md#local-workspace),
then run from `idempotency`:

```sh
go run ./cmd/orders
```

The [command](cmd/orders/main.go) uses an in-memory store and passes the same
message to a typed handler twice with `broker.NewDelivery`. The stored marker
skips the second call:

```text
handler calls: 1
total amount: 42
```

Markers disappear on exit. The [NATS example](examples/README.md) also verifies
real redelivery and acknowledgment. Its command and integration tests live in a
separate module.
