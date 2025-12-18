# broker/idempotency

Idempotency middleware for `github.com/velmie/broker` consumers, backed by `github.com/velmie/idempo`.

It helps prevent duplicate side effects when a broker redelivers the same message (at-least-once delivery, retries, timeouts, consumer restarts).

## Installation

```bash
go get github.com/velmie/broker/idempotency
```

Optional Redis store (separate module):

```bash
go get github.com/velmie/idempo/redis
```

## Quick Start

```go
package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"
)

type Payload struct {
	UserID string `json:"userId"`
}

func consumeFn(ctx context.Context, p Payload) error {
	// Perform side effects exactly once (e.g. create user, charge card, etc.).
	return nil
}

func main() {
	store := memory.New()
	defer func() { _ = store.Close() }()

	engine := idempo.NewEngine(store, idempo.WithResultTTL(24*time.Hour))

	mw := idempotency.Middleware(engine)

	handler := broker.CreateHandler(broker.DecoderFunc(json.Unmarshal), consumeFn, mw)

	_ = handler // pass to subscriber.Subscribe(...)
}
```

On the first delivery the handler runs and a completion marker is stored.
On subsequent deliveries with the same key and fingerprint the handler is skipped and `nil` is returned.

## Key & Fingerprint

**Key resolution (per event):**

1. `broker.Message.ID` (preferred)
2. `broker.Message.Header[HeaderName]` (default header: `Idempotency-Key`)

**Store key (default):** `topic + ":" + key` (prevents collisions across topics). 
Add `WithKeyPrefix(...)` when multiple services share the same store.

**Fingerprint (default):** `topic + sha256(body) + optional selected headers`.
Reusing the same key with a different fingerprint returns `idempo.ErrKeyConflict`.

## Options

`Middleware` requires an engine: `idempotency.Middleware(engine, opts...)`.

Available options:

- `WithHeaderName(name)`: changes fallback header name (useful when upstream already uses a different key, e.g. `X-Idempotency-Key`).
- `WithKeyPrefix(prefix)`: namespaces keys in a shared store (e.g. `"prod:wallet:"`).
- `WithUseTopicInKey(bool)`: disable if you intentionally want a single key to dedupe across multiple topics.
- `WithRequireKey(bool)`: when `true`, missing keys fail fast with `idempo.ErrMissingKey`; useful to enforce contracts for critical topics.
- `WithKeyValidator(func(string) error)`: tighten validation for untrusted producers (length/charset/prefix checks).
- `WithFingerprintHeaders(keys...)`: include specific `broker.Message.Header` keys in the default fingerprint (use when body is empty or important routing context lives in headers).
- `WithFingerprintFunc(func(broker.Event) (idempo.Fingerprint, error))`: full control over fingerprinting (e.g. ignore non-deterministic fields or hash a canonicalized JSON payload).
- `WithCommitTimeout(d)`: bounds store I/O for persisting/releasing records.
- `WithCommitErrorMode(mode)`:
  - `CommitFailOpen` (default): treat commit failures as success (idempotency degrades for that key).
  - `CommitFailClosedUnlock`: return commit error and unlock, allowing a retry to re-run the handler.
  - `CommitFailClosedKeepLock`: return commit error and keep the lock until it expires (reduces stampede, delays retries).
- `WithCommitErrorHandler(func(event, err))`: hook for logging/metrics when storing the marker fails.
- `WithOnReplay(func(event, resp))`: hook for replay observability (metrics/logging). It can also be used to explicitly `event.Ack()` in manual-ack setups. Note: `resp` is just a stored completion marker in this adapter (it does not contain the original message or handler result).

## Manual Ack (when you disabled AutoAck)

If you subscribe with `broker.DisableAutoAck()`, make sure you:

1. `Ack()` in the handler on the first successful processing, and
2. also `Ack()` on replays (otherwise the broker may redeliver the same message forever).

```go
mw := idempotency.Middleware(
	engine,
	idempotency.WithOnReplay(func(e broker.Event, _ *idempo.Response) {
		_ = e.Ack()
	}),
)
```

## In-Progress Behavior

When the same key is being processed concurrently, the middleware will normally return `idempo.ErrInProgress` for the non-owner attempt. What happens next depends on the broker implementation (e.g. NAK/retry/visibility timeout).

If you prefer to wait instead of failing fast, enable it at the engine level:

```go
engine := idempo.NewEngine(store, idempo.WithWaitForInProgress(true))
```

## Production Use (Redis store)

```go
package main

import (
	"time"

	redisv9 "github.com/redis/go-redis/v9"

	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
	idemporedis "github.com/velmie/idempo/redis"
)

func main() {
	rdb := redisv9.NewClient(&redisv9.Options{Addr: "localhost:6379"})
	store := idemporedis.New(rdb)
	engine := idempo.NewEngine(store, idempo.WithResultTTL(7*24*time.Hour))

	mw := idempotency.Middleware(
		engine,
		idempotency.WithKeyPrefix("prod:wallet:"),
	)
	_ = mw // use in handler chain
}
```

Choose `ResultTTL` to cover your expected "duplicate window" (how long the broker can redeliver old messages).

## Engine Tuning (idempo)

This middleware delegates persistence and concurrency policy to the idempotency engine, so tune it via idempo options:

- `WithResultTTL(...)`: how long "already processed" markers are kept (your dedupe window).
- `WithLockTTL(...)`: must be longer than your worst-case handler runtime; if the lock expires early, duplicates may execute concurrently.
- `WithWaitForInProgress(true)`: wait for another in-progress consumer with the same key to finish instead of returning `ErrInProgress` immediately (use with care).

## Nuances & Caveats

- **You need a stable key.** Set `broker.Message.ID` in producers to a deterministic, unique identifier for the business operation (not a random UUID per retry).
- **AutoAck matters.** This middleware is designed for `AutoAck=true` (broker default). If you disable auto-ack, replays won't be acknowledged unless you do it yourself (e.g. via `WithOnReplay(func(e, _){ _ = e.Ack() })`).
- **This is not HTTP replay.** The middleware stores only a completion marker, it does not reconstruct handler results. Design handlers so that "replay == no-op" is correct.
