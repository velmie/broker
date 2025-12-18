# Subscription Coordinator

`broker.SubscriptionCoordinator` is a small helper that manages a set of subscriptions as one unit.
You register "what to subscribe to" once, then start everything with one call and stop everything with one call.

## What problem it solves

In a real service you usually subscribe to multiple topics. Without a coordinator you end up with scattered `Subscribe(...)` calls, duplicated error handling, and ad-hoc shutdown logic.

The coordinator provides:

- Register subscriptions in one place
- Start them all with `SubscribeAll(ctx)`
- Stop them all with `UnsubscribeAll()`
- Keep names unique and fail fast on duplicates

## How it works

- You add named `SubscribeFunc` functions (or `SubscriptionEnvelope` structs).
- `SubscribeAll(ctx)` runs them sequentially, stores successful `broker.Subscription`s, and returns on the first error.
- `UnsubscribeAll()` tries to unsubscribe from everything it started (best effort).

Important detail: `SubscribeAll` does not roll back already started subscriptions if a later subscription fails. If you want a clean startup failure, call `UnsubscribeAll()` after `SubscribeAll` returns an error.

## Quick start

```go
coord := broker.NewSubscriptionCoordinator()
coord.SetLogger(logger)

if err := coord.AddSubscription(
	"orders.created",
	broker.CreateSubscribeFunc("orders.created", sub, handler),
); err != nil {
	// handle duplicate name
}

if err := coord.SubscribeAll(ctx); err != nil {
	// handle startup error (first failure stops the loop)
	coord.UnsubscribeAll()
	return err
}
defer coord.UnsubscribeAll()
```

## Using SubscriptionEnvelope (factory style)

If you build subscriptions in a factory, return `[]broker.SubscriptionEnvelope` and add them:

```go
for _, se := range factory.CreateSubscriptions() {
	if err := coord.AddSubscriptionE(se); err != nil {
		// handle duplicate name
	}
}
```

## Tips

- Use stable names (usually the topic string) so logs are easy to search.
- If your subscribe logic can block (network, backoff, readiness), put that logic inside your `SubscribeFunc` and honor `ctx.Done()`.
- Use middleware chains at handler creation time, then keep coordinator wiring focused on subscription lifecycle.
