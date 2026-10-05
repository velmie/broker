# Native JetStream sessions

These executable examples use the NATS SDK directly for features outside the
adapter's durable pull profile. Each session owns its native resources, completes
or joins outstanding work, and then cleans up temporary topology.

## Run a session

Prepare the [development workspace](../../../docs/development.md#local-workspace)
and start a disposable JetStream server. From `natsjs`:

```sh
go run ./cmd/native -url nats://127.0.0.1:4222 -profile all
```

Select one profile with `-profile`:

| Profile | Demonstrates |
| --- | --- |
| `async-publish` | Publication futures, backpressure, per-message acknowledgment and cleanup |
| `push` | Caller-created push and queue subscriptions, explicit ACK and joined drain |
| `iterator` | Sequential pull iteration with explicit settlement |
| `policies` | AckNone, AckAll, ordered delivery, BackOff and native delivery controls |
| `topology` | Stream and consumer provisioning and lifetime |
| `connection` | Connection readiness, deadlines and cleanup |

The command emits a JSON completion record for each successful profile and exits
nonzero on failure. `-url` also accepts the SDK's comma-separated server list.
TLS, authentication and reconnect policy belong to native connection wiring.

## Read the guarantees correctly

For asynchronous publication, a completed pending queue does not mean every
message succeeded. Inspect each future. Cleanup resolves outstanding futures,
but a timed-out request may already have been accepted.

Push callbacks perform their effect before `AckSync`. Draining is asynchronous:
wait for the subscription's closed notification before deleting topology or
closing the connection. Iterator callbacks remain sequential; a callback failure
gets no ACK, and a failed terminal operation is not followed by an opposite one.

Native policies have distinct consequences:

- AckNone cannot restore a message after an application failure.
- AckAll acknowledges cumulatively and requires an application policy compatible
  with the ordering of effects.
- Ordered consumers are ephemeral, non-queue consumers with AckNone semantics.
- Native BackOff controls timeout-based redelivery. It is separate from explicit
  delayed NAK and Broker's `RetryAfter`.
- Flow control, heartbeats and rate limits remain native consumer configuration.

Connection setup uses a shared readiness budget across candidate servers and
joins owned dial attempts. Supplied dialers must honor their context or deadline.
Late connections are closed before setup returns. See the
[connection implementation](connection.go) for the complete lifecycle.

Dependencies and test server selections live in [go.mod](../../go.mod) and
[CI configuration](../../../.github/workflows/github-ci.yml).
