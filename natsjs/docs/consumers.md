# Consumer progress and ownership

A JetStream consumer retains delivery and acknowledgment progress. Its delivery
policy selects the starting point when the consumer is created. Reusing the
consumer resumes that progress. Deleting and recreating it applies the initial
policy again. Stream retention and limits determine which messages remain.

## Bind an existing durable

Provision a durable pull consumer with explicit acknowledgments before starting
Broker. `NewConsumer` binds the configured stream, consumer and subject locally.
It acquires no subscription and creates no remote topology.

`Run` validates the remote configuration, then attaches an owned local pull
subscription with `nats.Bind`. It releases that subscription after admitted work,
renewal and shutdown observation join. The remote durable and its progress
remain. The connection stays open until its application owner closes it.

The [adapter guide](../README.md#bind-a-source) describes supported filters and
policies. Broker does not reconcile remote configuration. Inspect `ConsumerInfo`
when the initial policy matters and keep topology stable during an active run.

## Select backlog behavior

Set the native delivery policy when provisioning a new consumer:

- `DeliverAll` starts with retained matching messages.
- `DeliverNew` skips messages that precede consumer creation.

Reconnecting to a retained consumer does not reset that boundary. New messages
can remain available while the application is disconnected, subject to retention
and limits. Pending deliveries can become available again under the server's
redelivery rules. Successful acknowledgment does not make business effects
exactly once.

## Replace a failed local run

`broker.WithConsumerRecovery` can invoke the factory again after a failed `Run`
has joined. A replacement validates and attaches a new local subscription to the
same durable. It does not reset progress or replay a callback directly.

The [orders classifier](../cmd/orders/recovery.go) accepts only startup timeouts
and inspects every independent cause. Processing, renewal, uncertain settlement
and cleanup failures remain terminal under that policy. SDK connection recovery
is configured separately on the connection.

The example deletes its temporary stream after completion because it owns that
topology. Ordinary consumer shutdown leaves application-owned topology intact.
