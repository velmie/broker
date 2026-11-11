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

## Receive a copy in each instance

Create a separate named ephemeral pull consumer for each instance. Set native
`Name`, leave `Durable` empty and use explicit acknowledgments. Each consumer
retains its own progress. The stream's retention policy must allow overlapping
consumers, such as `LimitsPolicy`. A shared consumer distributes work between
instances instead of delivering a copy to each one.

Choose `DeliverNewPolicy` when only events published after consumer creation are
needed. Set `InactiveThreshold` to the intended cleanup interval. Broker releases
the local subscription when `Run` joins and leaves consumer deletion to the
application or the server's inactivity policy.

Once topology exists, bind the consumer and apply the ordinary origin middleware:

```go
package messaging

import (
	"context"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func ListenInstance(ctx context.Context, conn *nats.Conn,
	stream, consumerName, subject, instanceID string, handler broker.Handler,
) error {
	// This binding keeps the provisioned consumer's lifetime and delivery policy.
	consumer, err := natsjs.NewPullConsumer(conn, natsjs.ConsumerConfig{
		Stream: stream, Consumer: consumerName, Subject: subject,
	})
	if err != nil {
		return err
	}
	// Own events complete normally without invoking the business callback.
	return consumer.RunService(ctx, handler.WithMiddleware(broker.SkipOwnMessages(instanceID)))
}
```

Wrap the corresponding publisher with `broker.StampInstanceID(instanceID)`.
Instance IDs implement loop prevention, not authentication. The connection stays
owned by the application. `Source()` remains `stream/consumerName` for both
durable and ephemeral deliveries.

`NewPullConsumer` has the same filter, requirement, shutdown and operation
contracts as `NewConsumer`. It accepts either lifetime. `NewConsumer` continues
to reject ephemeral consumers. Neither constructor acquires a subscription.

## Select backlog behavior

Set the native delivery policy when provisioning a new consumer:

- `DeliverAll` starts with retained matching messages.
- `DeliverNew` skips messages that precede consumer creation.

Reconnecting to a retained consumer does not reset that boundary. New messages
can remain available while the application is disconnected, subject to retention
and limits. Pending deliveries can become available again under the server's
redelivery rules. Successful acknowledgment does not make business effects
exactly once.

## Filter by the received subject

A wildcard filter can receive messages on several subjects. Read the actual
subject through `natsjs.SubjectReader`. `Source()` keeps the stream/consumer
identity used by idempotency keys and tracing.

This middleware completes one selected subject before the typed decoder runs:

```go
package messaging

import (
	"context"
	"errors"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func SkipSubject(subject string) broker.HandlerMiddleware {
	return broker.HandlerMiddleware{Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
		return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			// The adapter supplies received metadata without settlement methods.
			reader, ok := delivery.(natsjs.SubjectReader)
			if !ok {
				return nil, errors.New("received subject unavailable")
			}
			if reader.Subject() == subject {
				return broker.Handled{}, nil
			}
			return next(ctx, delivery)
		}
	}}
}
```

Attach it with `handler.WithMiddleware(SkipSubject(ownSubject))`. The consumer
acknowledges skipped messages using the same settlement rules as other handled
messages. Read `Subject()` during the callback. Its returned string and the
`MetadataReader.Metadata()` snapshot may be retained after the callback returns.

## Recover pull receives

`Run` retries recognized pull receive interruptions using the same local
subscription. The application owns connection reconnect settings and connection
lifetime. While the connection is reconnecting, `Run` waits without sending
requests. After any interruption or reconnect, it looks up the bound consumer
once before the next pull, because some servers leave a pull for a deleted
consumer unanswered. A missing stream or consumer stops `Run` with the lookup
error. Each failed attempt selected for retry, including a temporary lookup
failure, is reported as a fetch observation with `OutcomeInterrupted` and its
original error.

Connection reconnect limits do not bound an outage of JetStream while the NATS
connection remains connected. If the management service is unavailable, `Run`
cannot conclusively distinguish unavailable topology from deleted topology.

A restart that interrupts settlement or renewal can still make `Run` return an
error. The adapter does not replay the callback or infer that an uncertain
settlement failed. A supervisor may restart the process, and pending deliveries
follow the server's redelivery policy.

Push consumers use the native synchronous subscription's reconnect behavior.
Pull receive recovery does not add a push recovery policy.

## Replace a failed local run

`broker.WithConsumerRecovery` can invoke the factory again for application-selected
failures that escape `Run`, after the failed run has joined. A replacement
validates and attaches a new local subscription to the same durable. It does not reset progress or replay a callback directly.

The [orders classifier](../cmd/orders/recovery.go) accepts only startup timeouts
and inspects every independent cause. Processing, renewal, uncertain settlement
and cleanup failures remain terminal under that policy. SDK connection recovery
is configured separately on the connection.

The example deletes its temporary stream after completion because it owns that
topology. Ordinary consumer shutdown leaves application-owned topology intact.
