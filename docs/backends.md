# Transport adapters

An adapter implements Broker's publication and consumption contracts for a
messaging system. Application handlers use the same message and disposition
types, while configuration and delivery guarantees remain specific to the
transport.

| Module | Publication | Consumption |
| --- | --- | --- |
| [NATS JetStream](../natsjs/README.md) | Subject-bound, waits for a JetStream response | Existing durable pull consumer with explicit acknowledgment |
| [Amazon SQS](../sqs/README.md) | Queue-bound, waits for `SendMessage` acceptance | Sequential receiving, visibility renewal and deletion |
| [Amazon SNS](../sns/README.md) | Topic-bound, waits for `Publish` acceptance | Use a downstream adapter, such as SQS |
| [Azure Service Bus](../azuresb/readme.md) | Bound SDK sender | Queue or subscription receiver in PeekLock or ReceiveAndDelete mode |

The application configures native clients and topology. Adapters borrow clients
or acquire the resources explicitly assigned to them, and document who closes
each resource. They do not infer deployment names or provision infrastructure.

## Capabilities and guarantees

Handlers declare requirements such as redelivery, attempt metadata or renewal.
Consumers validate the complete combination before processing. A shared
disposition does not imply identical native behavior: Azure supports immediate
abandonment for `RetryAfter`, while SQS accepts whole-second visibility delays.
Consult the adapter guide before selecting a policy.

Native metadata is available through adapter-specific read-only interfaces.
Keep it at the integration boundary when ordinary application code only needs
the decoded value. Publication acceptance never proves downstream processing.

## Implement another adapter

Implement `Publisher` for a bound destination and `Consumer` for a bound source.
Keep factories and `Validate` local and free of resource acquisition. `Run` owns
acquisition, remote checks and cleanup, and joins its work before returning.

Document supported requirements and concrete dispositions, message/header
mapping, publication guarantees, cancellation, resource ownership and uncertain
native outcomes. Preserve original errors with operation and source context.
Use the existing adapters as examples without importing their transport policy
into the core.
