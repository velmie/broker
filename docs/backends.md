# Transport adapters

An adapter implements Broker's publication and consumption contracts for a
messaging system. Application handlers use the same message and disposition
types, while configuration and delivery guarantees remain specific to the
transport.

| Module | Publication | Consumption |
| --- | --- | --- |
| [NATS JetStream](../natsjs/README.md) | Subject-bound, waits for a JetStream response | Existing named pull or push consumer with explicit acknowledgment |
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

## Native composition

`NewTypedPublisherWithResult` keeps metadata resolution, encoding and ordinary
publisher middleware while returning the operation's native result. The result
type belongs to the adapter or SDK. Existing `Publisher` and `NewTypedPublisher`
retain their error-only contracts. See [typed publication](publishing.md)
for result and middleware ownership, with links to adapter examples.

Each adapter keeps native types at its own boundary:

| Adapter | Native publication | Received data | Processing with caller-owned operations |
| --- | --- | --- | --- |
| [NATS](../natsjs/docs/native-publishing.md) | `PublishWithAck`, or `EncodeMessage` followed by SDK options, ACKs or async futures | `SubjectReader` and detached `MetadataReader`; original SDK message stays with its receiver | [`Processor.Process` / `ProcessBatch`](../natsjs/docs/native-consumption.md) with native push, pull or ordered subscriptions |
| [SQS](../sqs/docs/native.md) | `PublishWithResult`, or `BuildRequest` followed by native `SendMessage` | Detached `MetadataReader`; receipt handle stays with the native caller | `Processor.Process`; caller receives, renews visibility and deletes |
| [SNS](../sns/docs/native.md) | `PublishWithResult`, or `BuildRequest` followed by native `Publish` | `DecodeNotification` maps the downstream notification | Use the downstream receiver, such as SQS |
| [Azure](../azuresb/docs/native.md) | `EncodeMessage` followed by native send, batch or scheduling operations | Detached `MetadataReader`; original SDK message retains full properties and settlement state | `Processor.Process`; caller owns message/session renewal and native settlement |

Processors invoke ordinary Broker handlers and middleware without acquiring a
receive resource or performing native operations. They accept the documented
processing requirements and require `Handled` on success. Choose a managed
consumer when Broker should implement renewal and settlement. Choose a processor
when the application owns those operations and needs the SDK's native modes.
Neither path takes ownership of the application's client or topology.

Keep native received data read-only during processing. The caller of a processor
joins its own receive, callback and renewal work before closing resources. Use
the original SDK delivery for settlement. Detached metadata is data for inspection,
not a reconstructed receipt or a transfer of operational authority.

`Delivery.Source()` identifies the configured source. In managed NATS consumers
it remains `stream/consumer`; the actual received subject is separate. Keep that
stable identity when selecting a processor source, especially when idempotency
or tracing uses it. Native policies retain their own guarantees: cumulative ACK,
destructive receive, lock expiry and uncertain publication cannot be normalized
into stronger portable guarantees.

## Implement another adapter

Implement `Publisher` for a bound destination and `Consumer` for a bound source.
Keep factories and `Validate` local and free of resource acquisition. `Run` owns
acquisition, remote checks and cleanup, and joins its work before returning.

Document supported requirements and concrete dispositions, message/header
mapping, publication guarantees, cancellation, resource ownership and uncertain
native outcomes. Preserve original errors with operation and source context.
Use the existing adapters as examples without importing their transport policy
into the core.
