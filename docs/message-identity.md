# Message identity

`Message.ID` identifies a logical application message. Assign it where that
message is created and reuse it when retrying the same publication. Broker does
not generate IDs. A different business message needs its own identity.

Publication can fail after the service has accepted the message. Retrying with a
new ID defeats deduplication based on the original identity. A stable ID helps
only within the transport or application's actual deduplication rules and
retention window.

## Native mapping

| Adapter | Mapping |
| --- | --- |
| [NATS JetStream](../natsjs/README.md#message-mapping) | `Nats-Msg-Id` carries the logical ID |
| [SQS](../sqs/README.md#message-mapping) | The `id` message attribute carries it; the native SQS `MessageId` is separate |
| [SNS](../sns/README.md#message-mapping) | The `id` attribute carries it through subscriptions |
| [Azure Service Bus](../azuresb/readme.md#message-mapping) | The native `MessageID` carries it; an optional `id` property must agree |

Adapters validate conflicting or ambiguous representations. FIFO SQS and SNS
publication use the logical ID as the explicit deduplication ID and require a
message group. Native deduplication does not make arbitrary business effects
exactly once.

## Related identities

Keep these roles distinct:

- **Message ID:** stable identity of one logical message.
- **Correlation ID:** application-selected association between messages or work.
- **Response ID:** stable identity of a reply, selected by `ReplyResolver`.
- **Native delivery metadata:** transport sequence, receipt or delivery attempt.

`NewTypedPublisher` obtains the ID and headers from its metadata callback.
`NewReplyHandler` requires a nonempty response ID before business work starts.
It does not infer correlation metadata. See [request and reply](request-reply.md).

If an application also carries its ID in a custom header, use
`ValidateIDHeader(name)` to detect disagreement before publication. The helper
neither generates an ID nor fills a missing header.

## Duplicate processing

Transport delivery attempts count deliveries in an adapter-defined scope, not
business executions. `AttemptReader` may report `Known: false`.

[Idempotency middleware](../idempotency/README.md) can skip an effect after a
successful completion marker has been stored. Its key namespace and fingerprint
must remain stable while retained records are in use. Application effects,
marker persistence and acknowledgment are separate operations, so a crash between
them can still require business-level idempotency or transactional coordination.
