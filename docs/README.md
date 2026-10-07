# Broker documentation

Start with the [in-memory example](../readme.md#publish-and-handle-a-message),
then [connect an application](getting-started.md) to a transport adapter.

| Topic | Guide |
| --- | --- |
| Transport boundaries and adapter choice | [Adapters](backends.md) |
| Consumer startup and shutdown | [Coordinator](coordinator.md) |
| Processing and publication policies | [Middleware](middleware.md) |
| Failures, retries and structured diagnostics | [Error handling](error-handling.md) |
| Message metadata | [Headers](headers.md) and [identity](message-identity.md) |
| Responses to commands | [Request and reply](request-reply.md) |
| Native AWS client configuration | [AWS clients](aws-sdk.md) |
| Common questions | [FAQ](faq.md) |
| Upgrading an existing application | [Migration](migration.md) |
| Building and running this repository | [Development](development.md) |

Optional modules provide [idempotency](../idempotency/README.md) and
[OpenTelemetry](../otelbroker/README.md). Each [adapter guide](../readme.md#modules)
describes its configuration, delivery guarantees and executable examples.
