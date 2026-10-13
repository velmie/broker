# Native SNS publication

`Publisher.PublishWithResult` returns the native `PublishOutput` while preserving
the bound topic, operation timeout and observer. Native message IDs and FIFO
sequence numbers describe SNS acceptance. They do not prove subscriber delivery.
A result may accompany an instrumentation error after acceptance.

Use the shared typed result API to keep application values and ordinary
publisher middleware:

```go
package messaging

import (
	"context"
	"encoding/json"

	native "github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sns"
)

type Event struct { ID string }

func EventsWithResult(publisher *sns.Publisher, middleware ...broker.PublisherMiddleware) func(context.Context, Event) (*native.PublishOutput, error) {
	return broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		publisher.PublishWithResult,
		func(_ context.Context, event Event) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: event.ID}, nil
		}, middleware...)
}
```

See [typed publication](../../docs/publishing.md) for result, concurrency and
middleware contracts. Existing `Publish` remains available for error-only calls.

For native controls, call `Publisher.BuildRequest(message)`, adjust fields such
as `Subject`, then call the application-owned client's `Publish`. The builder
performs the same message, FIFO and raw-delivery budget checks and returns
detached request data. It makes no SDK call and emits no observation. Native
changes cannot mutate the Broker message or bound publisher configuration.
The caller owns request changes, SDK options, context, timeout and result.

The application also owns topics, subscriptions and client lifetime. SNS has no
receive or settlement operation. For a downstream SQS subscription, use a
managed SQS consumer or [SQS native processing](../../sqs/docs/native.md).
`DecodeNotification` validates a JSON notification and returns its Broker message.
Raw delivery preserves the body directly. See [SNS to SQS mapping](sqs-delivery.md)
for identity and metadata on both routes.

Native errors retain their SDK causes. Preserve safe source and operation context
when recording them, and redact service messages at the logging boundary. Failed
or canceled native calls may have been accepted. See
[AWS configuration and errors](../../docs/aws-sdk.md).
