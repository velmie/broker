# Native SQS operations

The application owns the AWS SDK client, queue topology and native request
configuration. Choose the owner of delivery operations before receiving:

| API | Responsibility |
| --- | --- |
| `Publisher.PublishWithResult` | Bound send, operation timeout and observer, returning `SendMessageOutput` |
| `Publisher.BuildRequest` | Validated, detached `SendMessageInput`, with no SDK call or observation |
| `Consumer.Run` | Receive, callbacks, visibility renewal, delete/redelivery and joined shutdown |
| `Processor.Process` | Detached message mapping and handler invocation only |

## Inspect a typed publication

```go
package messaging

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	native "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

type Order struct { ID string }

func OrdersWithResult(publisher *sqs.Publisher, middleware ...broker.PublisherMiddleware) func(context.Context, Order) (*native.SendMessageOutput, error) {
	return broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		publisher.PublishWithResult,
		func(_ context.Context, order Order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: order.ID}, nil
		}, middleware...)
}
```

The result contains native service identity and checksums. A result may accompany
an observer or middleware error after service acceptance. Existing `Publish`
retains its error-only contract. The shared [result and middleware rules](../../docs/publishing.md#middleware-ownership)
also apply to this typed function.

For native controls, call `BuildRequest`, set fields such as `DelaySeconds`, then
use the borrowed client's `SendMessage` with native options. The application owns
that call's context, deadline and result. Per-message delay is a standard-queue
operation; FIFO queues use their queue-level delay. The request starts with the
publisher's destination and FIFO settings. Changing it is an explicit native
operation and never mutates the original Broker message or publisher binding.

## Process a native delivery

Create `sqs.NewProcessor(handler, source)` once for an application-owned receive
loop. Select message attributes and system attributes in `ReceiveMessageInput`.
Include `ApproximateReceiveCount` when attempt metadata is needed. Missing attempt
metadata remains unknown.

This step processes a native delivery and then chooses native deletion:

```go
func ProcessAndDelete(ctx context.Context, client *native.Client, queueURL string,
	processor *sqs.Processor, message *types.Message) error {
	if err := processor.Process(ctx, message); err != nil {
		return err
	}
	// The native receipt remains with its owner, outside read-only Broker metadata.
	_, err := client.DeleteMessage(ctx, &native.DeleteMessageInput{
		QueueUrl: aws.String(queueURL), ReceiptHandle: message.ReceiptHandle,
	})
	return err
}
```

The processor requires `Handled` on success. It accepts `AttemptsRequirement`
and rejects keepalive, redelivery and unknown requirements before callbacks.
It does not renew visibility or delete a message. Long callbacks need a
caller-owned visibility policy, or a managed consumer. Keep the SDK message
read-only during `Process`. Broker message data and metadata are detached.

The native caller owns receive work, renewal, settlement, cancellation and join.
A callback failure leaves the native receipt available for the caller's chosen
operation. A failed delete or send can have an unknown outcome. Preserve original
errors and safe source/operation context at the logging boundary. See
[AWS configuration and errors](../../docs/aws-sdk.md).
