# Deliver SNS publications to SQS

Select the subscription's wire format explicitly. The SQS adapter keeps the
received body unchanged. Subscription creation, queue policies and native client
configuration belong to application wiring.

| Subscription setting | Received body |
| --- | --- |
| `RawMessageDelivery=true` | Original body, with native message attributes including logical `id` |
| `RawMessageDelivery=false` | SNS JSON notification containing nested `Message` and `MessageAttributes` |

SNS publication acceptance does not prove forwarding, processing or deletion.

## Raw delivery

Configure the subscription with `RawMessageDelivery=true` and the publisher with
`RawSQSDelivery: true`. The publisher option validates the route's maximum of ten
final attributes; it does not change the subscription.

Count attributes after middleware and the canonical `id` have been applied.
Seven application attributes, `id`, `traceparent` and `tracestate` already total
ten. The cap belongs to the raw SQS route, not every SNS publication. See the
[AWS raw delivery contract](https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html).

SQS preserves forwarded text and binary bytes. Logical identity comes from
canonical String `id`, not the provider's message ID.

## Notification envelopes

For a non-raw subscription, decode the envelope before typed application
processing. This function accepts the original SQS delivery and a configured
trusted topic ARN:

```go
package messaging

import (
	"github.com/velmie/broker"
	"github.com/velmie/broker/sns"
)

func Decode(delivery broker.Delivery, expectedTopicARN string) (broker.Message, error) {
	return sns.DecodeNotification(delivery.Message().Body, expectedTopicARN)
}
```

The decoder requires `Type: Notification` and the expected `TopicArn`. It extracts
message text and nested attributes. String, Number and String.Array retain their
text bytes; Binary is base64-decoded. Logical `id` must be String. Missing identity
stays empty. Returned storage is detached.

The entire envelope must fit within 1 MiB, including JSON escaping and metadata.
Duplicate object keys, relevant case aliases, malformed attributes and invalid
base64 fail with their field and original cause preserved.

The decoder does not authenticate signatures or fetch certificate, confirmation
or unsubscribe URLs. Restrict the queue's `sqs:SendMessage` policy to the intended
SNS service and source topic. Matching the topic field alone is not authentication.

The [orders example](../cmd/orders/main.go) replaces only the message presented
to the typed handler and forwards SQS metadata and attempt interfaces. Its
requirements remain intact, and decoding stays inside the original consumer
callback so deletion follows successful processing. `broker.NewDelivery` wraps
plain message data; it does not preserve capabilities from another delivery.
