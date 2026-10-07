# Configure AWS clients

The [SQS](../sqs/README.md) and [SNS](../sns/README.md) adapters borrow native AWS
SDK for Go v2 clients. The application owns region, credentials, endpoint,
HTTP transport, retry policy and SDK middleware. Adapters retain that configured
client and pass an explicit context to each operation.

After loading an `aws.Config` and resolving the queue URL and full topic ARN,
bind the publishers:

```go
package messaging

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

func Publishers(config aws.Config, queueURL, topicARN string) (*sqs.Publisher, *sns.Publisher, error) {
	queue, err := sqs.NewPublisher(nativeSQS.NewFromConfig(config), sqs.PublisherConfig{
		QueueURL: queueURL,
		Source:   "orders/queue",
	})
	if err != nil {
		return nil, nil, err
	}
	topic, err := sns.NewPublisher(nativeSNS.NewFromConfig(config), sns.PublisherConfig{
		TopicARN: topicARN,
		Source:   "orders/topic",
	})
	if err != nil {
		return nil, nil, err
	}
	return queue, topic, nil
}
```

Resolve names with native SDK operations such as `GetQueueUrl` at the wiring
boundary. Provision queues, topics, subscriptions and access policies explicitly.
Broker does not discover them or infer an account or region from a short name.

Use the normal credential chain for deployed applications. Local examples use
synthetic credentials and explicitly restricted loopback endpoints. Those
settings belong to their emulator scenario.

## Timeouts and errors

Loading SDK configuration and publishing a message are separate operations with
separate contexts. Adapter operation budgets bound the SDK calls cooperatively;
SDK retries occur within those calls. SQS's optional `ResilientReceive` policy
applies only to receiving.

A deadline after transmission leaves acceptance uncertain. Preserve logical IDs
when deciding to retry. Native causes remain available through `errors.As`,
including `smithy.APIError` and modeled service errors. Pass original errors to
the [logging adapter](error-handling.md#structured-diagnostics) and select approved
service codes instead of serializing arbitrary SDK messages.

See the [AWS configuration guide](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html)
for native configuration and [Broker migration](migration.md) when upgrading
from the subscriber API.
