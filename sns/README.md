# Amazon SNS publisher

`github.com/velmie/broker/sns` publishes Broker messages to a bound topic through
an application-configured AWS SDK for Go v2 client. The application owns client
configuration and subscription topology. Dependencies are listed in
[go.mod](go.mod).

## Bind a topic

Borrow the native client and bind a full topic ARN:

```go
package messaging

import (
	native "github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/velmie/broker/sns"
)

func Events(client *native.Client, topicARN string) (*sns.Publisher, error) {
	return sns.NewPublisher(client, sns.PublisherConfig{
		TopicARN: topicARN,
		Source:   "orders/events",
	})
}
```

The ARN passes through unchanged, including its partition. Broker does not
infer an account, discover topics or configure subscriptions. See
[AWS client configuration](../docs/aws-sdk.md).

`Source` is a diagnostic label of 1–128 ASCII letters, digits, `.`, `_`, `-` or
`/`. `OperationTimeout` bounds the native call, including SDK retries; zero
selects thirty seconds. Publication supports concurrent calls while input data
remains read-only.

A nil result means SNS accepted the request. It does not prove forwarding or
processing. A failed or canceled request can leave acceptance unknown.

## Message mapping

The body must be nonempty UTF-8 text. Headers become String attributes for valid
text and Binary attributes for other nonempty bytes. Native names, duplicate
names, empty values, reserved `id` case aliases and identity conflicts are
validated before publication. Native maps do not preserve attribute ordering.

A nonempty logical ID becomes the canonical String attribute `id`. Native SNS IDs
do not replace it. The adapter caps combined bodies and attributes at 1 MiB;
topic and subscription limits may be smaller.

For a FIFO topic, set `FIFO: true`, supply `MessageGroupID` and use a stable,
nonempty logical ID as the deduplication ID. Group and deduplication IDs accept
1–128 printable ASCII characters without spaces. Configured groups also pass
through for standard topics. Native deduplication does not guarantee exactly-once
business effects.

Set `RawSQSDelivery: true` for a raw SQS route to validate the maximum of ten final
attributes, including `id` and middleware metadata. This option does not configure
the subscription. For raw and JSON notification routes, see
[SNS to SQS delivery](docs/sqs-delivery.md).

## Observations

`Observer.Record` receives one synchronous publication record with source,
operation, outcome, duration and the original error. Hooks must return promptly
and support concurrent callers. A hook panic is returned alongside any native
failure after the operation, without retrying publication.

`OperationError` and `FieldError` retain context and original causes. The logging
adapter must redact SDK and application data while preserving useful codes and
fields. See [structured diagnostics](../docs/error-handling.md#structured-diagnostics).

## Run the example

Prepare the [development workspace](../docs/development.md#local-workspace) and
start a disposable Moto service at a loopback HTTP endpoint. From `sns`:

```sh
go run ./cmd/orders -endpoint http://127.0.0.1:5000
```

The command uses synthetic credentials and creates a topic, queue, policy and
subscription for each of two routes: raw and JSON notification. Each route
publishes an order, validates identity, processes it and confirms deletion. Owned
topology is removed afterward, with cleanup failures preserved.
