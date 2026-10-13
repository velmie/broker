# Native Service Bus composition

The application owns native clients, topology and sender/receiver configuration.
`EncodeMessage` converts a Broker message into a detached SDK `Message` for
native operations. `NewProcessor` connects a native received message to ordinary
Broker handlers without acquiring or operating a receiver.

| Path | Operations and owner |
| --- | --- |
| `Publisher.Publish` | Broker performs the bounded send and observation, borrowing the sender |
| `EncodeMessage` + native sender | Application chooses message properties, send, batch or schedule operations and their results |
| `Consumer.Run` | Broker owns its factory-created receiver, callbacks, message renewal, settlement and close |
| Native receiver + `Processor` | Application owns receive, callback admission, renewal, settlement, join and close |

## Schedule a typed message

Native scheduling returns sequence numbers. Keep those native results while
reusing metadata resolution, encoding and ordinary publisher middleware:

```go
package messaging

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	"github.com/velmie/broker/azuresb"
)

type Reminder struct { ID string }

func Reminders(sender *azservicebus.Sender, at time.Time,
	middleware ...broker.PublisherMiddleware) func(context.Context, Reminder) ([]int64, error) {
	return broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		func(ctx context.Context, message broker.Message) ([]int64, error) {
			native, err := azuresb.EncodeMessage(message)
			if err != nil {
				return nil, err
			}
			return sender.ScheduleMessages(ctx, []*azservicebus.Message{native}, at, nil)
		}, func(_ context.Context, reminder Reminder) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: reminder.ID}, nil
		}, middleware...)
}
```

The caller supplies an operation deadline and retains the sequence numbers for
native cancellation when needed. `SendMessage` itself returns only an error.
Broker adds no synthetic acknowledgment. The shared
[typed publication rules](../../docs/publishing.md) define middleware and result
ownership, including results returned alongside errors.

Set native fields such as `Subject`, `ContentType`, `SessionID` or `TimeToLive`
after conversion. Those changes cannot affect the Broker input. For batching,
use `Sender.NewMessageBatch`, add encoded messages with `AddMessage`, then call
`SendMessageBatch`. Batch capacity, scheduling, session routing and native options
remain SDK contracts.

## Process and settle a native message

Construct `azuresb.NewProcessor(handler, source)` once with the stable source
identity. The handler can use ordinary typed decoding and middleware.

```go
func ProcessAndComplete(ctx context.Context, receiver *azservicebus.Receiver,
	processor *azuresb.Processor, message *azservicebus.ReceivedMessage) error {
	if err := processor.Process(ctx, message); err != nil {
		return err
	}
	// Retain the original SDK object, including its settlement state.
	return receiver.CompleteMessage(ctx, message, nil)
}
```

The caller may instead select native abandonment, deferral or dead-lettering.
After deferral, call `ReceiveDeferredMessages` with the saved native sequence
number and pass the returned object through the processor. Use that same object
for settlement. Reconstructing a `ReceivedMessage` from Broker metadata loses
SDK state needed by the operation.

`Process` requires `Handled` on success and performs no native operation. It
accepts `AttemptsRequirement` and rejects keepalive, redelivery and unknown
requirements before callbacks. A processing error cannot undo business effects
or restore a message removed by ReceiveAndDelete. Keep native message data
read-only until processing returns. Join caller-owned work before closing its
receiver, sender and client, preserving cleanup errors alongside a primary error.

## Sessions and metadata

Use a native `SessionReceiver` with the same processor. Its `RenewSessionLock`
renews the session lock. It is different from `Receiver.RenewMessageLock` and
is owned by the native caller. Session state, receiver lifetime and any renewal
work also remain with that caller. A session receiver does not satisfy the
managed message-renewal receiver contract.

Broker's existing `MetadataReader` returns a detached snapshot of its documented
fields. Full SDK properties, including session ID, subject and raw AMQP sections,
remain on the original received message at the native boundary. Reading that
snapshot does not grant settlement authority or make every SDK property part of
an ordinary Broker handler's metadata.

The portable message represents a byte body and ordered headers. `EncodeMessage`
uses the SDK's ordinary data-message representation, and `Processor` reads
`ReceivedMessage.Body`. AMQP Value/Sequence bodies, footer and message annotations
belong to the SDK's separate annotated-message representation and are not
implicitly converted into portable bytes or headers.
