package azuresb

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// Sender is the native SDK send boundary. A Publisher borrows its required sender
// and never closes it or its client. The caller supplies native client options,
// chooses the queue or topic, and owns sender/client cleanup.
type Sender interface {
	SendMessage(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error
}
