package azuresb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

// Processor connects native Service Bus messages to a Broker handler. It owns
// no client, receive loop or background work. The caller owns message or session
// lock renewal, settlement and shutdown. Concurrent calls require a
// concurrent-safe handler and independent native messages.
type Processor struct {
	handler broker.Handler
	source  string
}

// NewProcessor validates a stable source label and processing requirements.
// AttemptsRequirement is supported. Its value remains unknown when native
// DeliveryCount is zero. Redelivery, keepalive and unknown requirements are
// rejected before any callback or native operation.
func NewProcessor(handler broker.Handler, source string) (*Processor, error) {
	if err := validateSource(source); err != nil {
		return nil, operationError(source, "validate", &FieldError{Field: "Source", Cause: err})
	}
	for _, requirement := range handler.Requirements() {
		if _, ok := requirement.(broker.AttemptsRequirement); !ok {
			return nil, operationError(source, "validate", &FieldError{Field: "Requirements",
				Cause: fmt.Errorf("processing without managed operations does not support %T", requirement)})
		}
	}
	return &Processor{handler: handler, source: source}, nil
}

// Process detaches received data and metadata before invoking the handler and
// middleware. It requires Handled for success and never renews locks or
// settles the message. The caller must keep native data read-only until return
// and then choose the native outcome. Retain the original received message for
// SDK settlement. Session lock renewal remains distinct from message lock
// renewal. Errors and cancellation preserve their causes but cannot undo
// completed business effects.
func (p *Processor) Process(ctx context.Context, message *azservicebus.ReceivedMessage) error {
	if err := context.Cause(ctx); err != nil {
		return operationError(p.source, OperationProcess, err)
	}
	delivery, err := detach(message, p.source)
	if err != nil {
		return operationError(p.source, OperationMetadata, err)
	}
	result, err := p.handler.Handle(ctx, delivery)
	if err = errors.Join(err, context.Cause(ctx)); err != nil {
		return operationError(p.source, OperationProcess, err)
	}
	if _, ok := result.(broker.Handled); !ok {
		return operationError(p.source, OperationDisposition, &FieldError{Field: "Disposition",
			Cause: fmt.Errorf("processing without managed settlement requires Handled, got %T", result)})
	}
	return nil
}
