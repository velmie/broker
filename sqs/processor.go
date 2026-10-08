package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
)

// Processor connects native SQS messages to a Broker handler without owning a
// client, receive loop or background work. The caller owns visibility renewal,
// deletion and shutdown. Concurrent calls require a concurrent-safe handler and
// independent native messages.
type Processor struct {
	handler broker.Handler
	source  string
}

// NewProcessor validates a stable source label and processing requirements.
// AttemptsRequirement is supported. Its value remains unknown when the native
// receive request omits ApproximateReceiveCount. Redelivery, keepalive and
// unknown requirements are rejected before any callback or native operation.
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
// middleware. It requires Handled for success and never renews visibility or
// deletes the message. The caller must keep native data read-only until return
// and then choose the native outcome. Request message and system attributes
// through the SDK to make them available to the handler. Errors and cancellation
// preserve their causes but cannot undo completed business effects.
func (p *Processor) Process(ctx context.Context, message *types.Message) error {
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
