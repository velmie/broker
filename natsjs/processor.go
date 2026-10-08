package natsjs

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// Processor connects a caller-owned native receive loop to a Broker handler.
// It owns no receive resource or goroutine. The caller retains all renewal,
// settlement and shutdown responsibilities. Use a Consumer when Broker should
// manage those operations. Concurrent Process calls require a concurrent-safe
// handler and independent native messages.
type Processor struct {
	handler broker.Handler
	source  string
}

// NewProcessor validates a handler for processing without managed delivery
// operations. AttemptsRequirement is supported. Redelivery, keepalive,
// termination and unknown requirements are rejected before any callback.
// Source is a required stable read identity, normally stream/consumer, rather
// than the subject of one message. The constructor acquires no native resources.
func NewProcessor(handler broker.Handler, source string) (*Processor, error) {
	if source == "" {
		return nil, operationFailure(source, "validate", fieldFailure("Source", "required", errors.New("Source: required")))
	}
	for _, requirement := range handler.Requirements() {
		if _, ok := requirement.(broker.AttemptsRequirement); !ok {
			return nil, operationFailure(source, "validate", fieldFailure("Requirements", "unsupported_requirement",
				fmt.Errorf("processing without managed operations does not support %T", requirement)))
		}
	}
	return &Processor{handler: handler, source: source}, nil
}

// Process detaches message data and metadata, then runs the handler and its
// middleware. Success requires Handled and never acknowledges the message.
// The caller must keep the message read-only until return and decide the native
// outcome afterwards. Errors and cancellation preserve causes; they cannot undo
// business effects or restore messages received under AckNone.
func (p *Processor) Process(ctx context.Context, message *nats.Msg) error {
	if err := context.Cause(ctx); err != nil {
		return operationFailure(p.source, "process", err)
	}
	delivery, err := detach(message, p.source)
	if err != nil {
		return operationFailure(p.source, "metadata", err)
	}
	result, err := p.handler.Handle(ctx, delivery)
	if err = errors.Join(err, context.Cause(ctx)); err != nil {
		return operationFailure(p.source, "process", err)
	}
	if _, ok := result.(broker.Handled); !ok {
		return operationFailure(p.source, OperationDisposition, fieldFailure("Disposition", "unsupported_disposition",
			fmt.Errorf("processing without managed settlement requires Handled, got %T", result)))
	}
	return nil
}
