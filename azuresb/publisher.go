package azuresb

import (
	"context"
	"errors"
	"time"

	"github.com/velmie/broker"
)

const defaultPublisherOperationTimeout = 30 * time.Second

// Publisher borrows a destination-bound Sender. Concurrent publication requires
// a concurrent-safe sender, as provided by the native Azure SDK. Input storage
// is copied before entering the sender. The caller retains sender/client ownership.
type Publisher struct {
	sender Sender
	config PublisherConfig
}

func NewPublisher(sender Sender, config PublisherConfig) (*Publisher, error) {
	if err := validateSource(config.Source); err != nil {
		return nil, err
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultPublisherOperationTimeout
	}
	if config.OperationTimeout < 0 {
		return nil, &FieldError{Field: operationTimeoutField, Cause: errors.New("must be positive")}
	}
	if config.Observer != nil {
		snapshot := *config.Observer
		config.Observer = &snapshot
	}
	return &Publisher{sender: sender, config: config}, nil
}

// Publish requires an explicit nonempty logical ID so Service Bus cannot
// synthesize an identity for this publication. Header values must be UTF-8 text.
// Callers explicitly encode binary metadata as text because Service Bus rejects
// native binary application properties. The body remains arbitrary bytes.
// Publish returns nil when the native SDK accepted the send, not when a consumer
// processed it. A failed or canceled native call may have an unknown outcome.
// Original native/context errors remain available through errors.Is and errors.As.
func (p *Publisher) Publish(ctx context.Context, message broker.Message) (err error) {
	start := time.Now()
	operation, outcome := OperationMetadata, OutcomeFailed
	observer := observation{observer: p.config.Observer}
	defer func() {
		observer.record(ctx, p.config.Source, operation, outcome, start, err, nil)
		err = errors.Join(err, observer.err)
	}()
	native, err := encodeMessage(message)
	if err != nil {
		return operationError(p.config.Source, operation, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	operation, outcome = OperationPublish, OutcomeAccepted
	err = p.sender.SendMessage(callCtx, native, nil)
	if err != nil {
		outcome = OutcomeUnknown
		err = operationError(p.config.Source, operation, err)
	}
	return err
}
