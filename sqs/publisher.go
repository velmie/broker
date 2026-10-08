package sqs

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
)

// Publisher borrows its required SDK client and supports concurrent publication.
type Publisher struct {
	client *awssqs.Client
	config PublisherConfig
}

func NewPublisher(client *awssqs.Client, config PublisherConfig) (*Publisher, error) {
	if err := validateBinding(config.QueueURL, config.Source); err != nil {
		return nil, err
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if config.OperationTimeout < 0 {
		return nil, errors.New("OperationTimeout: must be positive")
	}
	if config.FIFO && config.MessageGroupID == "" {
		return nil, errors.New("MessageGroupID: required for FIFO")
	}
	if config.MessageGroupID != "" && !validToken(config.MessageGroupID) {
		return nil, errors.New("MessageGroupID: expected 1..128 printable ASCII characters without spaces")
	}
	if config.Observer != nil {
		copyObserver := *config.Observer
		config.Observer = &copyObserver
	}
	return &Publisher{client: client, config: config}, nil
}

// Publish treats message data as read-only. Nil means SendMessage was accepted
// by the service. It does not mean consumption, persistence or exactly-once effects.
func (p *Publisher) Publish(ctx context.Context, message broker.Message) error {
	_, err := p.PublishWithResult(ctx, message)
	return err
}

// BuildRequest validates and detaches a message into a native request using the
// publisher's queue and FIFO binding. It performs no I/O or observation. The
// caller owns the returned request and may adjust native controls before calling
// its SDK client. Such changes are governed by the native SDK and queue contract.
func (p *Publisher) BuildRequest(message broker.Message) (*awssqs.SendMessageInput, error) {
	attrs, err := encodeMessage(message)
	if err != nil {
		return nil, operationError(p.config.Source, OperationMetadata, err)
	}
	if p.config.FIFO && !validToken(message.ID) {
		return nil, operationError(p.config.Source,
			OperationMetadata,
			&FieldError{Field: "ID",
				Cause: errors.New("FIFO requires 1..128 printable ASCII characters without spaces")})
	}
	input := &awssqs.SendMessageInput{QueueUrl: aws.String(p.config.QueueURL),
		MessageBody:       aws.String(string(message.Body)),
		MessageAttributes: attrs}
	if p.config.MessageGroupID != "" {
		input.MessageGroupId = aws.String(p.config.MessageGroupID)
	}
	if p.config.FIFO {
		input.MessageDeduplicationId = aws.String(message.ID)
	}
	return input, nil
}

// PublishWithResult returns the native SendMessage result using the same bounded
// operation and observations as Publish. A result is retained if observation fails
// after acceptance. Timeout or cancellation does not prove the message was unsent.
func (p *Publisher) PublishWithResult(ctx context.Context, message broker.Message) (output *awssqs.SendMessageOutput, err error) {
	start := time.Now()
	operation, result := OperationMetadata, OutcomeFailed
	observer := observation{observer: p.config.Observer}
	defer func() {
		observer.record(ctx, p.config.Source, operation, result, start, err, nil)
		err = errors.Join(err, observer.err)
	}()
	input, err := p.BuildRequest(message)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	operation = OperationPublish
	output, err = p.client.SendMessage(callCtx, input)
	result = OutcomeAccepted
	if err != nil {
		result = OutcomeUnknown
		err = operationError(p.config.Source, OperationPublish, err)
	}
	return output, err
}
