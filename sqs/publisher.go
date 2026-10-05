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
func (p *Publisher) Publish(ctx context.Context, message broker.Message) (err error) {
	start := time.Now()
	operation, result := OperationMetadata, OutcomeFailed
	observer := observation{observer: p.config.Observer}
	defer func() {
		observer.record(ctx, p.config.Source, operation, result, start, err, nil)
		err = errors.Join(err, observer.err)
	}()
	attrs, err := encodeMessage(message)
	if err != nil {
		return operationError(p.config.Source, OperationMetadata, err)
	}
	if p.config.FIFO && !validToken(message.ID) {
		return operationError(p.config.Source,
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
	callCtx, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	operation = OperationPublish
	_, err = p.client.SendMessage(callCtx, input)
	result = OutcomeAccepted
	if err != nil {
		result = OutcomeUnknown
		err = operationError(p.config.Source, OperationPublish, err)
	}
	return err
}
