// Package sns publishes through a borrowed native AWS SDK v2 client.
package sns

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	native "github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/velmie/broker"
)

const defaultOperationTimeout = 30 * time.Second

// PublisherConfig binds a full topic ARN without inferring region or account.
// RawSQSDelivery enforces the final ten-attribute raw subscription budget. It
// does not configure subscriptions. The service validates topic-specific limits.
type PublisherConfig struct {
	TopicARN         string
	Source           string
	FIFO             bool
	MessageGroupID   string
	RawSQSDelivery   bool
	OperationTimeout time.Duration
	Observer         *Observer
}

// Publisher borrows its required client. Concurrent publication is supported.
type Publisher struct {
	client *native.Client
	config PublisherConfig
}

//nolint:gocritic // Configuration is copied once to keep caller-owned settings immutable.
func NewPublisher(client *native.Client, config PublisherConfig) (*Publisher, error) {
	parsed, err := arn.Parse(config.TopicARN)
	if err != nil {
		return nil, &FieldError{Field: "TopicARN", Cause: err}
	}
	if parsed.Service != "sns" || parsed.Partition == "" || parsed.Region == "" || parsed.AccountID == "" || parsed.Resource == "" {
		return nil, fieldError("TopicARN", "expected a full SNS topic ARN")
	}
	if !validSource(config.Source) {
		return nil, fieldError("Source", "expected 1..128 safe label characters")
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if config.OperationTimeout < 0 {
		return nil, fieldError("OperationTimeout", "must be positive")
	}
	if config.FIFO != strings.HasSuffix(parsed.Resource, ".fifo") {
		return nil, fieldError("FIFO", "must match topic suffix")
	}
	if config.FIFO && config.MessageGroupID == "" {
		return nil, fieldError("MessageGroupID", "required for FIFO")
	}
	if config.MessageGroupID != "" && !validToken(config.MessageGroupID) {
		return nil, fieldError("MessageGroupID", "expected 1..128 printable ASCII characters without spaces")
	}
	if config.Observer != nil {
		snapshot := *config.Observer
		config.Observer = &snapshot
	}
	return &Publisher{client: client, config: config}, nil
}

// Publish leaves input unchanged. Nil means SNS accepted the request, not that
// any subscriber received it. Cancellation can leave native acceptance unknown.
func (p *Publisher) Publish(ctx context.Context, message broker.Message) (err error) {
	start := time.Now()
	operation, outcome := OperationMetadata, OutcomeFailed
	defer func() {
		err = errors.Join(err,
			record(p.config.Observer,
				ctx,
				Observation{Source: p.config.Source,
					Operation: operation,
					Outcome:   outcome,
					Duration:  time.Since(start),
					Err:       err}))
	}()
	attrs, err := encodeMessage(message, p.config.RawSQSDelivery)
	if err != nil {
		return &OperationError{Source: p.config.Source, Operation: operation, Cause: err}
	}
	if p.config.FIFO && !validToken(message.ID) {
		return &OperationError{Source: p.config.Source,
			Operation: operation,
			Cause: fieldError("ID",
				"FIFO requires 1..128 printable ASCII characters without spaces")}
	}
	input := &native.PublishInput{TopicArn: aws.String(p.config.TopicARN),
		Message:           aws.String(string(message.Body)),
		MessageAttributes: attrs}
	if p.config.MessageGroupID != "" {
		input.MessageGroupId = aws.String(p.config.MessageGroupID)
	}
	if p.config.FIFO {
		input.MessageDeduplicationId = aws.String(message.ID)
	}
	callCtx, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	operation, outcome = OperationPublish, OutcomeAccepted
	_, err = p.client.Publish(callCtx, input)
	if err != nil {
		outcome = OutcomeUnknown
		err = &OperationError{Source: p.config.Source, Operation: operation, Cause: err}
	}
	return err
}
