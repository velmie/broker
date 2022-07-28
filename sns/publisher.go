package sns

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

type Publisher struct {
	snsService     *sns.SNS
	messageGroupID string
	accountID      string
}

func NewPublisher(snsService *sns.SNS, messageGroupID, accountID string) *Publisher {
	return &Publisher{snsService, messageGroupID, accountID}
}

func (p *Publisher) Publish(topic string, message *broker.Message) error {
	const (
		partition = "aws"
		service   = "sns"
	)
	topicArn := arn.ARN{
		Partition: partition,
		Service:   service,
		Region:    *p.snsService.Config.Region,
		AccountID: p.accountID,
		Resource:  topic,
	}.String()

	broker.SetIDHeader(message)

	input := &sns.PublishInput{
		MessageAttributes: copyMessageHeader(message),
		Message:           aws.String(string(message.Body)),
		TopicArn:          &topicArn,
	}
	if isFifo(topic) {
		input.MessageGroupId = &p.messageGroupID
		input.MessageDeduplicationId = &message.ID
	}
	_, err := p.snsService.Publish(input)
	if err != nil {
		return errors.Wrapf(err, "SNS: cannot publish message to the topic %q", topic)
	}

	return nil
}
