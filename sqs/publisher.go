package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

type Publisher struct {
	sqsService      *sqs.SQS
	messageGroupID  string
	queueURLByTopic map[string]string
}

func NewPublisher(
	sqsService *sqs.SQS,
	messageGroupID string,
) *Publisher {
	return &Publisher{sqsService, messageGroupID, make(map[string]string)}
}

func (p *Publisher) Publish(topic string, message *broker.Message) (err error) {
	queueURL := p.queueURLByTopic[topic]
	if queueURL == "" {
		queueURL, err = getQueueURL(p.sqsService, topic)
		if err != nil {
			return err
		}
		p.queueURLByTopic[topic] = queueURL
	}

	broker.SetIDHeader(message)

	input := &sqs.SendMessageInput{
		MessageAttributes:      copyMessageHeader(message),
		MessageBody:            aws.String(string(message.Body)),
		MessageDeduplicationId: &message.ID,
		MessageGroupId:         &p.messageGroupID,
		QueueUrl:               &queueURL,
	}
	_, err = p.sqsService.SendMessage(input)
	if err != nil {
		return errors.Wrap(err, "SQS: cannot send message")
	}
	return nil
}
