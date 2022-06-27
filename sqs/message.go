package sqs

import (
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/velmie/broker"
)

type message struct {
	topic      string
	message    *broker.Message
	queueURL   string
	sqsMessage *sqs.Message
	sqsService Service
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) Message() *broker.Message {
	return m.message
}

func (m *message) Ack() error {
	deleteMsgInput := &sqs.DeleteMessageInput{
		QueueUrl:      &m.queueURL,
		ReceiptHandle: m.sqsMessage.ReceiptHandle,
	}
	_, err := m.sqsService.DeleteMessage(deleteMsgInput)
	return err
}
