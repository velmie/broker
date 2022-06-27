package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

func getQueueURL(sqsService Service, topic string) (string, error) {
	getURLInput := &sqs.GetQueueUrlInput{QueueName: &topic}
	out, err := sqsService.GetQueueUrl(getURLInput)
	if err != nil {
		return "", errors.Wrapf(err, "cannot get queue url by the given topic %q", topic)
	}
	return *out.QueueUrl, nil
}

func copyMessageHeader(m *broker.Message) (attribs map[string]*sqs.MessageAttributeValue) {
	attribs = make(map[string]*sqs.MessageAttributeValue)
	for k, v := range m.Header {
		attribs[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return attribs
}

func buildMessageHeader(attribs map[string]*sqs.MessageAttributeValue) map[string]string {
	res := make(map[string]string)

	for k, v := range attribs {
		res[k] = *v.StringValue
	}
	return res
}
