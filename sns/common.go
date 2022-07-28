package sns

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/velmie/broker"
)

func copyMessageHeader(m *broker.Message) (attribs map[string]*sns.MessageAttributeValue) {
	attribs = make(map[string]*sns.MessageAttributeValue)
	for k, v := range m.Header {
		attribs[k] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return attribs
}

func isFifo(topic string) bool {
	const suffix = ".fifo"
	return len(topic) > len(suffix) && topic[len(topic)-len(suffix):] == suffix
}
