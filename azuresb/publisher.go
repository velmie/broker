package azuresb

import (
	"context"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"
	"github.com/velmie/broker"
)

// ASBSender abstracts the methods of an Azure Service Bus sender
type ASBSender interface {
	SendMessage(ctx context.Context, msg *azservicebus.Message, opts *azservicebus.SendMessageOptions) error
}

// SenderFactory defines a factory interface for creating senders
type SenderFactory interface {
	// CreateSender returns a new ASBSender for the given topic
	CreateSender(topic string) (ASBSender, error)
}

// Publisher implements the broker.Publisher interface for Azure Service Bus
type Publisher struct {
	senderFactory  SenderFactory
	sendersByTopic sync.Map // map[string]ASBSender
}

// NewPublisher creates a new Publisher using the provided SenderFactory
func NewPublisher(factory SenderFactory) *Publisher {
	return &Publisher{
		senderFactory: factory,
	}
}

// Publish sends a message to the specified topic using Azure Service Bus
func (p *Publisher) Publish(topic string, message *broker.Message) error {
	broker.SetIDHeader(message)

	value, ok := p.sendersByTopic.Load(topic)
	var sender ASBSender
	if ok {
		sender = value.(ASBSender)
	} else {
		var err error
		sender, err = p.senderFactory.CreateSender(topic)
		if err != nil {
			return errors.Wrapf(err, "azuresb: cannot create sender for topic %q", topic)
		}
		actual, loaded := p.sendersByTopic.LoadOrStore(topic, sender)
		if loaded {
			sender = actual.(ASBSender)
		}
	}

	asbMsg := &azservicebus.Message{
		Body:                  message.Body,
		MessageID:             &message.ID,
		ApplicationProperties: stringMapToAnyMap(message.Header),
	}

	ctx := message.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	if err := sender.SendMessage(ctx, asbMsg, nil); err != nil {
		return errors.Wrap(err, "azuresb: cannot send message")
	}
	return nil
}

func stringMapToAnyMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
