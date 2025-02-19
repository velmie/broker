package azuresb

import (
	"fmt"

	"github.com/velmie/broker"
)

type ReceiverFactory interface {
	CreateReceiver(topic string) (Receiver, error)
}

// Subscriber is an Azure Service Bus subscriber implementation that conforms to the broker.Subscriber interface
type Subscriber struct {
	receiverFactory ReceiverFactory
}

func NewSubscriber(factory ReceiverFactory) *Subscriber {
	return &Subscriber{
		receiverFactory: factory,
	}
}

// Subscribe subscribes to a specific topic and starts receiving messages
func (s *Subscriber) Subscribe(topic string, handler broker.Handler, options ...broker.SubscribeOption) (broker.Subscription, error) {
	receiver, err := s.receiverFactory.CreateReceiver(topic)
	if err != nil {
		return nil, fmt.Errorf("azuresb: failed to create receiver for topic %q: %w", topic, err)
	}

	opts := broker.DefaultSubscribeOptions()
	for _, o := range options {
		o(opts)
	}

	sub := newSubscription(topic, receiver, handler, opts)
	sub.start()
	return sub, nil
}
