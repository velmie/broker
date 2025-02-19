package azuresb

import (
	"context"
	"fmt"
	"sync"

	"github.com/velmie/broker"
)

// subscription implements broker.Subscription for Azure Service Bus.
type subscription struct {
	topic      string
	receiver   Receiver
	handler    broker.Handler
	options    *broker.SubscribeOptions
	done       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

// newSubscription creates a new subscription instance.
func newSubscription(topic string, receiver Receiver, handler broker.Handler, options *broker.SubscribeOptions) *subscription {
	return &subscription{
		topic:    topic,
		receiver: receiver,
		handler:  handler,
		options:  options,
		done:     make(chan struct{}),
	}
}

// start begins receiving messages in a separate goroutine.
func (s *subscription) start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelFunc = cancel
	s.wg.Add(1)
	go s.receiveLoop(ctx)
}

// receiveLoop continuously receives messages and processes them.
func (s *subscription) receiveLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		default:
		}

		msg, err := s.receiver.Receive(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if s.options.ErrorHandler != nil {
				s.options.ErrorHandler(err, s)
			}
			continue
		}

		brokerMsg := &broker.Message{
			ID:     msg.ID,
			Body:   msg.Body,
			Header: msg.Header,
		}

		err = s.handler(&azureEvent{
			topic:    s.topic,
			message:  brokerMsg,
			azureMsg: msg,
		})
		if err != nil {
			if aErr := msg.Abandon(); aErr != nil {
				if s.options.Logger != nil {
					errM := fmt.Sprintf("azuresb.subscription.receiveLoop: Abandon message: %s", aErr)
					s.options.Logger.Error(errM)
				}
			}
			if s.options.ErrorHandler != nil {
				s.options.ErrorHandler(err, s)
			}
		} else {
			if s.options.AutoAck {
				if cErr := msg.Complete(); cErr != nil {
					if s.options.Logger != nil {
						errM := fmt.Sprintf("azuresb.subscription.receiveLoop: Complete message: %s", cErr)
						s.options.Logger.Error(errM)
					}
				}
			}
		}
	}
}

// Unsubscribe stops receiving messages and closes the subscription.
func (s *subscription) Unsubscribe() error {
	s.once.Do(func() {
		close(s.done)
		if s.cancelFunc != nil {
			s.cancelFunc()
		}
		s.wg.Wait()
		_ = s.receiver.Close()
	})
	return nil
}

// Done returns a channel that is closed when the subscription is unsubscribed.
func (s *subscription) Done() <-chan struct{} {
	return s.done
}

// Topic returns the topic of the subscription.
func (s *subscription) Topic() string {
	return s.topic
}

// InitOptions returns the initial subscription options; not supported so returns nil.
func (s *subscription) InitOptions() []broker.SubscribeOption {
	return nil
}

// Options returns the subscription options.
func (s *subscription) Options() *broker.SubscribeOptions {
	return s.options
}

// Handler returns the broker.Handler for this subscription.
func (s *subscription) Handler() broker.Handler {
	return s.handler
}

// azureEvent implements broker.Event for Azure Service Bus messages.
type azureEvent struct {
	topic    string
	message  *broker.Message
	azureMsg *Message
}

// Topic returns the event topic.
func (e *azureEvent) Topic() string {
	return e.topic
}

// Message returns the broker.Message.
func (e *azureEvent) Message() *broker.Message {
	return e.message
}

// Ack completes the Azure Service Bus message.
func (e *azureEvent) Ack() error {
	if err := e.azureMsg.Complete(); err != nil {
		return fmt.Errorf("azuresb: failed to complete message: %w", err)
	}
	return nil
}
