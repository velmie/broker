package sqs

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

const (
	defaultMaxNumberOfMessages = 1
	defaultWaitTimeSeconds     = 20
)

type Subscriber struct {
	sqsService    Service
	options       *subscriberOptions
	subscriptions map[string]struct{}
	mutex         sync.RWMutex
}

func NewSubscriber(sqsService Service, options ...SubscriberOption) *Subscriber {
	opts := &subscriberOptions{
		MaxNumberOfMessages: defaultMaxNumberOfMessages,
		WaitTimeSeconds:     defaultWaitTimeSeconds,
	}
	for _, o := range options {
		o(opts)
	}
	return &Subscriber{sqsService, opts, make(map[string]struct{}), sync.RWMutex{}}
}

func (s *Subscriber) Subscribe(
	topic string,
	handler broker.Handler,
	options ...broker.SubscribeOption,
) (broker.Subscription, error) {
	opts := broker.DefaultSubscribeOptions()
	for _, o := range options {
		o(opts)
	}
	log := opts.Logger
	s.mutex.RLock()
	if _, exist := s.subscriptions[topic]; exist {
		s.mutex.RUnlock()
		return nil, errors.Wrapf(
			broker.AlreadySubscribed,
			"SQS: the topic %q already has a subscription",
			topic,
		)
	}
	s.mutex.RUnlock()

	queueURL, err := getQueueURL(s.sqsService, topic)
	if err != nil {
		return nil, err
	}

	sub := &subscription{
		subscriber:          s,
		DefaultSubscription: broker.NewDefaultSubscription(topic, opts, options, handler),
	}
	s.mutex.Lock()
	s.subscriptions[topic] = struct{}{}
	s.mutex.Unlock()

	go s.startReceivingMessages(queueURL, sub, log)

	return sub, nil
}

func (s *Subscriber) startReceivingMessages(queueURL string, sub broker.Subscription, log broker.Logger) {
	const allAttributes = "All"

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: &s.options.MaxNumberOfMessages,
		MessageAttributeNames: aws.StringSlice([]string{
			allAttributes,
		}),
	}
	if s.options.WaitTimeSeconds > 0 {
		input.WaitTimeSeconds = &s.options.WaitTimeSeconds
	}
	if log != nil {
		info := "SQS: subscribed on the %q topic with parameters: " +
			"QueueUrl = %q, MaxNumberOfMessages = %d, WaitTimeSeconds = %d"
		log.Info(fmt.Sprintf(info, sub.Topic(), queueURL, s.options.MaxNumberOfMessages, s.options.WaitTimeSeconds))
	}
	done := sub.Done()
	options := sub.Options()
	topic := sub.Topic()
	handler := sub.Handler()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			out, err := s.sqsService.ReceiveMessage(input)
			select {
			case <-done:
				return
			default:
			}
			if err != nil {
				if options.ErrorHandler != nil {
					options.ErrorHandler(
						errors.Wrap(err, "cannot receive new message from queue"),
						sub,
					)
					continue
				} else {
					panic(err)
				}
			}
			for _, msg := range out.Messages {
				select {
				case <-done:
					return
				default:
				}
				brokerMessage := &broker.Message{
					Header: buildMessageHeader(msg.MessageAttributes),
					Body:   []byte(*msg.Body),
				}
				brokerMessage.ID = brokerMessage.Header["id"]

				eventMessage := &message{
					topic:      topic,
					message:    brokerMessage,
					queueURL:   queueURL,
					sqsMessage: msg,
					sqsService: s.sqsService,
				}
				if err = handler(eventMessage); err != nil {
					if options.ErrorHandler != nil {
						options.ErrorHandler(
							errors.Wrap(err, "cannot handle received message"),
							sub,
						)
						continue
					} else {
						panic(err)
					}
				}
				if options.AutoAck {
					if err = eventMessage.Ack(); err != nil {
						if options.ErrorHandler != nil {
							options.ErrorHandler(
								errors.Wrap(err, "cannot auto ack received message"),
								sub,
							)
							continue
						} else {
							panic(err)
						}
					}
				}
			}
		}
	}()
}

type subscriberOptions struct {
	// The maximum number of messages to return. Amazon SQS never returns more messages
	// than this value (however, fewer messages might be returned). Valid values:
	// 1 to 10. Default: 1.
	MaxNumberOfMessages int64
	// The duration (in seconds) for which the call waits for a message to arrive
	// in the queue before returning. If a message is available, the call returns
	// sooner than WaitTimeSeconds. If no messages are available and the wait time
	// expires, the call returns successfully with an empty list of messages.
	//
	// To avoid HTTP errors, ensure that the HTTP response timeout for ReceiveMessage
	// requests is longer than the WaitTimeSeconds parameter. For example, with
	// the Java SDK, you can set HTTP transport settings using the NettyNioAsyncHttpClient
	// (https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.html)
	// for asynchronous clients, or the ApacheHttpClient
	// (https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.html)
	// for synchronous clients.
	WaitTimeSeconds int64
}

type SubscriberOption func(options *subscriberOptions)

func LongPollingDuration(durationSeconds int64) SubscriberOption {
	return func(options *subscriberOptions) {
		options.WaitTimeSeconds = durationSeconds
	}
}

func RequestMultipleMessage(maxNumberPerRequest int64) SubscriberOption {
	return func(options *subscriberOptions) {
		options.MaxNumberOfMessages = maxNumberPerRequest
	}
}

type subscription struct {
	subscriber *Subscriber
	*broker.DefaultSubscription
}

func (s *subscription) Unsubscribe() error {
	select {
	case <-s.Done():
		return nil
	default:
	}
	s.subscriber.mutex.Lock()
	delete(s.subscriber.subscriptions, s.Topic())
	s.subscriber.mutex.Unlock()
	return s.DefaultSubscription.Unsubscribe()
}
