package natsjs

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

const (
	defaultMaxUnsubscribeWaitingTime = 20 * time.Second
	defaultDelayBetweenSubAttempt    = 5 * time.Second
	defaultMaxSubAttempts            = -1 // unlimited

	JSErrCodeConsumerFilterSubjectInvalid = 10093
)

type Subscriber struct {
	// usually project name is used
	streamName string
	// current service name, used as a queueGroup and as a prefix in consumer
	serviceName   string
	subFactory    SubscriptionFactory
	conFactory    ConsumerFactory
	js            nats.JetStreamContext
	nc            *nats.Conn
	options       *subscriberOptions
	subscriptions sync.Map
	mutex         sync.RWMutex
	isConnected   bool
}

type SubscriptionFactory func(js nats.JetStreamContext, subj, queue string, handler nats.MsgHandler, options ...nats.SubOpt) (*nats.Subscription, error)
type ConsumerFactory func(js nats.JetStreamContext, stream, consumer, subject, queue string) error

func DefaultSubscriptionFactory(customOptions ...nats.SubOpt) SubscriptionFactory {
	return func(js nats.JetStreamContext, subj, queue string, handler nats.MsgHandler, options ...nats.SubOpt) (*nats.Subscription, error) {
		return js.QueueSubscribe(subj, queue, handler, append(options, customOptions...)...)
	}
}

func DefaultConsumerFactory(cfg *nats.ConsumerConfig, opts ...nats.JSOpt) ConsumerFactory {
	return func(js nats.JetStreamContext, stream, consumer, subject, queue string) error {
		var config nats.ConsumerConfig
		if cfg == nil {
			config = nats.ConsumerConfig{
				DeliverPolicy: nats.DeliverLastPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
			}
		} else {
			config = *cfg
		}
		config.Durable = consumer
		config.FilterSubject = subject
		config.DeliverGroup = queue
		config.DeliverSubject = nats.NewInbox()

		_, err := js.AddConsumer(stream, &config, opts...)
		return err
	}
}

func NewSubscriber(
	streamName,
	serviceName string,
	connFactory ConnectionFactory,
	jsFactory JetStreamFactory,
	subFactory SubscriptionFactory,
	consumerFactory ConsumerFactory,
	options ...SubscriberOption,
) (*Subscriber, error) {
	opts := &subscriberOptions{
		ConnURL:                nats.DefaultURL,
		MaxSubAttempts:         defaultMaxSubAttempts,
		DelayBetweenSubAttempt: defaultDelayBetweenSubAttempt,
		UnsubscribeWaitingTime: defaultMaxUnsubscribeWaitingTime,
	}
	for _, o := range options {
		o(opts)
	}
	if serviceName == "" {
		return nil, ErrServiceName
	}
	if streamName == "" {
		return nil, ErrStreamName
	}

	subscriber := &Subscriber{
		streamName:  strings.ToUpper(streamName),
		serviceName: strings.ToUpper(serviceName),
		subFactory:  subFactory,
		conFactory:  consumerFactory,
		options:     opts,
		mutex:       sync.RWMutex{},
		isConnected: false,
	}

	nc, js, err := connect(
		opts.ConnURL,
		connFactory,
		jsFactory,
		nats.ConnectHandler(subscriber.connectedCb()),
		nats.ReconnectHandler(subscriber.reconnectCb()),
		nats.DisconnectErrHandler(subscriber.disconnectErrCb()),
		nats.RetryOnFailedConnect(true),
	)

	if err != nil {
		return nil, err
	}

	subscriber.js = js
	subscriber.nc = nc

	return subscriber, nil
}

func (s *Subscriber) Subscribe(
	subject string,
	handler broker.Handler,
	options ...broker.SubscribeOption,
) (broker.Subscription, error) {
	opts := broker.DefaultSubscribeOptions()
	for _, o := range options {
		o(opts)
	}

	subject, err := buildSubject(subject, "")
	if err != nil {
		return nil, err
	}

	consumer := buildConsumerName(subject, s.serviceName)
	queueGroup := s.serviceName

	if _, exist := s.subscriptions.Load(subject); exist {
		return nil, errors.Wrapf(
			broker.AlreadySubscribed,
			"NATS JetStream: the subject %q already has a subscription",
			subject,
		)
	}

	sub := &subscription{
		subscriber:          s,
		DefaultSubscription: broker.NewDefaultSubscription(subject, opts, options, handler),
		lock:                make(chan bool, 1),
	}

	s.subscriptions.Store(subject, sub)

	go s.subscribe(subject, consumer, queueGroup, sub)

	return sub, nil
}

func (p *Subscriber) Close() {
	p.nc.Close()
}

func (s *Subscriber) subscribe(subject, consumer, queueGroup string, sub *subscription) {
	logger := sub.Options().Logger
	done := sub.Done()
	attempts := 1
	for {
		select {
		case <-done:
			return
		default:
		}
		if s.options.MaxSubAttempts != -1 && attempts > s.options.MaxSubAttempts {
			if logger != nil {
				logger.Error(
					fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], subscription max attempts", subject, consumer),
				)
			}
			break
		}
		s.mutex.RLock()
		isConnected := s.isConnected
		s.mutex.RUnlock()
		if isConnected {
			if err := s.addConsumer(consumer, queueGroup, subject, logger); err != nil {
				isFailed := true
				var apiErr *nats.APIError
				if errors.As(err, &apiErr) && apiErr.ErrorCode == JSErrCodeConsumerFilterSubjectInvalid {
					isFailed = false
				} else if errors.Cause(err) == nats.ErrStreamNotFound {
					isFailed = false
				}
				if isFailed {
					if logger != nil {
						logger.Error(
							fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], %s", subject, consumer, err),
						)
					}
					break
				}
				if logger != nil {
					logger.Info(
						fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], resubscribe %s", subject, consumer, err),
					)
				}
			} else {
				natsSub, inErr := s.subFactory(
					s.js,
					subject,
					queueGroup,
					s.subCallback(subject, sub),
					nats.Durable(consumer),
					nats.ManualAck(),
					nats.DeliverLast(),
				)
				if inErr != nil {
					if logger != nil {
						logger.Error(
							fmt.Sprintf("NATS JetStream 2: subject [%q], consumer [%q], %s", subject, consumer, inErr),
						)
					}
					break
				}
				sub.nsub = natsSub

				if logger != nil {
					logger.Info(
						fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], subscribed", subject, consumer),
					)
				}
				if s.options.SubscribedCb != nil {
					s.options.SubscribedCb(subject)
				}
				break
			}
		}
		attempts += 1
		time.Sleep(s.options.DelayBetweenSubAttempt)
		if logger != nil {
			logger.Info(
				fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], reconnection [%d]", subject, consumer, attempts),
			)
		}
	}
}

func (s *Subscriber) addConsumer(consumer, queueGroup, subject string, logger broker.Logger) error {
	err := s.conFactory(s.js, s.streamName, consumer, subject, queueGroup)
	if err != nil {
		var apiErr *nats.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == nats.JSErrCodeConsumerNameExists {
			if logger != nil {
				logger.Info(fmt.Sprintf("NATS JetStream: the customer [%q] is already used", consumer))
			}
			return nil
		}
	}
	return err
}

func (s *Subscriber) subCallback(subject string, sub *subscription) func(msg *nats.Msg) {
	done := sub.Done()
	options := sub.Options()
	handler := sub.Handler()
	return func(msg *nats.Msg) {
		select {
		case <-done:
			return
		default:
		}

		brokerMessage := &broker.Message{
			Header: buildMessageHeader(&msg.Header),
			Body:   msg.Data,
		}
		brokerMessage.ID = brokerMessage.Header["id"]

		eventMessage := &message{subject: subject, message: brokerMessage, natsMessage: msg}
		sub.lock <- true
		if err := handler(eventMessage); err != nil {
			_ = msg.Nak()
			<-sub.lock
			if options.ErrorHandler != nil {
				options.ErrorHandler(
					errors.Wrap(err, "cannot handle received message"),
					sub,
				)
				return
			} else {
				panic(err)
			}
		}
		if options.AutoAck {
			if err := eventMessage.Ack(); err != nil {
				_ = msg.Nak()
				<-sub.lock
				if options.ErrorHandler != nil {
					options.ErrorHandler(
						errors.Wrap(err, "cannot auto ack received message"),
						sub,
					)
					return
				} else {
					panic(err)
				}
			}
		}
		<-sub.lock
	}
}

func (s *Subscriber) connectedCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		s.mutex.Lock()
		s.isConnected = true
		s.mutex.Unlock()
	}
}

func (s *Subscriber) reconnectCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		s.mutex.Lock()
		s.isConnected = true
		s.mutex.Unlock()

		// Always re-subscribe on server restart, because we can't know what the reboot was
		s.subscriptions.Range(func(k, v interface{}) bool {
			sub := v.(*subscription)
			log := sub.Options().Logger
			if sub.nsub == nil {
				if log != nil {
					msg := fmt.Sprintf("Nats JetStream: topic [%q] is not ready to reconnect: ", sub.Topic())
					log.Info(msg)
				}
			} else if err := sub.Unsubscribe(); err != nil && log != nil {
				msg := fmt.Sprintf("Nats JetStream: unable to unsubscribe from topic [%q]: %s", sub.Topic(), err)
				log.Error(msg)
			} else if err == nil {
				<-sub.Done()
				newSub, inErr := s.Subscribe(sub.Topic(), sub.Handler(), sub.InitOptions()...)
				if inErr == nil {
					s.subscriptions.Store(k, newSub)
				} else if log != nil {
					msg := fmt.Sprintf("NATS JetStream: unable to subscribe to the topic %q: %s", sub.Topic(), inErr)
					log.Error(msg)
				}
			}
			return true
		})
	}
}

func (s *Subscriber) disconnectErrCb() nats.ConnErrHandler {
	return func(nc *nats.Conn, _ error) {
		s.mutex.Lock()
		s.isConnected = false
		s.mutex.Unlock()
	}
}

type subscriberOptions struct {
	// connection URL
	ConnURL                string
	MaxSubAttempts         int
	DelayBetweenSubAttempt time.Duration
	// The timeout of message processing after unsubscribing
	UnsubscribeWaitingTime time.Duration
	// Subscribed callback
	SubscribedCb func(subject string)
}

type SubscriberOption func(options *subscriberOptions)

func SubscriberConnURL(url string) SubscriberOption {
	return func(options *subscriberOptions) {
		options.ConnURL = url
	}
}

func SubscribedCb(cb func(subject string)) SubscriberOption {
	return func(options *subscriberOptions) {
		options.SubscribedCb = cb
	}
}

func MaxSubAttempts(attempts int) SubscriberOption {
	return func(options *subscriberOptions) {
		options.MaxSubAttempts = attempts
	}
}

func DelayBetweenAttempts(delay time.Duration) SubscriberOption {
	return func(options *subscriberOptions) {
		options.DelayBetweenSubAttempt = delay
	}
}

func UnsubscribeWaitingTime(durationSeconds time.Duration) SubscriberOption {
	return func(options *subscriberOptions) {
		options.UnsubscribeWaitingTime = durationSeconds
	}
}

type subscription struct {
	nsub       *nats.Subscription
	subscriber *Subscriber
	*broker.DefaultSubscription
	lock chan bool
}

func (s *subscription) Unsubscribe() error {
	select {
	case <-s.Done():
		return nil
	default:
	}

	sub, ok := s.subscriber.subscriptions.Load(s.Topic())
	if !ok {
		return s.DefaultSubscription.Unsubscribe()
	}

	if err := sub.(*subscription).nsub.Unsubscribe(); err != nil {
		s.unsubscribe()
		return errors.Wrapf(err, "NATS JetStream: unsubscribe from subject %q", s.Topic())
	}

	s.unsubscribe()
	return nil
}

func (s *subscription) unsubscribe() {
	select {
	case s.lock <- true:
	case <-time.After(s.subscriber.options.UnsubscribeWaitingTime):
	}
	s.subscriber.subscriptions.Delete(s.Topic())
	_ = s.DefaultSubscription.Unsubscribe()
	close(s.lock)
}
