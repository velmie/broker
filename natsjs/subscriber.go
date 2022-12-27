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
	defaultMaxDrainDuration       = 20
	defaultDelayBetweenSubAttempt = 10 * time.Second
	defaultMaxSubAttempts         = -1 // unlimited

	JSErrCodeConsumerFilterSubjectInvalid = 10093
)

type Subscriber struct {
	// usually project name is used
	streamName string
	// current service name, used as a queueGroup and as a prefix in consumer
	serviceName string
	subFactory  SubscriptionFactory
	conFactory  ConsumerFactory
	jetStream   nats.JetStreamContext
	options     *subscriberOptions
	//subscriptions map[string]*subscription
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
	subFactory SubscriptionFactory,
	consumerFactory ConsumerFactory,
	options ...SubscriberOption,
) (*Subscriber, error) {
	opts := &subscriberOptions{
		DrainDuration:          defaultMaxDrainDuration,
		ConnURL:                nats.DefaultURL,
		MaxSubAttempts:         defaultMaxSubAttempts,
		DelayBetweenSubAttempt: defaultDelayBetweenSubAttempt,
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

	jetStream, err := connect(
		opts.ConnURL,
		connFactory,
		nats.ConnectHandler(subscriber.connectedCb()),
		nats.ReconnectHandler(subscriber.reconnectCb()),
		nats.DisconnectErrHandler(subscriber.disconnectErrCb()),
		nats.RetryOnFailedConnect(true),
	)

	if err != nil {
		return nil, err
	}

	subscriber.jetStream = jetStream

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

	fmt.Println(fmt.Sprintf("Consumer: [%q], QueueGroup: [%q]", consumer, queueGroup))

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
	}

	s.subscriptions.Store(subject, sub)

	go s.subscribe(subject, consumer, queueGroup, sub)

	return sub, nil
}

func (s *Subscriber) subscribe(subject, consumer, queueGroup string, sub *subscription) {
	logger := sub.Options().Logger
	done := sub.Done()
	attempts := 1
	for {
		if s.options.MaxSubAttempts != -1 && attempts > s.options.MaxSubAttempts {
			if logger != nil {
				logger.Error(
					fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], subscription max attempts", subject, consumer),
				)
			}
			<-done
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
					<-done
					break
				}
				if logger != nil {
					logger.Info(
						fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], resubscribe %s", subject, consumer, err),
					)
				}
			} else {
				natsSub, inErr := s.subFactory(
					s.jetStream,
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
					<-done
					break
				}
				sub.nsub = natsSub
				//s.subscriptions.Store(subject, sub)

				if logger != nil {
					logger.Info(
						fmt.Sprintf("NATS JetStream: subject [%q], consumer [%q], subscribed", subject, consumer),
					)
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
	err := s.conFactory(s.jetStream, s.streamName, consumer, subject, queueGroup)
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
		if err := handler(eventMessage); err != nil {
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
				if options.ErrorHandler != nil {
					options.ErrorHandler(
						errors.Wrap(err, "cannot auto ack received message"),
						sub,
					)
				} else {
					panic(err)
				}
			}
		}
	}
}

func (s *Subscriber) connectedCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		fmt.Println("Connected first time")
		s.mutex.Lock()
		s.isConnected = true
		s.mutex.Unlock()
	}
}

func (s *Subscriber) reconnectCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		fmt.Println("Reconnected")
		s.mutex.Lock()
		s.isConnected = true
		s.mutex.Unlock()

		// Always re-subscribe on server restart, because we can't know what the reboot was
		s.subscriptions.Range(func(_, v interface{}) bool {
			sub := v.(*subscription)
			log := sub.Options().Logger
			if err := sub.Unsubscribe(); err != nil && log != nil {
				msg := fmt.Sprintf("Nats JetStream: unable to unsubscribe from the topic %q: %s", sub.Topic(), err)
				log.Error(msg)
			} else if err == nil {
				<-sub.Done()
				_, err = s.Subscribe(sub.Topic(), sub.Handler(), sub.InitOptions()...)
				if err != nil && log != nil {
					msg := fmt.Sprintf("NATS JetStream: unable to subscribe to the topic %q: %s", sub.Topic(), err)
					log.Error(msg)
				}
			}
			return true
		})
	}
}

func (s *Subscriber) disconnectErrCb() nats.ConnErrHandler {
	return func(nc *nats.Conn, _ error) {
		fmt.Println("Disconnected")
		s.mutex.Lock()
		s.isConnected = false
		s.mutex.Unlock()
	}
}

type subscriberOptions struct {
	// The duration (in seconds) of message processing after unsubscribing
	DrainDuration int64
	// connection URL
	ConnURL                string
	MaxSubAttempts         int
	DelayBetweenSubAttempt time.Duration
}

type SubscriberOption func(options *subscriberOptions)

func SubcsriberConnURL(url string) SubscriberOption {
	return func(options *subscriberOptions) {
		options.ConnURL = url
	}
}

func DrainDuration(durationSeconds int64) SubscriberOption {
	return func(options *subscriberOptions) {
		options.DrainDuration = durationSeconds
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

type subscription struct {
	nsub       *nats.Subscription
	subscriber *Subscriber
	*broker.DefaultSubscription
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

	if err := sub.(*subscription).nsub.Drain(); err != nil {
		_ = s.unsubscribe()
		return errors.Wrapf(err, "NATS JetStream: subject %q", s.Topic())
	}

	fmt.Println(fmt.Sprintf("Drain waiting: [%q]", s.Topic()))

	done := make(chan bool)
	go func() {
		for {
			pmsgs, _, _ := sub.(*subscription).nsub.Pending()
			if pmsgs < 1 {
				done <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		time.Sleep(time.Second)
	case <-time.After(time.Duration(s.subscriber.options.DrainDuration) * time.Second):
	}

	fmt.Println(fmt.Sprintf("Drain done: [%q]", s.Topic()))

	return s.unsubscribe()
}

func (s *subscription) unsubscribe() error {
	s.subscriber.subscriptions.Delete(s.Topic())
	return s.DefaultSubscription.Unsubscribe()
}
