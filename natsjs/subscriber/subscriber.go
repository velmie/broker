package subscriber

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"

	"github.com/velmie/broker/natsjs/v2/conn"
)

// Subscriber represents event subscriber
type Subscriber struct {
	stream          string
	conn            *conn.Connection
	subFactory      SubscriptionFactoryFunc
	consFactory     ConsumerFactoryFunc
	grpNamerFactory GroupNamerFactoryFunc
}

// Connect establish conn.Connection from Subscriber to NATS server with Option(s) specified
func Connect(stream string, opts ...Option) (s *Subscriber, err error) {
	o := &Options{
		subFactory:      DefaultSubscriptionFactory(),
		grpNamerFactory: DefaultGroupNamerFactory(""),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	s = &Subscriber{
		stream:          stream,
		subFactory:      o.subFactory,
		consFactory:     o.consFactory,
		grpNamerFactory: o.grpNamerFactory,
	}

	// reuse connection
	if o.conn != nil {
		s.conn = o.conn
	} else {
		s.conn, err = conn.Establish(o.connOpts...)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Subscribe creates subscription on corresponding subject
func (s *Subscriber) Subscribe(
	subj string,
	handler broker.Handler,
	options ...broker.SubscribeOption,
) (broker.Subscription, error) {
	if err := s.createConsumer(subj); err != nil {
		return nil, err
	}

	groupNamer, err := s.grpNamerFactory(s.stream, subj)
	if err != nil {
		return nil, fmt.Errorf("failed to create group namer: %w", err)
	}
	scb, err := s.subFactory(subj, groupNamer)
	if err != nil {
		return nil, err
	}

	// build subscription and start listening
	ns, err := scb.subscribe(s.conn.JetStreamContext(), handler, options...)
	if err != nil {
		return nil, err
	}

	return ns, nil
}

// Close closes connection. Refer to nats.Conn #Close
func (s *Subscriber) Close() {
	s.conn.Close()
}

// Drain drains connection. Refer to nats.Conn #Drain
func (s *Subscriber) Drain() error {
	return s.conn.Drain()
}

// Connection returns connection to NATS
func (s *Subscriber) Connection() *conn.Connection {
	return s.conn
}

func (s *Subscriber) createConsumer(subj string) error {
	if s.consFactory == nil {
		return nil
	}

	groupNamer, err := s.grpNamerFactory(s.stream, subj)
	if err != nil {
		return fmt.Errorf("failed to create group namer: %w", err)
	}
	cfg, err := s.consFactory(subj, groupNamer)
	if err != nil {
		return fmt.Errorf("failed to create consumer config: %w", err)
	}
	if cfg.FilterSubject == "" {
		cfg.FilterSubject = subj
	}

	_, err = s.conn.JetStreamContext().ConsumerInfo(s.stream, cfg.Name)
	if errors.Is(err, nats.ErrConsumerNotFound) {
		_, err = s.conn.JetStreamContext().AddConsumer(s.stream, cfg)
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}
