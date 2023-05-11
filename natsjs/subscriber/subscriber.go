package subscriber

import (
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/velmie/broker"

	"github.com/velmie/broker/natsjs/v2/conn"
)

// Subscriber represents event subscriber
type Subscriber struct {
	stream      string
	conn        *conn.Connection
	subFactory  SubscriptionFactoryFunc
	consFactory ConsumerFactoryFunc
}

// Connect establish conn.Connection from Subscriber to NATS server with Option(s) specified
func Connect(stream string, opts ...Option) (*Subscriber, error) {
	o := &Options{subFactory: DefaultSubscriptionFactory()}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	conn, err := conn.Establish(o.connOpts...)
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		stream:      stream,
		conn:        conn,
		subFactory:  o.subFactory,
		consFactory: o.consFactory,
	}, nil
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

	scb := s.subFactory(s.stream, subj)

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

func (s *Subscriber) Drain() error {
	return s.conn.Drain()
}

func (s *Subscriber) createConsumer(subj string) error {
	if s.consFactory == nil {
		return nil
	}

	cfg := s.consFactory(s.stream, subj)

	_, err := s.conn.JetStreamContext().AddConsumer(s.stream, cfg)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return nil
		}
		return err
	}
	return nil
}
