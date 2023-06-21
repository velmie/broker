package subscriber

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/velmie/broker"
)

// subscription implementation of broker.Subscription
type subscription struct {
	subj      string
	doubleAck bool
	nackDelay time.Duration
	nsub      *nats.Subscription
	ackOpts   []nats.AckOpt
	*broker.DefaultSubscription
}

// Unsubscribe unsubscribes from subject and closes subscription
func (s *subscription) Unsubscribe() error {
	// error never occurs with DefaultSubscription
	if err := s.DefaultSubscription.Unsubscribe(); err != nil {
		return err
	}

	// under the hood nats.ErrBadSubscription is returned when already closed
	if err := s.nsub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
		return err
	}

	return nil
}

// msgHandler is a decorator of handler passed by client. Decorator is responsible for message conversion, ack, etc.
func (s *subscription) msgHandler(m *nats.Msg) {
	// build internal event for processing by handler func
	bm := s.brokerMsg(m)
	evt := &event{
		subj: s.subj,
		bm:   bm,
		nm:   m,
	}

	handler := s.Handler()
	errHandler := s.Options().ErrorHandler
	autoAck := s.Options().AutoAck

	// call handler
	err := handler(evt)
	if err != nil && errHandler != nil {
		// if err handler is defined -> call it for error processing
		errHandler(err, s)
	}

	// if auto ack is set, we must ack message or nack depending on error returned or not
	if autoAck {
		// we can't handle ack/nack errors
		if err != nil {
			if s.nackDelay > 0 {
				m.NakWithDelay(s.nackDelay, s.ackOpts...)
			} else {
				m.Nak(s.ackOpts...)
			}
			return
		}

		// if double ack is set -> perform sync ack
		if s.doubleAck {
			m.AckSync(s.ackOpts...)
		} else {
			m.Ack(s.ackOpts...)
		}
	}
}

// brokerMsg converts nats.Msg to broker.Message
func (s *subscription) brokerMsg(m *nats.Msg) *broker.Message {
	header := make(map[string]string)
	for k, v := range m.Header {
		if len(v) > 0 {
			header[k] = v[0]
		}
	}

	return &broker.Message{
		ID:     header["natsjs-msg-id"],
		Header: header,
		Body:   m.Data,
	}
}

// handlePull is handler for pull subscription
func (s *subscription) handlePull(batch int, opts ...nats.PullOpt) {
	errHandler := s.Options().ErrorHandler

PULL:
	for {
		// fetch n messages (n = batch)
		msgs, err := s.nsub.Fetch(batch, opts...)
		if err != nil {
			switch err {
			// if connection is closed already or unsubscribe has been done (ErrBadSubscription is raised in this case)
			case nats.ErrConnectionClosed, nats.ErrBadSubscription:
				break PULL
				// when connection is lost and fetch is not able to retrieve message nats.ErrTimeout is raised
			case nats.ErrTimeout:
				continue PULL
			}

			// if error handler is present -> delegate error handling and go to next iteration
			if errHandler != nil {
				errHandler(err, s)
			}
			continue PULL
		}

		// process each message individually
		for i := range msgs {
			s.msgHandler(msgs[i])
		}
	}
}
