package subscriber

import (
	"errors"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// event is an implementation of broker.Event
type event struct {
	subj string
	bm   *broker.Message
	nm   *nats.Msg
}

// Topic returns event subject
func (e *event) Topic() string {
	return e.subj
}

// Message returns broker.Message
func (e *event) Message() *broker.Message {
	return e.bm
}

// Ack simply perform message ack
func (e *event) Ack() error {
	if err := e.nm.Ack(); err != nil && !errors.Is(err, nats.ErrMsgAlreadyAckd) {
		return err
	}
	return nil
}
