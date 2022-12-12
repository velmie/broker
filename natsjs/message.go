package natsjs

import (
	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

type message struct {
	subject     string
	message     *broker.Message
	natsMessage *nats.Msg
}

func (m *message) Topic() string {
	return m.subject
}

func (m *message) Message() *broker.Message {
	return m.message
}

func (m *message) Ack() error {
	return m.natsMessage.AckSync()
}
