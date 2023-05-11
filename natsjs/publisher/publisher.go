package publisher

import (
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/velmie/broker"

	"github.com/velmie/broker/natsjs/v2/conn"
)

// Publisher represents events publisher
type Publisher struct {
	conn    *conn.Connection
	pubOpts []nats.PubOpt
}

// Connect establish conn.Connection from Publisher to NATS server with Option(s) specified
func Connect(opts ...Option) (*Publisher, error) {
	o := &Options{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	conn, err := conn.Establish(o.connOpts...)
	if err != nil {
		return nil, err
	}

	p := &Publisher{
		conn:    conn,
		pubOpts: o.pubOpts,
	}

	if o.jsCfg != nil {
		if err = p.createJetStream(o.jsCfg); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// Publish publishes message to JetStream
func (p *Publisher) Publish(subj string, msg *broker.Message) error {
	m := p.toNATSMsg(subj, msg)

	_, err := p.conn.JetStreamContext().PublishMsg(m, p.pubOpts...)
	if err != nil {
		return err
	}

	return nil
}

// Close closes connection. Refer to nats.Conn #Close
func (p *Publisher) Close() {
	p.conn.Close()
}

// Drain drains connection. Refer to nats.Conn #Drain
func (p *Publisher) Drain() error {
	return p.conn.Drain()
}

func (p *Publisher) createJetStream(cfg *nats.StreamConfig) error {
	if _, err := p.conn.JetStreamContext().AddStream(cfg); err != nil {
		if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return nil
		}
		return err
	}
	return nil
}

func (p *Publisher) toNATSMsg(subj string, m *broker.Message) *nats.Msg {
	msg := nats.NewMsg(subj)
	msg.Data = m.Body

	headers := make(nats.Header)

	headers.Set("natsjs-msg-id", m.ID)
	for k, v := range m.Header {
		headers.Set(k, v)
	}
	msg.Header = headers

	return msg
}
