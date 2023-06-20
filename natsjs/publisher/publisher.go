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
func Connect(opts ...Option) (p *Publisher, err error) {
	o := &Options{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	p = &Publisher{
		pubOpts: o.pubOpts,
	}

	// reuse connection
	if o.conn != nil {
		p.conn = o.conn
	} else {
		p.conn, err = conn.Establish(o.connOpts...)
		if err != nil {
			return nil, err
		}
	}

	if o.jsCfg != nil {
		if err = p.initJetStream(o.jsCfg); err != nil {
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

// Connection returns connection to NATS
func (p *Publisher) Connection() *conn.Connection {
	return p.conn
}

func (p *Publisher) initJetStream(cfg *nats.StreamConfig) error {
	js, err := p.conn.JetStreamContext().StreamInfo(cfg.Name)
	if err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}

		// create stream if missing
		if _, err = p.conn.JetStreamContext().AddStream(cfg); err != nil {
			return err
		}

		return nil
	}

	// collect existing subjects
	present := make(map[string]struct{}, len(js.Config.Subjects))
	for _, s := range js.Config.Subjects {
		present[s] = struct{}{}
	}

	// add subjects which are not present
	for _, s := range cfg.Subjects {
		if _, ok := present[s]; !ok {
			js.Config.Subjects = append(js.Config.Subjects, s)
		}
	}

	if _, err = p.conn.JetStreamContext().UpdateStream(&js.Config); err != nil {
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
