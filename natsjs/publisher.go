package natsjs

import (
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

type Publisher struct {
	// usually project name is used
	streamName string
	// usually service name is used
	subjectPrefix string
	nc            *nats.Conn
	js            nats.JetStreamContext
	options       *publisherOptions
}

func NewPublisher(
	streamName string,
	subjectPrefix string,
	connFactory ConnectionFactory,
	jsFactory JetStreamFactory,
	options ...PublisherOption,
) (*Publisher, error) {
	opts := &publisherOptions{
		ConnURL: nats.DefaultURL,
	}
	for _, o := range options {
		o(opts)
	}
	if subjectPrefix == "" {
		return nil, ErrSubjectPrefix
	}
	if streamName == "" {
		return nil, ErrStreamName
	}

	publisher := &Publisher{
		streamName:    strings.ToUpper(streamName),
		subjectPrefix: strings.ToUpper(subjectPrefix),
		options:       opts,
	}

	jsInit := make(chan interface{})
	defer close(jsInit)
	nc, js, err := connect(
		opts.ConnURL,
		connFactory,
		jsFactory,
		nats.ConnectHandler(publisher.connectedCb(jsInit)),
		nats.ReconnectHandler(publisher.reconnectCb(jsInit)),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, err
	}

	publisher.js = js
	publisher.nc = nc

	return publisher, nil
}

func (p *Publisher) Publish(subject string, message *broker.Message) error {
	subject, err := buildSubject(subject, p.subjectPrefix)
	if err != nil {
		return err
	}

	broker.SetIDHeader(message)

	msg := nats.NewMsg(subject)
	msg.Header = *copyMessageHeader(message)
	msg.Data = message.Body

	_, err = p.js.PublishMsg(msg)
	if err != nil {
		return errors.Wrap(err, "NATS JetStream: cannot send message")
	}
	return nil
}

func (p *Publisher) Close() {
	p.nc.Close()
}

func (p *Publisher) addStream() error {
	subject := p.subjectPrefix + ".>"
	si, err := p.js.StreamInfo(p.streamName)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			_, err = p.js.AddStream(&nats.StreamConfig{
				Name:      p.streamName,
				Subjects:  []string{subject},
				Retention: nats.InterestPolicy,
			})
		}
		return err
	}
	for _, v := range si.Config.Subjects {
		if v == subject {
			return nil
		}
	}
	si.Config.Subjects = append(si.Config.Subjects, subject)
	_, err = p.js.UpdateStream(&si.Config)
	return err
}

func (p *Publisher) connectedCb(jsInit chan interface{}) func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		// wait for jetStream init
		<-jsInit
		if err := p.addStream(); err != nil {
			panic(err)
		}
		cb := p.options.ConnectedCb
		if cb != nil {
			cb(nc)
		}
	}
}

func (p *Publisher) reconnectCb(jsInit chan interface{}) func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		<-jsInit
		if err := p.addStream(); err != nil {
			panic(err)
		}
		cb := p.options.ReconnectedCb
		if cb != nil {
			cb(nc)
		}
	}
}

type publisherOptions struct {
	// connection URL
	ConnURL string
	// connect callback
	ConnectedCb func(nc *nats.Conn)
	// reconnect callback
	ReconnectedCb func(nc *nats.Conn)
}

type PublisherOption func(options *publisherOptions)

func PublisherConnURL(url string) PublisherOption {
	return func(options *publisherOptions) {
		options.ConnURL = url
	}
}

func PublisherConnectCb(cb func(nc *nats.Conn)) PublisherOption {
	return func(options *publisherOptions) {
		options.ConnectedCb = cb
	}
}

func PublisherReconnectCb(cb func(nc *nats.Conn)) PublisherOption {
	return func(options *publisherOptions) {
		options.ReconnectedCb = cb
	}
}
