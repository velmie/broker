package natsjs

import (
	"fmt"
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
	jetStream     nats.JetStreamContext
	options       *publisherOptions
}

func NewPublisher(
	streamName string,
	subjectPrefix string,
	connFactory ConnectionFactory,
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

	jetStream, err := connect(
		opts.ConnURL,
		connFactory,
		nats.ConnectHandler(publisher.connectedCb()),
		nats.ReconnectHandler(publisher.reconnectCb()),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, err
	}

	publisher.jetStream = jetStream

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

	_, err = p.jetStream.PublishMsg(msg)
	if err != nil {
		return errors.Wrap(err, "NATS JetStream: cannot send message")
	}
	return nil
}

func (p *Publisher) addStream() error {
	subject := p.subjectPrefix + ".>"
	si, err := p.jetStream.StreamInfo(p.streamName)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			_, err = p.jetStream.AddStream(&nats.StreamConfig{
				Name:     p.streamName,
				Subjects: []string{subject},
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
	_, err = p.jetStream.UpdateStream(&si.Config)
	return err
}

func (p *Publisher) connectedCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		fmt.Println("Connected first time")
		if err := p.addStream(); err != nil {
			panic(err)
		}
	}
}

func (p *Publisher) reconnectCb() func(nc *nats.Conn) {
	return func(nc *nats.Conn) {
		fmt.Println("Reconnected and check stream")
		if err := p.addStream(); err != nil {
			panic(err)
		}
	}
}

type publisherOptions struct {
	// connection URL
	ConnURL string
}

type PublisherOption func(options *publisherOptions)

func PublisherConnURL(url string) PublisherOption {
	return func(options *publisherOptions) {
		options.ConnURL = url
	}
}
