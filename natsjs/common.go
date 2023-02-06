package natsjs

import (
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
)

type ConnectionFactory func(url string, options ...nats.Option) (*nats.Conn, error)

func DefaultConnectionFactory(customOptions ...nats.Option) ConnectionFactory {
	return func(url string, options ...nats.Option) (*nats.Conn, error) {
		return nats.Connect(url, append(options, customOptions...)...)
	}
}

type JetStreamFactory func(nc *nats.Conn, options ...nats.JSOpt) (nats.JetStreamContext, error)

func DefaultJetStreamFactory(customOptions ...nats.JSOpt) JetStreamFactory {
	return func(nc *nats.Conn, options ...nats.JSOpt) (nats.JetStreamContext, error) {
		return nc.JetStream(append(options, customOptions...)...)
	}
}

func copyMessageHeader(m *broker.Message) *nats.Header {
	header := make(nats.Header)
	for k, v := range m.Header {
		header.Set(k, v)
	}
	return &header
}

func buildMessageHeader(header *nats.Header) map[string]string {
	res := make(map[string]string)

	for k, v := range *header {
		res[k] = v[0]
	}
	return res
}

func connect(
	url string,
	connFactory ConnectionFactory,
	jsFactory JetStreamFactory,
	opts ...nats.Option,
) (*nats.Conn, nats.JetStreamContext, error) {
	nc, err := connFactory(url, opts...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "NATS JetStream: connection failed %q", url)
	}

	js, err := jsFactory(nc)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "NATS JetStream: connection failed %q", url)
	}
	return nc, js, nil
}

func buildSubject(subject, subjectPrefix string) (string, error) {
	subjParts := strings.Split(subject, ".")
	if len(subjParts) < 2 {
		return "", errors.Wrapf(ErrSubjectInvalid, "NATS JetStream: the subject %q", subject)
	}

	if subjectPrefix == "" {
		subjectPrefix = strings.ToUpper(subjParts[0])
	}

	if strings.ToUpper(subjParts[0]) == subjectPrefix {
		subjParts = subjParts[1:]
	}

	return strings.Join(append([]string{subjectPrefix}, subjParts...), "."), nil
}

func buildConsumerName(subject, serviceName string) string {
	subjParts := strings.Split(subject, ".")

	consumer := serviceName
	for _, v := range subjParts {
		consumer += fmt.Sprintf("-%s", strings.ToUpper(v))
	}
	return consumer
}
