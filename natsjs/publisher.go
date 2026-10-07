package natsjs

import (
	"bytes"
	"context"
	"errors"
	"net/textproto"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// Publisher borrows a connection and waits for a JetStream publication response.
// It never generates a logical ID or closes the connection. Concurrent calls
// may share a read-only Message. A failed response can leave publication unknown.
type Publisher struct {
	js     nats.JetStreamContext
	config PublisherConfig
}

type PublisherConfig struct {
	Subject string
	// OperationTimeout defaults to two seconds. Connection FlusherTimeout must
	// also be finite. Context cancellation never proves the message was not sent.
	OperationTimeout time.Duration
}

func NewPublisher(conn *nats.Conn, config PublisherConfig) (*Publisher, error) {
	if !validSubject(config.Subject, false) {
		return nil, fieldFailure("Subject", "invalid_destination", errors.New("publish Subject: invalid destination"))
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if config.OperationTimeout <= 0 {
		return nil, fieldFailure("OperationTimeout", "must_be_positive", errors.New("publish OperationTimeout: must be positive"))
	}
	if conn.Opts.FlusherTimeout <= 0 {
		return nil, fieldFailure("FlusherTimeout", "must_be_positive", errors.New("publish connection FlusherTimeout: must be positive"))
	}
	js, err := conn.JetStream(nats.MaxWait(config.OperationTimeout))
	if err != nil {
		return nil, &OperationError{Source: config.Subject, Operation: "configure", Cause: err}
	}
	return &Publisher{js: js, config: config}, nil
}

// Publish preserves representable case, bytes and duplicate values. Native
// headers have no cross-key ordering. CR/LF and boundary whitespace are rejected
// because the SDK sanitizes them. Nats-Msg-Id must be unambiguous, canonical and
// agree with Message.ID. The caller's storage is never modified.
func (p *Publisher) Publish(ctx context.Context, message broker.Message) error {
	if err := context.Cause(ctx); err != nil {
		return p.failure(err)
	}
	headers, err := publishHeaders(message)
	if err != nil {
		return p.failure(err)
	}
	bounded, cancel := context.WithTimeout(ctx, p.config.OperationTimeout)
	defer cancel()
	_, err = p.js.PublishMsg(&nats.Msg{Subject: p.config.Subject, Header: headers, Data: message.Body}, nats.Context(bounded))
	return p.failure(err)
}

func (p *Publisher) failure(err error) error {
	if err == nil {
		return nil
	}
	return &broker.StageError{Stage: broker.StagePublish, Cause: &OperationError{Source: p.config.Subject, Operation: "publish", Cause: err}}
}

func publishHeaders(message broker.Message) (nats.Header, error) {
	headers := make(nats.Header, len(message.Headers)+1)
	var foundID bool
	for _, header := range message.Headers {
		if !validHeaderName(header.Name) {
			return nil, fieldFailure("Headers.Name", "invalid_header_name", errors.New("Headers.Name: invalid NATS header name"))
		}
		value := string(header.Value)
		if bytes.ContainsAny(header.Value, "\r\n") || textproto.TrimString(value) != value {
			return nil, fieldFailure("Headers.Value", "unrepresentable_header_value",
				errors.New("Headers.Value: CR/LF or boundary whitespace cannot be preserved"),
			)
		}
		if strings.EqualFold(header.Name, nats.MsgIdHdr) {
			if foundID || header.Name != nats.MsgIdHdr {
				return nil, fieldFailure("Nats-Msg-Id", "ambiguous_header", errors.New("Nats-Msg-Id: duplicate or noncanonical header"))
			}
			if !utf8.ValidString(value) {
				return nil, fieldFailure("Nats-Msg-Id", "invalid_utf8", errors.New("Nats-Msg-Id: invalid UTF-8"))
			}
			if message.ID != "" && message.ID != value {
				return nil, fieldFailure("Nats-Msg-Id", "conflicting_message_id", errors.New("Nats-Msg-Id: conflicts with Message.ID"))
			}
			foundID = true
		}
		headers[header.Name] = append(headers[header.Name], value)
	}
	if message.ID != "" && !foundID {
		if !utf8.ValidString(message.ID) || strings.ContainsAny(message.ID, "\r\n") || textproto.TrimString(message.ID) != message.ID {
			return nil, fieldFailure("Message.ID", "unrepresentable_message_id", errors.New("Message.ID: not representable as Nats-Msg-Id"))
		}
		headers[nats.MsgIdHdr] = []string{message.ID}
	}
	return headers, nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if character < '!' || character > '~' || strings.ContainsRune("\"()/,:;<=>?@[\\]{}", character) {
			return false
		}
	}
	return true
}
