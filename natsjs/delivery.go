package natsjs

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// Terminate requests native JetStream termination. Cause retains optional
// diagnostic evidence. Termination does not publish to a DLQ.
type Terminate struct{ Cause error }

// TerminationRequirement declares use of Terminate.
type TerminationRequirement struct{}

// MetadataReader returns detached JetStream metadata without acknowledgment
// authority. Its method is valid during the callback; returned data may be kept.
type MetadataReader interface{ Metadata() nats.MsgMetadata }

func (Terminate) DeliveryDisposition()              {}
func (TerminationRequirement) DeliveryRequirement() {}

type delivery struct {
	message  broker.Message
	source   string
	metadata nats.MsgMetadata
}

func (d *delivery) Message() broker.Message    { return d.message }
func (d *delivery) Source() string             { return d.source }
func (d *delivery) Metadata() nats.MsgMetadata { return d.metadata }
func (d *delivery) Attempt() broker.Attempt {
	if d.metadata.NumDelivered == 0 {
		return broker.Attempt{}
	}
	return broker.Attempt{Number: d.metadata.NumDelivered, Known: true}
}

func detach(message *nats.Msg, source string) (*delivery, error) {
	metadata, err := message.Metadata()
	if err != nil {
		return nil, err
	}
	var keys []string
	for key := range message.Header {
		keys = append(keys, key)
	}
	sort.Strings(keys) // Native maps have already lost cross-key wire ordering.
	value := broker.Message{Body: bytes.Clone(message.Data)}
	var foundID bool
	for _, key := range keys {
		for _, header := range message.Header[key] {
			value.Headers = append(value.Headers, broker.Header{Name: key, Value: []byte(header)})
			if strings.EqualFold(key, nats.MsgIdHdr) {
				if foundID {
					return nil, errors.New("Nats-Msg-Id: duplicate or case-aliased header")
				}
				if !utf8.ValidString(header) {
					return nil, errors.New("Nats-Msg-Id: invalid UTF-8")
				}
				value.ID, foundID = header, true
			}
		}
	}
	return &delivery{message: value, source: source, metadata: *metadata}, nil
}
