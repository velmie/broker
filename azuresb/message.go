package azuresb

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
)

const (
	logicalIDProperty         = "id"
	maxDiagnosticPropertyName = 96
)

// Metadata is a detached native snapshot. It excludes lock tokens and settlement
// authority. Zero optional time/sequence values indicate absent SDK properties.
// LockedUntil never changes after delivery construction, including native renewal.
// ApplicationProperties retains supported scalar types, including null and bytes.
// Metadata values may be retained or mutated without changing the delivery.
type Metadata struct {
	MessageID             string
	ApplicationProperties map[string]any
	DeliveryCount         uint32
	LockedUntil           time.Time
	EnqueuedTime          time.Time
	SequenceNumber        int64
}

// MetadataReader returns an independently owned native snapshot.
type MetadataReader interface{ Metadata() Metadata }

type delivery struct {
	message  broker.Message
	source   string
	metadata Metadata
	attempt  broker.Attempt
}

func (d *delivery) Message() broker.Message { return d.message }
func (d *delivery) Source() string          { return d.source }
func (d *delivery) Attempt() broker.Attempt { return d.attempt }
func (d *delivery) Metadata() Metadata {
	snapshot := d.metadata
	snapshot.ApplicationProperties = make(map[string]any, len(d.metadata.ApplicationProperties))
	for name, value := range d.metadata.ApplicationProperties {
		if binary, ok := value.([]byte); ok {
			value = bytes.Clone(binary)
		}
		snapshot.ApplicationProperties[name] = value
	}
	return snapshot
}

func encodeMessage(message broker.Message) (*azservicebus.Message, error) {
	if message.ID == "" {
		return nil, messageFieldError("ID", "explicit logical identity is required")
	}
	if !utf8.ValidString(message.ID) {
		return nil, messageFieldError("ID", "expected UTF-8 string")
	}
	native := &azservicebus.Message{Body: bytes.Clone(message.Body), ApplicationProperties: make(map[string]any, len(message.Headers)+1)}
	for _, header := range message.Headers {
		if !validPropertyName(header.Name) {
			return nil, messageFieldError("ApplicationProperties[name]", "expected nonempty UTF-8 name")
		}
		if _, exists := native.ApplicationProperties[header.Name]; exists {
			return nil, messageFieldError(propertyField(header.Name), "duplicate name")
		}
		if strings.EqualFold(header.Name, logicalIDProperty) && (header.Name != logicalIDProperty || string(header.Value) != message.ID) {
			return nil, messageFieldError("ApplicationProperties.id", "reserved identity conflict or case alias")
		}
		if !utf8.Valid(header.Value) {
			return nil, messageFieldError(propertyField(header.Name), "binary properties require caller-defined text encoding")
		}
		native.ApplicationProperties[header.Name] = string(header.Value)
	}
	id := message.ID
	native.MessageID = &id
	native.ApplicationProperties[logicalIDProperty] = id
	return native, nil
}

func detach(native *azservicebus.ReceivedMessage, source string) (*delivery, error) {
	if native == nil {
		return nil, messageFieldError("Message", "missing native delivery")
	}
	if !utf8.ValidString(native.MessageID) {
		return nil, messageFieldError("MessageID", "expected UTF-8 string")
	}
	message := broker.Message{ID: native.MessageID, Body: bytes.Clone(native.Body)}
	metadata := Metadata{MessageID: native.MessageID,
		DeliveryCount: native.DeliveryCount,
		ApplicationProperties: make(map[string]any,
			len(native.ApplicationProperties))}
	names := make([]string, 0, len(native.ApplicationProperties))
	for name := range native.ApplicationProperties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !validPropertyName(name) {
			return nil, messageFieldError("ApplicationProperties[name]", "expected nonempty UTF-8 name")
		}
		if strings.EqualFold(name, logicalIDProperty) {
			value, ok := native.ApplicationProperties[name].(string)
			if name != logicalIDProperty || !ok || value != native.MessageID {
				return nil, &FieldError{Field: "ApplicationProperties.id", Cause: errors.New("reserved identity conflict, type or case alias"),
					ValueType: fmt.Sprintf("%T", native.ApplicationProperties[name])}
			}
		}
		value, err := propertyBytes(native.ApplicationProperties[name])
		if err != nil {
			return nil, &FieldError{Field: propertyField(name), Cause: err, ValueType: fmt.Sprintf("%T", native.ApplicationProperties[name])}
		}
		message.Headers = append(message.Headers, broker.Header{Name: name, Value: value})
		original := native.ApplicationProperties[name]
		if binary, ok := original.([]byte); ok {
			original = bytes.Clone(binary)
		}
		metadata.ApplicationProperties[name] = original
	}
	if native.LockedUntil != nil {
		metadata.LockedUntil = *native.LockedUntil
	}
	if native.EnqueuedTime != nil {
		metadata.EnqueuedTime = *native.EnqueuedTime
	}
	if native.SequenceNumber != nil {
		metadata.SequenceNumber = *native.SequenceNumber
	}
	attempt := broker.Attempt{}
	if native.DeliveryCount > 0 {
		attempt = broker.Attempt{Known: true, Number: uint64(native.DeliveryCount)}
	}
	return &delivery{message: message, source: source, metadata: metadata, attempt: attempt}, nil
}

// Scalar header text is deliberately defined instead of formatting arbitrary
// objects. Metadata separately preserves the original native scalar type.
//
//nolint:gocyclo // Each native scalar has an explicit conversion without arbitrary object formatting.
func propertyBytes(value any) ([]byte, error) {
	var text string
	switch scalar := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return bytes.Clone(scalar), nil
	case string:
		if !utf8.ValidString(scalar) {
			return nil, errors.New("expected UTF-8 string")
		}
		text = scalar
	case amqp.Symbol:
		if !utf8.ValidString(string(scalar)) {
			return nil, errors.New("expected UTF-8 symbol")
		}
		text = string(scalar)
	case bool:
		text = strconv.FormatBool(scalar)
	case int:
		text = strconv.FormatInt(int64(scalar), 10)
	case int8:
		text = strconv.FormatInt(int64(scalar), 10)
	case int16:
		text = strconv.FormatInt(int64(scalar), 10)
	case int32:
		text = strconv.FormatInt(int64(scalar), 10)
	case int64:
		text = strconv.FormatInt(scalar, 10)
	case uint:
		text = strconv.FormatUint(uint64(scalar), 10)
	case uint8:
		text = strconv.FormatUint(uint64(scalar), 10)
	case uint16:
		text = strconv.FormatUint(uint64(scalar), 10)
	case uint32:
		text = strconv.FormatUint(uint64(scalar), 10)
	case uint64:
		text = strconv.FormatUint(scalar, 10)
	case float32:
		text = strconv.FormatFloat(float64(scalar), 'g', -1, 32)
	case float64:
		text = strconv.FormatFloat(scalar, 'g', -1, 64)
	case time.Time:
		text = scalar.UTC().Format(time.RFC3339Nano)
	case amqp.UUID:
		text = scalar.String()
	default:
		return nil, fmt.Errorf("unsupported native property type %T", value)
	}
	return []byte(text), nil
}
func validPropertyName(name string) bool { return name != "" && utf8.ValidString(name) }
func propertyField(name string) string {
	if len(name) > maxDiagnosticPropertyName {
		name = name[:maxDiagnosticPropertyName]
	}
	return "ApplicationProperties." + strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-", r) {
			return r
		}
		return '_'
	}, name)
}
func messageFieldError(field, reason string) error {
	return &FieldError{Field: field, Cause: errors.New(reason)}
}
