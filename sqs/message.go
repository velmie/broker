package sqs

import (
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
)

const (
	maxMessageBytes      = 1048576
	maxAttributeName     = 256
	maxMessageAttributes = 10
	maxTokenLength       = 128
	logicalIDAttribute   = "id"
	stringAttributeType  = "String"
)

// Metadata is detached native data. Receipt handles are deliberately excluded.
// MessageID is the native service ID, distinct from the application logical ID.
type Metadata struct {
	MessageID         string
	Attributes        map[string]string
	MessageAttributes map[string]types.MessageAttributeValue
}

// MetadataReader returns detached values that callers may retain or mutate.
type MetadataReader interface{ Metadata() Metadata }

type delivery struct {
	message  broker.Message
	source   string
	metadata Metadata
	attempt  broker.Attempt
}

func (d *delivery) Message() broker.Message { return d.message }
func (d *delivery) Source() string          { return d.source }
func (d *delivery) Metadata() Metadata      { return copyMetadata(d.metadata) }
func (d *delivery) Attempt() broker.Attempt { return d.attempt }

func encodeMessage(message broker.Message) (map[string]types.MessageAttributeValue, error) {
	if err := validateBody(message.Body); err != nil {
		return nil, err
	}
	attrs := make(map[string]types.MessageAttributeValue, len(message.Headers)+1)
	for _, header := range message.Headers {
		if !validAttributeName(header.Name) {
			return nil, fieldError("MessageAttributes[name]", "invalid name")
		}
		if _, exists := attrs[header.Name]; exists {
			return nil, fieldError("MessageAttributes."+header.Name, "duplicate name")
		}
		if strings.EqualFold(header.Name, logicalIDAttribute) && (header.Name != logicalIDAttribute || string(header.Value) != message.ID) {
			return nil, fieldError("MessageAttributes.id", "reserved identity conflict or case alias")
		}
		if len(header.Value) == 0 {
			return nil, fieldError("MessageAttributes."+header.Name, "empty value")
		}
		value := types.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: bytes.Clone(header.Value)}
		if validText(header.Value) {
			value = types.MessageAttributeValue{DataType: aws.String(stringAttributeType), StringValue: aws.String(string(header.Value))}
		}
		attrs[header.Name] = value
	}
	if message.ID != "" {
		if !validText([]byte(message.ID)) {
			return nil, fieldError("ID", "invalid SQS text")
		}
		attrs[logicalIDAttribute] = types.MessageAttributeValue{DataType: aws.String(stringAttributeType), StringValue: aws.String(message.ID)}
	}
	if err := validateAttributeSize(message.Body, attrs); err != nil {
		return nil, err
	}
	return attrs, nil
}

func detach(native *types.Message, source string) (*delivery, error) {
	if native.ReceiptHandle == nil || *native.ReceiptHandle == "" {
		return nil, fieldError("ReceiptHandle", "missing receipt")
	}
	body := []byte(aws.ToString(native.Body))
	if err := validateBody(body); err != nil {
		return nil, err
	}
	if err := validateAttributeSize(body, native.MessageAttributes); err != nil {
		return nil, err
	}
	message := broker.Message{Body: body}
	keys := make([]string, 0, len(native.MessageAttributes))
	for key := range native.MessageAttributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !validAttributeName(key) {
			return nil, fieldError("MessageAttributes[name]", "invalid name")
		}
		if strings.EqualFold(key, logicalIDAttribute) && key != logicalIDAttribute {
			return nil, fieldError("MessageAttributes.id", "reserved case alias")
		}
		attr := native.MessageAttributes[key]
		value, err := decodeAttribute(key, &attr)
		if err != nil {
			return nil, err
		}
		if key == logicalIDAttribute {
			if attributeKind(aws.ToString(attr.DataType)) != stringAttributeType {
				return nil, fieldError("MessageAttributes.id", "logical ID requires String")
			}
			message.ID = string(value)
		}
		message.Headers = append(message.Headers, broker.Header{Name: key, Value: value})
	}
	attempt := broker.Attempt{}
	if raw, exists := native.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; exists {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, &FieldError{Field: "Attributes.ApproximateReceiveCount", Cause: err}
		}
		if n == 0 {
			return nil, fieldError("Attributes.ApproximateReceiveCount", "expected positive uint64")
		}
		attempt = broker.Attempt{Number: n, Known: true}
	}
	metadata := copyMetadata(Metadata{MessageID: aws.ToString(native.MessageId),
		Attributes:        native.Attributes,
		MessageAttributes: native.MessageAttributes})
	return &delivery{message: message, source: source, metadata: metadata, attempt: attempt}, nil
}

func decodeAttribute(key string, attr *types.MessageAttributeValue) ([]byte, error) {
	field := "MessageAttributes." + key
	if len(attr.StringListValues) != 0 || len(attr.BinaryListValues) != 0 {
		return nil, fieldError(field, "list values are unsupported")
	}
	dataType := aws.ToString(attr.DataType)
	if dataType == "" || len(dataType) > maxAttributeName {
		return nil, fieldError(field, "invalid data type")
	}
	switch attributeKind(dataType) {
	case stringAttributeType, "Number":
		if attr.StringValue == nil || len(attr.BinaryValue) != 0 || !validText([]byte(aws.ToString(attr.StringValue))) {
			return nil, fieldError(field, "expected nonempty valid text value")
		}
		return []byte(*attr.StringValue), nil
	case "Binary":
		if len(attr.BinaryValue) == 0 || attr.StringValue != nil {
			return nil, fieldError(field, "expected nonempty binary value")
		}
		return bytes.Clone(attr.BinaryValue), nil
	default:
		return nil, fieldError(field, "unsupported data type")
	}
}

func validateAttributeSize(body []byte, attrs map[string]types.MessageAttributeValue) error {
	if len(attrs) > maxMessageAttributes {
		return fieldError("MessageAttributes", "maximum 10 attributes including logical id")
	}
	size := len(body)
	for key, attr := range attrs {
		size += len(key) + len(aws.ToString(attr.DataType)) + len(aws.ToString(attr.StringValue)) + len(attr.BinaryValue)
	}
	if size > maxMessageBytes {
		return fieldError("Message", "body and attributes exceed 1 MiB")
	}
	return nil
}
func validateBody(body []byte) error {
	if len(body) > maxMessageBytes || !validText(body) {
		return fieldError("Body", "expected 1..1048576 bytes of valid SQS text")
	}
	return nil
}
func validText(value []byte) bool {
	if len(value) == 0 || !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		allowed := (r == '\t' || r == '\n' || r == '\r' || r >= 0x20 && r <= 0xd7ff || r >= 0xe000 && r <= 0xfffd ||
			r >= 0x10000 && r <= 0x10ffff)
		if !allowed {
			return false
		}
	}
	return true
}
func validToken(value string) bool {
	return value != "" && len(value) <= maxTokenLength && strings.IndexFunc(value, func(r rune) bool { return r < '!' || r > '~' }) < 0
}
func validAttributeName(name string) bool {
	lower := strings.ToLower(name)
	if name == "" || len(name) > maxAttributeName || strings.HasPrefix(lower,
		"aws.") || strings.HasPrefix(lower,
		"amazon.") || strings.HasPrefix(name,
		".") || strings.HasSuffix(name,
		".") || strings.Contains(name,
		"..") {
		return false
	}
	return strings.IndexFunc(name, func(r rune) bool {
		allowed := (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.')
		return !allowed
	}) < 0
}
func fieldError(field, reason string) error {
	return &FieldError{Field: field, Cause: errors.New(reason)}
}
func copyMetadata(metadata Metadata) Metadata {
	result := Metadata{MessageID: metadata.MessageID,
		Attributes: make(map[string]string,
			len(metadata.Attributes)),
		MessageAttributes: make(map[string]types.MessageAttributeValue,
			len(metadata.MessageAttributes))}
	for key, value := range metadata.Attributes {
		result.Attributes[key] = value
	}
	for key, value := range metadata.MessageAttributes {
		if value.DataType != nil {
			value.DataType = aws.String(*value.DataType)
		}
		if value.StringValue != nil {
			value.StringValue = aws.String(*value.StringValue)
		}
		value.BinaryValue = bytes.Clone(value.BinaryValue)
		value.StringListValues = append([]string(nil), value.StringListValues...)
		if value.BinaryListValues != nil {
			list := make([][]byte, len(value.BinaryListValues))
			for i, v := range value.BinaryListValues {
				list[i] = bytes.Clone(v)
			}
			value.BinaryListValues = list
		}
		result.MessageAttributes[key] = value
	}
	return result
}

func attributeKind(dataType string) string { kind, _, _ := strings.Cut(dataType, "."); return kind }
