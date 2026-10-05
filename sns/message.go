package sns

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/velmie/broker"
)

const (
	maxAttributeName    = 256
	maxRawAttributes    = 10
	maxTokenLength      = 128
	maxSourceLength     = 128
	maxMessageBytes     = 1048576
	logicalIDAttribute  = "id"
	stringAttributeType = "String"
)

func encodeMessage(message broker.Message, raw bool) (map[string]types.MessageAttributeValue, error) {
	if len(message.Body) == 0 || !utf8.Valid(message.Body) {
		return nil, fieldError("Body", "expected nonempty UTF-8 text")
	}
	attrs := make(map[string]types.MessageAttributeValue, len(message.Headers)+1)
	size := len(message.Body)
	for _, h := range message.Headers {
		if !validAttributeName(h.Name) {
			return nil, fieldError("MessageAttributes[name]", "invalid name")
		}
		if _, exists := attrs[h.Name]; exists {
			return nil, fieldError("MessageAttributes."+h.Name, "duplicate name")
		}
		if strings.EqualFold(h.Name, logicalIDAttribute) && (h.Name != logicalIDAttribute || string(h.Value) != message.ID) {
			return nil, fieldError("MessageAttributes.id", "reserved identity conflict or case alias")
		}
		if len(h.Value) == 0 {
			return nil, fieldError("MessageAttributes."+h.Name, "empty value")
		}
		value := types.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: bytes.Clone(h.Value)}
		if validText(h.Value) {
			value = types.MessageAttributeValue{DataType: aws.String(stringAttributeType), StringValue: aws.String(string(h.Value))}
		}
		attrs[h.Name] = value
	}
	if message.ID != "" {
		if !validText([]byte(message.ID)) {
			return nil, fieldError("ID", "expected nonempty valid identity text")
		}
		attrs[logicalIDAttribute] = types.MessageAttributeValue{DataType: aws.String(stringAttributeType), StringValue: aws.String(message.ID)}
	}
	if raw && len(attrs) > maxRawAttributes {
		return nil, fieldError("MessageAttributes", "maximum 10 attributes for raw SQS delivery")
	}
	for name, a := range attrs {
		size += len(name) + len(aws.ToString(a.DataType)) + len(aws.ToString(a.StringValue)) + len(a.BinaryValue)
	}
	if size > maxMessageBytes {
		return nil, fieldError("Message", "body and attributes exceed 1 MiB")
	}
	return attrs, nil
}
func validText(value []byte) bool {
	if len(value) == 0 || !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}
func validToken(value string) bool {
	return value != "" && len(value) <= maxTokenLength && strings.IndexFunc(value, func(r rune) bool { return r < '!' || r > '~' }) < 0
}
func validSource(value string) bool {
	return value != "" && len(value) <= maxSourceLength && strings.IndexFunc(value, func(r rune) bool {
		allowed := (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-/", r))
		return !allowed
	}) < 0
}
func validAttributeName(name string) bool {
	lower := strings.ToLower(name)
	if name == "" ||
		len(name) > maxAttributeName ||
		strings.HasPrefix(lower,
			"aws.") ||
		strings.HasPrefix(lower,
			"amazon.") ||
		strings.HasPrefix(name,
			".") ||
		strings.HasSuffix(name,
			".") ||
		strings.Contains(name,
			"..") {
		return false
	}
	return strings.IndexFunc(name, func(r rune) bool {
		allowed := (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.-", r))
		return !allowed
	}) < 0
}
func fieldError(field, reason string) error {
	return &FieldError{Field: field, Cause: errors.New(reason)}
}
