package sns

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/velmie/broker"
)

const (
	maxNotificationBytes  = 1048576
	notificationTypeField = "Type"
)

// DecodeNotification explicitly decodes an SNS notification from a trusted SQS
// subscription route. The serialized envelope is limited to 1 MiB, including its
// JSON escaping and metadata. It validates type and expected topic but does not
// verify signatures, fetch URLs or establish sender authentication. The queue
// policy must restrict publishers. Native notification IDs are never logical IDs.
// Returned message storage is detached. Relevant duplicate/case-aliased fields
// are rejected. Original JSON/base64 causes remain available through FieldError.
func DecodeNotification(body []byte, expectedTopicARN string) (broker.Message, error) {
	if len(body) > maxNotificationBytes {
		return broker.Message{}, fieldError("Notification", "serialized envelope exceeds 1 MiB")
	}
	if !utf8.Valid(body) {
		return broker.Message{}, fieldError("Notification", "expected UTF-8 JSON")
	}
	if expectedTopicARN == "" {
		return broker.Message{}, fieldError("TopicArn", "expected topic is required")
	}
	fields, err := readObject(body, "Notification", []string{notificationTypeField, "TopicArn", "Message", "MessageAttributes"})
	if err != nil {
		return broker.Message{}, err
	}
	kind, err := readString(fields[notificationTypeField], notificationTypeField)
	if err != nil {
		return broker.Message{}, err
	}
	if kind != "Notification" {
		return broker.Message{}, fieldError(notificationTypeField, "expected Notification")
	}
	topic, err := readString(fields["TopicArn"], "TopicArn")
	if err != nil {
		return broker.Message{}, err
	}
	if topic != expectedTopicARN {
		return broker.Message{}, fieldError("TopicArn", "unexpected topic")
	}
	text, err := readString(fields["Message"], "Message")
	if err != nil {
		return broker.Message{}, err
	}
	if text == "" {
		return broker.Message{}, fieldError("Message", "expected nonempty text")
	}
	message := broker.Message{Body: []byte(text)}
	if raw, exists := fields["MessageAttributes"]; exists {
		if decodeErr := decodeAttributes(raw, &message); decodeErr != nil {
			return broker.Message{}, decodeErr
		}
	}
	return message, nil
}

func decodeAttributes(raw []byte, message *broker.Message) error {
	attrs, err := readObject(raw, "MessageAttributes", nil)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !validAttributeName(name) {
			return fieldError("MessageAttributes[name]", "invalid name")
		}
		if strings.EqualFold(name, logicalIDAttribute) && name != logicalIDAttribute {
			return fieldError("MessageAttributes.id", "reserved case alias")
		}
		field := "MessageAttributes." + name
		parts, err := readObject(attrs[name], field, []string{notificationTypeField, "Value"})
		if err != nil {
			return err
		}
		kind, err := readString(parts[notificationTypeField], field+".Type")
		if err != nil {
			return err
		}
		text, err := readString(parts["Value"], field+".Value")
		if err != nil {
			return err
		}
		if name == logicalIDAttribute && kind != stringAttributeType {
			return fieldError(field, "logical ID requires String")
		}
		var value []byte
		switch kind {
		case stringAttributeType, "Number", "String.Array":
			if !validText([]byte(text)) {
				return fieldError(field, "expected nonempty valid text value")
			}
			value = []byte(text)
		case "Binary":
			value, err = base64.StdEncoding.DecodeString(text)
			if err != nil {
				return &FieldError{Field: field + ".Value", Cause: err}
			}
			if len(value) == 0 {
				return fieldError(field, "empty value")
			}
		default:
			return fieldError(field+".Type", "unsupported data type")
		}
		if name == logicalIDAttribute {
			message.ID = string(value)
		}
		message.Headers = append(message.Headers, broker.Header{Name: name, Value: value})
	}
	return nil
}

// readObject preserves exact key spelling and refuses duplicates before a map
// could silently overwrite a relevant value. Unknown fields remain uninterpreted.
func readObject(body []byte, field string, canonical []string) (map[string]json.RawMessage, error) {
	var valid json.RawMessage
	if err := json.Unmarshal(body, &valid); err != nil {
		return nil, &FieldError{Field: field, Cause: err}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil {
		return nil, &FieldError{Field: field, Cause: err}
	}
	if token != json.Delim('{') {
		return nil, fieldError(field, "expected JSON object")
	}
	values := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, &FieldError{Field: field, Cause: err}
		}
		key := token.(string)
		if _, exists := values[key]; exists {
			return nil, fieldError(field, "duplicate JSON key")
		}
		for _, name := range canonical {
			if strings.EqualFold(key, name) && key != name {
				return nil, fieldError(field+"."+name, "reserved case alias")
			}
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, &FieldError{Field: field, Cause: err}
		}
		values[key] = value
	}
	if _, err = decoder.Token(); err != nil {
		return nil, &FieldError{Field: field, Cause: err}
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, &FieldError{Field: field, Cause: err}
		}
		return nil, fieldError(field, "unexpected trailing JSON")
	}
	return values, nil
}
func readString(raw []byte, field string) (string, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fieldError(field, "expected JSON string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &FieldError{Field: field, Cause: err}
	}
	return value, nil
}
