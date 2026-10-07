package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const instanceIDHeader = "Instance-Id"

// StampInstanceID replaces every case-insensitive Instance-Id field with one
// canonical field. It copies the header list and retained values, preserving
// unrelated order, name case, duplicate values and binary data. ID and body are
// unchanged and remain read-only. The instance ID must be nonempty UTF-8.
// Invalid configuration fails at StageMetadata before the wrapped publisher.
func StampInstanceID(id string) PublisherMiddleware {
	configErr := validateInstanceID(id)
	return func(next Publisher) Publisher {
		return PublisherFunc(func(ctx context.Context, message Message) error {
			if configErr != nil {
				return &StageError{Stage: StageMetadata, Cause: configErr}
			}
			headers := make([]Header, 0, len(message.Headers)+1)
			for _, header := range message.Headers {
				if !strings.EqualFold(header.Name, instanceIDHeader) {
					headers = append(headers, Header{Name: header.Name, Value: bytes.Clone(header.Value)})
				}
			}
			headers = append(headers, Header{Name: instanceIDHeader, Value: []byte(id)})
			message.Headers = headers
			return next.Publish(ctx, message)
		})
	}
}

// SkipOwnMessages requests Handled without invoking the next callback when one
// Instance-Id value exactly matches id. Header names match case-insensitively.
// Missing/empty values proceed; duplicate aliases and invalid UTF-8 fail at the
// metadata stage. The configured id must be nonempty UTF-8. This is application
// loop prevention, not authentication: an untrusted sender can supply this field.
func SkipOwnMessages(id string) HandlerMiddleware {
	configErr := validateInstanceID(id)
	return HandlerMiddleware{Wrap: func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, delivery Delivery) (Disposition, error) {
			if configErr != nil {
				return nil, &StageError{Stage: StageMetadata, Cause: configErr}
			}
			origin, _, err := singleTextHeader(delivery.Message().Headers, instanceIDHeader)
			if err != nil {
				return nil, &StageError{Stage: StageMetadata, Cause: err}
			}
			if origin == id {
				return Handled{}, nil
			}
			return next(ctx, delivery)
		}
	}}
}

// ValidateIDHeader checks an optional application convention. An absent header
// or one UTF-8 value equal to Message.ID passes unchanged, including two empty
// IDs. Duplicates, case aliases, invalid UTF-8 and conflicts fail at StageMetadata
// before publication. It neither requires nor adds an ID. Adapters still enforce
// their own native mapping after wrappers. Name must be a nonempty ASCII token.
func ValidateIDHeader(name string) PublisherMiddleware {
	validName := metadataHeaderName(name)
	return func(next Publisher) Publisher {
		return PublisherFunc(func(ctx context.Context, message Message) error {
			if !validName {
				return &StageError{Stage: StageMetadata, Cause: errors.New("ID header name: must be a nonempty ASCII token")}
			}
			id, found, err := singleTextHeader(message.Headers, name)
			if err != nil {
				return &StageError{Stage: StageMetadata, Cause: err}
			}
			if found && id != message.ID {
				return &StageError{Stage: StageMetadata, Cause: fmt.Errorf("%s: conflicts with Message.ID", name)}
			}
			return next.Publish(ctx, message)
		})
	}
}

func validateInstanceID(id string) error {
	if id == "" || !utf8.ValidString(id) {
		return errors.New("Instance-Id configuration: must be nonempty UTF-8")
	}
	return nil
}

func metadataHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if character < '!' || character > '~' || strings.ContainsRune("\"(),/:;<=>?@[\\]{}", character) {
			return false
		}
	}
	return true
}
