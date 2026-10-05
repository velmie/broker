package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

const (
	redacted           = "redacted"
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
)

type commandError struct {
	Operation string
	Cause     error
}

func (e *commandError) Error() string { return fmt.Sprintf("%s: %v", e.Operation, e.Cause) }
func (e *commandError) Unwrap() error { return e.Cause }
func wrapCommand(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &commandError{Operation: operation, Cause: err}
}

// Original errors reach this configured boundary. Only fixed operation/source
// and native code allowlists enter output; arbitrary error text is never copied.
func redactAttribute(_ []string, attr slog.Attr) slog.Attr {
	err, ok := attr.Value.Any().(error)
	if !ok {
		return attr
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attr.Key, describeError(err, 0, &remaining))
}

type diagnostic struct {
	Type      string        `json:"type"`
	Operation string        `json:"operation,omitempty"`
	Source    string        `json:"source,omitempty"`
	Field     string        `json:"field,omitempty"`
	Code      string        `json:"code,omitempty"`
	Stage     string        `json:"stage,omitempty"`
	Cause     *diagnostic   `json:"cause,omitempty"`
	Causes    []*diagnostic `json:"causes,omitempty"`
	Truncated bool          `json:"truncated,omitempty"`
}

func describeError(err error, depth int, remaining *int) *diagnostic {
	d := &diagnostic{Type: fmt.Sprintf("%T", err)}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		d.Truncated = true
		return d
	}
	*remaining--
	switch value := err.(type) {
	case *commandError:
		d.Operation = knownOperation(value.Operation)
		d.Source = sourceName
	case *sqs.OperationError:
		d.Operation = knownOperation(value.Operation)
		d.Source = knownSource(value.Source)
	case *sns.OperationError:
		d.Operation = knownOperation(value.Operation)
		d.Source = knownSource(value.Source)
	case *sqs.FieldError:
		d.Field = knownField(value.Field)
	case *sns.FieldError:
		d.Field = knownField(value.Field)
	case *broker.StageError:
		d.Stage = knownStage(value.Stage)
	case smithy.APIError:
		d.Code = knownCode(value.ErrorCode())
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if cause == nil {
				continue
			}
			if *remaining <= 0 {
				d.Truncated = true
				break
			}
			d.Causes = append(d.Causes, describeError(cause, depth+1, remaining))
		}
	} else if cause := errors.Unwrap(err); cause != nil {
		d.Cause = describeError(cause, depth+1, remaining)
	}
	return d
}
func knownOperation(value string) string {
	switch value {
	case "publish", "receive", "process", "delete", "retry", "renew", "metadata", "disposition",
		"CreateQueue", "GetQueueAttributes", "SetQueueAttributes", "DeleteQueue", "CreateTopic", "Subscribe", "Unsubscribe", "DeleteTopic",
		"empty_readback", "flush", "shutdown", endpointOperation:
		return value
	}
	return redacted
}
func knownSource(value string) string {
	if value == sourceName {
		return value
	}
	return redacted
}
func knownField(value string) string {
	switch value {
	case "ID", "Body", "ReceiptHandle", "Source", "TopicARN", "MessageAttributes.id", "MessageAttributes.Command-Id",
		"MessageAttributes[name]", "Attributes.ApproximateReceiveCount", "Notification", "Type", "TopicArn", "Message":
		return value
	}
	if strings.HasPrefix(value, "MessageAttributes.") {
		return "MessageAttributes.[redacted]"
	}
	return redacted
}
func knownStage(value string) string {
	switch value {
	case broker.StageDecode, broker.StageMetadata, broker.StageEncode, broker.StageHandle, broker.StagePublish, broker.StageResolve:
		return value
	}
	return redacted
}
func knownCode(value string) string {
	switch value {
	case receiveThrottleCode, "InternalError", "AccessDenied", "AccessDeniedException", "InvalidParameterValue", "QueueDoesNotExist":
		return value
	}
	return redacted
}
