package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

const (
	redacted           = "redacted"
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
)

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
	ValueType string        `json:"value_type,omitempty"`
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
	case *adapter.OperationError:
		d.Operation = knownOperation(value.Operation)
		d.Source = knownSource(value.Source)
	case *adapter.FieldError:
		d.Field = knownField(value.Field)
		d.ValueType = knownValueType(value.ValueType)
	case *json.UnmarshalTypeError:
		d.Field = knownField(value.Field)
	case *broker.DecodeError:
		d.Stage = broker.StageDecode
	case *broker.StageError:
		d.Stage = knownStage(value.Stage)
	case *azservicebus.Error:
		d.Code = azureCode(value.Code)
	case *amqp.Error:
		d.Code = amqpCode(value.Condition)
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
	case adapter.OperationPublish, adapter.OperationReceive, adapter.OperationProcess, adapter.OperationComplete,
		adapter.OperationAbandon, adapter.OperationRenew, adapter.OperationMetadata, adapter.OperationDisposition, adapter.OperationClose,
		adapter.OperationAcquire, "create_client", "create_sender", "close_sender", "close_client", "flush", "shutdown":
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
	case logicalIDField, "Body", nativeIDField, "Source", deliveryCountField,
		"OperationTimeout", "ReceiveTimeout", "CloseTimeout", "ReceiveMode",
		"ApplicationProperties.id", "ApplicationProperties[name]", "ApplicationProperties.Command-Id",
		amountField, routeField, argumentsField, receiveModeFlag, idFlag, connectionEnvironment:
		return value
	}
	if strings.HasPrefix(value, "ApplicationProperties.") {
		return "ApplicationProperties.[redacted]"
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
func azureCode(value azservicebus.Code) string {
	switch value {
	case azservicebus.CodeConnectionLost, azservicebus.CodeLockLost, azservicebus.CodeTimeout, azservicebus.CodeUnauthorizedAccess:
		return string(value)
	}
	return redacted
}
func amqpCode(value amqp.ErrCond) string {
	switch value {
	case amqp.ErrCondNotFound, amqp.ErrCondUnauthorizedAccess, amqp.ErrCondConnectionForced, amqp.ErrCondInternalError,
		"com.microsoft:message-lock-lost", "com.microsoft:timeout", "com.microsoft:server-busy":
		return string(value)
	}
	return redacted
}

func knownValueType(value string) string {
	switch value {
	case "", "string", "[]string", "[]uint8", "map[string]interface {}", "map[string]string", "int", "int64", "float64", "bool":
		return value
	}
	return redacted
}
