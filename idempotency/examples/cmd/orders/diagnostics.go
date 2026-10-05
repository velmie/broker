package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/velmie/idempo"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/broker/natsjs/v3"
)

const (
	maxDiagnosticType  = 128
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
	redacted           = "redacted"
)

type commandError struct {
	Operation, Field string
	Cause            error
}

func (e *commandError) Error() string { return fmt.Sprintf("orders %s: %v", e.Operation, e.Cause) }
func (e *commandError) Unwrap() error { return e.Cause }
func failure(operation, field string, cause error) error {
	if cause == nil {
		return nil
	}
	return &commandError{Operation: operation, Field: field, Cause: cause}
}

// This command has one controlled source. Original errors reach the boundary;
// keys, lease tokens, message data and arbitrary native error text are not emitted.
func redactAttribute(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key == "source" {
		return slog.String("source", sourceName)
	}
	err, ok := attr.Value.Any().(error)
	if !ok {
		return attr
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attr.Key, describeError(err, 0, &remaining))
}

type diagnostic struct {
	Type      string        `json:"type"`
	Source    string        `json:"source,omitempty"`
	Operation string        `json:"operation,omitempty"`
	Field     string        `json:"field,omitempty"`
	Stage     string        `json:"stage,omitempty"`
	Code      string        `json:"code,omitempty"`
	Cause     *diagnostic   `json:"cause,omitempty"`
	Causes    []*diagnostic `json:"causes,omitempty"`
	Truncated bool          `json:"truncated,omitempty"`
}

func describeError(err error, depth int, remaining *int) *diagnostic {
	typeName := fmt.Sprintf("%T", err)
	if len(typeName) > maxDiagnosticType {
		typeName = typeName[:maxDiagnosticType]
	}
	node := &diagnostic{Type: typeName}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		node.Truncated = true
		return node
	}
	*remaining--
	switch value := err.(type) {
	case *commandError:
		node.Operation = knownOperation(value.Operation)
		node.Field = knownField(value.Field)
		node.Source = sourceName
	case *idempotency.Error:
		node.Operation = knownOperation(value.Operation)
		node.Field = knownField(value.Field)
		node.Source = sourceName
	case *natsjs.OperationError:
		node.Operation = knownOperation(value.Operation)
		node.Source = sourceName
	case *broker.StageError:
		node.Stage = knownStage(value.Stage)
	case *json.UnmarshalTypeError:
		node.Field = knownField(value.Field)
	case *nats.APIError:
		node.Code = fmt.Sprint(value.ErrorCode)
	default:
		node.Code = diagnosticCode(err)
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			if cause == nil {
				continue
			}
			if *remaining <= 0 {
				node.Truncated = true
				break
			}
			node.Causes = append(node.Causes, describeError(cause, depth+1, remaining))
		}
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			node.Cause = describeError(cause, depth+1, remaining)
		}
	}
	return node
}
func knownOperation(value string) string {
	switch value {
	case "configure", "key", "fingerprint", "acquire", "commit", "unlock", "process", "connect", "close_store",
		"readback", "read_marker", "create_stream", "create_consumer", "delete_stream",
		natsjs.OperationAck, natsjs.OperationNak, natsjs.OperationTerm, natsjs.OperationStartup, natsjs.OperationCleanup,
		natsjs.OperationRun, natsjs.OperationStop, natsjs.OperationFetch, natsjs.OperationDisposition, natsjs.OperationRenew:
		return value
	}
	return redacted
}
func knownField(value string) string {
	switch value {
	case "", "url", "amount", "Message.ID", "HeaderName", "CommitTimeout", "CommitErrorMode", "CommitErrorHandler",
		"KeyValidator", "FingerprintHeaders", "Idempotency-Key":
		return value
	}
	return redacted
}
func knownStage(value string) string {
	switch value {
	case broker.StageDecode, broker.StageMetadata, broker.StageHandle, broker.StagePublish, broker.StageEncode, broker.StageResolve:
		return value
	}
	return redacted
}
func diagnosticCode(err error) string {
	switch err {
	case context.Canceled:
		return "canceled"
	case context.DeadlineExceeded:
		return "deadline_exceeded"
	case nats.ErrConnectionClosed:
		return "connection_closed"
	case nats.ErrTimeout:
		return "timeout"
	case idempo.ErrWaitTimeout:
		return "wait_timeout"
	case idempo.ErrKeyConflict:
		return "key_conflict"
	case idempo.ErrInProgress:
		return "in_progress"
	case idempo.ErrMissingKey:
		return "missing_key"
	case idempo.ErrInvalidKey:
		return "invalid_key"
	case idempo.ErrKeyNotFound:
		return "key_not_found"
	case idempo.ErrKeyExpired:
		return "key_expired"
	case idempo.ErrResponseNil:
		return "response_nil"
	}
	return ""
}
