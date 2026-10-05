package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

const (
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
	maxDiagnosticType  = 128
	redacted           = "redacted"
)

// The configured logging boundary receives original errors and describes each
// branch separately. Native messages, addresses, certificate contents, arbitrary
// property names and application data never enter the serialized record.
func redactAttribute(_ []string, attribute slog.Attr) slog.Attr {
	err, ok := attribute.Value.Any().(error)
	if !ok {
		return attribute
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attribute.Key, describeError(err, 0, &remaining))
}

type diagnosticError struct {
	ValueType string             `json:"value_type,omitempty"`
	Type      string             `json:"type"`
	Source    string             `json:"source,omitempty"`
	Operation string             `json:"operation,omitempty"`
	Stage     string             `json:"stage,omitempty"`
	Field     string             `json:"field,omitempty"`
	Code      string             `json:"code,omitempty"`
	Kind      string             `json:"kind,omitempty"`
	Reason    string             `json:"reason,omitempty"`
	Offset    int64              `json:"offset,omitempty"`
	Cause     *diagnosticError   `json:"cause,omitempty"`
	Causes    []*diagnosticError `json:"causes,omitempty"`
	Truncated bool               `json:"truncated,omitempty"`
}

func describeError(err error, depth int, remaining *int) *diagnosticError {
	node := &diagnosticError{Type: boundedType(err)}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		node.Truncated = true
		return node
	}
	*remaining--
	describeNode(node, err)
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

func describeNode(node *diagnosticError, err error) {
	switch value := err.(type) {
	case *adapter.OperationError:
		node.Source = redacted
		if value.Source == sourceName {
			node.Source = sourceName
		}
		node.Operation = knownOperation(value.Operation)
	case *adapter.FieldError:
		node.Field = knownField(value.Field)
		node.ValueType = boundedLabel(value.ValueType)
	case *broker.StageError:
		node.Stage = knownStage(value.Stage)
	case *broker.DecodeError:
		node.Stage = broker.StageDecode
	case *azservicebus.Error:
		node.Code = azureCode(value.Code)
	case *amqp.Error:
		node.Code = amqpCode(value.Condition)

	case *json.SyntaxError:
		node.Kind = "json_syntax"
		node.Offset = value.Offset
	case *json.UnmarshalTypeError:
		node.Kind = "json_type"
		node.Field = knownField(value.Field)
		node.Offset = value.Offset
	case *strconv.NumError:
		node.Kind = "number"
		if value.Err == strconv.ErrSyntax {
			node.Kind = "number_syntax"
		}
		if value.Err == strconv.ErrRange {
			node.Kind = "number_range"
		}
	default:
		if describeNetwork(node, err) {
			return
		}
		switch err {
		case azservicebus.ErrMessageTooLarge:
			node.Kind = "message_too_large"
		case context.Canceled:
			node.Kind = "canceled"
		case context.DeadlineExceeded:
			node.Kind = "deadline_exceeded"
		default:
			node.Reason = knownReason(err)
		}
	}
}

func boundedType(value any) string {
	name := fmt.Sprintf("%T", value)
	if len(name) > maxDiagnosticType {
		name = name[:maxDiagnosticType]
	}
	return strings.Map(func(r rune) rune {
		if r >= 32 && r < 127 {
			return r
		}
		return '_'
	}, name)
}

func knownOperation(value string) string {
	switch value {
	case adapter.OperationRenew, adapter.OperationPublish, adapter.OperationReceive, adapter.OperationProcess, adapter.OperationComplete,
		adapter.OperationAbandon, adapter.OperationMetadata, adapter.OperationDisposition, adapter.OperationClose, adapter.OperationAcquire,
		"create_client", "create_sender", "close_client", "close_sender":
		return value
	default:
		return redacted
	}
}
func knownStage(value string) string {
	switch value {
	case broker.StageDecode, broker.StageMetadata, broker.StageHandle, broker.StageResolve, broker.StageEncode, broker.StagePublish:
		return value
	}
	return redacted
}
func knownField(value string) string {
	switch value {
	case deliveryCountField, "MessageID", "OperationTimeout", "ReceiveTimeout", "CloseTimeout", "ReceiveMode", "Source", "Shutdown",
		"ApplicationProperties.id", "ApplicationProperties[name]", "ApplicationProperties.Correlation-Id",
		routeField, receiveModeFlag, idFlag, workDurationFlag, renewEveryFlag, "amount", orderField, argumentsField, connectionStringEnvironment:
		return value
	}
	if strings.HasPrefix(value, "ApplicationProperties.") {
		return "ApplicationProperties.[redacted]"
	}
	return redacted
}
func knownNetworkOperation(value string) string {
	switch value {
	case "dial", "read", "write", "accept", "connect":
		return value
	}
	return redacted
}
func azureCode(value azservicebus.Code) string {
	switch value {
	case azservicebus.CodeUnauthorizedAccess, azservicebus.CodeConnectionLost, azservicebus.CodeLockLost, azservicebus.CodeTimeout:
		return string(value)
	default:
		return redacted
	}
}
func amqpCode(value amqp.ErrCond) string {
	switch value {
	case amqp.ErrCondDecodeError, amqp.ErrCondFrameSizeTooSmall, amqp.ErrCondIllegalState, amqp.ErrCondInternalError,
		amqp.ErrCondInvalidField, amqp.ErrCondNotAllowed, amqp.ErrCondNotFound, amqp.ErrCondNotImplemented,
		amqp.ErrCondPreconditionFailed, amqp.ErrCondResourceDeleted, amqp.ErrCondResourceLimitExceeded, amqp.ErrCondResourceLocked,
		amqp.ErrCondUnauthorizedAccess, amqp.ErrCondConnectionForced, amqp.ErrCondConnectionRedirect, amqp.ErrCondFramingError,
		amqp.ErrCondErrantLink, amqp.ErrCondHandleInUse, amqp.ErrCondUnattachedHandle, amqp.ErrCondWindowViolation,
		amqp.ErrCondDetachForced, amqp.ErrCondLinkRedirect, amqp.ErrCondMessageSizeExceeded,
		amqp.ErrCondStolen, amqp.ErrCondTransferLimitExceeded,
		"com.microsoft:timeout", "com.microsoft:message-lock-lost", "com.microsoft:server-busy",
		"com.microsoft:operation-cancelled", //nolint:misspell // Native Microsoft AMQP error condition.
		"com.microsoft:entity-disabled", "com.microsoft:session-cannot-be-locked":
		return string(value)
	default:
		return redacted
	}
}
func knownReason(err error) string {
	if fmt.Sprintf("%T", err) != "*errors.errorString" {
		return redacted
	}
	switch err.Error() {
	case "unexpected positional arguments", "choose queue or topic with subscription", "unsupported receive mode",
		"logical ID is required", "connection string is required", "unexpected order", "native delivery attempt is required",
		"order outcome was not observed", "demonstration retry before business work", "must be positive",
		"expected UTF-8 string", "expected nonempty UTF-8 name", "duplicate name",
		"reserved identity conflict or case alias", "reserved identity conflict, type or case alias":
		return err.Error()
	default:
		return redacted
	}
}

func boundedLabel(value string) string {
	if len(value) > maxDiagnosticType {
		value = value[:maxDiagnosticType]
	}
	return strings.Map(func(r rune) rune {
		if r >= 32 && r < 127 {
			return r
		}
		return '_'
	}, value)
}

func describeNetwork(node *diagnosticError, err error) bool {
	switch value := err.(type) {
	case *net.OpError:
		node.Operation = knownNetworkOperation(value.Op)
		node.Kind = "network"
		if value.Timeout() {
			node.Kind = "network_timeout"
		}
	case *net.DNSError:
		node.Kind = "dns"
		if value.IsNotFound {
			node.Kind = "dns_not_found"
		}
	case x509.HostnameError, *x509.HostnameError:
		node.Kind = "hostname_mismatch"
	case x509.UnknownAuthorityError, *x509.UnknownAuthorityError:
		node.Kind = "unknown_authority"
	case x509.CertificateInvalidError, *x509.CertificateInvalidError:
		node.Kind = "invalid_certificate"
	case tls.RecordHeaderError, *tls.RecordHeaderError:
		node.Kind = "tls_record_header"
	case *tls.CertificateVerificationError:
		node.Kind = "certificate_verification"
	default:
		return false
	}
	return true
}
