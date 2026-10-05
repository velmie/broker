package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/natsjs/v3"
)

const (
	maxDiagnosticLabel = 128
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
)

type diagnosticError struct {
	Type      string             `json:"type"`
	Source    string             `json:"source,omitempty"`
	Operation string             `json:"operation,omitempty"`
	Stage     string             `json:"stage,omitempty"`
	Field     string             `json:"field,omitempty"`
	Errno     uint64             `json:"errno,omitempty"`
	Code      int                `json:"code,omitempty"`
	ErrorCode uint16             `json:"errorcode,omitempty"`
	Network   string             `json:"network,omitempty"`
	Reason    string             `json:"reason,omitempty"`
	Cause     *diagnosticError   `json:"cause,omitempty"`
	Causes    []*diagnosticError `json:"causes,omitempty"`
	Truncated bool               `json:"truncated,omitempty"`
}

// ReplaceAttr sees original error objects, including bound attributes. Each
// unwrap branch retains its own source/operation without serializing addresses,
// subjects, credentials or arbitrary native error descriptions.
func redactAttribute(_ []string, attr slog.Attr) slog.Attr {
	failure, ok := attr.Value.Any().(error)
	if !ok {
		return attr
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attr.Key, describeError(failure, 0, &remaining))
}
func describeError(err error, depth int, remaining *int) *diagnosticError {
	node := &diagnosticError{Type: fmt.Sprintf("%T", err), Reason: nativeReason(err)}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		node.Truncated = true
		return node
	}
	*remaining--
	switch value := err.(type) {
	case *adapter.OperationError:
		node.Source, node.Operation = safeLabel(value.Source), safeLabel(value.Operation)
	case *broker.StageError:
		switch value.Stage {
		case broker.StageDecode, broker.StageMetadata, broker.StageHandle, broker.StageResolve, broker.StageEncode, broker.StagePublish:
			node.Stage = value.Stage
		default:
			node.Stage = "unknown"
		}
	case *fieldError:
		node.Field = safeLabel(value.Field)
	case *nats.APIError:
		node.Code, node.ErrorCode = value.Code, uint16(value.ErrorCode)
	case *net.OpError:
		node.Operation, node.Network = safeLabel(value.Op), safeLabel(value.Net)
	case syscall.Errno:
		node.Errno = uint64(value)
	case *tls.CertificateVerificationError:
		node.Reason = "certificate_verification_failed"
	case x509.UnknownAuthorityError:
		node.Reason = "unknown_authority"
	case x509.HostnameError:
		node.Reason = "hostname_mismatch"
	case x509.CertificateInvalidError:
		node.Reason = "invalid_certificate"
		node.Code = int(value.Reason)
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
func safeLabel(value string) string {
	if len(value) > maxDiagnosticLabel {
		value = value[:maxDiagnosticLabel]
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-/", r) {
			return r
		}
		return '_'
	}, value)
}

func nativeReason(err error) string {
	switch err {
	case context.Canceled:
		return "canceled"
	case context.DeadlineExceeded:
		return "deadline_exceeded"
	case nats.ErrAuthorization:
		return "authorization_violation"
	case nats.ErrPermissionViolation:
		return "permission_violation"
	case nats.ErrAuthExpired:
		return "authentication_expired"
	case nats.ErrAuthRevoked:
		return "authentication_revoked"
	case nats.ErrNoServers:
		return "no_servers"
	case nats.ErrConnectionClosed:
		return "connection_closed"
	case nats.ErrTimeout:
		return "timeout"
	case nats.ErrAsyncPublishTimeout:
		return "async_publish_timeout"
	case nats.ErrNoResponders:
		return "no_responders"
	case nats.ErrNoStreamResponse:
		return "no_stream_response"
	}
	switch err.Error() {
	case "unexpected stream or sequence", "expected stream mismatch", "expected no-stream response",
		"native retry delay not observed", "expected pending backpressure", "expected publisher closed",
		"pending futures remain after cleanup", "expected async acknowledgement timeout":
		return err.Error()
	default:
		return ""
	}
}

// fieldError associates a local verification failure with its configured field.
// The original cause remains available without publishing arbitrary error text.
type fieldError struct {
	Field string
	Cause error
}

func (e *fieldError) Error() string { return e.Field + ": " + e.Cause.Error() }
func (e *fieldError) Unwrap() error { return e.Cause }
