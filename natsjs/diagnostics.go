package natsjs

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	diagnosticOperationKey = "operation"
	diagnosticReasonKey    = "reason"
	diagnosticCodeKey      = "code"
	diagnosticErrorCodeKey = "errorcode"
)

// ErrorFields describes one NATS error node for broker.DescribeError. It does
// not traverse causes or serialize native descriptions, subjects in messages,
// credentials, or panic values. The source is application-configured: callers
// with sensitive source names must supply an earlier describer to redact it.
// Unknown errors return nil so another describer can handle them.
func ErrorFields(err error) map[string]any {
	switch value := err.(type) {
	case *OperationError:
		return map[string]any{"source": value.Source, diagnosticOperationKey: value.Operation}
	case *ObserverPanicError:
		return map[string]any{diagnosticOperationKey: value.Operation, diagnosticReasonKey: "observer_panic"}
	case *nats.APIError:
		return map[string]any{diagnosticCodeKey: value.Code, diagnosticErrorCodeKey: uint16(value.ErrorCode)}
	case *jetstream.APIError:
		return map[string]any{diagnosticCodeKey: value.Code, diagnosticErrorCodeKey: uint16(value.ErrorCode)}
	}
	reason := nativeErrorReason(err)
	if reason == "" {
		return nil
	}
	return map[string]any{diagnosticReasonKey: reason}
}

func nativeErrorReason(err error) string {
	switch err {
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
	case nats.ErrAsyncPublishTimeout, jetstream.ErrAsyncPublishTimeout:
		return "async_publish_timeout"
	case nats.ErrNoResponders:
		return "no_responders"
	case nats.ErrNoStreamResponse, jetstream.ErrNoStreamResponse:
		return "no_stream_response"
	case nats.ErrBadSubject:
		return "invalid_subject"
	case ErrGracePeriodExceeded:
		return "shutdown_grace_period_exceeded"
	default:
		return ""
	}
}
