package main

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

const (
	maxDiagnosticLabel = 128
	maxDiagnosticDepth = 8
	maxDiagnosticNodes = 32
)

// ReplaceAttr receives original error objects and redacts both bound and call-site
// attributes before JSON serialization. SDK codes preserve native failure detail
// without exposing messages that can contain URLs, credentials or request data.
func redactAttribute(_ []string, attribute slog.Attr) slog.Attr {
	err, ok := attribute.Value.Any().(error)
	if !ok {
		return attribute
	}
	remaining := maxDiagnosticNodes
	return slog.Any(attribute.Key, describeError(err, 0, &remaining))
}

// Each node describes only its own error. Unwrap edges preserve association of
// primary and cleanup failures without pairing metadata from different branches.
type diagnosticError struct {
	Type      string             `json:"type"`
	Operation string             `json:"operation,omitempty"`
	Source    string             `json:"source,omitempty"`
	Field     string             `json:"field,omitempty"`
	Code      string             `json:"code,omitempty"`
	Function  string             `json:"function,omitempty"`
	Kind      string             `json:"kind,omitempty"`
	Reason    string             `json:"reason,omitempty"`
	CauseType string             `json:"cause_type,omitempty"`
	Cause     *diagnosticError   `json:"cause,omitempty"`
	Causes    []*diagnosticError `json:"causes,omitempty"`
	Truncated bool               `json:"truncated,omitempty"`
}

func describeError(err error, depth int, remaining *int) *diagnosticError {
	node := &diagnosticError{Type: fmt.Sprintf("%T", err), Reason: safeValidationReason(err)}
	if depth >= maxDiagnosticDepth || *remaining <= 0 {
		node.Truncated = true
		return node
	}
	*remaining--
	switch value := err.(type) {
	case *sns.OperationError:
		node.Operation, node.Source = safeLabel(value.Operation), safeLabel(value.Source)
	case *sqs.OperationError:
		node.Operation, node.Source = safeLabel(value.Operation), safeLabel(value.Source)
	case *sqs.FieldError:
		node.Field = safeLabel(value.Field)
		node.Reason = safeValidationReason(value.Cause)
	case *sns.FieldError:
		node.Field = safeLabel(value.Field)
		node.Reason = safeValidationReason(value.Cause)
	case *strconv.NumError:
		node.Function = safeLabel(value.Func)
		switch value.Err {
		case strconv.ErrSyntax:
			node.Kind = "syntax"
		case strconv.ErrRange:
			node.Kind = "range"
		}
	case smithy.APIError:
		node.Code = safeLabel(value.ErrorCode())
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
			node.CauseType = fmt.Sprintf("%T", cause)
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
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-[]/", r) {
			return r
		}
		return '_'
	}, value)
}

// This example recognizes fixed adapter validation messages. Arbitrary application
// errors are not interpolated. Expand the selection for application-specific types.
func safeValidationReason(err error) string {
	switch err.Error() {
	case "endpoint must be a loopback HTTP URL", "forwarded order mismatch", "expected Notification", "unexpected topic",
		"duplicate JSON key", "expected JSON string", "expected JSON object",
		"expected UTF-8 JSON", "serialized envelope exceeds 1 MiB", "expected nonempty UTF-8 text",
		"maximum 10 attributes for raw SQS delivery",
		"invalid name",
		"duplicate name",
		"reserved identity conflict or case alias",
		"empty value",

		"invalid SQS text", "missing receipt", "reserved case alias", "logical ID requires String",
		"expected positive uint64", "list values are unsupported", "invalid data type",
		"expected nonempty valid text value", "expected nonempty binary value", "unsupported data type",
		"maximum 10 attributes including logical id", "body and attributes exceed 1 MiB",
		"expected 1..1048576 bytes of valid SQS text", "FIFO requires 1..128 printable ASCII characters without spaces":
		return err.Error()
	default:
		return ""
	}
}
