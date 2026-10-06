package main

import (
	"log/slog"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/natsjs/v3"
)

// ReplaceAttr receives original errors, including bound attributes. The shared
// projector keeps branch context while the example supplies its own policy.
func redactAttribute(_ []string, attr slog.Attr) slog.Attr {
	failure, ok := attr.Value.Any().(error)
	if !ok {
		return attr
	}
	return slog.Any(attr.Key, broker.DescribeError(failure, exampleErrorFields, adapter.ErrorFields))
}

func exampleErrorFields(err error) map[string]any {
	if value, ok := err.(*fieldError); ok {
		return map[string]any{"field": value.Field}
	}
	switch err.Error() {
	case "unexpected stream or sequence", "expected stream mismatch", "expected no-stream response",
		"native retry delay not observed", "expected pending backpressure", "expected publisher closed",
		"pending futures remain after cleanup", "expected async acknowledgement timeout":
		return map[string]any{"reason": err.Error()}
	default:
		return nil
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
