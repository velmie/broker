package natsjs

import "fmt"

// OperationError identifies a failed native binding operation. It preserves its
// original cause for diagnostics and application-owned recovery classification.
// A cause can contain application data. Logging adapters must inspect and redact
// it before serialization, preserving safe operation, source and field details.
// Neither the operation nor the cause alone establishes that retry is safe.
type OperationError struct {
	Source    string
	Operation string
	Cause     error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("nats %q %s: %v", e.Source, e.Operation, e.Cause)
}

func (e *OperationError) Unwrap() error { return e.Cause }
