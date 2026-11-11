package natsjs

import (
	"fmt"

	"github.com/velmie/broker"
)

// OperationError identifies a failed native operation and preserves its original
// cause. Run handles recognized pull receive interruptions internally. An error
// returned by Run does not by itself establish that restarting the run or
// replaying a delivery is safe. Causes are retained unchanged, including any
// descriptions supplied by a NATS server.
type OperationError struct {
	Source    string
	Operation string
	Cause     error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("nats %q %s: %v", e.Source, e.Operation, e.Cause)
}

func (e *OperationError) Unwrap() error { return e.Cause }

func operationFailure(source, operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &OperationError{Source: source, Operation: operation, Cause: cause}
}

func fieldFailure(field, reason string, cause error) error {
	return &broker.FieldError{Field: field, Reason: reason, Cause: cause}
}
