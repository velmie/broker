package sns

import (
	"context"
	"fmt"
	"time"
)

const (
	OperationPublish  = "publish"
	OperationMetadata = "metadata"
	OutcomeAccepted   = "accepted"
	OutcomeFailed     = "failed"
	OutcomeUnknown    = "unknown"
)

// Observer receives one synchronous record per publication. Record must return
// promptly and support concurrent callers. A panic is joined with the operation
// result after publication and does not trigger replay. The adapter snapshots
// the hook at construction. Logging adapters must redact application-specific
// data and SDK URLs/messages while retaining error types, codes, source and field.
type Observer struct {
	Record func(context.Context, Observation)
}

// Observation carries original errors without payload, header or topic values.
// Unknown means a failed request may nevertheless have been accepted by SNS.
type Observation struct {
	Source, Operation, Outcome string
	Duration                   time.Duration
	Err                        error
}

// OperationError preserves the source label, stage and original cause. Cause
// may contain sensitive SDK details and needs application-aware log redaction.
type OperationError struct {
	Source, Operation string
	Cause             error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("sns %q %s: %v", e.Source, e.Operation, e.Cause)
}
func (e *OperationError) Unwrap() error { return e.Cause }

// FieldError identifies malformed data without interpolating its value.
type FieldError struct {
	Field string
	Cause error
}

func (e *FieldError) Error() string { return fmt.Sprintf("%s: %v", e.Field, e.Cause) }
func (e *FieldError) Unwrap() error { return e.Cause }

// ObserverPanicError retains the original panic value for redacted diagnostics.
type ObserverPanicError struct{ Value any }

func (e *ObserverPanicError) Error() string { return fmt.Sprintf("sns observer panic (%T)", e.Value) }
func (e *ObserverPanicError) Unwrap() error { err, _ := e.Value.(error); return err }

func record(observer *Observer, ctx context.Context, value Observation) (err error) {
	if observer == nil || observer.Record == nil {
		return nil
	}
	defer func() {
		if cause := recover(); cause != nil {
			err = &ObserverPanicError{Value: cause}
		}
	}()
	observer.Record(ctx, value)
	return nil
}
