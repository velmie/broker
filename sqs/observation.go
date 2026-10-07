package sqs

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	OperationPublish     = "publish"
	OperationReceive     = "receive"
	OperationProcess     = "process"
	OperationDelete      = "delete"
	OperationRetry       = "retry"
	OperationRenew       = "renew"
	OperationMetadata    = "metadata"
	OperationDisposition = "disposition"
	OutcomeAccepted      = "accepted"
	OutcomeSucceeded     = "succeeded"
	OutcomeFailed        = "failed"
	OutcomeUnknown       = "unknown"
	OutcomeSuppressed    = "suppressed"
)

// Observer provides synchronous instrumentation. Hooks must return promptly and
// preserve cancellation ancestry. Shared observers must support concurrent
// publishers/runs. Within one run hooks are serialized, including renewal.
// Hooks must not wait for another hook or for the consumer to join. A panic disables this operation/run's hooks and is returned
// after normal delivery decisions finish. It never changes settlement or replays
// business work. No bodies, headers, URLs or receipts enter observation records.
type Observer struct {
	Begin func(context.Context, DeliveryInfo) context.Context
	// Record receives original error objects. The logging adapter must redact
	// application-specific sensitive values before serialization, while preserving
	// operation, field, safe source and error type/code. SDK errors can contain URLs.
	Record func(context.Context, Observation)
}

// DeliveryInfo establishes correlation before processing without receipt authority.
type DeliveryInfo struct{ Source string }

// Observation reports native service acceptance separately from processing.
// Unknown means the operation may have changed server state. Accepted delete
// is not an exactly-once guarantee. Err and Cause require application-aware
// redaction. Cause preserves a requested retry reason independently of Err.
type Observation struct {
	Source    string
	Operation string
	Outcome   string
	Duration  time.Duration
	Err       error
	Cause     error
}

// OperationError preserves the original operation failure and safe source label.
// Its cause may contain sensitive application or SDK details requiring redaction.
type OperationError struct {
	Source, Operation string
	Cause             error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("sqs %q %s: %v", e.Source, e.Operation, e.Cause)
}
func (e *OperationError) Unwrap() error { return e.Cause }

// FieldError identifies malformed message data without interpolating its value.
type FieldError struct {
	Field string
	Cause error
}

func (e *FieldError) Error() string { return fmt.Sprintf("%s: %v", e.Field, e.Cause) }
func (e *FieldError) Unwrap() error { return e.Cause }

// ObserverPanicError retains panic evidence for application-aware redaction.
type ObserverPanicError struct{ Value any }

func (e *ObserverPanicError) Error() string { return fmt.Sprintf("sqs observer panic (%T)", e.Value) }
func (e *ObserverPanicError) Unwrap() error { v, _ := e.Value.(error); return v }

type observation struct {
	mu       sync.Mutex
	observer *Observer
	err      error
}

func (o *observation) guard() {
	if v := recover(); v != nil {
		o.err = &ObserverPanicError{Value: v}
		o.observer = nil
	}
}
func (o *observation) begin(ctx context.Context, source string) (result context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	result = ctx
	if o.observer != nil && o.observer.Begin != nil {
		defer o.guard()
		result = o.observer.Begin(ctx, DeliveryInfo{Source: source})
	}
	return result
}
func (o *observation) record(ctx context.Context, source, operation, outcome string, start time.Time, err, cause error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.observer != nil && o.observer.Record != nil {
		defer o.guard()
		o.observer.Record(ctx,
			Observation{Source: source,
				Operation: operation,
				Outcome:   outcome,
				Duration:  time.Since(start),
				Err:       err,
				Cause:     cause})
	}
}
func operationError(source, operation string, err error) error {
	return &OperationError{Source: source, Operation: operation, Cause: err}
}
