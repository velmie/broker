package broker

import "fmt"

const (
	stagePanic    = "panic"
	StageDecode   = "decode"
	StageMetadata = "metadata"
	StageHandle   = "handle"
	StageResolve  = "resolve"
	StageEncode   = "encode"
	StagePublish  = "publish"
)

// StageError identifies the failed invocation. It does not prove retry safety.
// Cause is required and can contain application data. A logging adapter must
// inspect and redact causes before serialization, preserving useful diagnostics.
type StageError struct {
	Stage string
	Cause error
}

// DecodeError preserves a codec error or a rejected nil-pointer DTO result.
// Cause is required and may contain application-specific data.
type DecodeError struct{ Cause error }

// FieldError associates an original failure with an approved field name and a
// controlled diagnostic reason. Field and Reason must describe the contract, not
// contain rejected input. Cause is required and remains available for inspection.
type FieldError struct {
	Field  string
	Reason string
	Cause  error
}

// RegistrationError identifies the coordinator binding and operation that failed.
// Name is the configured diagnostic name. Cause is required and may contain
// application data requiring redaction at the logging boundary.
type RegistrationError struct {
	Name      string
	Operation string
	Cause     error
}

func (e *StageError) Error() string  { return e.Stage + ": " + e.Cause.Error() }
func (e *StageError) Unwrap() error  { return e.Cause }
func (e *DecodeError) Error() string { return StageDecode + ": " + e.Cause.Error() }
func (e *DecodeError) Unwrap() error { return e.Cause }
func (e *FieldError) Error() string  { return e.Cause.Error() }
func (e *FieldError) Unwrap() error  { return e.Cause }

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("%s %s: %v", e.Name, e.Operation, e.Cause)
}
func (e *RegistrationError) Unwrap() error { return e.Cause }
