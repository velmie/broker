package broker

const (
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

func (e *StageError) Error() string  { return e.Stage + ": " + e.Cause.Error() }
func (e *StageError) Unwrap() error  { return e.Cause }
func (e *DecodeError) Error() string { return StageDecode + ": " + e.Cause.Error() }
func (e *DecodeError) Unwrap() error { return e.Cause }
