package main

// fieldError associates a local verification failure with its configured field.
type fieldError struct {
	Field string
	Cause error
}

func (e *fieldError) Error() string { return e.Field + ": " + e.Cause.Error() }
func (e *fieldError) Unwrap() error { return e.Cause }
