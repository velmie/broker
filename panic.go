package broker

import (
	"context"
	"fmt"
	"runtime/debug"
)

// PanicError retains an opted-in processing panic. Value and Stack can contain
// application data and require redaction before logging. Error never formats the
// value or stack. An error panic remains available through errors.Is/errors.As.
type PanicError struct {
	Value any
	Stack []byte
}

// RecoverPanics turns a processing panic into failure without a disposition or
// automatic retry. It calls the wrapped handler once. Put logging/tracing outside
// this wrapper so they observe the recovered failure. It does not recover panics
// in other goroutines or terminate an uncooperative callback.
func RecoverPanics() HandlerMiddleware {
	return HandlerMiddleware{Wrap: func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, delivery Delivery) (result Disposition, err error) {
			returned := false
			defer func() {
				if !returned {
					result = nil
					err = &PanicError{Value: recover(), Stack: debug.Stack()}
				}
			}()
			result, err = next(ctx, delivery)
			returned = true
			return result, err
		}
	}}
}

func (e *PanicError) Error() string { return fmt.Sprintf("processing panic (%T)", e.Value) }
func (e *PanicError) Unwrap() error { cause, _ := e.Value.(error); return cause }
