package broker

import (
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// PanicRecoveryMiddleware creates a middleware to recover from panics.
// It converts the panic into a regular error that can be returned and handled.
func PanicRecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(e Event) (err error) {
			defer func() {
				if r := recover(); r != nil {
					// Formatting error message and stack trace
					errMsg := fmt.Sprintf("panic recovered: %v\n%s", r, debug.Stack())
					// Converting the panic to a regular error
					err = errors.New(errMsg)
				}
			}()
			return next(e)
		}
	}
}

// LoggingMiddleware creates a middleware for logging the results of event processing.
// It logs whether the processing was successful (OK) or resulted in an error (ERR),
// along with the event's ID and the time taken to process it.
func LoggingMiddleware(logger Logger, options ...LoggingMiddlewareOption) Middleware {
	opts := &loggingMiddlewareOptions{
		logError:  true,
		logHeader: false,
		logHeaderFunc: func(e Event) string {
			return fmt.Sprintf("%+v", e.Message().Header)
		},
		logBodyFunc: func(e Event) string {
			const logBodyMax = 4096
			data := e.Message().Body
			if len(data) > logBodyMax {
				return string(data[:logBodyMax])
			}
			return string(data)
		},
	}

	// Apply options
	for _, opt := range options {
		opt(opts)
	}

	return func(next Handler) Handler {
		return func(e Event) error {
			startTime := time.Now()
			err := next(e)
			duration := time.Since(startTime)

			m := e.Message()
			args := []any{
				"messageId", m.ID,
				"topic", e.Topic(),
				"duration", duration,
			}
			if err != nil && opts.logError {
				args = append(args, "error", err.Error())
			}
			if opts.logHeader {
				args = append(args, "header", opts.logHeaderFunc(e))
			}
			logF := logger.Info
			if err != nil {
				logF = logger.Error
			}
			if opts.logBody || err != nil && opts.logBodyOnError {
				args = append(args, "body", opts.logBodyFunc(e))
			}
			logF("event processed", args...)

			return err
		}
	}
}

// loggingMiddlewareOptions holds configuration options for the logging middleware.
type loggingMiddlewareOptions struct {
	logError       bool                 // Whether to log errors.
	logHeader      bool                 // Whether to log headers.
	logHeaderFunc  func(e Event) string // Function to format the header for logging.
	logBody        bool                 // Whether to log the body.
	logBodyOnError bool                 // Whether to log the body only on errors.
	logBodyFunc    func(e Event) string // Function to format the body for logging.
}

// LoggingMiddlewareOption defines a function type for setting options on loggingMiddlewareOptions.
type LoggingMiddlewareOption func(*loggingMiddlewareOptions)

// WithLogError returns a LoggingMiddlewareOption setting the logError flag.
// If set to true, errors encountered during processing will be logged.
func WithLogError(logError bool) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logError = logError
	}
}

// WithLogHeader returns a LoggingMiddlewareOption setting the logHeader flag.
// If set to true, headers will be logged.
func WithLogHeader(logHeader bool) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logHeader = logHeader
	}
}

// WithLogHeaderFunc returns a LoggingMiddlewareOption for setting a custom function
// to format the header for logging. The function takes a Header object and returns a string.
func WithLogHeaderFunc(logHeaderFunc func(e Event) string) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logHeaderFunc = logHeaderFunc
	}
}

// WithLogBody returns a LoggingMiddlewareOption setting the LogBody flag.
// If set to true, the body will be logged.
func WithLogBody(logBody bool) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logBody = logBody
	}
}

// WithLogBodyOnError returns a LoggingMiddlewareOption setting the LogBodyOnError flag.
// If set to true, the body will be logged only when an error is encountered.
func WithLogBodyOnError(logBodyOnError bool) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logBodyOnError = logBodyOnError
	}
}

// WithLogBodyFunc returns a LoggingMiddlewareOption for setting a custom function
// to format the body for logging. The function takes a byte slice and returns a string.
func WithLogBodyFunc(logBodyFunc func(e Event) string) LoggingMiddlewareOption {
	return func(o *loggingMiddlewareOptions) {
		o.logBodyFunc = logBodyFunc
	}
}

// LoopbackPreventionMiddleware returns a Middleware function that prevents the handling of messages
// originating from the specified instance. This middleware is useful in scenarios where an application
// should not process its own outputs to avoid loops in message processing, particularly in distributed
// systems where instances can receive messages they have sent.
//
// Parameters:
//
//		instanceID - The unique identifier of the instance to ignore messages from.
//	 logger - An optional Logger for logging debug information.
//
// Returns:
//
//	A Middleware function that takes a Handler and returns a new Handler, which intercepts each event,
//	checks if it originates from the specified instance, and skips processing if so, passing others to
//	the next handler in the chain.
func LoopbackPreventionMiddleware(instanceID string, logger ...Logger) Middleware {
	var l Logger
	if len(logger) > 0 {
		l = logger[0]
	}
	return func(next Handler) Handler {
		return func(e Event) error {
			if e.Message().Header.GetInstanceID() == instanceID {
				if l != nil {
					l.Debug(
						"skipping message handling as it originates from the same instance",
						"instanceId",
						instanceID,
						"topic",
						e.Topic(),
						"messageId",
						e.Message().ID,
					)
				}
				return nil
			}
			return next(e)
		}
	}
}
