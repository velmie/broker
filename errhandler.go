package broker

import (
	"fmt"
	"time"
)

// LogErrorHandler logs passed error
func LogErrorHandler(log Logger) ErrorHandler {
	return func(err error, sub Subscription) {
		log.Error(fmt.Sprintf("broker(%s): %s", sub.Topic(), err.Error()))
	}
}

// DelayErrorHandler waits for the specified duration
func DelayErrorHandler(duration time.Duration, logger ...Logger) ErrorHandler {
	var log Logger
	if len(logger) > 0 {
		log = logger[0]
	}
	return func(_ error, _ Subscription) {
		if log != nil {
			log.Debug(fmt.Sprintf("broker.DelayErrorHandler: waiting %s", duration))
		}
		time.Sleep(duration)
	}
}

// CombineErrorHandlers combines multiple error handlers by calling them sequentially
func CombineErrorHandlers(handlers ...ErrorHandler) ErrorHandler {
	return func(err error, sub Subscription) {
		for _, handler := range handlers {
			handler(err, sub)
		}
	}
}

// WithDefaultErrorHandler is SubscribeOption which sets default error handler
func WithDefaultErrorHandler(subscriber Subscriber, log Logger) SubscribeOption {
	const defaultDelay = 5 * time.Second
	return func(options *SubscribeOptions) {
		handler := CombineErrorHandlers(
			LogErrorHandler(log),
			DelayErrorHandler(defaultDelay, log),
			ResubscribeErrorHandler(subscriber, ResubscribeWithLogger(log)),
		)
		options.ErrorHandler = handler
	}
}
