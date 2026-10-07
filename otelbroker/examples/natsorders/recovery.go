package main

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v3"
)

// This application's recovery policy permits only startup timeouts. Every
// independent error must match. An uncertain terminal request is never retried.
func retryStartupTimeout(err error) bool {
	if branches, ok := err.(interface{ Unwrap() []error }); ok {
		children := branches.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryStartupTimeout(child) {
				return false
			}
		}
		return true
	}
	if native, ok := err.(*natsjs.OperationError); ok {
		return native.Operation == natsjs.OperationStartup && onlyTimeouts(native.Cause)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return retryStartupTimeout(cause)
	}
	return false
}

func onlyTimeouts(err error) bool {
	if branches, ok := err.(interface{ Unwrap() []error }); ok {
		children := branches.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyTimeouts(child) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyTimeouts(cause)
	}
	return err == context.DeadlineExceeded || err == nats.ErrTimeout
}
