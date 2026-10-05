package main

import (
	"errors"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker/sqs"
)

const receiveThrottleCode = "RequestThrottled"

// Every independent branch must be an explicitly classified receive throttle.
// Native request attempts and generation replacement remain separate policies.
func retryReceiveThrottle(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryReceiveThrottle(child) {
				return false
			}
		}
		return true
	}
	if native, ok := err.(*sqs.OperationError); ok {
		return native.Operation == sqs.OperationReceive && onlyThrottle(native.Cause)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return retryReceiveThrottle(cause)
	}
	return false
}
func onlyThrottle(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyThrottle(child) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyThrottle(cause)
	}
	native, ok := err.(smithy.APIError)
	return ok && native.ErrorCode() == receiveThrottleCode
}
