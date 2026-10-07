package main

import (
	"context"
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	adapter "github.com/velmie/broker/azuresb"
)

func retryStartup(owner context.Context, entered bool, err error) bool {
	return owner.Err() == nil && !entered && onlyReceiveConnection(err)
}
func onlyReceiveConnection(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyReceiveConnection(child) {
				return false
			}
		}
		return true
	}
	if native, ok := err.(*adapter.OperationError); ok {
		return native.Operation == adapter.OperationReceive && onlyConnectionLost(native.Cause)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyReceiveConnection(cause)
	}
	return false
}
func onlyConnectionLost(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyConnectionLost(child) {
				return false
			}
		}
		return true
	}
	if _, ok := err.(*adapter.OperationError); ok {
		return false
	}
	// The SDK Error does not expose its private inner cause through Unwrap.
	if native, ok := err.(*azservicebus.Error); ok {
		return native.Code == azservicebus.CodeConnectionLost
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyConnectionLost(cause)
	}
	return false
}
