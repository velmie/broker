package sqs

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const maxReceiveFailures = 4

func (c *Consumer) receiveResilient(ctx context.Context, observer *observation) ([]types.Message, error) {
	var lastFailure error
	for failures := 1; ; failures++ {
		if ctx.Err() != nil {
			return nil, errors.Join(lastFailure, context.Cause(ctx))
		}
		messages, err := c.receive(ctx, observer)
		if err == nil {
			return messages, nil
		}
		if ctx.Err() != nil {
			return nil, errors.Join(lastFailure, err, context.Cause(ctx))
		}
		if !c.config.ResilientReceive || failures >= maxReceiveFailures || !retryableReceive(err) {
			return nil, err
		}
		lastFailure = err
		timer := time.NewTimer(time.Second << (failures - 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(err, context.Cause(ctx))
		case <-timer.C:
		}
	}
}

func retryableReceive(err error) bool {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "AccessDenied", "AccessDeniedException", "NotAuthorized", "KmsAccessDenied",
			"ExpiredToken", "ExpiredTokenException", "RequestExpired", "InvalidSecurity", "InvalidClientTokenId",
			"SignatureDoesNotMatch", "UnrecognizedClientException", "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue",
			"InvalidParameterValue", "InvalidParameter", "ValidationError", "MissingParameter", "InvalidAddress",
			"KmsDisabled", "KmsNotFound", "KmsInvalidState", "KmsInvalidKeyUsage", "RequestCanceled":
			return false
		case "RequestThrottled", "RequestThrottledException", "Throttling", "ThrottlingException", "RequestLimitExceeded",
			"KmsThrottled", "OverLimit", "RequestTimeout", "RequestTimeoutException", "ResponseTimeout",
			"InternalFailure", "InternalError", "ServiceUnavailable", "ServiceUnavailableException":
			return true
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		switch response.HTTPStatusCode() {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
	}
	if apiError != nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) {
		if network.Timeout() {
			return true
		}
		//nolint:staticcheck // Preserve the baseline classification of temporary transport failures.
		if network.Temporary() {
			return true
		}
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}
