package sqs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func enableResilientReceive(t *testing.T, c *adapter.ConsumerConfig) {
	t.Helper()
	c.ResilientReceive = true
}

func TestSDKReceiveRecoveryResetsAndExhausts(t *testing.T) {
	var calls atomic.Int32
	var times []time.Time
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		n := calls.Add(1)
		times = append(times, time.Now())
		if n == 3 {
			return 200, request{}
		}
		return 503, request{"__type": "ServiceUnavailable", "message": "failure marker"}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { enableResilientReceive(t, c) })
	err := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("callback after failed/empty receives")
		return broker.Handled{}, nil
	}))
	var apiError smithy.APIError
	if calls.Load() != 7 || !errors.As(err, &apiError) || apiError.ErrorCode() != "ServiceUnavailable" {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
	for _, interval := range []struct {
		index   int
		minimum time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {4, time.Second}, {5, 2 * time.Second}, {6, 4 * time.Second}} {
		if elapsed := times[interval.index].Sub(times[interval.index-1]); elapsed < interval.minimum {
			t.Errorf("retry %d took %s, want >=%s", interval.index, elapsed, interval.minimum)
		}
	}
}

func TestSDKReceiveRecoveryTerminalCodesOverrideStatus(t *testing.T) {
	for _, code := range []string{"AccessDenied", "QueueDoesNotExist", "InvalidParameterValue", "UnknownFailure"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			status := 503
			if code == "UnknownFailure" {
				status = 400
			}
			client, url := newWire(t, func(context.Context, string, request) (int, any) {
				calls.Add(1)
				return status, request{"__type": code, "message": "secret diagnostic"}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { enableResilientReceive(t, c) })
			err := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("callback")
				return broker.Handled{}, nil
			}))
			var apiError smithy.APIError
			if calls.Load() != 1 || !errors.As(err, &apiError) || apiError.ErrorCode() != code {
				t.Fatalf("terminal calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestSDKReceiveRecoveryCancellationRetainsLastFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		calls.Add(1)
		return 429, request{"__type": "RequestThrottled", "message": "throttle marker"}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		enableResilientReceive(t, c)
		c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
			if o.Operation == adapter.OperationReceive {
				cancel()
			}
		}}
	})
	err := consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("callback")
		return broker.Handled{}, nil
	}))
	var apiError smithy.APIError
	if calls.Load() != 1 || !errors.Is(err, context.Canceled) || !errors.As(err, &apiError) || apiError.ErrorCode() != "RequestThrottled" {
		t.Fatalf("cause after retry cancellation: %v", err)
	}
}
