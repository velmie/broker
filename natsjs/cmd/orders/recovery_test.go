package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v3"
)

func TestRetryStartupTimeoutClassifiesCompleteErrorTree(t *testing.T) {
	startup := func(cause error) error {
		return &natsjs.OperationError{Source: "orders/worker", Operation: natsjs.OperationStartup, Cause: cause}
	}
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"deadline", startup(context.DeadlineExceeded), true},
		{"sdk-timeout", startup(nats.ErrTimeout), true},
		{"wrapped", fmt.Errorf("run: %w", startup(context.DeadlineExceeded)), true},
		{"joined-startups", errors.Join(startup(context.DeadlineExceeded), startup(nats.ErrTimeout)), true},
		{"nil", nil, false},
		{"plain-timeout", context.DeadlineExceeded, false},
		{"startup-other", startup(nats.ErrConnectionClosed), false},
		{"startup-nested-independent", startup(errors.Join(context.DeadlineExceeded, errors.New("independent failure"))), false},
		{"joined-independent", errors.Join(startup(context.DeadlineExceeded), errors.New("cleanup failed")), false},
		{"joined-cleanup", errors.Join(startup(nats.ErrTimeout),
			&natsjs.OperationError{Operation: natsjs.OperationCleanup, Cause: context.DeadlineExceeded}), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := retryStartupTimeout(test.err); got != test.want {
				t.Fatalf("got=%v want=%v error=%v", got, test.want, test.err)
			}
		})
	}
	for _, operation := range []string{
		natsjs.OperationProcess, natsjs.OperationAck, natsjs.OperationRenew, natsjs.OperationCleanup, natsjs.OperationStop,
	} {
		t.Run(operation, func(t *testing.T) {
			if retryStartupTimeout(&natsjs.OperationError{Operation: operation, Cause: context.DeadlineExceeded}) {
				t.Fatal("non-startup timeout was accepted")
			}
		})
	}
}
