package sqs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
)

type deliveryRequirements struct {
	redelivery    bool
	renewInterval time.Duration
}

func (c *Consumer) requirements(requirements []broker.Requirement) (deliveryRequirements, error) {
	var result deliveryRequirements
	for _, requirement := range requirements {
		switch value := requirement.(type) {
		case broker.AttemptsRequirement:
		case broker.RedeliveryRequirement:
			result.redelivery = true
		case broker.KeepAliveRequirement:
			if value.Interval <= 0 || value.Interval >= c.config.VisibilityTimeout {
				return result, errors.New("keepalive interval must be positive and shorter than visibility")
			}
			if result.renewInterval != 0 && result.renewInterval != value.Interval {
				return result, errors.New("conflicting keepalive intervals")
			}
			result.renewInterval = value.Interval
		default:
			return result, fmt.Errorf("sqs requirement %T is unsupported", requirement)
		}
	}
	return result, nil
}

func validateDisposition(disposition broker.Disposition, redelivery bool) (string, time.Duration, error) {
	switch value := disposition.(type) {
	case broker.Handled:
		return OperationDelete, 0, nil
	case broker.RetryAfter:
		if !redelivery {
			return "", 0, errors.New("RetryAfter requires declared redelivery")
		}
		if value.Delay < 0 || value.Delay > maxVisibility || value.Delay%time.Second != 0 {
			return "", 0, errors.New("retry delay must be whole seconds in 0..43200")
		}
		return OperationRetry, value.Delay, nil
	default:
		return "", 0, fmt.Errorf("unsupported result %T", disposition)
	}
}

func (c *Consumer) retryMessage(ctx context.Context, receipt string, delay time.Duration, cause error, observer *observation) error {
	operationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.OperationTimeout)
	defer cancel()
	start := time.Now()
	err := c.changeVisibility(operationCtx, receipt, delay)
	outcome := OutcomeAccepted
	if err != nil {
		outcome = OutcomeUnknown
		err = operationError(c.config.Source, OperationRetry, err)
	}
	observer.record(ctx, c.config.Source, OperationRetry, outcome, start, err, cause)
	return err
}

func (c *Consumer) changeVisibility(ctx context.Context, receipt string, visibility time.Duration) error {
	_, err := c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.config.QueueURL), ReceiptHandle: aws.String(receipt),
		// #nosec G115 -- configured visibility and retry delay are bounded to 0..43200 seconds.
		VisibilityTimeout: int32(visibility / time.Second),
	})
	return err
}

// renewalRun owns a single delivery's sequential native renewal requests. Closing
// stop prevents new admission. It never cancels an already admitted request.
// Forced processing cancellation owns cancel; finish joins before terminal action.
type renewalRun struct {
	mu         sync.Mutex
	stopped    bool
	started    bool
	stop, done chan struct{}
	cancel     context.CancelFunc
	execute    func()
	err        error
}

func (c *Consumer) newRenewal(ctx context.Context,
	state *processingState,
	receipt string,
	interval time.Duration,
	observer *observation) *renewalRun {
	run := &renewalRun{}
	if interval == 0 {
		return run
	}
	renewalCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	run.stop, run.done, run.cancel = make(chan struct{}), make(chan struct{}), cancel
	state.renewCancel = cancel
	run.execute = func() {
		defer close(run.done)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-run.stop:
				return
			case <-renewalCtx.Done():
				return
			case <-timer.C:
			}
			run.mu.Lock()
			admitted := !run.stopped && renewalCtx.Err() == nil
			run.mu.Unlock()
			if !admitted {
				return
			}
			start := time.Now()
			operationCtx, stopOperation := context.WithTimeout(renewalCtx, c.config.OperationTimeout)
			err := c.changeVisibility(operationCtx, receipt, c.config.VisibilityTimeout)
			stopOperation()
			if err != nil {
				if renewalCtx.Err() != nil && onlyCanceled(err) {
					return
				}
				run.err = operationError(c.config.Source, OperationRenew, err)
				state.mu.Lock()
				state.force(run.err)
				state.mu.Unlock()
				observer.record(ctx, c.config.Source, OperationRenew, OutcomeFailed, start, run.err, nil)
				return
			}
			observer.record(ctx, c.config.Source, OperationRenew, OutcomeAccepted, start, nil, nil)
			timer.Reset(interval)
		}
	}
	return run
}
func (r *renewalRun) start() {
	if r.execute != nil {
		r.started = true
		go r.execute()
	}
}
func (r *renewalRun) finish() error {
	if r.execute == nil {
		return nil
	}
	r.mu.Lock()
	r.stopped = true
	close(r.stop)
	r.mu.Unlock()
	// A run not admitted by process has no worker to join.
	if !r.started {
		close(r.done)
	}
	<-r.done
	r.cancel()
	return r.err
}

func onlyCanceled(err error) bool {
	if err == context.Canceled {
		return true
	}
	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		causes := value.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyCanceled(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		cause := value.Unwrap()
		return cause != nil && onlyCanceled(cause)
	default:
		return false
	}
}
