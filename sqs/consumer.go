package sqs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
)

// Consumer borrows its required client. One Run owns the sequential receive loop.
// Concurrent Run calls fail locally. After return it may be run again. Prefetched
// work is left invisible until SQS visibility expires when shutdown stops admission.
type Consumer struct {
	client  *awssqs.Client
	config  ConsumerConfig
	running atomic.Bool
}

// NewConsumer snapshots configuration without acquiring remote resources.
//
//nolint:gocritic // Value configuration preserves immutable constructor ownership.
func NewConsumer(client *awssqs.Client, config ConsumerConfig) (*Consumer, error) {
	normalized, err := normalizeConsumer(config)
	if err != nil {
		return nil, err
	}
	if normalized.Observer != nil {
		copyObserver := *normalized.Observer
		normalized.Observer = &copyObserver
	}
	return &Consumer{client: client, config: normalized}, nil
}

// Validate rejects unsupported capabilities before any receive or callback.
func (c *Consumer) Validate(requirements ...broker.Requirement) error {
	_, err := c.requirements(requirements)
	return err
}

// Run stops receive/admission at lifecycle cancellation. Graceful shutdown drains
// admitted work. Cancel/expired grace cancels its callback and suppresses a not-yet
// admitted delete. An admitted delete finishes with its independent finite budget.
// All owned work joins before return. A callback must cooperate with cancellation.
func (c *Consumer) Run(ctx context.Context, handler broker.Handler) error {
	if err := c.Validate(handler.Requirements()...); err != nil {
		return err
	}
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("sqs Run is already active")
	}
	defer c.running.Store(false)
	observer := observation{observer: c.config.Observer}
	var runErr error
	for ctx.Err() == nil {
		messages, err := c.receiveResilient(ctx, &observer)
		if err != nil {
			runErr = err
			break
		}
		for i := range messages {
			if ctx.Err() != nil {
				break
			}
			if err = c.process(ctx, handler, &messages[i], &observer); err != nil {
				runErr = err
				break
			}
		}
		if runErr != nil {
			break
		}
	}
	return errors.Join(runErr, context.Cause(ctx), observer.err)
}

func (c *Consumer) receive(ctx context.Context, observer *observation) ([]types.Message, error) {
	receiveCtx, cancel := context.WithTimeout(ctx, c.config.ReceiveTimeout)
	defer cancel()
	start := time.Now()
	result, err := c.client.ReceiveMessage(receiveCtx, &awssqs.ReceiveMessageInput{
		// #nosec G115 -- normalizeConsumer limits batch to 1..10 and wait to 0..20.
		QueueUrl: aws.String(c.config.QueueURL), MaxNumberOfMessages: int32(c.config.BatchSize), WaitTimeSeconds: int32(c.config.WaitTimeSeconds),
		// #nosec G115 -- normalizeConsumer limits visibility to 1..43200 seconds.
		VisibilityTimeout: int32(c.config.VisibilityTimeout / time.Second), MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil {
		wrapped := operationError(c.config.Source, OperationReceive, err)
		observer.record(ctx, c.config.Source, OperationReceive, OutcomeFailed, start, wrapped, nil)
		return nil, wrapped
	}
	return result.Messages, nil
}

func (c *Consumer) process(ctx context.Context, handler broker.Handler, native *types.Message, observer *observation) error {
	start := time.Now()
	value, err := detach(native, c.config.Source)
	if err != nil {
		wrapped := operationError(c.config.Source, OperationMetadata, err)
		observer.record(ctx, c.config.Source, OperationMetadata, OutcomeFailed, start, wrapped, nil)
		return wrapped
	}
	processCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)
	state := &processingState{cancel: cancel}
	requirements, _ := c.requirements(handler.Requirements()) // Run validated this immutable descriptor.
	processCtx = observer.begin(processCtx, c.config.Source)
	renewal := c.newRenewal(processCtx, state, aws.ToString(native.ReceiptHandle), requirements.renewInterval, observer)
	done, joined := make(chan struct{}), make(chan struct{})
	go state.watch(ctx, c.config.Shutdown, done, joined)
	defer func() { close(done); <-joined }()
	state.mu.Lock()
	admitted := ctx.Err() == nil && !state.forced
	state.mu.Unlock()
	if !admitted {
		return errors.Join(context.Cause(ctx), renewal.finish())
	}
	renewal.start()
	var renewalErr error
	disposition, callbackErr := func() (broker.Disposition, error) {
		defer func() { renewalErr = renewal.finish() }()
		return handler.Handle(processCtx, value)
	}()
	var cause error
	if retry, ok := disposition.(broker.RetryAfter); ok {
		cause = retry.Cause
	}
	if callbackErr != nil {
		callbackErr = operationError(c.config.Source, OperationProcess, callbackErr)
		observer.record(processCtx, c.config.Source, OperationProcess, OutcomeFailed, start, callbackErr, cause)
		return errors.Join(callbackErr, renewalErr)
	}
	if renewalErr != nil {
		observer.record(processCtx, c.config.Source, OperationProcess, OutcomeSucceeded, start, nil, cause)
		return renewalErr
	}
	operation, delay, err := validateDisposition(disposition, requirements.redelivery)
	if err != nil {
		wrapped := operationError(c.config.Source, OperationDisposition, err)
		observer.record(processCtx, c.config.Source, OperationDisposition, OutcomeFailed, start, wrapped, cause)
		return wrapped
	}
	observer.record(processCtx, c.config.Source, OperationProcess, OutcomeSucceeded, start, nil, cause)
	state.mu.Lock()
	if ctx.Err() != nil && c.config.Shutdown.Mode == broker.ShutdownCancel {
		state.force(context.Cause(ctx))
	}
	allowed := !state.forced && processCtx.Err() == nil
	state.mu.Unlock()
	if !allowed {
		err = context.Cause(processCtx)
		observer.record(processCtx, c.config.Source, operation, OutcomeSuppressed, time.Now(), err, cause)
		return err
	}
	if operation == OperationRetry {
		return c.retryMessage(processCtx, aws.ToString(native.ReceiptHandle), delay, cause, observer)
	}
	return c.deleteMessage(processCtx, aws.ToString(native.ReceiptHandle), observer)
}

func (c *Consumer) deleteMessage(ctx context.Context, receipt string, observer *observation) error {
	operationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.OperationTimeout)
	defer cancel()
	start := time.Now()
	_,
		err := c.client.DeleteMessage(operationCtx,
		&awssqs.DeleteMessageInput{QueueUrl: aws.String(c.config.QueueURL),
			ReceiptHandle: aws.String(receipt)})
	result := OutcomeAccepted
	if err != nil {
		result = OutcomeUnknown
		err = operationError(c.config.Source, OperationDelete, err)
	}
	observer.record(ctx, c.config.Source, OperationDelete, result, start, err, nil)
	return err
}

type processingState struct {
	mu          sync.Mutex
	forced      bool
	cancel      context.CancelCauseFunc
	renewCancel context.CancelFunc
}

func (s *processingState) force(cause error) {
	s.forced = true
	s.cancel(cause)
	if s.renewCancel != nil {
		s.renewCancel()
	}
}
func (s *processingState) watch(ctx context.Context, policy broker.ShutdownPolicy, done <-chan struct{}, joined chan<- struct{}) {
	defer close(joined)
	select {
	case <-done:
		return
	case <-ctx.Done():
	}
	if policy.Mode == broker.ShutdownGraceful {
		if policy.GracePeriod == 0 {
			<-done
			return
		}
		timer := time.NewTimer(policy.GracePeriod)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
		}
	}
	s.mu.Lock()
	s.force(context.Cause(ctx))
	s.mu.Unlock()
}
