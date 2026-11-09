package azuresb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

// Consumer owns one sequential receive loop per Run. Concurrent runs are rejected.
// Its required factory returns a fresh exclusive receiver and cleans up partial
// acquisitions on error. Successive runs acquire new receivers. Client ownership
// and all SDK options remain with the caller.
type Consumer struct {
	newReceiver func() (Receiver, error)
	config      ConsumerConfig
	running     atomic.Bool
}

// NewConsumer snapshots configuration and validates it without acquiring resources.
func NewConsumer(newReceiver func() (Receiver, error), config ConsumerConfig) (*Consumer, error) {
	c, err := normalizeConsumer(config)
	if err != nil {
		return nil, err
	}
	if c.Observer != nil {
		o := *c.Observer
		c.Observer = &o
	}
	return &Consumer{newReceiver: newReceiver, config: c}, nil
}

// Validate checks every requirement before factory acquisition or native effects.
// Immediate abandonment and lock renewal are supported only in PeekLock mode.
func (c *Consumer) Validate(requirements ...broker.Requirement) error {
	var interval time.Duration
	for _, r := range requirements {
		switch value := r.(type) {
		case broker.AttemptsRequirement:
		case broker.KeepAliveRequirement:
			if c.config.ReceiveMode != azservicebus.ReceiveModePeekLock {
				return errors.New("lock renewal requires PeekLock")
			}
			if value.Interval <= 0 {
				return errors.New("keepalive interval must be positive")
			}
			if interval != 0 && interval != value.Interval {
				return errors.New("conflicting keepalive intervals")
			}
			interval = value.Interval
		case broker.RedeliveryRequirement:
			if c.config.ReceiveMode != azservicebus.ReceiveModePeekLock {
				return errors.New("redelivery requires PeekLock")
			}
		default:
			return fmt.Errorf("unsupported requirement %T", r)
		}
	}
	return nil
}

// Run joins all owned work and receiver closure before returning. Graceful stop
// drains an admitted callback. Forced stop cancels it and suppresses a terminal
// operation not yet admitted. An admitted terminal operation retains its budget.
// Callbacks must cooperate with cancellation. Panics unwind through cleanup.
func (c *Consumer) Run(ctx context.Context, handler broker.Handler) error {
	return c.run(ctx, handler, false)
}

// RunService joins the same work as Run, but a parent stop alone succeeds.
// Independent failures and an expired grace period remain errors. A callback
// returning exactly its delivery context error after forced shutdown cooperates.
func (c *Consumer) RunService(ctx context.Context, handler broker.Handler) error {
	return c.run(ctx, handler, true)
}

func (c *Consumer) run(ctx context.Context, handler broker.Handler, service bool) (result error) {
	if err := c.Validate(handler.Requirements()...); err != nil {
		return err
	}
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("azuresb Run already active")
	}
	defer c.running.Store(false)
	if err := context.Cause(ctx); err != nil {
		if service {
			return nil
		}
		return err
	}
	observer := observation{observer: c.config.Observer}
	defer func() {
		if !service {
			result = errors.Join(result, context.Cause(ctx), observer.err)
			return
		}
		result = errors.Join(result, observer.err)
	}()
	start := time.Now()
	receiver, err := c.newReceiver()
	if err != nil {
		err = operationError(c.config.Source, OperationAcquire, err)
		observer.record(ctx, c.config.Source, OperationAcquire, OutcomeFailed, start, err, nil)
		return err
	}
	defer func() { result = errors.Join(result, c.closeReceiver(ctx, receiver, &observer)) }()
	for ctx.Err() == nil {
		messages, err := c.receive(ctx, receiver, &observer, service)
		if err != nil {
			return err
		}
		for _, native := range messages {
			if ctx.Err() != nil {
				break
			}
			if processErr := c.process(ctx, receiver, handler, native, &observer, service); processErr != nil {
				return processErr
			}
		}
	}
	return nil
}
func (c *Consumer) closeReceiver(ctx context.Context, r Receiver, o *observation) error {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.CloseTimeout)
	defer cancel()
	start := time.Now()
	err := r.Close(closeCtx)
	outcome := OutcomeSucceeded
	if err != nil {
		err = operationError(c.config.Source, OperationClose, err)
		outcome = OutcomeFailed
	}
	o.record(ctx, c.config.Source, OperationClose, outcome, start, err, nil)
	return err
}
func (c *Consumer) receive(ctx context.Context, r Receiver, o *observation, service bool) ([]*azservicebus.ReceivedMessage, error) {
	poll, cancel := context.WithTimeout(ctx, c.config.ReceiveTimeout)
	defer cancel()
	start := time.Now()
	messages, err := r.ReceiveMessages(poll, 1, nil)
	if err != nil {
		if service && ctx.Err() != nil && receiveCanceled(err, ctx.Err()) {
			return nil, nil
		}
		if ctx.Err() == nil && poll.Err() == context.DeadlineExceeded && onlyCause(err, context.DeadlineExceeded) {
			return nil, nil
		}
		err = operationError(c.config.Source, OperationReceive, err)
		o.record(ctx, c.config.Source, OperationReceive, OutcomeFailed, start, err, nil)
		return nil, err
	}
	return messages, nil
}

//nolint:gocyclo,funlen // Processing joins the callback, renewal, shutdown and settlement outcomes.
func (c *Consumer) process(
	ctx context.Context, r Receiver, h broker.Handler, native *azservicebus.ReceivedMessage, o *observation, service bool,
) (result error) {
	start := time.Now()
	value, err := detach(native, c.config.Source)
	if err != nil {
		err = operationError(c.config.Source, OperationMetadata, err)
		o.record(ctx, c.config.Source, OperationMetadata, OutcomeFailed, start, err, nil)
		return err
	}
	work, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)
	state := processingState{cancel: cancel, service: service}
	done, joined := make(chan struct{}), make(chan struct{})
	go state.watch(ctx, c.config.Shutdown, done, joined)
	defer func() {
		close(done)
		<-joined
		if service && state.graceExpired {
			result = errors.Join(result, operationError(c.config.Source, "stop", ErrGracePeriodExceeded))
		}
	}()
	if ctx.Err() != nil {
		if service {
			return nil
		}
		return context.Cause(ctx)
	}
	work = o.begin(work, c.config.Source)
	renewal := c.startRenewal(work, r, native, h.Requirements(), &state, o)
	var renewalErr, callbackContextErr, callbackContextCause error
	disposition, err := func() (broker.Disposition, error) {
		defer func() { renewalErr = renewal.finish() }()
		disposition, callbackErr := h.Handle(work, value)
		callbackContextErr = work.Err()
		callbackContextCause = context.Cause(work)
		return disposition, callbackErr
	}()
	var cause error
	if retry, ok := disposition.(broker.RetryAfter); ok {
		cause = retry.Cause
	}
	if err != nil {
		state.mu.Lock()
		cooperative := service && state.shutdown != nil && callbackContextCause == state.shutdown && err == callbackContextErr
		state.mu.Unlock()
		if cooperative {
			return renewalErr
		}
		err = operationError(c.config.Source, OperationProcess, err)
		o.record(work, c.config.Source, OperationProcess, OutcomeFailed, start, err, cause)
		return errors.Join(err, renewalErr)
	}
	o.record(work, c.config.Source, OperationProcess, OutcomeSucceeded, start, nil, cause)
	if renewalErr != nil {
		return renewalErr
	}
	operation, err := c.disposition(disposition, h.Requirements())
	if err != nil {
		err = operationError(c.config.Source, OperationDisposition, err)
		o.record(work, c.config.Source, OperationDisposition, OutcomeFailed, start, err, cause)
		return err
	}
	state.mu.Lock()
	if ctx.Err() != nil && c.config.Shutdown.Mode == broker.ShutdownCancel {
		state.forceStop(context.Cause(ctx), false)
	}
	allowed := !state.forced && work.Err() == nil
	state.mu.Unlock()
	if !allowed {
		err = context.Cause(work)
		o.record(work, c.config.Source, operation, OutcomeSuppressed, time.Now(), err, cause)
		state.mu.Lock()
		stopped := service && state.shutdown != nil && err == state.shutdown
		state.mu.Unlock()
		if stopped {
			return nil
		}
		return err
	}
	if operation == "" {
		return nil
	}
	return c.settle(work, r, native, operation, cause, o)
}
func (c *Consumer) disposition(d broker.Disposition, requirements []broker.Requirement) (string, error) {
	switch result := d.(type) {
	case broker.Handled:
		if c.config.ReceiveMode == azservicebus.ReceiveModeReceiveAndDelete {
			return "", nil
		}
		return OperationComplete, nil
	case broker.RetryAfter:
		if result.Delay != 0 {
			return "", errors.New("RetryAfter.Delay: only zero is supported")
		}
		for _, r := range requirements {
			if _, ok := r.(broker.RedeliveryRequirement); ok {
				return OperationAbandon, nil
			}
		}
		return "", errors.New("RetryAfter requires RedeliveryRequirement")
	default:
		return "", fmt.Errorf("unsupported disposition %T", d)
	}
}
func (c *Consumer) settle(
	ctx context.Context, r Receiver, native *azservicebus.ReceivedMessage,
	operation string, cause error, o *observation,
) error {
	terminal, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.OperationTimeout)
	defer cancel()
	start := time.Now()
	var err error
	if operation == OperationComplete {
		err = r.CompleteMessage(terminal, native, nil)
	} else {
		err = r.AbandonMessage(terminal, native, nil)
	}
	outcome := OutcomeAccepted
	if err != nil {
		outcome = OutcomeUnknown
		err = operationError(c.config.Source, operation, err)
	}
	o.record(ctx, c.config.Source, operation, outcome, start, err, cause)
	return err
}

// onlyCause excludes independent branches in joined SDK failures.
func onlyCause(err, target error) bool {
	if err == target {
		return true
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyCause(child, target) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return onlyCause(e.Unwrap(), target)
	default:
		return false
	}
}

// receiveCanceled recognizes the cancellation returned by ReceiveMessages and
// by net.Dialer when the SDK is still opening its connection.
func receiveCanceled(err, cause error) bool {
	if err == cause {
		return true
	}
	dial, ok := err.(*net.OpError)
	return ok && dial.Op == "dial" && errors.Is(dial, cause)
}
