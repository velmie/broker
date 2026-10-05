package natsjs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// ErrGracePeriodExceeded is retained when graceful shutdown escalates to
// cooperative cancellation. It does not mean the callback has terminated.
var ErrGracePeriodExceeded = errors.New("shutdown grace period expired")

type consumerRun struct {
	consumer  *Consumer
	parent    context.Context
	work      context.Context
	cancel    context.CancelCauseFunc
	done      chan struct{}
	watchDone chan struct{}
	// mu serializes callback/terminal admission with forced shutdown. It is
	// never held during user code, observations, renewal or terminal I/O.
	mu                sync.Mutex
	stopped           bool
	stopAt            time.Time
	observationMu     sync.Mutex
	observationErrors []error
}

type renewal struct {
	mu       sync.Mutex
	stopping bool
	stop     chan struct{}
	done     chan struct{}
	err      error
}

func newConsumerRun(consumer *Consumer, parent context.Context) *consumerRun {
	work, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	run := &consumerRun{consumer: consumer, parent: parent, work: work, cancel: cancel,
		done: make(chan struct{}), watchDone: make(chan struct{})}
	go run.watch()
	return run
}

func (r *consumerRun) watch() {
	defer close(r.watchDone)
	select {
	case <-r.done:
		return
	case <-r.parent.Done():
	}
	r.mu.Lock()
	r.syncStopLocked()
	stopAt := r.stopAt
	r.mu.Unlock()
	r.record(r.parent, Observation{Operation: OperationStop, Outcome: OutcomeStarted, Cause: context.Cause(r.parent)})
	policy := r.consumer.config.Shutdown
	if policy.Mode == broker.ShutdownCancel || policy.GracePeriod == 0 {
		return
	}
	timer := time.NewTimer(time.Until(stopAt.Add(policy.GracePeriod)))
	defer timer.Stop()
	select {
	case <-r.done:
		return
	case <-timer.C:
		r.mu.Lock()
		r.cancel(ErrGracePeriodExceeded)
		r.mu.Unlock()
		r.record(r.work, Observation{Operation: OperationStop, Outcome: OutcomeCanceled, Cause: ErrGracePeriodExceeded})
	}
}

func (r *consumerRun) syncStopLocked() {
	if r.parent.Err() == nil {
		return
	}
	if !r.stopped {
		r.stopped = true
		r.stopAt = time.Now()
	}
	policy := r.consumer.config.Shutdown
	if policy.Mode == broker.ShutdownCancel {
		r.cancel(context.Cause(r.parent))
	}
	if policy.GracePeriod > 0 && time.Since(r.stopAt) >= policy.GracePeriod {
		r.cancel(ErrGracePeriodExceeded)
	}
}

func (r *consumerRun) allowCallback() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncStopLocked()
	return !r.stopped
}

func (r *consumerRun) finish(result error) error {
	close(r.done)
	<-r.watchDone
	result = errors.Join(result, r.consumer.failure("stop", context.Cause(r.parent)))
	if cause := context.Cause(r.work); cause != nil && !errors.Is(result, cause) {
		result = errors.Join(result, r.consumer.failure("stop", cause))
	}
	r.cancel(context.Canceled)
	r.record(context.WithoutCancel(r.parent), Observation{Operation: OperationRun, Outcome: outcome(result), Err: result})
	r.observationMu.Lock()
	defer r.observationMu.Unlock()
	return errors.Join(append([]error{result}, r.observationErrors...)...)
}

func (r *consumerRun) process(message *nats.Msg, maxDeliver int, required requirements, handler broker.Handler) error {
	delivery, err := detach(message, r.consumer.source())
	if err != nil {
		err = r.consumer.failure("metadata", err)
		r.record(r.work, Observation{Operation: OperationProcess, Outcome: OutcomeFailed, Err: err})
		return err
	}
	base, cancel := context.WithCancelCause(r.work)
	defer cancel(context.Canceled)
	ctx := r.begin(base, DeliveryInfo{Source: delivery.source, Metadata: delivery.metadata})
	var renew *renewal
	if required.keepAlive != 0 {
		renew = r.startRenewal(ctx, cancel, message, delivery, required.keepAlive)
	}
	r.record(ctx, Observation{Metadata: delivery.metadata, Operation: OperationProcess, Outcome: OutcomeStarted})
	started := time.Now()
	result, callbackErr := handler.Handle(ctx, delivery)
	duration := time.Since(started)
	if renew != nil {
		renew.stopScheduling()
	}
	callbackErr = r.consumer.failure("process", callbackErr)
	cause := dispositionCause(result)
	r.record(ctx, Observation{Metadata: delivery.metadata, Operation: OperationProcess, Outcome: outcome(callbackErr),
		Duration: duration, Disposition: result, Cause: cause, Err: callbackErr})
	var renewalErr error
	if renew != nil {
		<-renew.done
		renewalErr = renew.err
	}
	if processingErr := errors.Join(callbackErr, renewalErr, context.Cause(ctx)); processingErr != nil {
		r.record(ctx, Observation{Metadata: delivery.metadata, Operation: terminalName(result), Outcome: OutcomeSuppressed,
			Disposition: result, Cause: cause, Err: processingErr})
		return r.consumer.failure("delivery", processingErr)
	}
	operation, err := r.validateResult(result, delivery, maxDeliver, required)
	if err != nil {
		err = r.consumer.failure(OperationDisposition, err)
		r.record(ctx, Observation{Metadata: delivery.metadata, Operation: operation, Outcome: OutcomeSuppressed,
			Disposition: result, Cause: cause, Err: err})
		return err
	}
	// Admission is the linearization point. Forced shutdown after this lock is
	// released cannot cancel or reverse the admitted bounded operation.
	r.mu.Lock()
	r.syncStopLocked()
	err = context.Cause(ctx)
	r.mu.Unlock()
	if err != nil {
		r.record(ctx, Observation{Metadata: delivery.metadata, Operation: operation, Outcome: OutcomeSuppressed,
			Disposition: result, Cause: cause, Err: err})
		return r.consumer.failure("terminal admission", err)
	}
	return r.settle(ctx, message, delivery, result, operation)
}

func (r *consumerRun) startRenewal(
	ctx context.Context, cancel context.CancelCauseFunc, message *nats.Msg, delivery *delivery, interval time.Duration,
) *renewal {
	worker := &renewal{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-worker.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			worker.mu.Lock()
			if worker.stopping || ctx.Err() != nil {
				worker.mu.Unlock()
				return
			}
			operation, finish := context.WithTimeout(ctx, r.consumer.config.OperationTimeout)
			worker.mu.Unlock()
			r.record(ctx, Observation{Metadata: delivery.metadata, Operation: OperationRenew, Outcome: OutcomeStarted})
			started := time.Now()
			err := message.InProgress(nats.Context(operation))
			finish()
			status := OutcomeConfirmed
			forced := err == context.Canceled && ctx.Err() == context.Canceled && context.Cause(r.work) != nil
			if err != nil {
				status = OutcomeUnknown
			}
			if forced {
				status = OutcomeCanceled
				err = context.Cause(r.work)
			}
			err = r.consumer.failure("renew", err)
			r.record(ctx, Observation{Metadata: delivery.metadata, Operation: OperationRenew, Outcome: status,
				Duration: time.Since(started), Err: err})
			if forced {
				return
			}
			if err != nil {
				cancel(err)
				worker.err = err
				return
			}
		}
	}()
	return worker
}

func (w *renewal) stopScheduling() {
	w.mu.Lock()
	w.stopping = true
	close(w.stop)
	w.mu.Unlock()
}

func (r *consumerRun) validateResult(
	result broker.Disposition, delivery *delivery, maxDeliver int, required requirements,
) (string, error) {
	switch value := result.(type) {
	case broker.Handled:
		return OperationAck, nil
	case broker.RetryAfter:
		if !required.redelivery {
			return OperationNak, errors.New("RedeliveryRequirement: undeclared")
		}
		if value.Delay < 0 {
			return OperationNak, errors.New("RetryAfter.Delay: negative")
		}
		// The server stores pending timestamps in signed Unix nanoseconds. Leave
		// the finite request budget before that representation's upper boundary.
		if value.Delay > 0 && value.Delay > time.Until(time.Unix(0, math.MaxInt64))-r.consumer.config.OperationTimeout {
			return OperationNak, errors.New("RetryAfter.Delay: exceeds server timestamp range")
		}
		if maxDeliver > 0 && delivery.metadata.NumDelivered >= uint64(maxDeliver) {
			return OperationNak, errors.New("RetryAfter: Consumer.MaxDeliver exhausted")
		}
		return OperationNak, nil
	case Terminate:
		if !required.termination {
			return OperationTerm, errors.New("TerminationRequirement: undeclared")
		}
		return OperationTerm, nil
	default:
		return OperationDisposition, fmt.Errorf("unsupported disposition %T", result)
	}
}

func (r *consumerRun) settle(
	ctx context.Context, message *nats.Msg, delivery *delivery, result broker.Disposition, operation string,
) error {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.consumer.config.OperationTimeout)
	defer cancel()
	cause := dispositionCause(result)
	r.record(ctx, Observation{Metadata: delivery.metadata, Operation: operation, Outcome: OutcomeStarted, Disposition: result, Cause: cause})
	started := time.Now()
	var err error
	switch value := result.(type) {
	case broker.Handled:
		err = message.AckSync(nats.Context(bounded))
	case broker.RetryAfter:
		err = message.NakWithDelay(value.Delay, nats.Context(bounded))
	case Terminate:
		err = message.Term(nats.Context(bounded))
	}
	status := OutcomeConfirmed
	if err != nil {
		status = OutcomeUnknown
		err = r.consumer.failure(operation, errors.Join(err, cause))
	}
	r.record(ctx, Observation{Metadata: delivery.metadata, Operation: operation, Outcome: status,
		Duration: time.Since(started), Disposition: result, Cause: cause, Err: err})
	return err
}

//nolint:gocritic // Pass detached observation metadata by value to the hook.
func (r *consumerRun) begin(ctx context.Context, info DeliveryInfo) (result context.Context) {
	result = ctx
	if r.consumer.config.Observer.Begin == nil || !r.observationEnabled() {
		return result
	}
	defer r.recoverObserver("begin")
	return r.consumer.config.Observer.Begin(ctx, info)
}

//nolint:gocritic // Each concurrent observer call receives its own event value.
func (r *consumerRun) record(ctx context.Context, event Observation) {
	if r.consumer.config.Observer.Record == nil || !r.observationEnabled() {
		return
	}
	event.Source = r.consumer.source()
	defer r.recoverObserver(event.Operation)
	r.consumer.config.Observer.Record(ctx, event)
}

func (r *consumerRun) observationEnabled() bool {
	r.observationMu.Lock()
	defer r.observationMu.Unlock()
	return len(r.observationErrors) == 0
}

func (r *consumerRun) recoverObserver(operation string) {
	if value := recover(); value != nil {
		r.observationMu.Lock()
		r.observationErrors = append(r.observationErrors, r.consumer.failure("observe", &ObserverPanicError{Operation: operation, Value: value}))
		r.observationMu.Unlock()
	}
}

func dispositionCause(result broker.Disposition) error {
	switch value := result.(type) {
	case broker.RetryAfter:
		return value.Cause
	case Terminate:
		return value.Cause
	}
	return nil
}

func terminalName(result broker.Disposition) string {
	switch result.(type) {
	case broker.Handled:
		return OperationAck
	case broker.RetryAfter:
		return OperationNak
	case Terminate:
		return OperationTerm
	default:
		return OperationDisposition
	}
}
