package azuresb

import (
	"context"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

// renewalRun owns sequential native message mutation until finish joins it.
// Stopping scheduling does not cancel an admitted operation. Processing shutdown
// owns cancellation and each native operation has its own finite budget.
type renewalRun struct {
	mu         sync.Mutex
	stopped    bool
	stop, done chan struct{}
	err        error
}

func (c *Consumer) startRenewal(ctx context.Context, receiver Receiver, native *azservicebus.ReceivedMessage,
	requirements []broker.Requirement, state *processingState, observer *observation) *renewalRun {
	var interval time.Duration
	for _, requirement := range requirements {
		if keepalive, ok := requirement.(broker.KeepAliveRequirement); ok {
			interval = keepalive.Interval
		}
	}
	run := &renewalRun{}
	if interval == 0 {
		return run
	}
	run.stop, run.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(run.done)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-run.stop:
				return
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			run.mu.Lock()
			admitted := !run.stopped && ctx.Err() == nil
			run.mu.Unlock()
			if !admitted {
				return
			}
			start := time.Now()
			operation, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
			err := receiver.RenewMessageLock(operation, native, nil)
			cancel()
			if err != nil {
				if ctx.Err() != nil && onlyCause(err, context.Canceled) {
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
	}()
	return run
}
func (r *renewalRun) finish() error {
	if r.done == nil {
		return nil
	}
	r.mu.Lock()
	r.stopped = true
	close(r.stop)
	r.mu.Unlock()
	<-r.done
	return r.err
}
