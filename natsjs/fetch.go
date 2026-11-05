package natsjs

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	fetchRetryInterval      = 250 * time.Millisecond
	serverShutdownErrorText = "nats: Server Shutdown"
	maxWaitingErrorText     = "nats: Exceeded MaxWaiting"
)

type pullRecovery struct {
	recovering bool
	reconnects uint64
}

// resumePull verifies the consumer after an interruption before receiving again.
func (c *Consumer) resumePull(parent context.Context, run *consumerRun, js nats.JetStreamContext, state *pullRecovery) error {
	for {
		switch c.conn.Status() {
		case nats.CLOSED:
			return nats.ErrConnectionClosed
		case nats.DRAINING_SUBS, nats.DRAINING_PUBS:
			return nats.ErrConnectionDraining
		case nats.CONNECTED:
		default:
			state.recovering = true
			if err := waitForFetchRetry(parent); err != nil {
				return err
			}
			continue
		}
		current := c.conn.Stats().Reconnects
		if current != state.reconnects {
			state.recovering = true
		}
		if !state.recovering {
			return nil
		}
		err := c.probePullConsumer(parent, js)
		if err == nil {
			state.recovering = false
			state.reconnects = current
			return nil
		}
		if parent.Err() != nil || !retryableLookupError(err) {
			return err
		}
		run.record(parent, Observation{
			Operation: OperationFetch,
			Outcome:   OutcomeInterrupted,
			Err:       c.failure(OperationFetch, err),
		})
		if err := waitForFetchRetry(parent); err != nil {
			return err
		}
	}
}

// probePullConsumer looks up the bound consumer with a bounded operation time.
func (c *Consumer) probePullConsumer(parent context.Context, js nats.JetStreamContext) error {
	ctx, cancel := context.WithTimeout(parent, c.config.OperationTimeout)
	defer cancel()
	_, err := js.ConsumerInfo(c.config.Stream, c.config.Consumer, nats.Context(ctx))
	return err
}

// retryableLookupError identifies temporary consumer lookup failures.
func retryableLookupError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrJetStreamNotEnabled) || errors.Is(err, nats.ErrReconnectBufExceeded)
}

// retryableFetchError identifies interrupted pulls that can be retried.
func retryableFetchError(err error) bool {
	return errors.Is(err, nats.ErrFetchDisconnected) ||
		errors.Is(err, nats.ErrConsumerLeadershipChanged) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrReconnectBufExceeded) ||
		err.Error() == serverShutdownErrorText ||
		err.Error() == maxWaitingErrorText
}

// waitForFetchRetry delays a retry unless the caller cancels.
func waitForFetchRetry(ctx context.Context) error {
	timer := time.NewTimer(fetchRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
