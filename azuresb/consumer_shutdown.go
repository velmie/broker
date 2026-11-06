package azuresb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/velmie/broker"
)

// ErrGracePeriodExceeded reports escalation after the configured grace period.
var ErrGracePeriodExceeded = errors.New("shutdown grace period expired")

type shutdownCause struct {
	cause error
}

func (e *shutdownCause) Error() string { return "consumer shutdown: " + e.cause.Error() }
func (e *shutdownCause) Unwrap() error { return e.cause }

type processingState struct {
	mu           sync.Mutex
	service      bool
	shutdown     *shutdownCause
	graceExpired bool
	forced       bool
	cancel       context.CancelCauseFunc
}

func (s *processingState) force(cause error) { s.forced = true; s.cancel(cause) }
func (s *processingState) forceStop(cause error, grace bool) {
	if s.service {
		if s.shutdown != nil {
			return
		}
		s.shutdown = &shutdownCause{
			cause: cause,
		}
		s.graceExpired = grace
		s.force(s.shutdown)
		return
	}
	s.force(cause)
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
	cause := context.Cause(ctx)
	grace := policy.Mode == broker.ShutdownGraceful
	if grace && s.service {
		cause = ErrGracePeriodExceeded
	}
	s.forceStop(cause, grace)
	s.mu.Unlock()
}
