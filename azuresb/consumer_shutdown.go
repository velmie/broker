package azuresb

import (
	"context"
	"sync"
	"time"

	"github.com/velmie/broker"
)

type processingState struct {
	mu     sync.Mutex
	forced bool
	cancel context.CancelCauseFunc
}

func (s *processingState) force(cause error) { s.forced = true; s.cancel(cause) }
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
