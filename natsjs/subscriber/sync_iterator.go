package subscriber

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

// SyncIterator is used to iterate over sync NATS subscriptions
type SyncIterator struct {
	sub *subscription
}

// NewSyncIterator builds new SyncIterator from sync NATS subscription
func NewSyncIterator(sub broker.Subscription) (*SyncIterator, error) {
	ns, ok := sub.(*subscription)
	if !ok || ns.nsub.Type() != nats.SyncSubscription {
		return nil, errors.New("provided subscription must be NATS sync subscription")
	}
	return &SyncIterator{sub: ns}, nil
}

// Next retrieves message within provided timeout
func (i *SyncIterator) Next(timeout time.Duration) error {
	m, err := i.sub.nsub.NextMsg(timeout)
	if err != nil {
		return err
	}
	i.sub.msgHandler(m)
	return nil
}

// NextCtx retrieves message with context
func (i *SyncIterator) NextCtx(ctx context.Context) error {
	m, err := i.sub.nsub.NextMsgWithContext(ctx)
	if err != nil {
		return err
	}
	i.sub.msgHandler(m)
	return nil
}
