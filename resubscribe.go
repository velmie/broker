package broker

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	defaultDelayBetweenSubscriptionAttempt = 10 * time.Second
	defaultMaxSubscriptionAttempts         = -1 // unlimited
)

type ResubscribeOptions struct {
	Logger                          Logger
	MaxSubscriptionAttempts         int
	DelayBetweenSubscriptionAttempt time.Duration
}

type ResubscribeOption func(options *ResubscribeOptions)

// ResubscribeErrorHandler unsubscribes from the given DefaultSubscription once error is occurred
// and then subscribes again
func ResubscribeErrorHandler(
	subscriber Subscriber,
	options ...ResubscribeOption,
) ErrorHandler {
	opts := &ResubscribeOptions{
		MaxSubscriptionAttempts:         defaultMaxSubscriptionAttempts,
		DelayBetweenSubscriptionAttempt: defaultDelayBetweenSubscriptionAttempt,
	}
	for _, option := range options {
		option(opts)
	}
	log := opts.Logger
	return func(_ error, sub Subscription) {
		if log != nil {
			log.Debug("broker.ResubscribeErrorHandler: resubscribing now")
		}
		if err := sub.Unsubscribe(); err != nil && log != nil {
			msg := fmt.Sprintf(
				"broker.ResubscribeErrorHandler failed: unable to unsubscribe from the topic %q: %s",
				sub.Topic(),
				err,
			)
			log.Error(msg)
		} else if err == nil {
			const subscriptionAttemptDelay = time.Second * 10
			<-sub.Done()
			for {
				_, err = subscriber.Subscribe(sub.Topic(), sub.Handler(), sub.InitOptions()...)
				if err != nil && log != nil {
					msg := fmt.Sprintf(
						"broker.ResubscribeErrorHandler failed: unable to Subscribe to the topic %q: %s",
						sub.Topic(),
						err,
					)
					log.Error(msg)
				}
				if err == nil || errors.Cause(err) == AlreadySubscribed {
					break
				}
				time.Sleep(subscriptionAttemptDelay)
			}
		}
	}
}

func ResubscribeWithLogger(logger Logger) ResubscribeOption {
	return func(options *ResubscribeOptions) {
		options.Logger = logger
	}
}

func ResubscribeWithMaxSubscriptionAttempts(maxAttempts int) ResubscribeOption {
	return func(options *ResubscribeOptions) {
		options.MaxSubscriptionAttempts = maxAttempts
	}
}

func ResubscribeWithDelayBetweenSubscriptionAttempts(delay time.Duration) ResubscribeOption {
	return func(options *ResubscribeOptions) {
		options.DelayBetweenSubscriptionAttempt = delay
	}
}
