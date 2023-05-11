package subscriber

import (
	"fmt"
	"regexp"

	"github.com/nats-io/nats.go"
	"github.com/velmie/broker"

	"github.com/velmie/broker/natsjs/v2/conn"
)

// Subscriptor represents subscription bundler behavior
type Subscriptor interface {
	subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error)
}

// SubscriptionFactoryFunc defines behavior for subscriptions construction
type SubscriptionFactoryFunc func(stream string, subj string) Subscriptor

// ConsumerFactoryFunc defines behavior for consumers construction
type ConsumerFactoryFunc func(stream string, subj string) *nats.ConsumerConfig

// Options represents Subscriber options
type Options struct {
	connOpts    []conn.Option
	subFactory  SubscriptionFactoryFunc
	consFactory ConsumerFactoryFunc
}

// Option allows to set Subscriber options
type Option func(options *Options)

// ConnectionOptions allows to set conn.Connection options
func ConnectionOptions(opts ...conn.Option) Option {
	return func(o *Options) {
		o.connOpts = append(o.connOpts, opts...)
	}
}

// SubscriptionFactory allows to set subscription factory which is used to initialize subscriptions.
// If nothing is set DefaultSubscriptionFactory is used.
func SubscriptionFactory(sf SubscriptionFactoryFunc) Option {
	return func(o *Options) {
		if sf != nil {
			o.subFactory = sf
		}
	}
}

// ConsumerFactory allows to set consumer factory which is used to initialize consumers.
// There is no default and if not set no consumers will be initialized explicitly
func ConsumerFactory(cf ConsumerFactoryFunc) Option {
	return func(o *Options) {
		o.consFactory = cf
	}
}

// DefaultSubscriptionFactory is subscription factory used if nothing is set. It constructs pull consumer with
// DeliveryLast, AckExplicit, ReplayInstant policies. Durable name is constructed as "{stream}-{subject}" normalized -
// all '*','>','.' and space symbols are replaced with '-'
func DefaultSubscriptionFactory() SubscriptionFactoryFunc {
	return func(stream string, subj string) Subscriptor {
		drb := normalizedDurable(fmt.Sprintf("%s-%s", stream, subj))

		return PullSubscription().
			Subject(subj).
			Durable(drb).
			SubOptions(
				nats.DeliverLast(),
				nats.AckExplicit(),
				nats.ReplayInstant(),
			)
	}
}

func normalizedDurable(drb string) string {
	exp := regexp.MustCompile("[*>. ]")
	return string(exp.ReplaceAll([]byte(drb), []byte("-")))
}
