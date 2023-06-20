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

// ConsumerGroupNamer defines behavior for retrieval of queue/durable names
type GroupNamer interface {
	Name() string
}

// SubscriptionFactoryFunc defines behavior for subscriptions construction
type SubscriptionFactoryFunc func(subject string, namer GroupNamer) Subscriptor

// ConsumerFactoryFunc defines behavior for consumers construction
type ConsumerFactoryFunc func(subject string, namer GroupNamer) *nats.ConsumerConfig

// Options represents Subscriber options
type Options struct {
	connOpts    []conn.Option
	subFactory  SubscriptionFactoryFunc
	consFactory ConsumerFactoryFunc
	conn        *conn.Connection
	grpNamer    GroupNamer
}

// Option allows to set Subscriber options
type Option func(options *Options)

// ConnectionOptions allows to set conn.Connection options
func ConnectionOptions(opts ...conn.Option) Option {
	return func(o *Options) {
		o.connOpts = append(o.connOpts, opts...)
	}
}

// UseConnection allows to reuse existing connection for subscriber
func UseConnection(c *conn.Connection) Option {
	return func(o *Options) {
		o.conn = c
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

// ConsumerGroupNamer allows to set subscription namer. If nothing is specified DefaultSubscriptionNamer will be used.
// Your own subscription namer can be implemented via embedding of DefaultSubscriptionNamer into your struct and
// redefinition of methods
func ConsumerGroupNamer(namer GroupNamer) Option {
	return func(o *Options) {
		o.grpNamer = namer
	}
}

// DefaultSubscriptionFactory is subscription factory used if nothing is set. It constructs async queue push consumer with
// DeliveryLast, AckExplicit, ReplayInstant policies. Queue name is constructed via call to GroupName of SubNamer
func DefaultSubscriptionFactory() SubscriptionFactoryFunc {
	return func(subj string, grpNamer GroupNamer) Subscriptor {
		return AsyncQueueSubscription().
			Subject(subj).
			Queue(grpNamer.Name()).
			SubOptions(
				nats.DeliverLast(),
				nats.AckExplicit(),
				nats.ReplayInstant(),
			)
	}
}

// DefaultConsumerFactory can be used for consumer creation. It creates consumer with name and durable retrieved from
// GroupNamer method Name and DeliverLastPolicy
func DefaultConsumerFactory() ConsumerFactoryFunc {
	return func(subject string, namer GroupNamer) *nats.ConsumerConfig {
		return &nats.ConsumerConfig{
			Name:          namer.Name(),
			Durable:       namer.Name(),
			DeliverPolicy: nats.DeliverLastPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
		}
	}
}

// DefaultConsumerGroupNamer is default consumer group namer used if nothing is set.
type DefaultConsumerGroupNamer struct {
	Stream  string
	Subject string
}

func (n *DefaultConsumerGroupNamer) Name() string {
	name := fmt.Sprintf("%s-%s", n.Stream, n.Subject)
	exp := regexp.MustCompile("[*>. ]")
	return string(exp.ReplaceAll([]byte(name), []byte("-")))
}
