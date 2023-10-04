package broker

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source subscriber.go -destination ./mock/subscriber.go

// Subscription represents a DefaultSubscription to a specific topic
type Subscription interface {
	Topic() string
	InitOptions() []SubscribeOption
	Options() *SubscribeOptions
	Handler() Handler
	Unsubscribe() error
	Done() <-chan struct{}
}

// Subscriber allows subscribing to a specific topic
type Subscriber interface {
	Subscribe(topic string, handler Handler, options ...SubscribeOption) (Subscription, error)
}

// SubscribeOptions represents options which could be applied to a DefaultSubscription
type SubscribeOptions struct {
	// AutoAck defaults to true. When a handler returns
	// with a nil error the message is acked.
	AutoAck bool
	// ErrorHandler processes DefaultSubscription errors
	ErrorHandler ErrorHandler
	// Logger logs important events
	Logger Logger
}

// DefaultSubscribeOptions creates options with default values
func DefaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		AutoAck: true,
	}
}

// SubscribeOption provides a way to interact with the DefaultSubscription options
type SubscribeOption func(*SubscribeOptions)

// DisableAutoAck sets the option which disables auto ack functionality
func DisableAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = false
	}
}

// WithErrorHandler sets the option which provides error handler
func WithErrorHandler(handler ErrorHandler) SubscribeOption {
	return func(options *SubscribeOptions) {
		options.ErrorHandler = handler
	}
}

// WithLogger sets the logger
func WithLogger(log Logger) SubscribeOption {
	return func(options *SubscribeOptions) {
		options.Logger = log
	}
}

type DefaultSubscription struct {
	topic       string
	options     *SubscribeOptions
	initOptions []SubscribeOption
	handler     Handler
	done        chan struct{}
}

func NewDefaultSubscription(
	topic string,
	options *SubscribeOptions,
	initOptions []SubscribeOption,
	handler Handler,
) *DefaultSubscription {
	done := make(chan struct{})
	return &DefaultSubscription{topic, options, initOptions, handler, done}
}

func (s *DefaultSubscription) InitOptions() []SubscribeOption {
	return s.initOptions
}

func (s *DefaultSubscription) Options() *SubscribeOptions {
	return s.options
}

func (s *DefaultSubscription) Handler() Handler {
	return s.handler
}

func (s *DefaultSubscription) Topic() string {
	return s.topic
}

func (s *DefaultSubscription) Unsubscribe() error {
	select {
	case <-s.done:
		return nil
	default:
		close(s.done)
		return nil
	}
}

func (s *DefaultSubscription) Done() <-chan struct{} {
	return s.done
}
