package conn

import "github.com/nats-io/nats.go"

// Options represents options for NATS server Connection and nats.JetStreamContext
type Options struct {
	url      string
	natsOpts []nats.Option
	jsOpts   []nats.JSOpt
	noWait   bool
}

// Option allows to set Connection options
type Option func(options *Options)

// URL specifies NATS server URL
func URL(url string) Option {
	return func(o *Options) {
		o.url = url
	}
}

// NATSOptions specifies various of nats.Option(s) used for connection
func NATSOptions(opts ...nats.Option) Option {
	return func(o *Options) {
		o.natsOpts = append(o.natsOpts, opts...)
	}
}

// JetStreamContextOptions specifies various of nats.JSOpt(s) used for nats.JetStreamContext
func JetStreamContextOptions(opts ...nats.JSOpt) Option {
	return func(o *Options) {
		o.jsOpts = append(o.jsOpts, opts...)
	}
}

// NoWaitFailedConnectRetry specifies if await for healthy connection is required (when nats.RetryOnFailedConnect is set to true)
// If this option is set with RetryOnFailedConnect is equal to true and no server(s) available (or server(s) can't accept connections)
// unhealthy connection will be returned in RECONNECTING state and all requests in such state will be buffered on client side
func NoWaitFailedConnectRetry() Option {
	return func(o *Options) {
		o.noWait = true
	}
}
