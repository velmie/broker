package publisher

import (
	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v2/conn"
)

// Options represents Publisher options
type Options struct {
	connOpts []conn.Option
	jsCfg    *nats.StreamConfig
	pubOpts  []nats.PubOpt
}

// Option allows to set Publisher options
type Option func(options *Options)

// ConnectionOptions allows to set conn.Connection options
func ConnectionOptions(opts ...conn.Option) Option {
	return func(o *Options) {
		o.connOpts = append(o.connOpts, opts...)
	}
}

// PubOptions allows to set various of nats.PubOpt
func PubOptions(opts ...nats.PubOpt) Option {
	return func(o *Options) {
		o.pubOpts = append(o.pubOpts, opts...)
	}
}

// InitJetStream allows to initialize JetStream after connection is established. Note, if stream with the same name
// is already present no action will be taken
func InitJetStream(cfg *nats.StreamConfig) Option {
	return func(o *Options) {
		o.jsCfg = cfg
	}
}
