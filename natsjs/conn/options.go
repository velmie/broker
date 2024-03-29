package conn

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Options represents options for NATS server Connection and nats.JetStreamContext
type Options struct {
	urls     []string
	natsOpts []nats.Option
	jsOpts   []nats.JSOpt
	noWait   bool
}

// Option allows to set Connection options
type Option func(options *Options)

// URL specifies NATS server URL
func URL(url string) Option {
	return func(o *Options) {
		o.urls = []string{
			url,
		}
	}
}

func URLs(urls []string) Option {
	return func(o *Options) {
		o.urls = urls
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

func DefaultConnection(url string) (*Connection, error) {
	return Establish(
		URL(url),
		NATSOptions(
			nats.MaxReconnects(-1),
			nats.ReconnectWait(5*time.Second),
			nats.Timeout(2*time.Second),
			nats.DrainTimeout(20*time.Second),
			nats.RetryOnFailedConnect(false),
		),
	)
}

func DefaultConnectionWithLogger(urls []string, logger Logger) (*Connection, error) {
	return Establish(
		URLs(urls),
		NATSOptions(
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
			nats.Timeout(2*time.Second),
			nats.DrainTimeout(20*time.Second),
			nats.RetryOnFailedConnect(false),
			nats.ConnectHandler(func(conn *nats.Conn) {
				logger.Info(fmt.Sprintf(
					"NATS connection established: server %s, address %s",
					conn.ConnectedUrl(),
					conn.ConnectedAddr(),
				))
			}),
			nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
				// `conn.ConnectedUrl()` and `conn.ConnectedAddr()` are empty when a disconnect occurred,
				// therefore we cannot use them to log.
				if err != nil {
					logger.Warningf("NATS connection lost; err: %s", err.Error())
					return
				}
				logger.Warning("NATS connection lost; error is empty")
			}),
			nats.ReconnectHandler(func(conn *nats.Conn) {
				logger.Info(fmt.Sprintf(
					"NATS connection established again: server %s, address %s",
					conn.ConnectedUrl(),
					conn.ConnectedAddr(),
				))
			}),
		),
	)
}
