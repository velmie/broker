package conn

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Connection represents connection to NATS server. Contains nats.Conn and nats.JetStreamContext
type Connection struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

// Establish tries to establish connection to NATS server and construct JetStreamContext. Can be configured via various of Option(s)
func Establish(opts ...Option) (*Connection, error) {
	o := &Options{urls: []string{nats.DefaultURL}}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	// connect to NATS with options specified
	nc, err := nats.Connect(strings.Join(o.urls, ","), o.natsOpts...)
	if err != nil {
		return nil, err
	}

	// if not connected and NoWaitFailedConnectRetry is not set we wait until connection is established or closed after max attempts
	// we check status with ReconnectWait backoff (the backoff NATS client uses between each establish try)
	if !o.noWait && !nc.IsConnected() {
		var s nats.Status
		for s = nc.Status(); s != nats.CONNECTED && s != nats.CLOSED; s = nc.Status() {
			<-time.After(nc.Opts.ReconnectWait)
		}

		// NATS client failed to connect and closed connection
		if s == nats.CLOSED {
			return nil, nc.LastError()
		}
	}

	js, err := nc.JetStream(o.jsOpts...)
	if err != nil {
		return nil, err
	}

	return &Connection{nc: nc, js: js}, nil
}

// Conn returns nats.Conn
func (c *Connection) Conn() *nats.Conn {
	return c.nc
}

// JetStreamContext returns nats.JetStreamContext
func (c *Connection) JetStreamContext() nats.JetStreamContext {
	return c.js
}

// Close closes connection. Refer to nats.Conn #Close
func (c *Connection) Close() {
	c.nc.Close()
}

// Drain drains connection. Refer to nats.Conn #Drain
func (c *Connection) Drain() error {
	return c.nc.Drain()
}
