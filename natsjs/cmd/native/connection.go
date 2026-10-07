package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/nats-io/nats.go"
)

// connectWithOptions owns failed connections and lends successful connections to
// the command. A shared readiness deadline covers native TCP dialing and all
// handshakes. Startup socket cancellation is joined before the connection is lent.
// Caller dialers should implement DialContext for cancellation. An arbitrary
// synchronous custom Dial (or custom SDK hostname lookup) must itself be bounded:
// user code cannot be forcibly interrupted without abandoning its execution.
func connectWithOptions(ctx context.Context, url string, options ...nats.Option) (*nats.Conn, error) {
	readyCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := context.Cause(readyCtx); err != nil {
		return nil, operationError("connection", "connect", err)
	}
	guard := &readinessDialer{ctx: readyCtx, active: true}
	stopped := make(chan struct{})
	stop := context.AfterFunc(readyCtx, func() { defer close(stopped); guard.abort() })
	finish := func() error {
		if !stop() {
			<-stopped
		}
		guard.mu.Lock()
		defer guard.mu.Unlock()
		guard.active = false
		guard.connection = nil
		return guard.closeErr
	}
	selected := append([]nats.Option(nil), options...)
	selected = append(selected, func(opts *nats.Options) error {
		guard.delegate = opts.CustomDialer
		if guard.delegate == nil {
			timeout := opts.Timeout
			if timeout == 0 {
				timeout = nats.DefaultTimeout
			}
			dialer := &net.Dialer{Timeout: timeout}
			if opts.Dialer != nil {
				copyDialer := *opts.Dialer
				dialer = &copyDialer
			}
			if opts.AllowMultipathTCP != nil {
				dialer.SetMultipathTCP(*opts.AllowMultipathTCP)
			}
			guard.delegate = dialer
			// net.Dialer resolves hostnames with the shared context instead of the
			// SDK's independent blocking net.LookupHost call before dialing.
			opts.SkipHostLookup = true
		}
		opts.CustomDialer = guard
		return nil
	})
	conn, err := nats.Connect(url, selected...)
	if err != nil {
		return nil, operationError("connection", "connect", errors.Join(err, context.Cause(readyCtx), finish()))
	}
	changes := conn.StatusChanged(nats.CONNECTED, nats.CLOSED)
	defer conn.RemoveStatusListener(changes)
	for {
		if readyErr := context.Cause(readyCtx); readyErr != nil {
			conn.Close()
			return nil, operationError("connection", "readiness", errors.Join(readyErr, conn.LastError(), finish()))
		}
		if conn.IsConnected() {
			cleanupErr := finish()
			if readyErr := context.Cause(readyCtx); readyErr != nil || cleanupErr != nil {
				conn.Close()
				return nil, operationError("connection", "readiness", errors.Join(readyErr, cleanupErr, conn.LastError()))
			}
			return conn, nil
		}
		if conn.IsClosed() {
			return nil, operationError("connection", "readiness", errors.Join(nats.ErrConnectionClosed, conn.LastError(), finish()))
		}
		select {
		case <-readyCtx.Done():
		case <-changes:
		}
	}
}

type readinessDialer struct {
	mu         sync.Mutex
	ctx        context.Context
	delegate   nats.CustomDialer
	active     bool
	connection net.Conn
	closeErr   error
}

func (d *readinessDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	active := d.active
	d.mu.Unlock()
	if !active {
		return d.delegate.Dial(network, address)
	}
	if err := context.Cause(d.ctx); err != nil {
		return nil, err
	}
	var conn net.Conn
	var err error
	if dialer, ok := d.delegate.(interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	}); ok {
		conn, err = dialer.DialContext(d.ctx, network, address)
	} else {
		conn, err = d.delegate.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if cause := context.Cause(d.ctx); d.active && cause != nil {
		d.mu.Unlock()
		return nil, errors.Join(cause, conn.Close())
	}
	if d.active {
		d.connection = conn
	}
	d.mu.Unlock()
	return conn, nil
}
func (d *readinessDialer) SkipTLSHandshake() bool {
	skipper, ok := d.delegate.(interface{ SkipTLSHandshake() bool })
	return ok && skipper.SkipTLSHandshake()
}
func (d *readinessDialer) abort() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.connection != nil {
		closeErr := d.connection.Close()
		if !errors.Is(closeErr, net.ErrClosed) {
			d.closeErr = closeErr
		}
	}
}

func runConnection(ctx context.Context, conn *nats.Conn) error {
	callCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := conn.FlushWithContext(callCtx); err != nil {
		return operationError("connection", "flush", err)
	}
	if !conn.IsConnected() {
		return operationError("connection", "state", errors.Join(nats.ErrConnectionClosed, conn.LastError()))
	}
	return nil
}
