package natsjs_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/velmie/broker/natsjs/v4"
)

func TestOpenConnectionCancelsDNSLookup(t *testing.T) {
	original := net.DefaultResolver
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, errors.New("resolver released")
			}
		},
	}
	t.Cleanup(func() {
		close(release)
		<-done
		net.DefaultResolver = original
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://slow.invalid:4222",
		Username:       "user",
		Password:       "password",
		TLSMode:        "verify",
		ConnectTimeout: 10 * time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX",
	}
	result := make(chan error, 1)
	go func() {
		defer close(done)
		conn, err := natsjs.OpenConnection(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("DNS lookup did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected connection failure")
		}
	case <-time.After(time.Second):
		t.Fatal("OpenConnection did not return after cancellation")
	}
}
