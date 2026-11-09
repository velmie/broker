package natsjs_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/velmie/x/svc/validation"

	"github.com/velmie/broker/natsjs/v3"
)

func TestConnectionConfigValidation(t *testing.T) {
	base := natsjs.ConnectionConfig{
		URL:            "nats://127.0.0.1:4222",
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		MaxReconnects:  0,
		InboxPrefix:    "_INBOX.bookstore",
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	badPort := base
	badPort.URL = "nats://127.0.0.1:invalid"
	var numberError *strconv.NumError
	if err := badPort.Validate(); !errors.As(err, &numberError) {
		t.Fatalf("numeric port cause missing: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*natsjs.ConnectionConfig)
		field  string
	}{
		{
			name:   "url credentials",
			change: func(c *natsjs.ConnectionConfig) { c.URL = "nats://u:p@localhost:4222" },
			field:  "url",
		},
		{
			name:   "remote credentials",
			change: func(c *natsjs.ConnectionConfig) { c.URL = "nats://example.com:4222" },
			field:  "username",
		},
		{
			name: "remote TLS",
			change: func(c *natsjs.ConnectionConfig) {
				c.URL = "nats://example.com:4222"
				c.Username = "a"
				c.Password = "b"
			},
			field: "tls_mode",
		},
		{
			name:   "zero timeout",
			change: func(c *natsjs.ConnectionConfig) { c.ConnectTimeout = 0 },
			field:  "connect_timeout",
		},
		{
			name:   "invalid inbox",
			change: func(c *natsjs.ConnectionConfig) { c.InboxPrefix = "_INBOX.*" },
			field:  "inbox_prefix",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.change(&cfg)
			var failure *validation.Error
			if err := cfg.Validate(); !errors.As(err, &failure) {
				t.Fatalf("expected validation error: %v", err)
			}
			for _, violation := range failure.Violations {
				if violation.Field == tc.field {
					return
				}
			}
			t.Fatalf("missing %s: %v", tc.field, failure)
		})
	}
}

func TestOpenConnectionCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://127.0.0.1:4222",
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX",
	}
	_, err := natsjs.OpenConnection(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestOpenConnectionRealServerAndCAFailure(t *testing.T) {
	cfg := natsjs.ConnectionConfig{
		URL:            serverURL,
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX.bookstore",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	conn, err := natsjs.OpenConnection(t.Context(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	cfg.TLSMode = "verify"
	cfg.TLSCAFile = t.TempDir() + "/missing.pem"
	_, err = natsjs.OpenConnection(t.Context(), cfg, logger)
	var pathError *os.PathError
	if !errors.As(err, &pathError) || !strings.Contains(err.Error(), "tls_ca_file") {
		t.Fatalf("expected CA field and filesystem cause: %v", err)
	}
}

func TestConfiguredConnectionReconnectLimit(t *testing.T) {
	cfg := natsjs.ConnectionConfig{
		URL:            serverURL,
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  50 * time.Millisecond,
		MaxReconnects:  0,
		InboxPrefix:    "_INBOX",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	conn, err := natsjs.OpenConnection(t.Context(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	restartServer(t)
	waitUntil(t, conn.IsClosed)
}

func TestConnectionFailureRetainsTLSAndDialCauses(t *testing.T) {
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://127.0.0.1:4222",
		TLSMode:        "verify",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX",
	}
	cfg.TLSCAFile = t.TempDir() + "/invalid.pem"
	if err := os.WriteFile(cfg.TLSCAFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := natsjs.OpenConnection(t.Context(), cfg, logger); err == nil || !strings.Contains(err.Error(), "tls_ca_file") {
		t.Fatalf("missing CA field: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg.URL = "nats://" + listener.Addr().String()
	cfg.TLSMode = "disabled"
	cfg.TLSCAFile = ""
	cfg.ConnectTimeout = 50 * time.Millisecond
	started := time.Now()
	if conn, err := natsjs.OpenConnection(t.Context(), cfg, logger); err == nil {
		conn.Close()
		t.Fatal("stalled protocol accepted")
	}
	if time.Since(started) > time.Second {
		t.Fatal("dial exceeded the configured deadline")
	}
}
