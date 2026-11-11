package natsjs

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	tlsModeVerify   = "verify"
	tlsModeDisabled = "disabled"
)

// ConnectionConfig describes a single NATS endpoint using username/password and verified TLS.
type ConnectionConfig struct {
	URL            string        `k:"url" default:"nats://127.0.0.1:4222" validate:"required" sensitive:"true" hint:"NATS host and port without embedded credentials, path, query or fragment"` //nolint:lll // Keep the configuration tag readable.
	Username       string        `k:"username" sensitive:"true" hint:"Paired with password, required outside literal loopback"`
	Password       string        `k:"password" sensitive:"true" hint:"Paired with username, required outside literal loopback"`
	TLSMode        string        `k:"tls_mode" default:"verify" validate:"oneof=verify disabled"`
	TLSCAFile      string        `k:"tls_ca_file" hint:"Optional trust roots when TLS is enabled"`
	TLSServerName  string        `k:"tls_server_name" hint:"Optional verified server name when TLS is enabled"`
	ConnectTimeout time.Duration `k:"connect_timeout" default:"2s" validate:"gt=0"`
	// ReconnectWait defaults to 250ms. With three reconnects, an outage longer
	// than about one second can permanently close the connection.
	ReconnectWait time.Duration `k:"reconnect_wait" default:"250ms" validate:"gt=0"`
	// MaxReconnects defaults to three. The finite retry budget is fail-fast.
	MaxReconnects int    `k:"max_reconnects" default:"3" validate:"gte=-1"`
	InboxPrefix   string `k:"inbox_prefix" default:"_INBOX" validate:"required"`
}

// Validate checks configuration without opening resources and reports relative k paths.
//
//nolint:gocritic // The configuration is intentionally a value contract.
func (cfg ConnectionConfig) Validate() error {
	var failures configErrors
	failures.required("url", cfg.URL)
	failures.positive("connect_timeout", int64(cfg.ConnectTimeout))
	failures.positive("reconnect_wait", int64(cfg.ReconnectWait))
	if cfg.MaxReconnects < -1 {
		failures.add("max_reconnects", ruleGreaterOrEqual, "max_reconnects must be greater than or equal to -1")
	}
	failures.required("inbox_prefix", cfg.InboxPrefix)
	if cfg.InboxPrefix != "" && !validConfigSubject(cfg.InboxPrefix, false) {
		failures.add("inbox_prefix", ruleType, "inbox_prefix must be a valid NATS subject")
	}
	if cfg.TLSMode != tlsModeVerify && cfg.TLSMode != tlsModeDisabled {
		failures.add("tls_mode", ruleOneOf, "tls_mode must be one of verify or disabled")
	}
	if cfg.URL != "" {
		loopback, err := validateConnectionURL(cfg.URL)
		if err != nil {
			failures.addCause("url", ruleType, "url must be a single nats://host:port endpoint without credentials, path, query or fragment", err)
		} else {
			if !loopback || cfg.Password != "" {
				failures.required("username", cfg.Username)
			}
			if !loopback || cfg.Username != "" {
				failures.required("password", cfg.Password)
			}
			if !loopback && cfg.TLSMode == tlsModeDisabled {
				failures.add("tls_mode", ruleEqual, "tls_mode must equal verify outside literal loopback")
			}
		}
	}
	if cfg.TLSMode == tlsModeDisabled {
		if cfg.TLSCAFile != "" {
			failures.add("tls_ca_file", ruleEqual, "tls_ca_file must be empty when TLS is disabled")
		}
		if cfg.TLSServerName != "" {
			failures.add("tls_server_name", ruleEqual, "tls_server_name must be empty when TLS is disabled")
		}
	}
	return failures.err()
}

// OpenConnection opens a connection whose dialing is governed by ctx, including reconnects.
// The context must outlive the connection. Close the connection after its users join.
// A nil logger is rejected.
//
//nolint:gocritic // The configuration is intentionally a value contract.
func OpenConnection(ctx context.Context, cfg ConnectionConfig, logger *slog.Logger) (*nats.Conn, error) {
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, operationFailure("connection", "connect", err)
	}
	options := []nats.Option{
		nats.CustomInboxPrefix(cfg.InboxPrefix),
		nats.FlusherTimeout(time.Second),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.RetryOnFailedConnect(false),
		nats.IgnoreDiscoveredServers(),
		nats.SkipHostLookup(),
		nats.NoCallbacksAfterClientClose(),
		nats.SetCustomDialer(&connectionDialer{
			ctx: ctx,
			dialer: net.Dialer{
				Timeout: cfg.ConnectTimeout,
			},
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logConnectionError(logger, "disconnect", err, slog.LevelWarn)
			}
		}),
		nats.ClosedHandler(func(connection *nats.Conn) {
			if ctx.Err() == nil {
				err := connection.LastError()
				if err == nil {
					err = errors.New("connection closed")
				}
				logConnectionError(logger, "closed", err, slog.LevelError)
			}
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logConnectionError(logger, "asynchronous", err, slog.LevelError)
		}),
	}
	if cfg.Username != "" {
		options = append(options, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.TLSMode == tlsModeVerify {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: cfg.TLSServerName,
		}
		options = append(options, nats.Secure(tlsConfig))
		if cfg.TLSCAFile != "" {
			rootCAs := nats.RootCAs(cfg.TLSCAFile)
			options = append(options, func(options *nats.Options) error {
				if err := rootCAs(options); err != nil {
					return fmt.Errorf("tls_ca_file: %w", err)
				}
				return nil
			})
		}
	}
	connection, err := nats.Connect(cfg.URL, options...)
	if err != nil {
		return nil, operationFailure("connection", "connect", err)
	}
	if err := ctx.Err(); err != nil {
		connection.Close()
		return nil, operationFailure("connection", "connect", err)
	}
	return connection, nil
}

func validateConnectionURL(raw string) (bool, error) {
	// Reject credentials before parsing so that parse errors, which quote the
	// input, never contain them.
	if strings.ContainsAny(raw, "@?#,\r\n\t ") {
		return false, errors.New("expected a single NATS endpoint without credentials, query or fragment")
	}
	authority, ok := strings.CutPrefix(raw, "nats://")
	if !ok || strings.Contains(authority, "/") {
		return false, errors.New("expected nats://host:port")
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		return false, err
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return false, err
	}
	if host == "" || number < 1 || number > 65535 {
		return false, errors.New("expected host and port in range 1 to 65535")
	}
	if _, err := url.Parse(raw); err != nil {
		return false, err
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback(), nil
}

type connectionDialer struct {
	ctx    context.Context
	dialer net.Dialer
}

func (d *connectionDialer) Dial(network, address string) (net.Conn, error) {
	return d.dialer.DialContext(d.ctx, network, address)
}

func logConnectionError(logger *slog.Logger, operation string, err error, level slog.Level) {
	logger.Log(context.Background(), level, "NATS connection failed", slog.String("operation", operation), slog.Any("error", err))
}
