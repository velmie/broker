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

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/velmie/x/svc/apperror"
	"github.com/velmie/x/svc/validation"
)

// ConnectionConfig describes a single NATS endpoint using username/password and verified TLS.
type ConnectionConfig struct {
	URL            string        `k:"url" default:"nats://127.0.0.1:4222" validate:"required" sensitive:"true" hint:"NATS host and port without embedded credentials, path, query or fragment"` //nolint:lll // Keep the configuration tag readable.
	Username       string        `k:"username" sensitive:"true" hint:"Paired with password, required outside literal loopback"`
	Password       string        `k:"password" sensitive:"true" hint:"Paired with username, required outside literal loopback"`
	TLSMode        string        `k:"tls_mode" default:"verify" validate:"oneof=verify disabled"`
	TLSCAFile      string        `k:"tls_ca_file" sensitive:"true" hint:"Optional trust roots when TLS is enabled"`
	TLSServerName  string        `k:"tls_server_name" sensitive:"true" hint:"Optional verified server name when TLS is enabled"`
	ConnectTimeout time.Duration `k:"connect_timeout" default:"2s" validate:"gt=0"`
	ReconnectWait  time.Duration `k:"reconnect_wait" default:"250ms" validate:"gt=0"`
	MaxReconnects  int           `k:"max_reconnects" default:"3" validate:"gte=-1"`
	InboxPrefix    string        `k:"inbox_prefix" default:"_INBOX" validate:"required"`
}

var configValidator = newConfigValidator()

// Validate checks configuration without opening resources and reports relative k paths.
//
//nolint:gocritic // The configuration is intentionally a value contract.
func (cfg ConnectionConfig) Validate() error {
	var failures validation.Error
	failures.AddStruct(cfg, configValidator.Struct(cfg))
	loopback, err := validateConnectionURL(cfg.URL)
	if err != nil {
		failures.Add("url", apperror.RuleType, "NATS URL", err)
		return failures.Err()
	}
	if (cfg.Username == "") != (cfg.Password == "") || (!loopback && cfg.Username == "") ||
		strings.ContainsAny(cfg.Username, "\r\n") || strings.ContainsAny(cfg.Password, "\r\n") {
		failures.Add("username", apperror.RuleType, "broker credentials", nil)
		failures.Add("password", apperror.RuleType, "broker credentials", nil)
	}
	if cfg.TLSMode == "disabled" {
		if !loopback {
			failures.Add("tls_mode", apperror.RuleType, "broker TLS mode", nil)
		}
		if cfg.TLSCAFile != "" {
			failures.Add("tls_ca_file", apperror.RuleEQ, "", nil)
		}
		if cfg.TLSServerName != "" {
			failures.Add("tls_server_name", apperror.RuleEQ, "", nil)
		}
	}
	if cfg.InboxPrefix != "" && !validNamespaceSubject(cfg.InboxPrefix) {
		failures.Add("inbox_prefix", apperror.RuleType, "NATS subject", nil)
	}
	return failures.Err()
}

// OpenConnection opens a connection whose dialing is governed by ctx, including reconnects.
// The context must outlive the connection. Close the connection after its users join.
//
//nolint:gocritic // The configuration is intentionally a value contract.
func OpenConnection(ctx context.Context, cfg ConnectionConfig, logger *slog.Logger) (*nats.Conn, error) {
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
	if cfg.TLSMode == "verify" {
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

func newConfigValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled(), validator.WithTagNameFuncBlankOmit())
	v.RegisterTagNameFunc(validation.FieldNames("k"))
	return v
}

func validateConnectionURL(raw string) (bool, error) {
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
	endpoint, err := url.Parse(raw)
	if err != nil {
		return false, err
	}
	if endpoint.Scheme != "nats" || endpoint.User != nil || endpoint.Opaque != "" || endpoint.Path != "" || endpoint.ForceQuery {
		return false, errors.New("expected nats://host:port")
	}
	if host == "" || number < 1 || number > 65535 {
		return false, fmt.Errorf("expected host and port in range 1 to 65535")
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
