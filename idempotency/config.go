package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/velmie/idempo"

	"github.com/velmie/broker"
)

const (
	// DefaultHeaderName is the header key used as a fallback idempotency key source
	// when broker.Message.ID is empty.
	DefaultHeaderName = "Idempotency-Key"

	defaultKeyLength     = 255
	defaultCommitTimeout = 5 * time.Second
)

const (
	// CommitFailOpen reports commit errors to the required hook and requests Handled.
	// Idempotency degrades for the message key when commit fails.
	CommitFailOpen CommitErrorMode = iota
	// CommitFailClosedUnlock returns the commit error and unlocks the key, allowing a retry.
	CommitFailClosedUnlock
	// CommitFailClosedKeepLock returns the commit error and keeps the lock until it expires.
	CommitFailClosedKeepLock
)

// CommitErrorMode defines middleware behavior when committing the idempotency record fails.
type CommitErrorMode int

// Config defines idempotency middleware behavior.
//
// Provide the backing engine via Middleware(engine, ...); use functional Options via Middleware(...)
// instead of constructing Config directly.
type Config struct {
	// HeaderName is used as a fallback when broker.Message.ID is empty.
	HeaderName string
	// KeyPrefix is prepended to the computed store key.
	KeyPrefix string
	// UseSourceInKey scopes the key by source (storeKey = KeyPrefix + source + ":" + key).
	UseSourceInKey bool

	// RequireKey makes missing keys an error (ErrMissingKey). When false and a key is missing,
	// the middleware becomes a no-op and passes the delivery through.
	RequireKey bool
	// KeyValidator validates the resolved key (Message.ID or HeaderName). Returning an error
	// prevents processing and is propagated to the caller.
	KeyValidator func(string) error

	// FingerprintFunc allows overriding how a message fingerprint is computed.
	// Fingerprints protect against accidental key reuse with different payloads.
	FingerprintFunc func(context.Context, broker.Delivery) (idempo.Fingerprint, error)
	// FingerprintHeaders is a list of broker.Message.Headers names to include in the default
	// fingerprint calculation. Only used when FingerprintFunc is nil.
	FingerprintHeaders []string

	// CommitTimeout bounds calls used to persist/release idempotency records.
	CommitTimeout time.Duration
	// CommitErrorMode defines how commit failures affect handler result.
	CommitErrorMode CommitErrorMode
	// CommitErrorHandler receives the original wrapped failure and is required for fail-open.
	// Use it for diagnostics only. A panic preserves the marker or lease and propagates
	// with the commit cause. Redact application-specific values before logging.
	CommitErrorHandler func(context.Context, broker.Delivery, error)

	// OnReplay receives an independently owned response when a stored record is replayed.
	// A panic propagates without changing the marker or invoking business processing.
	OnReplay func(context.Context, broker.Delivery, *idempo.Response)
}

// Option configures the middleware.
type Option func(*Config)

// WithHeaderName sets the header key used as a fallback idempotency key source when Message.ID is empty.
func WithHeaderName(name string) Option {
	return func(c *Config) {
		c.HeaderName = name
	}
}

// WithKeyPrefix prepends prefix to the computed store key (useful for shared stores).
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) {
		c.KeyPrefix = prefix
	}
}

// WithUseSourceInKey toggles whether the delivery source is included in the store key.
func WithUseSourceInKey(enabled bool) Option {
	return func(c *Config) {
		c.UseSourceInKey = enabled
	}
}

// WithRequireKey toggles whether a missing key is treated as an error.
func WithRequireKey(required bool) Option {
	return func(c *Config) {
		c.RequireKey = required
	}
}

// WithKeyValidator sets a custom key validator.
func WithKeyValidator(validator func(string) error) Option {
	return func(c *Config) {
		c.KeyValidator = validator
	}
}

// WithFingerprintFunc sets a custom fingerprint builder.
func WithFingerprintFunc(fn func(context.Context, broker.Delivery) (idempo.Fingerprint, error)) Option {
	return func(c *Config) {
		c.FingerprintFunc = fn
	}
}

// WithFingerprintHeaders sets header keys to include in the default fingerprint calculation.
func WithFingerprintHeaders(headers ...string) Option {
	return func(c *Config) {
		c.FingerprintHeaders = append([]string(nil), headers...)
	}
}

// WithCommitTimeout sets the timeout applied to Commit and Unlock calls.
func WithCommitTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.CommitTimeout = timeout
	}
}

// WithCommitErrorMode sets how commit failures affect handler outcome.
func WithCommitErrorMode(mode CommitErrorMode) Option {
	return func(c *Config) {
		c.CommitErrorMode = mode
	}
}

// WithCommitErrorHandler sets a callback invoked when Engine.Commit fails.
func WithCommitErrorHandler(handler func(context.Context, broker.Delivery, error)) Option {
	return func(c *Config) {
		c.CommitErrorHandler = handler
	}
}

// WithOnReplay sets a callback invoked when an delivery is treated as a replay.
func WithOnReplay(onReplay func(context.Context, broker.Delivery, *idempo.Response)) Option {
	return func(c *Config) {
		c.OnReplay = onReplay
	}
}

// NewConfig applies options and fills defaults.
//
// Middleware calls NewConfig internally and then validates configuration.
// NewConfig itself returns the configured values without validating them.
func NewConfig(opts ...Option) Config {
	c := Config{
		HeaderName:      DefaultHeaderName,
		UseSourceInKey:  true,
		CommitTimeout:   defaultCommitTimeout,
		CommitErrorMode: CommitFailClosedKeepLock,
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.KeyValidator == nil {
		c.KeyValidator = defaultKeyValidator
	}
	c.HeaderName = strings.TrimSpace(c.HeaderName)
	c.FingerprintHeaders = normalizeHeaders(c.FingerprintHeaders)
	return c
}

func normalizeHeaders(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, h := range in {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func defaultKeyValidator(k string) error {
	k = strings.TrimSpace(k)
	if k == "" {
		return idempo.ErrMissingKey
	}
	if len(k) > defaultKeyLength {
		return fmt.Errorf("%w: too long (max %d bytes)", idempo.ErrInvalidKey, defaultKeyLength)
	}

	return nil
}

func validateConfig(cfg *Config) error {
	if cfg.CommitTimeout <= 0 {
		return failure("configure", "CommitTimeout", errors.New("must be positive"))
	}
	switch cfg.CommitErrorMode {
	case CommitFailOpen:
		if cfg.CommitErrorHandler == nil {
			return failure("configure", "CommitErrorHandler", errors.New("required for fail-open"))
		}
	case CommitFailClosedUnlock, CommitFailClosedKeepLock:
	default:
		return failure("configure", "CommitErrorMode", errors.New("unknown mode"))
	}
	if !utf8.ValidString(cfg.HeaderName) {
		return failure("configure", "HeaderName", errors.New("must be UTF-8"))
	}
	for i, name := range cfg.FingerprintHeaders {
		if !utf8.ValidString(name) {
			return failure("configure", "FingerprintHeaders", errors.New("names must be UTF-8"))
		}
		for _, other := range cfg.FingerprintHeaders[:i] {
			if strings.EqualFold(name, other) {
				return failure("configure", "FingerprintHeaders", errors.New("ambiguous names"))
			}
		}
	}
	return nil
}
