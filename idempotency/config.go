package idempotency

import (
	"fmt"
	"strings"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/idempo"
)

const (
	// DefaultHeaderName is the header key used as a fallback idempotency key source
	// when broker.Message.ID is empty.
	DefaultHeaderName = "Idempotency-Key"

	defaultKeyLength     = 255
	defaultCommitTimeout = 5 * time.Second
)

// CommitErrorMode defines middleware behavior when committing the idempotency record fails.
type CommitErrorMode int

const (
	// CommitFailOpen ignores commit errors and lets the handler succeed.
	// Idempotency degrades for the message key when commit fails.
	CommitFailOpen CommitErrorMode = iota
	// CommitFailClosedUnlock returns the commit error and unlocks the key, allowing a retry.
	CommitFailClosedUnlock
	// CommitFailClosedKeepLock returns the commit error and keeps the lock until it expires.
	CommitFailClosedKeepLock
)

// Config defines idempotency middleware behavior.
//
// Provide the backing engine via Middleware(engine, ...); use functional Options via Middleware(...)
// instead of constructing Config directly.
type Config struct {
	// HeaderName is used as a fallback when broker.Message.ID is empty.
	HeaderName string
	// KeyPrefix is prepended to the computed store key.
	KeyPrefix string
	// UseTopicInKey scopes the key by topic (storeKey = KeyPrefix + topic + ":" + key).
	UseTopicInKey bool

	// RequireKey makes missing keys an error (ErrMissingKey). When false and a key is missing,
	// the middleware becomes a no-op and passes the event through.
	RequireKey bool
	// KeyValidator validates the resolved key (Message.ID or HeaderName). Returning an error
	// prevents processing and is propagated to the caller.
	KeyValidator func(string) error

	// FingerprintFunc allows overriding how a message fingerprint is computed.
	// Fingerprints protect against accidental key reuse with different payloads.
	FingerprintFunc func(broker.Event) (idempo.Fingerprint, error)
	// FingerprintHeaders is a list of broker.Message.Header keys to include in the default
	// fingerprint calculation. Only used when FingerprintFunc is nil.
	FingerprintHeaders []string

	// CommitTimeout bounds calls used to persist/release idempotency records.
	CommitTimeout time.Duration
	// CommitErrorMode defines how commit failures affect handler result.
	CommitErrorMode CommitErrorMode
	// CommitErrorHandler is called when storing the completion marker fails; it should be used for logging/metrics only.
	CommitErrorHandler func(broker.Event, error)

	// OnReplay is called when a stored record is replayed (i.e. the handler is skipped).
	OnReplay func(broker.Event, *idempo.Response)
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

// WithUseTopicInKey toggles whether the event topic is included in the store key.
func WithUseTopicInKey(enabled bool) Option {
	return func(c *Config) {
		c.UseTopicInKey = enabled
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
func WithFingerprintFunc(fn func(broker.Event) (idempo.Fingerprint, error)) Option {
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
func WithCommitErrorHandler(handler func(broker.Event, error)) Option {
	return func(c *Config) {
		c.CommitErrorHandler = handler
	}
}

// WithOnReplay sets a callback invoked when an event is treated as a replay.
func WithOnReplay(onReplay func(broker.Event, *idempo.Response)) Option {
	return func(c *Config) {
		c.OnReplay = onReplay
	}
}

// NewConfig applies options and fills defaults.
//
// Middleware calls NewConfig internally; it is exposed for tests and advanced configuration
// (for example, to inspect computed defaults).
func NewConfig(opts ...Option) Config {
	c := Config{
		HeaderName:      DefaultHeaderName,
		UseTopicInKey:   true,
		CommitTimeout:   defaultCommitTimeout,
		CommitErrorMode: CommitFailOpen,
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.KeyValidator == nil {
		c.KeyValidator = defaultKeyValidator
	}
	c.HeaderName = strings.TrimSpace(c.HeaderName)
	for i := range c.FingerprintHeaders {
		c.FingerprintHeaders[i] = strings.TrimSpace(c.FingerprintHeaders[i])
	}
	return c
}

func defaultKeyValidator(k string) error {
	k = strings.TrimSpace(k)
	if k == "" {
		return idempo.ErrMissingKey
	}
	if len(k) > defaultKeyLength {
		return fmt.Errorf("%w: too long (max %d chars)", idempo.ErrInvalidKey, defaultKeyLength)
	}

	return nil
}
