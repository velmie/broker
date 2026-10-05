// Package sqs adapts a borrowed AWS SDK v2 client to destination-bound broker
// publication and sequential consumption. The caller owns SDK configuration.
package sqs

import (
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/velmie/broker"
)

const (
	defaultOperationTimeout = 30 * time.Second
	defaultVisibility       = 30 * time.Second
	maxPollSeconds          = 20
	maxVisibility           = 12 * time.Hour
	maxBatchSize            = 10
	maxSourceLength         = 128
)

// PublisherConfig binds one queue. Source is a safe diagnostic name (not a URL).
// FIFO requires a group and uses each message's logical ID for deduplication.
// Standard queues also support MessageGroupID. Zero OperationTimeout is 30s.
type PublisherConfig struct {
	QueueURL         string
	Source           string
	FIFO             bool
	MessageGroupID   string
	OperationTimeout time.Duration
	Observer         *Observer
}

// ConsumerConfig binds one queue. Zero BatchSize means one. Poll wait is an
// integer 0..20 seconds. Zero is omitted by the SDK and uses the queue default.
// ReceiveTimeout must exceed the effective maximum poll wait (20s when omitted).
// Its default is that wait plus 30s. VisibilityTimeout defaults to 30s and must
// be positive whole seconds up to 12h. Prefetched messages have no renewal.
// Each callback runs sequentially. OperationTimeout bounds delete, retry and renewal (default 30s).
type ConsumerConfig struct {
	// ResilientReceive retries transient receive failures after 1s, 2s and 4s.
	// A successful receive resets the sequence. Other operations never use it.
	ResilientReceive  bool
	QueueURL          string
	Source            string
	BatchSize         int
	WaitTimeSeconds   int
	VisibilityTimeout time.Duration
	ReceiveTimeout    time.Duration
	OperationTimeout  time.Duration
	Shutdown          broker.ShutdownPolicy
	Observer          *Observer
}

func validateBinding(queueURL, source string) error {
	u, err := url.Parse(queueURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("QueueURL: expected HTTP(S) queue URL without credentials, query or fragment")
	}
	if source == "" || len(source) > maxSourceLength || strings.IndexFunc(source, func(r rune) bool {
		allowed := (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-/", r))
		return !allowed
	}) >= 0 {
		return errors.New("Source: expected 1..128 safe ASCII label characters")
	}
	return nil
}

// normalizeConsumer snapshots user configuration before applying defaults.
//
//nolint:gocritic // Configuration copying is intentional at construction, not a hot path.
func normalizeConsumer(c ConsumerConfig) (ConsumerConfig, error) {
	if err := validateBinding(c.QueueURL, c.Source); err != nil {
		return c, err
	}
	if c.BatchSize == 0 {
		c.BatchSize = 1
	}
	if c.BatchSize < 1 || c.BatchSize > maxBatchSize {
		return c, errors.New("BatchSize: expected 1..10")
	}
	if c.WaitTimeSeconds < 0 || c.WaitTimeSeconds > maxPollSeconds {
		return c, errors.New("WaitTimeSeconds: expected 0..20")
	}
	wait := time.Duration(c.WaitTimeSeconds) * time.Second
	if wait == 0 {
		wait = maxPollSeconds * time.Second
	}
	if c.ReceiveTimeout == 0 {
		c.ReceiveTimeout = wait + defaultOperationTimeout
	}
	if c.ReceiveTimeout <= wait {
		return c, errors.New("ReceiveTimeout: must exceed maximum poll wait")
	}
	if c.VisibilityTimeout == 0 {
		c.VisibilityTimeout = defaultVisibility
	}
	if c.VisibilityTimeout < time.Second || c.VisibilityTimeout > maxVisibility || c.VisibilityTimeout%time.Second != 0 {
		return c, errors.New("VisibilityTimeout: expected whole seconds in 1..43200")
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = defaultOperationTimeout
	}
	if c.OperationTimeout < 0 {
		return c, errors.New("OperationTimeout: must be positive")
	}
	if err := validateShutdown(c.Shutdown); err != nil {
		return c, err
	}
	return c, nil
}
func validateShutdown(s broker.ShutdownPolicy) error {
	if s.Mode != broker.ShutdownGraceful && s.Mode != broker.ShutdownCancel ||
		s.GracePeriod < 0 || s.Mode == broker.ShutdownCancel && s.GracePeriod != 0 {
		return errors.New("shutdown: invalid policy")
	}
	return nil
}
