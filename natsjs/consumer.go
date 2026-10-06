package natsjs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

const (
	defaultFetchTimeout     = time.Second
	defaultOperationTimeout = 2 * time.Second
)

// Consumer binds an existing durable, explicit-ack JetStream pull consumer.
// Each Run processes callbacks sequentially and owns its local subscription.
// The connection is borrowed and must remain open until all its users join.
type Consumer struct {
	conn   *nats.Conn
	config ConsumerConfig
}

// ConsumerConfig describes the supported pull profile. Stream, Consumer and
// Subject are required. Provisioning and changes to the remote configuration
// belong to the application and must not race with Run.
type ConsumerConfig struct {
	Stream   string
	Consumer string
	Subject  string
	// Domain and APIPrefix select the JetStream management/fetch namespace.
	// Both empty use the default; at most one may be nonempty. APIPrefix accepts
	// one optional trailing dot, matching the native SDK.
	Domain    string
	APIPrefix string
	// BatchSize bounds each fetched batch. Zero selects one. Waiting messages
	// have no keepalive and are left pending when shutdown stops admission.
	BatchSize int
	// FetchTimeout and OperationTimeout must be positive, or zero for defaults
	// of one and two seconds. Operations also depend on the borrowed connection's
	// finite FlusherTimeout. These are cooperative bounds, not a hard Run deadline.
	FetchTimeout     time.Duration
	OperationTimeout time.Duration
	Shutdown         broker.ShutdownPolicy
	Observer         Observer
}

// NewConsumer validates local configuration without acquiring receive resources.
// The required connection must have a positive FlusherTimeout.
//
//nolint:gocritic // Copy configuration to keep ownership independent of the caller.
func NewConsumer(conn *nats.Conn, config ConsumerConfig) (*Consumer, error) {
	if config.BatchSize == 0 {
		config.BatchSize = 1
	}
	if config.FetchTimeout == 0 {
		config.FetchTimeout = defaultFetchTimeout
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	c := &Consumer{conn: conn, config: config}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks only local configuration and the complete capability set.
// Equal built-in declarations are idempotent. Pointers to built-in values are
// not supported concrete forms, even when their method sets implement markers.
func (c *Consumer) Validate(requirements ...broker.Requirement) error {
	if err := c.validateConfig(); err != nil {
		return c.failure("validate", err)
	}
	_, err := parseRequirements(requirements)
	return c.failure("validate", err)
}

// Run joins callbacks, renewal, shutdown observation and subscription cleanup.
// Cancellation requests the configured policy and is retained in the result.
func (c *Consumer) Run(parent context.Context, handler broker.Handler) (result error) {
	if err := c.Validate(handler.Requirements()...); err != nil {
		return err
	}
	if err := context.Cause(parent); err != nil {
		return c.failure("stop", err)
	}
	requirements, _ := parseRequirements(handler.Requirements()) // Validated above.
	run := newConsumerRun(c, parent)
	defer func() { result = run.finish(result) }()
	subscription, remote, err := c.subscribe(parent, requirements)
	if err != nil {
		err = c.failure("startup", err)
		run.record(parent, Observation{Operation: OperationStartup, Outcome: OutcomeFailed, Err: err})
		return err
	}
	defer func() {
		err := c.failure("cleanup", subscription.Unsubscribe())
		run.record(run.work, Observation{Operation: OperationCleanup, Outcome: outcome(err), Err: err})
		result = errors.Join(result, err)
	}()
	run.record(parent, Observation{Operation: OperationStartup, Outcome: OutcomeSucceeded})
	for run.allowCallback() {
		ctx, cancel := context.WithTimeout(parent, c.config.FetchTimeout)
		messages, fetchErr := subscription.Fetch(c.config.BatchSize, nats.Context(ctx))
		cancel()
		if fetchErr != nil {
			if parent.Err() != nil {
				break
			}
			if errors.Is(fetchErr, context.DeadlineExceeded) || errors.Is(fetchErr, nats.ErrTimeout) {
				run.record(parent, Observation{Operation: OperationFetch, Outcome: OutcomeIdle})
				continue
			}
			fetchErr = c.failure("fetch", fetchErr)
			run.record(parent, Observation{Operation: OperationFetch, Outcome: OutcomeFailed, Err: fetchErr})
			return fetchErr
		}
		run.record(parent, Observation{Operation: OperationFetch, Outcome: OutcomeSucceeded})
		for _, message := range messages {
			if !run.allowCallback() {
				break
			}
			if err := run.process(message, remote.MaxDeliver, requirements, handler); err != nil {
				return err
			}
		}
	}
	return nil
}

type requirements struct {
	redelivery  bool
	termination bool
	keepAlive   time.Duration
}

func (c *Consumer) source() string { return c.config.Stream + "/" + c.config.Consumer }

func (c *Consumer) failure(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &OperationError{Source: c.source(), Operation: operation, Cause: err}
}

func (c *Consumer) validateConfig() error {
	if err := c.validateNamespace(); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{{"Stream", c.config.Stream}, {"Consumer", c.config.Consumer}} {
		if field.value == "" || strings.ContainsAny(field.value, ".*>/\\ \t\r\n") {
			return fieldFailure(field.name, "invalid_name", fmt.Errorf("%s: invalid name", field.name))
		}
	}
	if !validSubject(c.config.Subject, true) {
		return fieldFailure("Subject", "invalid_filter", errors.New("consumer Subject: invalid filter"))
	}
	if c.config.BatchSize < 1 {
		return fieldFailure("BatchSize", "must_be_positive", errors.New("BatchSize: must be positive"))
	}
	if c.config.FetchTimeout <= 0 {
		return fieldFailure("FetchTimeout", "must_be_positive", errors.New("FetchTimeout: must be positive"))
	}
	if c.config.OperationTimeout <= 0 {
		return fieldFailure("OperationTimeout", "must_be_positive", errors.New("OperationTimeout: must be positive"))
	}
	if c.conn.Opts.FlusherTimeout <= 0 {
		return fieldFailure("FlusherTimeout", "must_be_positive", errors.New("connection FlusherTimeout: must be positive"))
	}
	policy := c.config.Shutdown
	if policy.Mode != broker.ShutdownGraceful && policy.Mode != broker.ShutdownCancel {
		return fieldFailure("Shutdown.Mode", "unsupported", errors.New("Shutdown.Mode: unsupported"))
	}
	if policy.GracePeriod < 0 {
		return fieldFailure("Shutdown.GracePeriod", "must_not_be_negative", errors.New("Shutdown.GracePeriod: must not be negative"))
	}
	if policy.Mode == broker.ShutdownCancel && policy.GracePeriod != 0 {
		return fieldFailure("Shutdown.GracePeriod", "invalid_in_cancel_mode", errors.New("Shutdown.GracePeriod: invalid in cancel mode"))
	}
	return nil
}

func parseRequirements(values []broker.Requirement) (requirements, error) {
	var result requirements
	for _, value := range values {
		switch value := value.(type) {
		case broker.RedeliveryRequirement:
			result.redelivery = true
		case broker.AttemptsRequirement:
		case TerminationRequirement:
			result.termination = true
		case broker.KeepAliveRequirement:
			if value.Interval <= 0 {
				return result, fieldFailure("KeepAliveRequirement.Interval", "must_be_positive",
					fmt.Errorf("KeepAliveRequirement.Interval: must be positive, got %s", value.Interval),
				)
			}
			if result.keepAlive != 0 && result.keepAlive != value.Interval {
				return result, fieldFailure("KeepAliveRequirement.Interval", "conflicting_intervals",
					fmt.Errorf("KeepAliveRequirement.Interval: conflicting %s and %s", result.keepAlive, value.Interval),
				)
			}
			result.keepAlive = value.Interval
		default:
			return result, fieldFailure("Requirements", "unsupported_requirement", fmt.Errorf("unsupported requirement %T", value))
		}
	}
	return result, nil
}

func (c *Consumer) subscribe(parent context.Context, required requirements) (*nats.Subscription, nats.ConsumerConfig, error) {
	ctx, cancel := context.WithTimeout(parent, c.config.OperationTimeout)
	defer cancel()
	options := []nats.JSOpt{nats.MaxWait(c.config.OperationTimeout)}
	if c.config.Domain != "" {
		options = append(options, nats.Domain(c.config.Domain))
	}
	if c.config.APIPrefix != "" {
		options = append(options, nats.APIPrefix(c.config.APIPrefix))
	}
	js, err := c.conn.JetStream(options...)
	if err != nil {
		return nil, nats.ConsumerConfig{}, err
	}
	info, err := js.ConsumerInfo(c.config.Stream, c.config.Consumer, nats.Context(ctx))
	if err != nil {
		return nil, nats.ConsumerConfig{}, err
	}
	remote := info.Config
	if remote.Durable != c.config.Consumer || remote.DeliverSubject != "" || remote.AckPolicy != nats.AckExplicitPolicy {
		return nil, remote, fieldFailure("Consumer", "incompatible_consumer",
			errors.New("Consumer: requires an existing durable explicit-ack pull consumer"),
		)
	}
	if remote.FilterSubject != c.config.Subject || len(remote.FilterSubjects) != 0 {
		return nil, remote, fieldFailure("Subject", "filter_mismatch",
			errors.New("consumer Subject: must equal the consumer's single FilterSubject"),
		)
	}
	if len(remote.BackOff) != 0 {
		return nil, remote, fieldFailure("Consumer.BackOff", "unsupported", errors.New("Consumer.BackOff: unsupported in this profile"))
	}
	if remote.MaxRequestBatch > 0 && c.config.BatchSize > remote.MaxRequestBatch {
		return nil, remote, fieldFailure("BatchSize", "exceeds_max_request_batch", errors.New("BatchSize: exceeds Consumer.MaxRequestBatch"))
	}
	if remote.MaxRequestExpires > 0 && c.config.FetchTimeout > remote.MaxRequestExpires {
		return nil, remote, fieldFailure("FetchTimeout", "exceeds_max_request_expires",
			errors.New("FetchTimeout: exceeds Consumer.MaxRequestExpires"),
		)
	}
	if required.keepAlive != 0 && required.keepAlive >= remote.AckWait {
		return nil, remote, fieldFailure("KeepAliveRequirement.Interval", "exceeds_ack_wait",
			errors.New("KeepAliveRequirement.Interval: must be shorter than Consumer.AckWait"),
		)
	}
	// ConsumerInfo performed the remote check. Avoid a subscription-lifetime
	// context and its SDK goroutine. This binding neither creates nor deletes
	// the durable, and every Fetch supplies its own finite context.
	subscription, err := js.PullSubscribe(c.config.Subject, c.config.Consumer,
		nats.Bind(c.config.Stream, c.config.Consumer), nats.SkipConsumerLookup(), nats.ManualAck(), nats.AckExplicit())
	return subscription, remote, err
}

func validSubject(subject string, filter bool) bool {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n\x00") {
		return false
	}
	tokens := strings.Split(subject, ".")
	for i, token := range tokens {
		if token == "" {
			return false
		}
		if filter && (token == "*" || token == ">" && i == len(tokens)-1) {
			continue
		}
		if strings.ContainsAny(token, "*>") {
			return false
		}
	}
	return true
}

func (c *Consumer) validateNamespace() error {
	if c.config.Domain != "" && c.config.APIPrefix != "" {
		return fieldFailure("Domain/APIPrefix", "conflicting_namespaces",
			errors.New("consumer Domain and APIPrefix: select at most one namespace"),
		)
	}
	if domain := c.config.Domain; domain != "" {
		if strings.ContainsAny(domain, ".*>") || !validNamespaceSubject(domain) {
			return fieldFailure("Domain", "invalid_namespace", errors.New("consumer Domain: invalid namespace name"))
		}
	}
	if prefix := c.config.APIPrefix; prefix != "" {
		if !validNamespaceSubject(strings.TrimSuffix(prefix, ".")) {
			return fieldFailure("APIPrefix", "invalid_namespace", errors.New("consumer APIPrefix: invalid namespace subject"))
		}
	}
	return nil
}
func validNamespaceSubject(subject string) bool {
	if !utf8.ValidString(subject) || strings.IndexFunc(subject, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	return validSubject(subject, false)
}
