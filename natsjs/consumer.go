package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

const (
	defaultFetchTimeout     = time.Second
	defaultOperationTimeout = 2 * time.Second
)

// Consumer binds an existing explicit-ack JetStream consumer.
// Each Run processes callbacks sequentially and owns its local subscription.
// The connection is borrowed and must remain open until all its users join.
type Consumer struct {
	conn           *nats.Conn
	config         ConsumerConfig
	allowEphemeral bool
	push           bool
}

// ConsumerConfig describes the supported explicit-ack profile. Stream, Consumer and
// Subjects are required. Provisioning and changes to the remote configuration
// belong to the application and must not race with Run.
type ConsumerConfig struct {
	Stream   string
	Consumer string
	// Subjects is a nonempty set of unique NATS filters. Order is ignored.
	// Constructors copy the slice.
	Subjects []string
	// Domain and APIPrefix select the JetStream management/fetch namespace.
	// Both empty use the default; at most one may be nonempty. APIPrefix accepts
	// one optional trailing dot, matching the native SDK.
	Domain    string
	APIPrefix string
	// BatchSize bounds each fetched pull batch. Zero selects one; push requires
	// one. Waiting messages
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

// NewConsumer binds an existing durable pull consumer. It validates local
// configuration without acquiring receive resources.
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
	config.Subjects = slices.Clone(config.Subjects)
	c := &Consumer{conn: conn, config: config}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewPullConsumer binds an existing named pull consumer, durable or ephemeral.
// The remote consumer must use explicit acknowledgments and the supported
// ConsumerConfig profile. Validation and defaults match NewConsumer.
// Run owns its local subscription and leaves remote topology and the borrowed
// connection to the application. An ephemeral consumer's configured inactivity
// threshold still governs its server-side lifetime after Run stops.
//
//nolint:gocritic // Copy configuration to keep ownership independent of the caller.
func NewPullConsumer(conn *nats.Conn, config ConsumerConfig) (*Consumer, error) {
	c, err := NewConsumer(conn, config)
	if err != nil {
		return nil, err
	}
	c.allowEphemeral = true
	return c, nil
}

// NewPushConsumer binds an existing named push consumer with explicit
// acknowledgments. Run uses the native DeliverGroup when present and processes
// messages sequentially through an owned synchronous subscription. The native
// consumer and connection remain application-owned. Configuration, requirements
// and shutdown follow NewPullConsumer; BatchSize must be one (or zero for default).
//
//nolint:gocritic // Keep configuration ownership independent of the caller.
func NewPushConsumer(conn *nats.Conn, config ConsumerConfig) (*Consumer, error) {
	c, err := NewPullConsumer(conn, config)
	if err != nil {
		return nil, err
	}
	c.push = true
	if err = c.Validate(); err != nil {
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
	return c.run(parent, handler, false)
}

// RunService joins the same work as Run, but a parent stop alone is successful.
// Independent failures and an expired shutdown grace period remain errors. A
// callback returning exactly its delivery context's error after adapter-initiated
// cancellation cooperates with shutdown. Wrapped or joined errors remain failures.
func (c *Consumer) RunService(parent context.Context, handler broker.Handler) error {
	return c.run(parent, handler, true)
}

func (c *Consumer) run(parent context.Context, handler broker.Handler, service bool) (result error) {
	if err := c.Validate(handler.Requirements()...); err != nil {
		return err
	}
	if err := context.Cause(parent); err != nil {
		if service {
			return newConsumerRun(c, parent, true).finish(nil)
		}
		return c.failure("stop", err)
	}
	requirements, _ := parseRequirements(handler.Requirements()) // Validated above.
	run := newConsumerRun(c, parent, service)
	defer func() { result = run.finish(result) }()
	// Capture before startup lookup so a reconnect during binding is checked.
	pull := pullRecovery{
		reconnects: c.conn.Stats().Reconnects,
	}
	js, err := c.jetStream()
	var subscription *nats.Subscription
	var remote nats.ConsumerConfig
	var startupStopped bool
	if err == nil {
		subscription, remote, startupStopped, err = c.subscribe(parent, js, requirements)
	}
	if err != nil {
		if service && startupStopped {
			return nil
		}
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
	return c.consume(parent, run, js, subscription, remote.MaxDeliver, requirements, handler, &pull)
}

func (c *Consumer) consume(
	parent context.Context, run *consumerRun, js nats.JetStreamContext, subscription *nats.Subscription,
	maxDeliver int, required requirements, handler broker.Handler, pull *pullRecovery,
) error {
	for run.allowCallback() {
		if !c.push {
			if err, stopped := c.resumePull(parent, run, js, pull); err != nil {
				if (!run.service && parent.Err() != nil) || (run.service && stopped) {
					break
				}
				return c.fetchFailure(parent, run, err)
			}
		}
		fetchDeadline := errors.New("fetch deadline")
		ctx, cancel := context.WithTimeoutCause(parent, c.config.FetchTimeout, fetchDeadline)
		messages, fetchErr := c.receive(ctx, subscription)
		fetchStopped := stoppedOperation(parent, ctx, fetchErr, fetchDeadline)
		cancel()
		if fetchErr != nil {
			stop, err := c.handleFetchError(parent, run, fetchErr, fetchStopped, pull)
			if err != nil {
				return err
			}
			if stop {
				break
			}
			continue
		}
		run.record(
			parent,
			Observation{
				Operation: OperationFetch,
				Outcome:   OutcomeSucceeded,
			},
		)
		for _, message := range messages {
			if !run.allowCallback() {
				break
			}
			if err := run.process(message, maxDeliver, required, handler); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consumer) handleFetchError(
	parent context.Context, run *consumerRun, err error, stopped bool, pull *pullRecovery,
) (bool, error) {
	if (!run.service && parent.Err() != nil) || (run.service && stopped) {
		return true, nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		if run.service && parent.Err() != nil {
			return true, nil
		}
		run.record(
			parent,
			Observation{
				Operation: OperationFetch,
				Outcome:   OutcomeIdle,
			},
		)
		return false, nil
	}
	if run.service && parent.Err() != nil {
		return false, c.fetchFailure(parent, run, err)
	}
	if !c.push {
		if stopErr := c.pullConnectionError(); stopErr != nil {
			return false, c.fetchFailure(parent, run, errors.Join(err, stopErr))
		}
		if retryableFetchError(err) {
			run.record(
				parent,
				Observation{
					Operation: OperationFetch,
					Outcome:   OutcomeInterrupted,
					Err:       c.failure(OperationFetch, err),
				},
			)
			pull.recovering = true
			return waitForFetchRetry(parent) != nil, nil
		}
	}
	return false, c.fetchFailure(parent, run, err)
}

type requirements struct {
	redelivery  bool
	termination bool
	rejection   bool
	keepAlive   time.Duration
}

func (c *Consumer) source() string { return c.config.Stream + "/" + c.config.Consumer }

func (c *Consumer) failure(operation string, err error) error {
	return operationFailure(c.source(), operation, err)
}

// pullConnectionError reports terminal connection states during a pull.
func (c *Consumer) pullConnectionError() error {
	switch c.conn.Status() {
	case nats.CLOSED:
		return nats.ErrConnectionClosed
	case nats.DRAINING_SUBS, nats.DRAINING_PUBS:
		return nats.ErrConnectionDraining
	default:
		return nil
	}
}

// fetchFailure records a terminal fetch failure and preserves its cause.
func (c *Consumer) fetchFailure(parent context.Context, run *consumerRun, err error) error {
	err = c.failure(OperationFetch, err)
	run.record(parent, Observation{
		Operation: OperationFetch,
		Outcome:   OutcomeFailed,
		Err:       err,
	})
	return err
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
	var failures configErrors
	validateFilterSubjects(&failures, c.config.Subjects)
	if err := failures.err(); err != nil {
		return err
	}
	if c.config.BatchSize < 1 {
		return fieldFailure("BatchSize", "must_be_positive", errors.New("BatchSize: must be positive"))
	}
	if c.push && c.config.BatchSize != 1 {
		return fieldFailure("BatchSize", "unsupported_push_batch", errors.New("BatchSize: push requires one"))
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
		case broker.RejectRequirement:
			result.rejection = true
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

func (c *Consumer) jetStream() (nats.JetStreamContext, error) {
	options := []nats.JSOpt{nats.MaxWait(c.config.OperationTimeout)}
	if c.config.Domain != "" {
		options = append(options, nats.Domain(c.config.Domain))
	}
	if c.config.APIPrefix != "" {
		options = append(options, nats.APIPrefix(c.config.APIPrefix))
	}
	return c.conn.JetStream(options...)
}

func (c *Consumer) subscribe(
	parent context.Context,
	js nats.JetStreamContext,
	required requirements,
) (*nats.Subscription, nats.ConsumerConfig, bool, error) {
	deadline := errors.New("startup deadline")
	ctx, cancel := context.WithTimeoutCause(parent, c.config.OperationTimeout, deadline)
	defer cancel()
	info, err := js.ConsumerInfo(c.config.Stream, c.config.Consumer, nats.Context(ctx))
	if err != nil {
		return nil, nats.ConsumerConfig{}, stoppedOperation(parent, ctx, err, deadline), err
	}
	remote := info.Config
	if validationErr := c.validateConsumer(info); validationErr != nil {
		return nil, remote, false, validationErr
	}
	if !equalFilterSubjects(c.config.Subjects, remote.FilterSubject, remote.FilterSubjects) {
		return nil, remote, false, fieldFailure("Subjects", "filter_mismatch",
			fmt.Errorf("consumer filters: have %v, want %v", nativeFilterSubjects(remote.FilterSubject, remote.FilterSubjects), c.config.Subjects),
		)
	}
	if len(remote.BackOff) != 0 {
		return nil, remote, false, fieldFailure("Consumer.BackOff", "unsupported", errors.New("Consumer.BackOff: unsupported in this profile"))
	}
	if remote.MaxRequestBatch > 0 && c.config.BatchSize > remote.MaxRequestBatch {
		return nil, remote, false, fieldFailure(
			"BatchSize", "exceeds_max_request_batch", errors.New("BatchSize: exceeds Consumer.MaxRequestBatch"),
		)
	}
	if remote.MaxRequestExpires > 0 && c.config.FetchTimeout > remote.MaxRequestExpires {
		return nil, remote, false, fieldFailure("FetchTimeout", "exceeds_max_request_expires",
			errors.New("FetchTimeout: exceeds Consumer.MaxRequestExpires"),
		)
	}
	if required.keepAlive != 0 && required.keepAlive >= remote.AckWait {
		return nil, remote, false, fieldFailure("KeepAliveRequirement.Interval", "exceeds_ack_wait",
			errors.New("KeepAliveRequirement.Interval: must be shorter than Consumer.AckWait"),
		)
	}
	if c.push {
		subscription, config, subscribeErr := c.subscribePush(js, &remote)
		return subscription, config, false, subscribeErr
	}
	// ConsumerInfo performed the remote check. Avoid a subscription-lifetime
	// context and its SDK goroutine. This binding neither creates nor deletes
	// remote consumer, and every Fetch supplies its own finite context. Passing
	// a durable name for an ephemeral would change the requested native profile.
	subscription, err := js.PullSubscribe(c.subscriptionSubject(), remote.Durable,
		nats.Bind(c.config.Stream, c.config.Consumer), nats.SkipConsumerLookup(), nats.ManualAck(), nats.AckExplicit())
	return subscription, remote, false, err
}

// stoppedOperation recognizes the SDK's direct context result while retaining
// an independently expired operation budget, even if the parent also stops.
func stoppedOperation(parent, operation context.Context, err, deadline error) bool {
	return err != nil && parent.Err() != nil && err == operation.Err() && context.Cause(operation) != deadline
}

func (c *Consumer) validateConsumer(info *nats.ConsumerInfo) error {
	remote := info.Config
	if c.push && remote.DeliverGroup == "" && info.PushBound {
		return fieldFailure("Consumer.PushBound", "already_bound", errors.New("non-queue consumer already has a subscription"))
	}
	identityMatches := remote.Durable == c.config.Consumer ||
		(c.allowEphemeral && remote.Durable == "" && info.Name == c.config.Consumer)
	if identityMatches && (remote.DeliverSubject != "") == c.push && remote.AckPolicy == nats.AckExplicitPolicy {
		return nil
	}
	profile := "durable explicit-ack pull consumer"
	if c.allowEphemeral {
		profile = "named explicit-ack pull consumer"
	}
	if c.push {
		profile = "named explicit-ack push consumer"
	}
	return fieldFailure("Consumer", "incompatible_consumer",
		fmt.Errorf("Consumer: requires an existing %s", profile),
	)
}

func (c *Consumer) subscribePush(js nats.JetStreamContext, remote *nats.ConsumerConfig) (
	*nats.Subscription, nats.ConsumerConfig, error,
) {
	// The bounded ConsumerInfo call above supplied this snapshot. Reproduce its
	// receive controls while skipping the SDK's second, context-free lookup.
	options := []nats.SubOpt{nats.Bind(c.config.Stream, c.config.Consumer), nats.SkipConsumerLookup(),
		nats.ManualAck(), nats.AckExplicit(), nats.DeliverSubject(remote.DeliverSubject), nats.MaxAckPending(remote.MaxAckPending)}
	if remote.FlowControl {
		options = append(options, nats.EnableFlowControl())
	}
	if remote.Heartbeat > 0 {
		options = append(options, nats.IdleHeartbeat(remote.Heartbeat))
	}
	var subscription *nats.Subscription
	var err error
	if remote.DeliverGroup != "" {
		subscription, err = js.QueueSubscribeSync(c.subscriptionSubject(), remote.DeliverGroup, options...)
	} else {
		subscription, err = js.SubscribeSync(c.subscriptionSubject(), options...)
	}
	return subscription, *remote, err
}

func (c *Consumer) subscriptionSubject() string {
	if len(c.config.Subjects) == 1 {
		return c.config.Subjects[0]
	}
	return ""
}

func (c *Consumer) receive(ctx context.Context, subscription *nats.Subscription) ([]*nats.Msg, error) {
	if !c.push {
		return subscription.Fetch(c.config.BatchSize, nats.Context(ctx))
	}
	message, err := subscription.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return []*nats.Msg{message}, nil
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
		if strings.ContainsAny(domain, ".*>") || !validConfigSubject(domain, false) {
			return fieldFailure("Domain", "invalid_namespace", errors.New("consumer Domain: invalid namespace name"))
		}
	}
	if prefix := c.config.APIPrefix; prefix != "" {
		if !validConfigSubject(strings.TrimSuffix(prefix, "."), false) {
			return fieldFailure("APIPrefix", "invalid_namespace", errors.New("consumer APIPrefix: invalid namespace subject"))
		}
	}
	return nil
}
