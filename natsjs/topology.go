package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// LimitsStreamConfig selects an owned limits-retention stream. Name and Subjects
// must match an existing stream exactly (subject order is ignored).
// DuplicateWindow is a minimum for an existing stream.
type LimitsStreamConfig struct {
	Name            string        `k:"name" validate:"required"`
	Subjects        []string      `k:"subjects" validate:"required,min=1"`
	DuplicateWindow time.Duration `k:"duplicate_window" validate:"gt=0"`
}

// WorkQueueStreamConfig selects an owned work queue. MaxMessages and MaxBytes
// are creation settings and do not constrain compatible existing streams. Name
// and Subject must match exactly. DuplicateWindow and MaxMessageBytes are
// minimums for an existing stream. An unlimited message size satisfies the minimum.
type WorkQueueStreamConfig struct {
	Name            string        `k:"name" validate:"required"`
	Subject         string        `k:"subject" validate:"required"`
	MaxMessageBytes int32         `k:"max_message_bytes" validate:"gt=0"`
	DuplicateWindow time.Duration `k:"duplicate_window" validate:"gt=0"`
	MaxMessages     int64         `k:"max_messages" validate:"gt=0"`
	MaxBytes        int64         `k:"max_bytes" validate:"gt=0"`
}

// RetainedStreamConfig selects an externally owned retained stream. Use it with
// RequireRetainedStream and EnsureDurableConsumer, which verifies filter coverage.
// Stream must match exactly. MaxMessageBytes is a minimum, also satisfied by an
// unlimited message size.
type RetainedStreamConfig struct {
	Stream          string `k:"stream" validate:"required"`
	MaxMessageBytes int32  `k:"max_message_bytes" validate:"gt=0"`
}

// DurableConsumerConfig selects a durable pull consumer. MaxAckPending is a
// creation setting and does not constrain compatible existing consumers. Stream
// identifies the stream; Durable and Subjects must match exactly, ignoring filter
// order. AckWait is a minimum. MaxDeliveries must match exactly, including -1
// for unlimited deliveries.
type DurableConsumerConfig struct {
	Stream  string `k:"stream" validate:"required"`
	Durable string `k:"durable" validate:"required"`
	// Subjects is a nonempty set of unique NATS filters. Order is ignored.
	Subjects      []string      `k:"subjects" validate:"required,min=1,unique,dive,required" hint:"Unique filters; order is ignored"`
	AckWait       time.Duration `k:"ack_wait" validate:"gt=0"`
	MaxDeliveries int           `k:"max_deliveries" validate:"eq=-1|gt=0"`
	MaxAckPending int           `k:"max_ack_pending" hint:"Maximum unacknowledged messages when creating the consumer" validate:"gt=0"`
	// StartTime selects the earliest message time for a newly created consumer.
	// Zero starts from the beginning unless DeliverNew is set. Existing consumers
	// retain their position.
	StartTime time.Time `k:"-"`
	// DeliverNew starts after consumer creation. It cannot be combined with
	// StartTime and has no effect on an existing consumer.
	DeliverNew bool `k:"-"`
}

// Validate checks the profile without I/O.
func (cfg LimitsStreamConfig) Validate() error {
	var failures configErrors
	validateTopologyName(&failures, "name", cfg.Name)
	if len(cfg.Subjects) == 0 {
		failures.add("subjects", ruleRequired, "subjects must contain at least one subject")
	}
	for _, subject := range cfg.Subjects {
		if !validConfigSubject(subject, true) {
			failures.add("subjects", ruleType, "subjects must contain valid NATS subject filters")
			break
		}
	}
	failures.positive("duplicate_window", int64(cfg.DuplicateWindow))
	return failures.err()
}

// Validate checks the profile without I/O.
func (cfg WorkQueueStreamConfig) Validate() error {
	var failures configErrors
	validateTopologyName(&failures, "name", cfg.Name)
	validateTopologySubject(&failures, cfg.Subject)
	failures.positive("max_message_bytes", int64(cfg.MaxMessageBytes))
	failures.positive("duplicate_window", int64(cfg.DuplicateWindow))
	failures.positive("max_messages", cfg.MaxMessages)
	failures.positive("max_bytes", cfg.MaxBytes)
	return failures.err()
}

// Validate checks the profile without I/O.
func (cfg RetainedStreamConfig) Validate() error {
	var failures configErrors
	validateTopologyName(&failures, "stream", cfg.Stream)
	failures.positive("max_message_bytes", int64(cfg.MaxMessageBytes))
	return failures.err()
}

// Validate checks the profile without I/O.
//
//nolint:gocritic // The configuration is intentionally a value contract.
func (cfg DurableConsumerConfig) Validate() error {
	var failures configErrors
	validateTopologyName(&failures, "stream", cfg.Stream)
	validateTopologyName(&failures, "durable", cfg.Durable)
	validateFilterSubjects(&failures, cfg.Subjects)
	failures.positive("ack_wait", int64(cfg.AckWait))
	if cfg.MaxDeliveries != -1 && cfg.MaxDeliveries <= 0 {
		failures.add("max_deliveries", ruleOneOf, "max_deliveries must equal -1 or be greater than 0")
	}
	failures.positive("max_ack_pending", int64(cfg.MaxAckPending))
	if cfg.DeliverNew && !cfg.StartTime.IsZero() {
		failures.add("deliver_new", ruleOneOf, "deliver_new and start_time cannot both be set")
	}
	return failures.err()
}

// EnsureLimitsStream creates a missing stream or verifies an existing one without updating it.
func EnsureLimitsStream(ctx context.Context, js jetstream.JetStream, cfg LimitsStreamConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	want := jetstream.StreamConfig{
		Name:       cfg.Name,
		Subjects:   cfg.Subjects,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: cfg.DuplicateWindow,
	}
	return ensureStream(ctx, js, &want, func(actual *jetstream.StreamConfig) error {
		if actual.Name != cfg.Name {
			return mismatch("name", actual.Name, cfg.Name)
		}
		a, b := slices.Clone(actual.Subjects), slices.Clone(cfg.Subjects)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) {
			return mismatch("subjects", actual.Subjects, cfg.Subjects)
		}
		if actual.Storage != jetstream.FileStorage {
			return mismatch("storage", actual.Storage, jetstream.FileStorage)
		}
		if actual.Retention != jetstream.LimitsPolicy {
			return mismatch("retention", actual.Retention, jetstream.LimitsPolicy)
		}
		if actual.Duplicates < cfg.DuplicateWindow {
			return mismatch("duplicate_window", actual.Duplicates, cfg.DuplicateWindow)
		}
		if actual.NoAck {
			return mismatch("no_ack", actual.NoAck, false)
		}
		if actual.Sealed {
			return mismatch("sealed", actual.Sealed, false)
		}
		if actual.Mirror != nil {
			return mismatch("mirror", actual.Mirror != nil, false)
		}
		if len(actual.Sources) != 0 {
			return mismatch("sources", len(actual.Sources), 0)
		}
		if actual.SubjectTransform != nil {
			return mismatch("subject_transform", actual.SubjectTransform != nil, false)
		}
		return nil
	})
}

// EnsureWorkQueueStream creates a missing stream or verifies an existing one without updating it.
func EnsureWorkQueueStream(ctx context.Context, js jetstream.JetStream, cfg WorkQueueStreamConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	want := jetstream.StreamConfig{
		Name:       cfg.Name,
		Subjects:   []string{cfg.Subject},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardNew,
		MaxMsgs:    cfg.MaxMessages,
		MaxBytes:   cfg.MaxBytes,
		MaxMsgSize: cfg.MaxMessageBytes,
		Duplicates: cfg.DuplicateWindow,
	}
	return ensureStream(ctx, js, &want, func(actual *jetstream.StreamConfig) error {
		if actual.Name != cfg.Name {
			return mismatch("name", actual.Name, cfg.Name)
		}
		if len(actual.Subjects) != 1 || actual.Subjects[0] != cfg.Subject {
			return mismatch("subject", actual.Subjects, []string{cfg.Subject})
		}
		if actual.Retention != jetstream.WorkQueuePolicy {
			return mismatch("retention", actual.Retention, jetstream.WorkQueuePolicy)
		}
		if actual.Duplicates < cfg.DuplicateWindow {
			return mismatch("duplicate_window", actual.Duplicates, cfg.DuplicateWindow)
		}
		return retentionMismatch(actual, cfg.MaxMessageBytes)
	})
}

// RequireRetainedStream verifies an external stream and never creates it.
// Pair it with EnsureDurableConsumer, which verifies consumer filter coverage.
func RequireRetainedStream(ctx context.Context, js jetstream.JetStream, cfg RetainedStreamConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	stream, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		return operationFailure(cfg.Stream, "stream_lookup", err)
	}
	actual := stream.CachedInfo().Config
	if actual.Name != cfg.Stream {
		err = mismatch("name", actual.Name, cfg.Stream)
	}
	if err == nil && actual.Retention != jetstream.LimitsPolicy {
		err = mismatch("retention", actual.Retention, jetstream.LimitsPolicy)
	}
	if err == nil {
		err = retentionMismatch(&actual, cfg.MaxMessageBytes)
	}
	return operationFailure(cfg.Stream, "stream_verify", err)
}

// EnsureDurableConsumer creates a missing durable or verifies it without updating it.
// StartTime and DeliverNew apply only on creation. An existing durable resumes from its
// acknowledgement floor, regardless of its original starting position.
//
//nolint:gocritic // Preserve the value configuration contract.
func EnsureDurableConsumer(ctx context.Context, js jetstream.JetStream, cfg DurableConsumerConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	source := cfg.Stream + "/" + cfg.Durable
	stream, err := js.Stream(ctx, cfg.Stream)
	if err != nil {
		return operationFailure(cfg.Stream, "stream_lookup", err)
	}
	for _, subject := range cfg.Subjects {
		covered := false
		for _, pattern := range stream.CachedInfo().Config.Subjects {
			if matchesSubject(pattern, subject) {
				covered = true
				break
			}
		}
		if !covered {
			return operationFailure(cfg.Stream, "stream_verify", mismatch("subjects", stream.CachedInfo().Config.Subjects, subject))
		}
	}
	consumer, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	var createErr error
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		createConfig := jetstream.ConsumerConfig{
			Name:          cfg.Durable,
			Durable:       cfg.Durable,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       cfg.AckWait,
			MaxDeliver:    cfg.MaxDeliveries,
			MaxAckPending: cfg.MaxAckPending,
		}
		if len(cfg.Subjects) == 1 {
			createConfig.FilterSubject = cfg.Subjects[0]
		} else {
			createConfig.FilterSubjects = cfg.Subjects
		}
		if cfg.DeliverNew {
			createConfig.DeliverPolicy = jetstream.DeliverNewPolicy
		} else if !cfg.StartTime.IsZero() {
			createConfig.DeliverPolicy = jetstream.DeliverByStartTimePolicy
			createConfig.OptStartTime = &cfg.StartTime
		}
		consumer, createErr = js.CreateConsumer(
			ctx,
			cfg.Stream,
			createConfig,
		)
		err = createErr
		if createErr != nil {
			consumer, err = js.Consumer(ctx, cfg.Stream, cfg.Durable)
		}
	}
	if err != nil {
		operation := "consumer_lookup"
		if createErr != nil {
			operation = "consumer_create"
		}
		return operationFailure(source, operation, errors.Join(createErr, err))
	}
	if err = consumerMismatch(&cfg, &consumer.CachedInfo().Config); err != nil {
		return operationFailure(source, "consumer_verify", errors.Join(createErr, err))
	}
	return nil
}

// CheckStreams checks only whether the named streams are available.
func CheckStreams(ctx context.Context, js jetstream.JetStream, names ...string) error {
	for _, name := range names {
		if _, err := js.Stream(ctx, name); err != nil {
			return operationFailure(name, "stream_lookup", err)
		}
	}
	return nil
}

func ensureStream(
	ctx context.Context,
	js jetstream.JetStream,
	want *jetstream.StreamConfig,
	verify func(*jetstream.StreamConfig) error,
) error {
	stream, err := js.Stream(ctx, want.Name)
	var createErr error
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		stream, createErr = js.CreateStream(ctx, *want)
		err = createErr
		if createErr != nil {
			stream, err = js.Stream(ctx, want.Name)
		}
	}
	if err != nil {
		operation := "stream_lookup"
		if createErr != nil {
			operation = "stream_create"
		}
		return operationFailure(want.Name, operation, errors.Join(createErr, err))
	}
	if err = verify(&stream.CachedInfo().Config); err != nil {
		return operationFailure(want.Name, "stream_verify", errors.Join(createErr, err))
	}
	return nil
}

func validateTopologyName(failures *configErrors, field, name string) {
	failures.required(field, name)
	if name != "" && (strings.ContainsAny(name, ".*>/\\ \t\r\n") || !validConfigSubject(name, false)) {
		failures.add(field, ruleType, field+" must be a valid NATS name")
	}
}

func validateTopologySubject(failures *configErrors, subject string) {
	failures.required("subject", subject)
	if subject != "" && !validConfigSubject(subject, true) {
		failures.add("subject", ruleType, "subject must be a valid NATS subject")
	}
}

func mismatch(field string, have, want any) error {
	return fmt.Errorf("incompatible %s: have %v, want %v", field, have, want)
}

func retentionMismatch(cfg *jetstream.StreamConfig, maxBytes int32) error {
	switch {
	case cfg.Storage != jetstream.FileStorage:
		return mismatch("storage", cfg.Storage, jetstream.FileStorage)
	case cfg.Discard != jetstream.DiscardNew:
		return mismatch("discard", cfg.Discard, jetstream.DiscardNew)
	case cfg.MaxMsgsPerSubject > 0 && !cfg.DiscardNewPerSubject:
		return mismatch("discard_new_per_subject", cfg.DiscardNewPerSubject, true)
	case cfg.MaxAge != 0:
		return mismatch("max_age", cfg.MaxAge, time.Duration(0))
	case cfg.AllowMsgTTL:
		return mismatch("allow_msg_ttl", cfg.AllowMsgTTL, false)
	case cfg.MaxMsgSize >= 0 && cfg.MaxMsgSize < maxBytes:
		return mismatch("max_message_bytes", cfg.MaxMsgSize, maxBytes)
	case cfg.NoAck:
		return mismatch("no_ack", cfg.NoAck, false)
	case cfg.Sealed:
		return mismatch("sealed", cfg.Sealed, false)
	case cfg.AllowRollup:
		return mismatch("allow_rollup", cfg.AllowRollup, false)
	case cfg.Mirror != nil:
		return mismatch("mirror", cfg.Mirror != nil, false)
	case len(cfg.Sources) != 0:
		return mismatch("sources", len(cfg.Sources), 0)
	case cfg.SubjectTransform != nil:
		return mismatch("subject_transform", cfg.SubjectTransform != nil, false)
	default:
		return nil
	}
}

func matchesSubject(pattern, subject string) bool {
	tokens := strings.Split(subject, ".")
	parts := strings.Split(pattern, ".")
	for i, part := range parts {
		if i >= len(tokens) {
			return false
		}
		if part == ">" {
			return i == len(parts)-1
		}
		if tokens[i] == ">" || (tokens[i] == "*" && part != "*") {
			return false
		}
		if part != "*" && part != tokens[i] {
			return false
		}
	}
	return len(parts) == len(tokens)
}

func consumerMismatch(spec *DurableConsumerConfig, cfg *jetstream.ConsumerConfig) error {
	switch {
	case cfg.Name != spec.Durable:
		return mismatch("name", cfg.Name, spec.Durable)
	case cfg.Durable != spec.Durable:
		return mismatch("durable", cfg.Durable, spec.Durable)
	case cfg.AckPolicy != jetstream.AckExplicitPolicy:
		return mismatch("ack_policy", cfg.AckPolicy, jetstream.AckExplicitPolicy)
	case cfg.AckWait < spec.AckWait:
		return mismatch("ack_wait", cfg.AckWait, spec.AckWait)
	case cfg.MaxDeliver != spec.MaxDeliveries:
		return mismatch("max_deliveries", cfg.MaxDeliver, spec.MaxDeliveries)
	case !equalFilterSubjects(spec.Subjects, cfg.FilterSubject, cfg.FilterSubjects):
		return mismatch("subjects", nativeFilterSubjects(cfg.FilterSubject, cfg.FilterSubjects), spec.Subjects)
	case cfg.DeliverSubject != "" || cfg.DeliverGroup != "":
		return mismatch("push_delivery", []string{cfg.DeliverSubject, cfg.DeliverGroup}, "pull")
	case len(cfg.BackOff) != 0:
		return mismatch("backoff", cfg.BackOff, "none")
	case cfg.HeadersOnly:
		return mismatch("headers_only", cfg.HeadersOnly, false)
	case cfg.MemoryStorage:
		return mismatch("memory_storage", cfg.MemoryStorage, false)
	case cfg.InactiveThreshold != 0:
		return mismatch("inactive_threshold", cfg.InactiveThreshold, time.Duration(0))
	default:
		return nil
	}
}
