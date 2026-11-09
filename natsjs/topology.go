package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/velmie/x/svc/apperror"
	"github.com/velmie/x/svc/validation"
)

// LimitsStreamConfig selects an owned limits-retention stream.
type LimitsStreamConfig struct {
	Name            string        `k:"name" validate:"required"`
	Subjects        []string      `k:"subjects" validate:"required,min=1"`
	DuplicateWindow time.Duration `k:"duplicate_window" validate:"gt=0"`
}

// WorkQueueStreamConfig selects an owned work queue. MaxMessages and MaxBytes
// are creation settings and do not constrain compatible existing streams.
type WorkQueueStreamConfig struct {
	Name            string        `k:"name" validate:"required"`
	Subject         string        `k:"subject" validate:"required"`
	MaxMessageBytes int32         `k:"max_message_bytes" validate:"gt=0"`
	DuplicateWindow time.Duration `k:"duplicate_window" validate:"gt=0"`
	MaxMessages     int64         `k:"max_messages" validate:"gt=0"`
	MaxBytes        int64         `k:"max_bytes" validate:"gt=0"`
}

// RetainedStreamConfig selects an externally owned retained stream.
type RetainedStreamConfig struct {
	Stream          string `k:"stream" validate:"required"`
	Subject         string `k:"subject" validate:"required"`
	MaxMessageBytes int32  `k:"max_message_bytes" validate:"gt=0"`
}

// DurableConsumerConfig selects a durable pull consumer. MaxAckPending is a
// creation setting and does not constrain compatible existing consumers.
type DurableConsumerConfig struct {
	Stream        string        `k:"stream" validate:"required"`
	Durable       string        `k:"durable" validate:"required"`
	Subject       string        `k:"subject" validate:"required"`
	AckWait       time.Duration `k:"ack_wait" validate:"gt=0"`
	MaxDeliveries int           `k:"max_deliveries" validate:"eq=-1|gt=0"`
	MaxAckPending int           `k:"max_ack_pending" hint:"Maximum unacknowledged messages when creating the consumer" validate:"gt=0"`
}

// Validate checks the profile without I/O.
func (cfg LimitsStreamConfig) Validate() error {
	var failures validation.Error
	failures.AddStruct(cfg, configValidator.Struct(cfg))
	validateTopologyName(&failures, "name", cfg.Name)
	for _, subject := range cfg.Subjects {
		if !validTopologySubject(subject, true) {
			failures.Add("subjects", apperror.RuleType, "NATS subject filter", nil)
			break
		}
	}
	return failures.Err()
}

// Validate checks the profile without I/O.
func (cfg WorkQueueStreamConfig) Validate() error {
	var failures validation.Error
	failures.AddStruct(cfg, configValidator.Struct(cfg))
	validateTopologyName(&failures, "name", cfg.Name)
	validateTopologySubject(&failures, cfg.Subject, true)
	return failures.Err()
}

// Validate checks the profile without I/O.
func (cfg RetainedStreamConfig) Validate() error {
	var failures validation.Error
	failures.AddStruct(cfg, configValidator.Struct(cfg))
	validateTopologyName(&failures, "stream", cfg.Stream)
	validateTopologySubject(&failures, cfg.Subject, false)
	return failures.Err()
}

// Validate checks the profile without I/O.
func (cfg DurableConsumerConfig) Validate() error {
	var failures validation.Error
	failures.AddStruct(cfg, configValidator.Struct(cfg))
	validateTopologyName(&failures, "stream", cfg.Stream)
	validateTopologyName(&failures, "durable", cfg.Durable)
	validateTopologySubject(&failures, cfg.Subject, true)
	return failures.Err()
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
			return mismatch("name")
		}
		a, b := slices.Clone(actual.Subjects), slices.Clone(cfg.Subjects)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) {
			return mismatch("subjects")
		}
		if actual.Storage != jetstream.FileStorage {
			return mismatch("storage")
		}
		if actual.Retention != jetstream.LimitsPolicy {
			return mismatch("retention")
		}
		if actual.Duplicates != cfg.DuplicateWindow {
			return mismatch("duplicate_window")
		}
		if actual.NoAck {
			return mismatch("no_ack")
		}
		if actual.Sealed {
			return mismatch("sealed")
		}
		if actual.Mirror != nil {
			return mismatch("mirror")
		}
		if len(actual.Sources) != 0 {
			return mismatch("sources")
		}
		if actual.SubjectTransform != nil {
			return mismatch("subject_transform")
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
			return mismatch("name")
		}
		if len(actual.Subjects) != 1 || actual.Subjects[0] != cfg.Subject {
			return mismatch("subject")
		}
		if actual.Retention != jetstream.WorkQueuePolicy {
			return mismatch("retention")
		}
		if actual.Duplicates < cfg.DuplicateWindow {
			return mismatch("duplicate_window")
		}
		return retentionMismatch(actual, cfg.MaxMessageBytes)
	})
}

// RequireRetainedStream verifies an external stream and never creates it.
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
		err = mismatch("name")
	}
	covered := false
	for _, pattern := range actual.Subjects {
		if matchesSubject(pattern, cfg.Subject) {
			covered = true
			break
		}
	}
	if err == nil && !covered {
		err = mismatch("subject")
	}
	if err == nil && actual.Retention != jetstream.LimitsPolicy {
		err = mismatch("retention")
	}
	if err == nil {
		err = retentionMismatch(&actual, cfg.MaxMessageBytes)
	}
	return operationFailure(cfg.Stream, "stream_verify", err)
}

// EnsureDurableConsumer creates a missing durable or verifies it without updating it.
func EnsureDurableConsumer(ctx context.Context, js jetstream.JetStream, cfg DurableConsumerConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	source := cfg.Stream + "/" + cfg.Durable
	consumer, err := js.Consumer(ctx, cfg.Stream, cfg.Durable)
	var createErr error
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		consumer, createErr = js.CreateConsumer(
			ctx,
			cfg.Stream,
			jetstream.ConsumerConfig{
				Name:          cfg.Durable,
				Durable:       cfg.Durable,
				DeliverPolicy: jetstream.DeliverAllPolicy,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       cfg.AckWait,
				MaxDeliver:    cfg.MaxDeliveries,
				MaxAckPending: cfg.MaxAckPending,
				FilterSubject: cfg.Subject,
			},
		)
		err = createErr
		if createErr != nil {
			consumer, err = js.Consumer(ctx, cfg.Stream, cfg.Durable)
		}
	}
	if err != nil {
		return operationFailure(source, "consumer_lookup", errors.Join(createErr, err))
	}
	if err = consumerMismatch(cfg, &consumer.CachedInfo().Config); err != nil {
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
		return operationFailure(want.Name, "stream_lookup", errors.Join(createErr, err))
	}
	if err = verify(&stream.CachedInfo().Config); err != nil {
		return operationFailure(want.Name, "stream_verify", errors.Join(createErr, err))
	}
	return nil
}

func validateTopologyName(failures *validation.Error, field, name string) {
	if name != "" && (strings.ContainsAny(name, ".*>/\\ \t\r\n") || !validNamespaceSubject(name)) {
		failures.Add(field, apperror.RuleType, "NATS name", nil)
	}
}

func validateTopologySubject(failures *validation.Error, subject string, filter bool) {
	if subject != "" && !validTopologySubject(subject, filter) {
		failures.Add("subject", apperror.RuleType, "NATS subject", nil)
	}
}

func validTopologySubject(subject string, filter bool) bool {
	return utf8.ValidString(subject) && strings.IndexFunc(subject, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) < 0 && validSubject(subject, filter)
}

func mismatch(field string) error { return fmt.Errorf("incompatible %s", field) }

func retentionMismatch(cfg *jetstream.StreamConfig, maxBytes int32) error {
	switch {
	case cfg.Storage != jetstream.FileStorage:
		return mismatch("storage")
	case cfg.Discard != jetstream.DiscardNew:
		return mismatch("discard")
	case cfg.MaxMsgsPerSubject > 0 && !cfg.DiscardNewPerSubject:
		return mismatch("max_msgs_per_subject")
	case cfg.MaxAge != 0:
		return mismatch("max_age")
	case cfg.AllowMsgTTL:
		return mismatch("allow_msg_ttl")
	case cfg.MaxMsgSize >= 0 && cfg.MaxMsgSize < maxBytes:
		return mismatch("max_message_bytes")
	case cfg.NoAck:
		return mismatch("no_ack")
	case cfg.Sealed:
		return mismatch("sealed")
	case cfg.AllowRollup:
		return mismatch("allow_rollup")
	case cfg.Mirror != nil:
		return mismatch("mirror")
	case len(cfg.Sources) != 0:
		return mismatch("sources")
	case cfg.SubjectTransform != nil:
		return mismatch("subject_transform")
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
		if part != "*" && part != tokens[i] {
			return false
		}
	}
	return len(parts) == len(tokens)
}

func consumerMismatch(spec DurableConsumerConfig, cfg *jetstream.ConsumerConfig) error {
	switch {
	case cfg.Name != spec.Durable:
		return mismatch("name")
	case cfg.Durable != spec.Durable:
		return mismatch("durable")
	case cfg.DeliverPolicy != jetstream.DeliverAllPolicy:
		return mismatch("deliver_policy")
	case cfg.AckPolicy != jetstream.AckExplicitPolicy:
		return mismatch("ack_policy")
	case cfg.AckWait < spec.AckWait:
		return mismatch("ack_wait")
	case cfg.MaxDeliver != -1 && (spec.MaxDeliveries == -1 || cfg.MaxDeliver < spec.MaxDeliveries):
		return mismatch("max_deliveries")
	case cfg.FilterSubject != spec.Subject || len(cfg.FilterSubjects) != 0:
		return mismatch("subject")
	case cfg.DeliverSubject != "" || cfg.DeliverGroup != "":
		return mismatch("push_delivery")
	case len(cfg.BackOff) != 0:
		return mismatch("backoff")
	case cfg.HeadersOnly:
		return mismatch("headers_only")
	case cfg.MemoryStorage:
		return mismatch("memory_storage")
	case cfg.InactiveThreshold != 0:
		return mismatch("inactive_threshold")
	default:
		return nil
	}
}
