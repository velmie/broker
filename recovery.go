package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// RecoveryConfig selects bounded replacement of failed consumer runs.
type RecoveryConfig struct {
	// Source identifies the binding in diagnostics. It must be nonempty.
	Source string
	// MaxRestarts counts replacement runs after the initial run. Zero disables them.
	MaxRestarts int
	// Backoff is a fixed, nonnegative delay between joined generations.
	Backoff time.Duration
	// Retryable must explicitly recognize safe Run failures, including every
	// independent cause. It must exclude uncertain settlement and application
	// failures that cannot safely start another receiving generation.
	Retryable func(error) bool
	// Logger records original failures before a replacement is scheduled. Its
	// handler owns redaction and sink failures, as documented for LogProcessing.
	Logger *slog.Logger
}

// WithConsumerRecovery wraps local binding in bounded, sequential recovery.
// The factory still acquires no resources. Initial binding and validation errors
// fail coordinator preflight. Each replacement is created only after the preceding
// Run has joined, and is validated against the handler's requirements before Run.
// Factory and validation failures are never retried. A finite success ends recovery.
//
// Factory, Retryable and Logger are required when used. User callbacks must not
// panic. Use each returned consumer for one Run at a time. No callback is replayed
// directly, no SDK reconnect is configured, and borrowed clients remain owned by
// the caller. The classifier must account for server redelivery after a restart.
//
// Scheduled restarts log their original cause once. A later finite success returns
// nil, with earlier failures retained in those records. Terminal failure returns
// all failed generation causes, including cancellation and replacement failures.
func WithConsumerRecovery(factory ConsumerFactory, config RecoveryConfig) ConsumerFactory {
	return func() (Consumer, error) {
		if strings.TrimSpace(config.Source) == "" {
			return nil, errors.New("recovery Source must be nonempty")
		}
		if config.MaxRestarts < 0 || config.Backoff < 0 {
			return nil, errors.New("recovery MaxRestarts and Backoff must be nonnegative")
		}
		first, err := factory()
		if err != nil {
			return nil, fmt.Errorf("consumer %q initial factory: %w", diagnosticSource(config.Source), err)
		}
		return &recoveringConsumer{first: first, factory: factory, config: config}, nil
	}
}

type recoveringConsumer struct {
	first   Consumer
	factory ConsumerFactory
	config  RecoveryConfig
}

func (c *recoveringConsumer) Validate(requirements ...Requirement) error {
	return c.first.Validate(requirements...)
}

func (c *recoveringConsumer) Run(ctx context.Context, handler Handler) error {
	current := c.first
	var failures []error
	for generation := 1; ; generation++ {
		if cause := context.Cause(ctx); cause != nil {
			return errors.Join(append(failures, cause)...)
		}
		if err := current.Validate(handler.Requirements()...); err != nil {
			return errors.Join(append(failures, c.failure(generation, "validate", err))...)
		}
		err := current.Run(ctx, handler)
		if err != nil {
			failures = append(failures, c.failure(generation, "run", err))
		}
		if cause := context.Cause(ctx); cause != nil {
			return errors.Join(append(failures, cause)...)
		}
		if err == nil {
			return nil
		}
		if generation > c.config.MaxRestarts || !c.config.Retryable(err) {
			return errors.Join(failures...)
		}
		c.config.Logger.WarnContext(ctx, "consumer restart scheduled",
			slog.String("operation", "recover"), slog.String("stage", "run"),
			slog.String("source", diagnosticSource(c.config.Source)), slog.String("outcome", "restart_scheduled"),
			slog.Int("generation", generation), slog.Int("next_generation", generation+1),
			slog.Duration("backoff", c.config.Backoff), slog.Any("error", err))
		if cause := waitForRecovery(ctx, c.config.Backoff); cause != nil {
			return errors.Join(append(failures, cause)...)
		}
		current, err = c.factory()
		if err != nil {
			return errors.Join(append(failures, c.failure(generation+1, "factory", err))...)
		}
	}
}

func (c *recoveringConsumer) failure(generation int, operation string, err error) error {
	return fmt.Errorf("consumer %q generation %d %s: %w", diagnosticSource(c.config.Source), generation, operation, err)
}

func waitForRecovery(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	return context.Cause(ctx)
}
