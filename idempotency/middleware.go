package idempotency

import (
	"context"
	"errors"
	"runtime/debug"
	"strings"

	"github.com/velmie/idempo"

	"github.com/velmie/broker"
)

const keySeparator = ":"

// Middleware commits an empty marker only after concrete Handled without error.
// Replays skip processing and request Handled. Native settlement remains with the
// consumer. The required engine and its store remain caller-owned.
//
// Hooks are synchronous and must support the caller's concurrency. Replay-hook
// panics propagate without changing the marker. A commit-hook panic keeps the
// lease/marker untouched and preserves both panic evidence and the commit cause.
func Middleware(engine *idempo.Engine, opts ...Option) (broker.HandlerMiddleware, error) {
	cfg := NewConfig(opts...)
	if err := validateConfig(&cfg); err != nil {
		return broker.HandlerMiddleware{}, err
	}
	return broker.HandlerMiddleware{Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
		return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			key, hasKey, err := resolveStoreKey(delivery, &cfg)
			if err != nil {
				return nil, err
			}
			if !hasKey {
				return next(ctx, delivery)
			}
			fp, err := buildFingerprint(ctx, delivery, &cfg)
			if err != nil {
				return nil, err
			}
			for attempt := 0; attempt < 2; attempt++ {
				result, processErr := engine.Process(ctx, key, fp)
				if processErr != nil {
					if attempt == 0 && errors.Is(processErr, idempo.ErrKeyNotFound) {
						continue
					}
					return nil, failure("acquire", "", processErr)
				}
				if result.Response != nil {
					if cfg.OnReplay != nil {
						cfg.OnReplay(ctx, delivery, idempo.CloneResponse(result.Response))
					}
					return broker.Handled{}, nil
				}
				if !result.IsOwner {
					return nil, failure("acquire", "", idempo.ErrInProgress)
				}
				return handleOwner(engine, ctx, next, delivery, &cfg, key, result.Token)
			}
			return nil, failure("acquire", "", idempo.ErrKeyNotFound)
		}
	}}, nil
}

// Error identifies the failed middleware operation and, when available, a
// configuration or message field. Operations are configure, key, fingerprint,
// acquire, commit and unlock. Cause can contain application or store data.
// Logging adapters must retain its type and cause while redacting sensitive text.
// Field contains a stable field name, never a key, header value or source label.
type Error struct {
	Operation, Field string
	Cause            error
}

func (e *Error) Error() string {
	return "idempotency " + e.Operation + " " + e.Field + ": " + e.Cause.Error()
}
func (e *Error) Unwrap() error { return e.Cause }

func failure(operation, field string, cause error) error {
	return &Error{Operation: operation, Field: field, Cause: cause}
}

func resolveStoreKey(delivery broker.Delivery, cfg *Config) (storeKey string, hasKey bool, err error) {
	msg := delivery.Message()
	key := strings.TrimSpace(msg.ID)
	field := "Message.ID"
	if key == "" && cfg.HeaderName != "" {
		text, err := selectedText(msg.Headers, cfg.HeaderName)
		if err != nil {
			return "", false, failure("key", "HeaderName", err)
		}
		key = strings.TrimSpace(text)
		field = "HeaderName"
	}
	if key == "" {
		if cfg.RequireKey {
			return "", false, failure("key", field, idempo.ErrMissingKey)
		}
		return "", false, nil
	}
	if err := cfg.KeyValidator(key); err != nil {
		return "", false, failure("key", field, err)
	}
	if cfg.UseSourceInKey {
		key = delivery.Source() + keySeparator + key
	}
	return cfg.KeyPrefix + key, true, nil
}

func handleOwner(engine *idempo.Engine, parent context.Context, next broker.HandlerFunc, delivery broker.Delivery,
	cfg *Config, key, token string) (result broker.Disposition, err error) {
	unlock := true
	var reportedCommitErr error
	defer func() {
		panicValue := recover()
		var cleanupErr error
		if unlock {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(parent), cfg.CommitTimeout)
			cleanupErr = engine.Unlock(cleanup, key, token)
			cancel()
			if cleanupErr != nil {
				cleanupErr = failure("unlock", "", cleanupErr)
			}
		}
		if panicValue != nil {
			if cleanupErr != nil {
				panic(errors.Join(&broker.PanicError{Value: panicValue, Stack: debug.Stack()}, cleanupErr))
			}
			panic(panicValue)
		}
		if cleanupErr != nil {
			err = errors.Join(err, reportedCommitErr, cleanupErr)
		}
	}()
	result, err = next(parent, delivery)
	if err != nil {
		return result, err
	}
	if _, ok := result.(broker.Handled); !ok {
		return result, nil
	}
	commit, cancel := context.WithTimeout(context.WithoutCancel(parent), cfg.CommitTimeout)
	commitErr := engine.Commit(commit, key, token, &idempo.Response{})
	cancel()
	if commitErr == nil {
		unlock = false
		return result, nil
	}
	commitErr = failure("commit", "", commitErr)
	// Disable cleanup before a hook can panic: an ambiguous accepted marker must
	// not be deleted by diagnostic code. The selected policy is applied afterward.
	unlock = false
	if cfg.CommitErrorHandler != nil {
		func() {
			defer func() {
				if value := recover(); value != nil {
					panic(errors.Join(&broker.PanicError{Value: value, Stack: debug.Stack()}, commitErr))
				}
			}()
			cfg.CommitErrorHandler(parent, delivery, commitErr)
		}()
	}
	switch cfg.CommitErrorMode {
	case CommitFailOpen:
		reportedCommitErr = commitErr
		unlock = true
		return result, nil
	case CommitFailClosedUnlock:
		unlock = true
		return result, commitErr
	default:
		return result, commitErr
	}
}
