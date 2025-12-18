package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/velmie/broker"
	"github.com/velmie/idempo"
)

const defaultOperation = "consume"
const keySeparator = ":"

// Middleware returns a broker.Middleware that enforces idempotent message processing.
//
// Key resolution:
//   - primary: broker.Message.ID
//   - fallback: broker.Message.Header[HeaderName] (default: "Idempotency-Key")
//
// Store key is optionally scoped by topic (default) and can be prefixed via WithKeyPrefix.
//
// Default fingerprint uses the event topic + SHA-256(body) + optional selected headers
// (see WithFingerprintHeaders). Reusing the same key with a different fingerprint returns idempo.ErrKeyConflict.
//
// On replay, the wrapped handler is skipped and Middleware returns nil.
// This assumes subscriptions use AutoAck=true; otherwise replays may never be acknowledged.
func Middleware(engine *idempo.Engine, opts ...Option) broker.Middleware {
	cfg := NewConfig(opts...)
	if engine == nil {
		panic("broker/idempotency: nil engine")
	}

	return func(next broker.Handler) broker.Handler {
		return func(event broker.Event) error {
			msg := event.Message()

			storeKey, hasKey, err := resolveStoreKey(event, cfg)
			if err != nil {
				return err
			}
			if !hasKey {
				return next(event)
			}

			fp, err := buildFingerprint(event, cfg, cfg.FingerprintHeaders)
			if err != nil {
				return err
			}

			ctx := msg.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			// When waiting is enabled, the key can disappear if the owner unlocks on failure.
			// Retry once to attempt to acquire a fresh lock instead of returning ErrKeyNotFound.
			for attempt := 0; attempt < 2; attempt++ {
				result, err := engine.Process(ctx, storeKey, fp)
				if err != nil {
					if attempt == 0 && errors.Is(err, idempo.ErrKeyNotFound) {
						continue
					}
					return err
				}

				if result.Response != nil {
					if cfg.OnReplay != nil {
						cfg.OnReplay(event, result.Response)
					}
					if cfg.AckOnReplay {
						if err := event.Ack(); err != nil {
							return err
						}
					}
					return nil
				}

				if !result.IsOwner {
					return idempo.ErrInProgress
				}

				return handleOwner(engine, ctx, next, event, cfg, storeKey, result.Token)
			}

			return idempo.ErrKeyNotFound
		}
	}
}

func resolveStoreKey(event broker.Event, cfg Config) (storeKey string, hasKey bool, err error) {
	msg := event.Message()

	rawKey := strings.TrimSpace(msg.ID)
	if rawKey == "" && msg.Header != nil && cfg.HeaderName != "" {
		rawKey = strings.TrimSpace(msg.Header.Get(cfg.HeaderName))
	}

	if rawKey == "" {
		if cfg.RequireKey {
			return "", false, idempo.ErrMissingKey
		}
		return "", false, nil
	}

	if err := cfg.KeyValidator(rawKey); err != nil {
		return "", false, err
	}

	key := rawKey
	if cfg.UseTopicInKey {
		key = event.Topic() + keySeparator + rawKey
	}

	return cfg.KeyPrefix + key, true, nil
}

func buildFingerprint(event broker.Event, cfg Config, fingerprintHeaders []string) (idempo.Fingerprint, error) {
	if cfg.FingerprintFunc != nil {
		return cfg.FingerprintFunc(event)
	}

	msg := event.Message()

	return idempo.Fingerprint{
		Operation:   defaultOperation,
		Target:      event.Topic(),
		HeadersHash: hashHeaders(msg.Header, fingerprintHeaders),
		BodyHash:    hashBody(msg.Body),
	}, nil
}

func hashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func hashHeaders(header broker.Header, keys []string) string {
	if header == nil || len(keys) == 0 {
		return ""
	}

	var b strings.Builder
	for _, k := range keys {
		if k == "" {
			continue
		}
		b.WriteString(strconv.Quote(k))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(header.Get(k)))
		b.WriteByte('\n')
	}

	if b.Len() == 0 {
		return ""
	}

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func handleOwner(
	engine *idempo.Engine,
	parentCtx context.Context,
	next broker.Handler,
	event broker.Event,
	cfg Config,
	storeKey string,
	token string,
) error {
	unlockOnReturn := true
	defer func() {
		if !unlockOnReturn {
			return
		}
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), cfg.CommitTimeout)
		defer cancel()
		_ = engine.Unlock(unlockCtx, storeKey, token)
	}()

	if err := next(event); err != nil {
		return err
	}

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), cfg.CommitTimeout)
	defer cancel()
	if err := engine.Commit(commitCtx, storeKey, token, &idempo.Response{}); err != nil {
		if cfg.CommitErrorHandler != nil {
			cfg.CommitErrorHandler(event, err)
		}

		switch cfg.CommitErrorMode {
		case CommitFailOpen:
			return nil
		case CommitFailClosedUnlock:
			return err
		case CommitFailClosedKeepLock:
			unlockOnReturn = false
			return err
		default:
			return nil
		}
	}

	unlockOnReturn = false
	return nil
}
