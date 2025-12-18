// Package idempotency provides a broker middleware for idempotent message handling backed by
// github.com/velmie/idempo.
//
// The middleware acquires an idempotency lock per message key, runs the wrapped handler once,
// and commits a completion marker. Subsequent deliveries with the same key and fingerprint are
// treated as replays and are acknowledged by returning nil without executing the handler.
package idempotency
