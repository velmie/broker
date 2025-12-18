package idempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"
)

func TestMiddleware_PanicsOnNilEngine(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()

	_ = idempotency.Middleware(nil)
}

func TestNewConfig_NormalizesFingerprintHeaders(t *testing.T) {
	cfg := idempotency.NewConfig(idempotency.WithFingerprintHeaders(" B ", "A", "", "A", "B", " "))

	if len(cfg.FingerprintHeaders) != 2 {
		t.Fatalf("expected 2 headers, got: %v", cfg.FingerprintHeaders)
	}
	if cfg.FingerprintHeaders[0] != "A" || cfg.FingerprintHeaders[1] != "B" {
		t.Fatalf("expected sorted unique headers [A B], got: %v", cfg.FingerprintHeaders)
	}
}

func TestNewConfig_NormalizesCommitOptions(t *testing.T) {
	cfg := idempotency.NewConfig(
		idempotency.WithCommitTimeout(0),
		idempotency.WithCommitErrorMode(idempotency.CommitErrorMode(123)),
	)

	if cfg.CommitTimeout <= 0 {
		t.Fatalf("expected CommitTimeout to be normalized to a positive default, got: %v", cfg.CommitTimeout)
	}
	if cfg.CommitErrorMode != idempotency.CommitFailOpen {
		t.Fatalf("expected CommitErrorMode to be normalized to CommitFailOpen, got: %v", cfg.CommitErrorMode)
	}
}

func TestMiddleware_CommitFailOpen_IgnoresCommitErrorAndUnlocks(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("commit failed")
	spy := newStoreSpy(store)
	spy.failSetResponseErr = commitErr
	spy.failSetResponseRemaining = 1

	engine := idempo.NewEngine(spy)

	commitErrCalls := 0
	mw := idempotency.Middleware(
		engine,
		idempotency.WithCommitErrorMode(idempotency.CommitFailOpen),
		idempotency.WithCommitErrorHandler(func(broker.Event, error) { commitErrCalls++ }),
	)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected nil, got error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to be called once, calls=%d", calls)
	}
	if commitErrCalls != 1 {
		t.Fatalf("expected commit error handler to be called once, calls=%d", commitErrCalls)
	}

	snap := spy.Snapshot()
	if snap.DeleteCalls != 1 {
		t.Fatalf("expected key to be unlocked on commit failure, deleteCalls=%d", snap.DeleteCalls)
	}

	if _, err := store.Get(context.Background(), "topic:1"); !errors.Is(err, idempo.ErrKeyNotFound) {
		t.Fatalf("expected key to be deleted on commit failure, got: %v", err)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected handler to be called again after failed commit, calls=%d", calls)
	}
	if commitErrCalls != 1 {
		t.Fatalf("expected commit error handler to not be called on success, calls=%d", commitErrCalls)
	}

	snap = spy.Snapshot()
	if snap.DeleteCalls != 1 {
		t.Fatalf("expected no unlock after successful commit, deleteCalls=%d", snap.DeleteCalls)
	}

	entry, err := store.Get(context.Background(), "topic:1")
	if err != nil {
		t.Fatalf("expected stored entry after retry, got: %v", err)
	}
	if entry.Response == nil {
		t.Fatalf("expected stored response marker after retry")
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay to return nil, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_CommitFailClosedUnlock_ReturnsCommitErrorAndUnlocks(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("commit failed")
	spy := newStoreSpy(store)
	spy.failSetResponseErr = commitErr
	spy.failSetResponseRemaining = 1

	engine := idempo.NewEngine(spy)

	commitErrCalls := 0
	mw := idempotency.Middleware(
		engine,
		idempotency.WithCommitErrorMode(idempotency.CommitFailClosedUnlock),
		idempotency.WithCommitErrorHandler(func(broker.Event, error) { commitErrCalls++ }),
	)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); !errors.Is(err, commitErr) {
		t.Fatalf("expected commit error, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to be called once, calls=%d", calls)
	}
	if commitErrCalls != 1 {
		t.Fatalf("expected commit error handler to be called once, calls=%d", commitErrCalls)
	}

	snap := spy.Snapshot()
	if snap.DeleteCalls != 1 {
		t.Fatalf("expected key to be unlocked on commit failure, deleteCalls=%d", snap.DeleteCalls)
	}

	if _, err := store.Get(context.Background(), "topic:1"); !errors.Is(err, idempo.ErrKeyNotFound) {
		t.Fatalf("expected key to be deleted on commit failure, got: %v", err)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected handler to run again after commit failure, calls=%d", calls)
	}
	if commitErrCalls != 1 {
		t.Fatalf("expected commit error handler to not be called on success, calls=%d", commitErrCalls)
	}

	snap = spy.Snapshot()
	if snap.DeleteCalls != 1 {
		t.Fatalf("expected no unlock after successful commit, deleteCalls=%d", snap.DeleteCalls)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay to return nil, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_CommitFailClosedKeepLock_KeepsLockUntilTTL(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	commitErr := errors.New("commit failed")
	spy := newStoreSpy(store)
	spy.failSetResponseErr = commitErr
	spy.failSetResponseRemaining = 1

	engine := idempo.NewEngine(spy, idempo.WithLockTTL(30*time.Millisecond))

	commitErrCalls := 0
	mw := idempotency.Middleware(
		engine,
		idempotency.WithCommitErrorMode(idempotency.CommitFailClosedKeepLock),
		idempotency.WithCommitErrorHandler(func(broker.Event, error) { commitErrCalls++ }),
	)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); !errors.Is(err, commitErr) {
		t.Fatalf("expected commit error, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to be called once, calls=%d", calls)
	}
	if commitErrCalls != 1 {
		t.Fatalf("expected commit error handler to be called once, calls=%d", commitErrCalls)
	}

	snap := spy.Snapshot()
	if snap.DeleteCalls != 0 {
		t.Fatalf("expected lock to be kept (no unlock), deleteCalls=%d", snap.DeleteCalls)
	}

	entry, err := store.Get(context.Background(), "topic:1")
	if err != nil {
		t.Fatalf("expected in-progress entry to remain, got: %v", err)
	}
	if entry.Response != nil {
		t.Fatalf("expected no response marker on commit failure")
	}

	if err := h(evt); !errors.Is(err, idempo.ErrInProgress) {
		t.Fatalf("expected ErrInProgress before lock TTL expires, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected in-progress attempt to skip handler, calls=%d", calls)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		err := h(evt)
		if err == nil {
			break
		}
		if !errors.Is(err, idempo.ErrInProgress) {
			t.Fatalf("expected ErrInProgress while waiting for lock TTL, got: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for lock TTL expiration, last err: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls != 2 {
		t.Fatalf("expected handler to run again after lock TTL expiration, calls=%d", calls)
	}

	entry, err = store.Get(context.Background(), "topic:1")
	if err != nil {
		t.Fatalf("expected stored entry after retry, got: %v", err)
	}
	if entry.Response == nil {
		t.Fatalf("expected stored response marker after retry")
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay to return nil, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_KeyResolution_PrefersMessageIDOverHeader(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "msg-id",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set(idempotency.DefaultHeaderName, "header-id")

	if err := h(evt); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	if _, err := store.Get(context.Background(), "topic:msg-id"); err != nil {
		t.Fatalf("expected entry by message id, got: %v", err)
	}
	if _, err := store.Get(context.Background(), "topic:header-id"); !errors.Is(err, idempo.ErrKeyNotFound) {
		t.Fatalf("expected no entry by header id, got: %v", err)
	}
}

func TestMiddleware_KeyResolution_FallsBackToHeader(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set(idempotency.DefaultHeaderName, "header-id")

	if err := h(evt); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	entry, err := store.Get(context.Background(), "topic:header-id")
	if err != nil {
		t.Fatalf("expected entry by header id, got: %v", err)
	}
	if entry.Response == nil {
		t.Fatalf("expected stored response marker")
	}
}

func TestMiddleware_KeyResolution_UsesCustomHeaderName(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithHeaderName("X-Idempotency-Key"))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set("X-Idempotency-Key", "custom-id")

	if err := h(evt); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	if _, err := store.Get(context.Background(), "topic:custom-id"); err != nil {
		t.Fatalf("expected entry by custom header id, got: %v", err)
	}
}

func TestMiddleware_NoKey_NoOpByDefault(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	spy := newStoreSpy(store)
	engine := idempo.NewEngine(spy)
	mw := idempotency.Middleware(engine)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	snap := spy.Snapshot()
	if snap.CreateCalls != 0 || snap.SetResponseCalls != 0 || snap.DeleteCalls != 0 {
		t.Fatalf("expected no store interaction, got: %+v", snap)
	}
}

func TestMiddleware_HeaderNameEmpty_DisablesHeaderFallback(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	spy := newStoreSpy(store)
	engine := idempo.NewEngine(spy)
	mw := idempotency.Middleware(engine, idempotency.WithHeaderName(""))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set(idempotency.DefaultHeaderName, "header-id")

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	snap := spy.Snapshot()
	if snap.CreateCalls != 0 || snap.SetResponseCalls != 0 || snap.DeleteCalls != 0 {
		t.Fatalf("expected no store interaction, got: %+v", snap)
	}
}

func TestMiddleware_WithKeyValidator_ErrorPreventsProcessing(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	validationErr := errors.New("bad key")

	spy := newStoreSpy(store)
	engine := idempo.NewEngine(spy)
	mw := idempotency.Middleware(engine, idempotency.WithKeyValidator(func(string) error { return validationErr }))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); !errors.Is(err, validationErr) {
		t.Fatalf("expected validation error, got: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected handler to not be called, calls=%d", calls)
	}

	snap := spy.Snapshot()
	if snap.CreateCalls != 0 || snap.GetCalls != 0 || snap.SetResponseCalls != 0 || snap.DeleteCalls != 0 {
		t.Fatalf("expected no store interaction, got: %+v", snap)
	}
}

func TestMiddleware_WithKeyPrefix_PrependsPrefix(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithKeyPrefix("prod:"))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	if _, err := store.Get(context.Background(), "prod:topic:1"); err != nil {
		t.Fatalf("expected entry with prefix, got: %v", err)
	}
}

func TestMiddleware_WithFingerprintHeaders_DetectsHeaderChangeAsConflict(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithFingerprintHeaders("X-Trace"))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set("X-Trace", "a")

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	evt2 := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt2.msg.Header.Set("X-Trace", "b")

	err := h(evt2)
	if err == nil || !errors.Is(err, idempo.ErrKeyConflict) {
		t.Fatalf("expected ErrKeyConflict, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected conflict to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_WithFingerprintHeaders_HashIsUnambiguous(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithFingerprintHeaders("A", "B"))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt.msg.Header.Set("A", "1\nB=2")

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	evt2 := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	evt2.msg.Header.Set("A", "1")
	evt2.msg.Header.Set("B", "2\nB=")

	err := h(evt2)
	if err == nil || !errors.Is(err, idempo.ErrKeyConflict) {
		t.Fatalf("expected ErrKeyConflict, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected conflict to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_WithFingerprintFunc_CanDedupAcrossTopicsWhenUseTopicInKeyDisabled(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)

	fp := idempo.Fingerprint{
		Operation: "test",
		Target:    "",
		BodyHash:  "same",
	}

	mw := idempotency.Middleware(
		engine,
		idempotency.WithUseTopicInKey(false),
		idempotency.WithFingerprintFunc(func(broker.Event) (idempo.Fingerprint, error) {
			return fp, nil
		}),
	)

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt1 := &testEvent{
		topic: "topic-a",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("a"),
		},
	}

	if err := h(evt1); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}

	evt2 := &testEvent{
		topic: "topic-b",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("b"),
		},
	}

	if err := h(evt2); err != nil {
		t.Fatalf("expected replay to succeed, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}

	entry, err := store.Get(context.Background(), "1")
	if err != nil {
		t.Fatalf("expected stored entry, got: %v", err)
	}
	if entry.Response == nil {
		t.Fatalf("expected stored response marker")
	}
}

func TestMiddleware_OnReplay_IsCalledAndCanAck(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)

	replays := 0
	mw := idempotency.Middleware(engine, idempotency.WithOnReplay(func(e broker.Event, resp *idempo.Response) {
		replays++
		if resp == nil {
			t.Fatalf("expected non-nil response marker on replay")
		}
		_ = e.Ack()
	}))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}
	if replays != 0 {
		t.Fatalf("expected no replay calls, replays=%d", replays)
	}
	if evt.ackCalls != 0 {
		t.Fatalf("expected Ack to not be called on first delivery, calls=%d", evt.ackCalls)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
	if replays != 1 {
		t.Fatalf("expected one replay callback, replays=%d", replays)
	}
	if evt.ackCalls != 1 {
		t.Fatalf("expected Ack to be called on replay, calls=%d", evt.ackCalls)
	}
}

func TestMiddleware_AckOnReplay_AcksAndReturnsAckError(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithAckOnReplay(true))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}
	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler call, calls=%d", calls)
	}
	if evt.ackCalls != 0 {
		t.Fatalf("expected Ack to not be called on first delivery, calls=%d", evt.ackCalls)
	}

	evt.ackErr = errors.New("ack failed")

	err := h(evt)
	if err == nil || !errors.Is(err, evt.ackErr) {
		t.Fatalf("expected ack error, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
	if evt.ackCalls != 1 {
		t.Fatalf("expected Ack to be called on replay, calls=%d", evt.ackCalls)
	}
}

func TestMiddleware_RetriesOnErrKeyNotFoundWhenWaiting(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	spy := newStoreSpy(store)
	spy.deleteOnCreateOnce = true

	fp := idempo.Fingerprint{
		Operation: "test",
		Target:    "",
		BodyHash:  "same",
	}

	// Pre-create an in-progress entry so engine.Process will go into the waiting path.
	entry, created, err := store.Create(context.Background(), "topic:1", fp, time.Second)
	if err != nil {
		t.Fatalf("failed to pre-create entry: %v", err)
	}
	if !created || entry == nil {
		t.Fatalf("expected pre-created entry")
	}

	engine := idempo.NewEngine(
		spy,
		idempo.WithWaitForInProgress(true),
		idempo.WithInProgressTimeout(100*time.Millisecond),
		idempo.WithPollInterval(1*time.Millisecond),
	)

	mw := idempotency.Middleware(engine, idempotency.WithFingerprintFunc(func(broker.Event) (idempo.Fingerprint, error) {
		return fp, nil
	}))

	calls := 0
	h := mw(func(broker.Event) error {
		calls++
		return nil
	})

	evt := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to be called once, calls=%d", calls)
	}

	snap := spy.Snapshot()
	if snap.GetCalls == 0 {
		t.Fatalf("expected waiting path to poll the store at least once")
	}

	stored, err := store.Get(context.Background(), "topic:1")
	if err != nil {
		t.Fatalf("expected stored entry after retry, got: %v", err)
	}
	if stored.Response == nil {
		t.Fatalf("expected stored response marker after retry")
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay success, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}
