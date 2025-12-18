package idempotency_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"
)

type testEvent struct {
	topic string
	msg   *broker.Message
}

func (e *testEvent) Topic() string            { return e.topic }
func (e *testEvent) Message() *broker.Message { return e.msg }
func (e *testEvent) Ack() error               { return nil }

func TestMiddleware_StoresSuccessAndSkipsReplay(t *testing.T) {
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
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	entry, err := store.Get(context.Background(), "topic:1")
	if err != nil {
		t.Fatalf("expected stored entry, got error: %v", err)
	}
	if entry.Response == nil {
		t.Fatalf("expected stored response marker")
	}

	evt2 := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("hello"),
		},
	}

	if err := h(evt2); err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_UnlocksOnHandlerError(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine)

	calls := 0
	fail := true
	h := mw(func(broker.Event) error {
		calls++
		if fail {
			fail = false
			return errors.New("boom")
		}
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

	if err := h(evt); err == nil {
		t.Fatalf("expected error on first attempt")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}

	if err := h(evt); err != nil {
		t.Fatalf("expected replay success, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected replay to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_ReturnsKeyConflict(t *testing.T) {
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
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("a"),
		},
	}
	if err := h(evt); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	evt2 := &testEvent{
		topic: "topic",
		msg: &broker.Message{
			ID:     "1",
			Header: make(broker.Header),
			Body:   []byte("b"),
		},
	}
	err := h(evt2)
	if err == nil || !errors.Is(err, idempo.ErrKeyConflict) {
		t.Fatalf("expected ErrKeyConflict, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected conflict to skip handler, calls=%d", calls)
	}
}

func TestMiddleware_RequireKey(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)
	mw := idempotency.Middleware(engine, idempotency.WithRequireKey(true))

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

	err := h(evt)
	if err == nil || !errors.Is(err, idempo.ErrMissingKey) {
		t.Fatalf("expected ErrMissingKey, got: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected handler to not be called, calls=%d", calls)
	}
}

func TestMiddleware_ReturnsInProgress(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	engine := idempo.NewEngine(store)

	body := []byte("hello")
	sum := sha256.Sum256(body)
	fp := idempo.Fingerprint{
		Operation: "consume",
		Target:    "topic",
		BodyHash:  hex.EncodeToString(sum[:]),
	}

	lock, err := engine.Process(context.Background(), "topic:1", fp)
	if err != nil {
		t.Fatalf("failed to create in-progress entry: %v", err)
	}
	if !lock.IsOwner {
		t.Fatalf("expected lock owner")
	}
	t.Cleanup(func() { _ = engine.Unlock(context.Background(), "topic:1", lock.Token) })

	mw := idempotency.Middleware(engine)
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
			Body:   body,
		},
	}

	err = h(evt)
	if err == nil || !errors.Is(err, idempo.ErrInProgress) {
		t.Fatalf("expected ErrInProgress, got: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected handler to not be called, calls=%d", calls)
	}
}
