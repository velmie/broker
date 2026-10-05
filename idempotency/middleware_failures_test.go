package idempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/idempotency"
)

func TestCommitModesPreserveFailuresAndAmbiguousMarker(t *testing.T) {
	for _, mode := range []adapter.CommitErrorMode{adapter.CommitFailOpen, adapter.CommitFailClosedUnlock, adapter.CommitFailClosedKeepLock} {
		for _, accepted := range []bool{false, true} {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			commitErr, unlockErr := errors.New("commit failed"), errors.New("unlock failed")
			commits, unlocks, hooks := 0, 0, 0
			spy := &storeSpy{Store: store, commit: func(ctx context.Context, key, token string, response *idempo.Response, ttl time.Duration) error {
				commits++
				if accepted {
					if err := store.SetResponse(ctx, key, token, response, ttl); err != nil {
						t.Fatal(err)
					}
				}
				return commitErr
			}, unlock: func(context.Context, string, string) error { unlocks++; return unlockErr }}
			mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithCommitErrorMode(mode),
				adapter.WithCommitErrorHandler(func(_ context.Context, _ broker.Delivery, err error) {
					hooks++
					if !errors.Is(err, commitErr) {
						t.Error("hook lost cause")
					}
				}))
			if err != nil {
				t.Fatal(err)
			}
			result, err := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				return broker.Handled{}, nil
			}).WithMiddleware(mw).
				Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
			if _, ok := result.(broker.Handled); !ok {
				t.Fatal("lost result")
			}
			if hooks != 1 || commits != 1 {
				t.Fatalf("hooks=%d commits=%d", hooks, commits)
			}
			if mode == adapter.CommitFailClosedKeepLock {
				if unlocks != 0 || !errors.Is(err, commitErr) {
					t.Fatalf("keep lock: error=%v unlocks=%d", err, unlocks)
				}
				entry, getErr := store.Get(context.Background(), "orders:one")
				if getErr != nil || (entry.Response != nil) != accepted {
					t.Fatalf("retained entry=%v error=%v", entry, getErr)
				}
			} else if unlocks != 1 || !errors.Is(err, unlockErr) || !errors.Is(err, commitErr) {
				t.Fatalf("mode=%v error=%v unlocks=%d", mode, err, unlocks)
			}
		}
	}
}

func TestCommitFailOpenReportsFailureAndUnlocks(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	marker := errors.New("store failed")
	observed := false
	spy := &storeSpy{Store: store, commit: func(context.Context, string, string, *idempo.Response, time.Duration) error { return marker }}
	mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithCommitErrorMode(adapter.CommitFailOpen),
		adapter.WithCommitErrorHandler(func(_ context.Context, _ broker.Delivery, err error) { observed = errors.Is(err, marker) }))
	if err != nil {
		t.Fatal(err)
	}
	d, err := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, nil
	}).WithMiddleware(mw).
		Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
	if _, ok := d.(broker.Handled); !ok || err != nil || !observed {
		t.Fatalf("result=%v error=%v observed=%v", d, err, observed)
	}
	if _, err = store.Get(context.Background(), "orders:one"); !errors.Is(err, idempo.ErrKeyNotFound) {
		t.Fatalf("lock retained: %v", err)
	}
}

func TestDetachedBoundedCleanupAndCallbackCause(t *testing.T) {
	for _, handled := range []bool{false, true} {
		store := memory.New()
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		marker := errors.New("callback failed")
		cleanupErr := errors.New("cleanup failed")
		type contextKey struct{}
		parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "retained"))
		check := func(ctx context.Context) {
			if ctx.Err() != nil || ctx.Value(contextKey{}) != "retained" {
				t.Error("lost detached parent values")
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Error("unbounded operation")
			}
		}
		spy := &storeSpy{Store: store, unlock: func(ctx context.Context, _, _ string) error {
			check(ctx)
			<-ctx.Done()
			return errors.Join(ctx.Err(), cleanupErr)
		},
			commit: func(ctx context.Context, _, _ string, _ *idempo.Response, _ time.Duration) error {
				check(ctx)
				<-ctx.Done()
				return errors.Join(ctx.Err(), cleanupErr)
			}}
		mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithCommitTimeout(time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			cancel()
			if handled {
				return broker.Handled{}, nil
			}
			return nil, marker
		}).WithMiddleware(mw).
			Handle(parent, &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
		if !errors.Is(err, cleanupErr) || !errors.Is(err, context.DeadlineExceeded) || (!handled && !errors.Is(err, marker)) {
			t.Fatalf("lost causes: %v", err)
		}
	}
}

func TestPanicCleanupPreservesOriginalAndCleanup(t *testing.T) {
	for _, cleanupFails := range []bool{false, true} {
		store := memory.New()
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		marker := errors.New("original panic")
		cleanupErr := errors.New("cleanup failed")
		spy := &storeSpy{Store: store}
		if cleanupFails {
			spy.unlock = func(context.Context, string, string) error { return cleanupErr }
		}
		mw, err := adapter.Middleware(idempo.NewEngine(spy))
		if err != nil {
			t.Fatal(err)
		}
		h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { panic(marker) }).WithMiddleware(mw)
		recovered := catchPanic(func() {
			_, _ = h.Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
		})
		if !cleanupFails {
			if recovered != marker {
				t.Fatalf("panic=%v", recovered)
			}
			if _, err = store.Get(context.Background(), "orders:one"); !errors.Is(err, idempo.ErrKeyNotFound) {
				t.Fatal("panic retained lock")
			}
		} else {
			err, ok := recovered.(error)
			var panicErr *broker.PanicError
			if !ok || !errors.Is(err, marker) || !errors.Is(err, cleanupErr) || !errors.As(err, &panicErr) || panicErr.Value != marker {
				t.Fatalf("lost panic/cleanup: %v", recovered)
			}
		}
	}
}

func TestCommitHookPanicDoesNotRemoveAmbiguousMarker(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	marker, hookErr := errors.New("commit outcome unknown"), errors.New("hook panic")
	spy := &storeSpy{Store: store, commit: func(ctx context.Context, key, token string, response *idempo.Response, ttl time.Duration) error {
		if err := store.SetResponse(ctx, key, token, response, ttl); err != nil {
			t.Fatal(err)
		}
		return marker
	}}
	mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithCommitErrorMode(adapter.CommitFailClosedUnlock),
		adapter.WithCommitErrorHandler(func(context.Context, broker.Delivery, error) { panic(hookErr) }))
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, nil
	}).WithMiddleware(mw)
	recovered := catchPanic(func() {
		_, _ = h.Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
	})
	joined, ok := recovered.(error)
	if !ok || !errors.Is(joined, marker) || !errors.Is(joined, hookErr) {
		t.Fatalf("lost panic causes: %v", recovered)
	}
	entry, err := store.Get(context.Background(), "orders:one")
	if err != nil || entry.Response == nil {
		t.Fatalf("marker removed: %v", err)
	}
}

func catchPanic(fn func()) (value any) { defer func() { value = recover() }(); fn(); return nil }
