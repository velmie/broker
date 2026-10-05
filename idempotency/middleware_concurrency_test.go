package idempotency_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/idempotency"
)

func TestConcurrentCallsHaveOneOwner(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "fail-fast", true: "wait"}[wait], func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			admitted, release, duplicate := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var creates, calls atomic.Int32
			spy := &storeSpy{Store: store, create: func(
				ctx context.Context, key string, fp idempo.Fingerprint, ttl time.Duration,
			) (*idempo.Entry, bool, error) {
				entry, created, err := store.Create(ctx, key, fp, ttl)
				if creates.Add(1) == 2 {
					close(duplicate)
				}
				return entry, created, err
			}}
			mw, err := adapter.Middleware(idempo.NewEngine(spy, idempo.WithWaitForInProgress(wait), idempo.WithPollInterval(time.Millisecond)))
			if err != nil {
				t.Fatal(err)
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls.Add(1)
				close(admitted)
				<-release
				return broker.Handled{}, nil
			}).WithMiddleware(mw)
			d := &testDelivery{source: "orders", msg: broker.Message{ID: "one"}}
			owner, other := make(chan error, 1), make(chan error, 1)
			go func() { _, e := h.Handle(context.Background(), d); owner <- e }()
			<-admitted
			go func() { _, e := h.Handle(context.Background(), d); other <- e }()
			<-duplicate
			if !wait {
				if err = <-other; !errors.Is(err, idempo.ErrInProgress) {
					t.Fatalf("duplicate=%v", err)
				}
			}
			close(release)
			if err = <-owner; err != nil {
				t.Fatal(err)
			}
			if wait {
				if err = <-other; err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("business effects=%d", calls.Load())
			}
		})
	}
}

func TestWaitCancellationAndBoundedReacquisition(t *testing.T) {
	for _, scenario := range []string{"cancel", "timeout", "reacquire", "exhausted", "store"} {
		t.Run(scenario, func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := errors.New("get failed")
			creates := 0
			spy := &storeSpy{Store: store, create: func(
				ctx context.Context, key string, fp idempo.Fingerprint, ttl time.Duration,
			) (*idempo.Entry, bool, error) {
				creates++
				if scenario == "reacquire" && creates == 2 {
					return store.Create(ctx, key, fp, ttl)
				}
				if scenario == "cancel" {
					cancel()
				}
				return &idempo.Entry{Key: key, Fingerprint: fp, Token: "existing-owner"}, false, nil
			}, get: func(context.Context, string) (*idempo.Entry, error) {
				if scenario == "store" {
					return nil, marker
				}
				if scenario == "reacquire" || scenario == "exhausted" {
					return nil, idempo.ErrKeyNotFound
				}
				return nil, idempo.ErrInProgress
			}}
			mw, err := adapter.Middleware(idempo.NewEngine(spy, idempo.WithWaitForInProgress(true), idempo.WithPollInterval(time.Millisecond),
				idempo.WithInProgressTimeout(time.Millisecond*3)))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "timeout" {
				spy.get = func(context.Context, string) (*idempo.Entry, error) {
					return &idempo.Entry{Fingerprint: legacyFingerprint("orders", nil, nil, nil)}, nil
				}
			}
			calls := 0
			_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls++
				return broker.Handled{}, nil
			}).WithMiddleware(mw).
				Handle(ctx, &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
			expected := map[string]struct {
				err            error
				calls, creates int
			}{
				"cancel": {context.Canceled, 0, 1}, "timeout": {idempo.ErrWaitTimeout, 0, 1},
				"reacquire": {nil, 1, 2}, "exhausted": {idempo.ErrKeyNotFound, 0, 2}, "store": {marker, 0, 1},
			}[scenario]
			if !errors.Is(err, expected.err) || calls != expected.calls || creates != expected.creates {
				t.Fatalf("error=%v calls=%d creates=%d expected=%v", err, calls, creates, expected)
			}
		})
	}
}

func TestAcquisitionFailureRetainsCause(t *testing.T) {
	marker := errors.New("store unavailable")
	spy := &storeSpy{create: func(context.Context, string, idempo.Fingerprint, time.Duration) (*idempo.Entry, bool, error) {
		return nil, false, marker
	}}
	mw, err := adapter.Middleware(idempo.NewEngine(spy))
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("business after store failure")
		return nil, nil
	}).WithMiddleware(mw).
		Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
	var diagnostic *adapter.Error
	if !errors.Is(err, marker) || !errors.As(err, &diagnostic) || diagnostic.Operation != "acquire" {
		t.Fatalf("error=%v", err)
	}
}

func TestReplayClonesResponseAndPanicKeepsMarker(t *testing.T) {
	response := &idempo.Response{StatusCode: 201, Metadata: map[string][]string{"example": {"one"}}, Body: []byte("one"), Truncated: true}
	fp := legacyFingerprint("orders", nil, nil, nil)
	spy := &storeSpy{create: func(context.Context, string, idempo.Fingerprint, time.Duration) (*idempo.Entry, bool, error) {
		return &idempo.Entry{Fingerprint: fp, Response: response}, false, nil
	}}
	marker := errors.New("replay hook panic")
	mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithOnReplay(func(_ context.Context, _ broker.Delivery, r *idempo.Response) {
		if r.StatusCode != 201 || !r.Truncated {
			t.Error("response fields lost")
		}
		r.Body[0] = 'x'
		r.Metadata["example"][0] = "changed"
		panic(marker)
	}))
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("replay invoked business")
		return nil, nil
	}).WithMiddleware(mw)
	for range 2 {
		recovered := catchPanic(func() {
			_, _ = h.Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
		})
		if recovered != marker || string(response.Body) != "one" || response.Metadata["example"][0] != "one" {
			t.Fatalf("panic=%v response=%v", recovered, response)
		}
	}
}
