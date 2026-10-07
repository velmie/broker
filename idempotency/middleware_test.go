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

func TestMiddlewareHandledReplayAndConflict(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	engine := idempo.NewEngine(store)
	calls, replays := 0, 0
	mw, err := adapter.Middleware(engine, adapter.WithOnReplay(func(context.Context, broker.Delivery, *idempo.Response) { replays++ }))
	if err != nil {
		t.Fatal(err)
	}
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		return broker.Handled{}, nil
	}, broker.WithKeepAlive(time.Second)).WithMiddleware(mw)
	d := &testDelivery{source: "orders", msg: broker.Message{ID: " one ", Body: []byte("body")}}
	for range 2 {
		result, handleErr := h.Handle(context.Background(), d)
		if _, ok := result.(broker.Handled); !ok || handleErr != nil {
			t.Fatalf("result=%v error=%v", result, handleErr)
		}
	}
	if calls != 1 || replays != 1 || len(h.Requirements()) != 1 {
		t.Fatalf("calls=%d replays=%d requirements=%v", calls, replays, h.Requirements())
	}
	d.msg.Body = []byte("other")
	if _, err = h.Handle(context.Background(), d); !errors.Is(err, idempo.ErrKeyConflict) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestMiddlewareOnlyHandledCommits(t *testing.T) {
	marker := errors.New("application failure")
	for _, tc := range []struct {
		name        string
		disposition broker.Disposition
		err         error
	}{
		{name: "retry", disposition: broker.RetryAfter{Delay: time.Second}},
		{name: "custom", disposition: customDisposition{}}, {name: "nil"},
		{name: "error", disposition: broker.Handled{}, err: marker},
		{name: "pointer-handled", disposition: &broker.Handled{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			mw, err := adapter.Middleware(idempo.NewEngine(store))
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls++
				return tc.disposition, tc.err
			}).WithMiddleware(mw)
			for range 2 {
				d, e := h.Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
				if d != tc.disposition || e != tc.err {
					t.Fatalf("result=%v error=%v", d, e)
				}
			}
			if calls != 2 {
				t.Fatal("non-completion committed")
			}
		})
	}
}

type testDelivery struct {
	source string
	msg    broker.Message
}

func (d *testDelivery) Source() string          { return d.source }
func (d *testDelivery) Message() broker.Message { return d.msg }

type customDisposition struct{}

func (customDisposition) DeliveryDisposition() {}
