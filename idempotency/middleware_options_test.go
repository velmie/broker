package idempotency_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/idempotency"
)

func TestMiddlewareRejectsInvalidConfiguration(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, options := range [][]adapter.Option{
		{adapter.WithCommitTimeout(0)}, {adapter.WithCommitTimeout(-time.Second)},
		{adapter.WithCommitErrorMode(adapter.CommitErrorMode(99))},
		{adapter.WithCommitErrorMode(adapter.CommitFailOpen)},
	} {
		if _, err := adapter.Middleware(idempo.NewEngine(store), options...); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
	_, err := adapter.Middleware(idempo.NewEngine(store), adapter.WithCommitErrorMode(adapter.CommitFailOpen),
		adapter.WithCommitErrorHandler(func(context.Context, broker.Delivery, error) {}))
	if err != nil {
		t.Fatal(err)
	}
	if adapter.NewConfig().CommitErrorMode != adapter.CommitFailClosedKeepLock {
		t.Fatal("unsafe default")
	}
}

func TestMissingAndInvalidKeysHaveFieldEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		msg     broker.Message
		options []adapter.Option
		cause   error
		field   string
	}{
		{name: "required", options: []adapter.Option{adapter.WithRequireKey(true)}, cause: idempo.ErrMissingKey, field: "HeaderName"},
		{name: "oversized", msg: broker.Message{ID: strings.Repeat("x", 256)}, cause: idempo.ErrInvalidKey, field: "Message.ID"},
		{name: "binary-fallback", msg: broker.Message{Headers: []broker.Header{{Name: adapter.DefaultHeaderName, Value: []byte{255}}}},
			field: "HeaderName"},
		{name: "duplicate-fallback", msg: broker.Message{Headers: []broker.Header{
			{Name: adapter.DefaultHeaderName, Value: []byte("one")}, {Name: adapter.DefaultHeaderName, Value: []byte("one")}}}, field: "HeaderName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			mw, err := adapter.Middleware(idempo.NewEngine(store), tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("invalid callback")
				return nil, nil
			}).
				WithMiddleware(mw).Handle(context.Background(), &testDelivery{source: "orders", msg: tc.msg})
			var diagnostic *adapter.Error
			if !errors.As(err, &diagnostic) || diagnostic.Field != tc.field || diagnostic.Cause == nil ||
				(tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("diagnostic=%v", err)
			}
		})
	}
}
