package idempotency_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/idempotency"
)

func TestLegacyFingerprintsRemainReadable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers []broker.Header
		old     map[string]string
	}{
		{name: "nil"}, {name: "empty", headers: []broker.Header{}, old: map[string]string{}},
		{name: "absent", headers: []broker.Header{{Name: "unrelated", Value: []byte{255}}}, old: map[string]string{}},
		{name: "empty-value", headers: []broker.Header{{Name: "X-A", Value: []byte{}}}, old: map[string]string{"X-A": ""}},
		{name: "values", headers: []broker.Header{{Name: "X-B", Value: []byte("b\n=\"a")}, {Name: "X-A", Value: []byte("one")}},
			old: map[string]string{"X-A": "one", "X-B": "b\n=\"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			engine := idempo.NewEngine(store)
			fp := legacyFingerprint("orders", []byte("body"), tc.old, []string{"X-B", "X-A"})
			owner, err := engine.Process(context.Background(), "orders:one", fp)
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.Commit(context.Background(), "orders:one", owner.Token, &idempo.Response{}); err != nil {
				t.Fatal(err)
			}
			mw, err := adapter.Middleware(engine, adapter.WithFingerprintHeaders(" X-B ", "X-A", "X-A", ""))
			if err != nil {
				t.Fatal(err)
			}
			result, err := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("legacy marker not replayed")
				return nil, nil
			}).
				WithMiddleware(mw).Handle(context.Background(), &testDelivery{
				source: "orders", msg: broker.Message{ID: " one ", Body: []byte("body"), Headers: tc.headers}})
			if _, ok := result.(broker.Handled); !ok || err != nil {
				t.Fatalf("replay=%v error=%v", result, err)
			}
		})
	}
}

func TestExplicitLegacySourceMigration(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	engine := idempo.NewEngine(store)
	fp := legacyFingerprint("old-topic", []byte("body"), nil, nil)
	owner, err := engine.Process(context.Background(), "app:old-topic:one", fp)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.Commit(context.Background(), "app:old-topic:one", owner.Token, &idempo.Response{}); err != nil {
		t.Fatal(err)
	}
	mw, err := adapter.Middleware(engine, adapter.WithUseSourceInKey(false), adapter.WithKeyPrefix("app:old-topic:"),
		adapter.WithFingerprintFunc(func(_ context.Context, d broker.Delivery) (idempo.Fingerprint, error) {
			return legacyFingerprint("old-topic", d.Message().Body, nil, nil), nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("migration replayed business")
		return nil, nil
	}).WithMiddleware(mw).
		Handle(context.Background(), &testDelivery{source: "new-source", msg: broker.Message{ID: "one", Body: []byte("body")}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSelectedHeadersRejectAmbiguityWithoutChangingUnrelatedBytes(t *testing.T) {
	for _, headers := range [][]broker.Header{
		{{Name: "X-A", Value: []byte("one")}, {Name: "X-A", Value: []byte("two")}},
		{{Name: "X-A", Value: []byte("one")}, {Name: "x-a", Value: []byte("one")}},
		{{Name: "x-a", Value: []byte("one")}}, {{Name: "X-A", Value: []byte{255}}},
	} {
		store := memory.New()
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		effects := 0
		spy := &storeSpy{Store: store, create: func(context.Context, string, idempo.Fingerprint, time.Duration) (*idempo.Entry, bool, error) {
			effects++
			return nil, false, nil
		}}
		mw, err := adapter.Middleware(idempo.NewEngine(spy), adapter.WithFingerprintHeaders("X-A"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			effects++
			return nil, nil
		}).WithMiddleware(mw).
			Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one", Headers: headers}})
		var diagnostic *adapter.Error
		if effects != 0 || !errors.As(err, &diagnostic) || diagnostic.Field != "FingerprintHeaders" || diagnostic.Cause == nil {
			t.Fatalf("error=%v effects=%d", err, effects)
		}
	}
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	headers := []broker.Header{{Name: "opaque", Value: []byte{0, 255}}, {Name: "OPAQUE", Value: []byte{254}},
		{Name: "X-A", Value: []byte("yes")}}
	before := []broker.Header{{Name: "opaque", Value: []byte{0, 255}}, {Name: "OPAQUE", Value: []byte{254}},
		{Name: "X-A", Value: []byte("yes")}}
	mw, err := adapter.Middleware(idempo.NewEngine(store), adapter.WithFingerprintHeaders("X-A"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
		if !reflect.DeepEqual(d.Message().Headers, before) {
			t.Error("headers changed")
		}
		return broker.Handled{}, nil
	}).WithMiddleware(mw).
		Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one", Headers: headers}})
	if err != nil || !reflect.DeepEqual(headers, before) {
		t.Fatalf("headers changed or error=%v", err)
	}
}

func TestKeyResolutionAndValidationCauses(t *testing.T) {
	for _, tc := range []struct {
		name, id, header, expected string
		options                    []adapter.Option
		missing                    bool
	}{
		{name: "id-first", id: " one ", header: "two", expected: "orders:one"},
		{name: "fallback", id: " ", header: " two ", expected: "orders:two"},
		{name: "custom", header: "two", expected: "orders:two", options: []adapter.Option{adapter.WithHeaderName("Custom")}},
		{name: "prefix", id: "one", expected: "app:orders:one", options: []adapter.Option{adapter.WithKeyPrefix("app:")}},
		{name: "unscoped", id: "one", expected: "one", options: []adapter.Option{adapter.WithUseSourceInKey(false)}},
		{name: "missing-bypass", missing: true},
		{name: "disabled-fallback", header: "two", missing: true, options: []adapter.Option{adapter.WithHeaderName("")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.New()
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			key := ""
			spy := &storeSpy{Store: store, create: func(
				ctx context.Context, k string, fp idempo.Fingerprint, ttl time.Duration,
			) (*idempo.Entry, bool, error) {
				key = k
				return store.Create(ctx, k, fp, ttl)
			}}
			mw, err := adapter.Middleware(idempo.NewEngine(spy), tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			name := adapter.DefaultHeaderName
			if tc.name == "custom" {
				name = "Custom"
			}
			calls := 0
			_, err = broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls++
				return broker.Handled{}, nil
			}).WithMiddleware(mw).
				Handle(context.Background(), &testDelivery{source: "orders",
					msg: broker.Message{ID: tc.id, Headers: []broker.Header{{Name: name, Value: []byte(tc.header)}}}})
			if err != nil || calls != 1 || key != tc.expected {
				t.Fatalf("key=%q calls=%d error=%v", key, calls, err)
			}
		})
	}
	marker := errors.New("validator secret original")
	for _, tc := range []struct {
		options []adapter.Option
		field   string
		cause   error
	}{
		{options: []adapter.Option{adapter.WithKeyValidator(func(string) error { return marker })}, field: "Message.ID", cause: marker},
		{options: []adapter.Option{adapter.WithFingerprintFunc(func(context.Context, broker.Delivery) (idempo.Fingerprint, error) {
			return idempo.Fingerprint{}, marker
		})}, field: "FingerprintFunc", cause: marker},
	} {
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
		}).WithMiddleware(mw).
			Handle(context.Background(), &testDelivery{source: "orders", msg: broker.Message{ID: "one"}})
		var diagnostic *adapter.Error
		if !errors.Is(err, tc.cause) || !errors.As(err, &diagnostic) || diagnostic.Field != tc.field {
			t.Fatalf("diagnostic=%v", err)
		}
	}
}

func legacyFingerprint(source string, body []byte, headers map[string]string, selected []string) idempo.Fingerprint {
	hash := func(value []byte) string { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }
	fp := idempo.Fingerprint{Operation: "consume", Target: source, BodyHash: hash(body)}
	if headers == nil || len(selected) == 0 {
		return fp
	}
	selected = append([]string(nil), selected...)
	sort.Strings(selected)
	var text strings.Builder
	for _, key := range selected {
		text.WriteString(strconv.Quote(key))
		text.WriteByte('=')
		text.WriteString(strconv.Quote(headers[key]))
		text.WriteByte('\n')
	}
	fp.HeadersHash = hash([]byte(text.String()))
	return fp
}
