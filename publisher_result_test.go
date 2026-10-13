package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/velmie/broker"
)

func TestTypedPublisherWithResultPreservesOrderAndInput(t *testing.T) {
	type command struct{ ID string }
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "caller")
	fields := broker.MessageMetadata{ID: "one", Headers: []broker.Header{{Name: "Binary", Value: []byte{0, 255}}}}
	var order []string
	wrap := func(name string) broker.PublisherMiddleware {
		return func(next broker.Publisher) broker.Publisher {
			return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
				order = append(order, name)
				return next.Publish(ctx, message)
			})
		}
	}
	publish := broker.NewTypedPublisherWithResult(broker.EncoderFunc(func(value any) ([]byte, error) {
		order = append(order, "encode")
		return json.Marshal(value)
	}), func(received context.Context, message broker.Message) (int, error) {
		order = append(order, "publish")
		if received != ctx || message.ID != fields.ID || !reflect.DeepEqual(message.Headers, fields.Headers) ||
			string(message.Body) != `{"ID":"one"}` {
			t.Fatalf("publication input changed: %+v", message)
		}
		return 42, nil
	}, func(received context.Context, value command) (broker.MessageMetadata, error) {
		if received != ctx || value.ID != "one" {
			t.Fatal("metadata input changed")
		}
		order = append(order, "metadata")
		return fields, nil
	}, wrap("outer"), wrap("inner"))
	result, err := publish(ctx, command{ID: "one"})
	if err != nil || result != 42 {
		t.Fatalf("result=%d err=%v", result, err)
	}
	if !reflect.DeepEqual(order, []string{"metadata", "encode", "outer", "inner", "publish"}) {
		t.Fatalf("order=%v", order)
	}
	if !reflect.DeepEqual(fields.Headers[0].Value, []byte{0, 255}) {
		t.Fatal("input headers changed")
	}
}

func TestTypedPublisherWithResultConcurrentCalls(t *testing.T) {
	var group sync.WaitGroup
	publish := broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		func(_ context.Context, message broker.Message) (string, error) { return message.ID, nil },
		func(_ context.Context, value string) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: value}, nil
		}, func(next broker.Publisher) broker.Publisher {
			called := false
			return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
				if called {
					return errors.New("middleware wrapper reused between invocations")
				}
				called = true
				return next.Publish(ctx, message)
			})
		})
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func(value string) {
			defer group.Done()
			result, err := publish(context.Background(), value)
			if err != nil || result != value {
				t.Errorf("value=%s result=%s err=%v", value, result, err)
			}
		}(strconv.Itoa(i))
	}
	group.Wait()
}

func TestTypedPublisherWithResultFailureContracts(t *testing.T) {
	marker := errors.New("publication marker")
	for _, test := range []struct {
		name        string
		stage       string
		metadataErr error
		encodeErr   error
		nativeErr   error
		invoke      func(context.Context, broker.Message, broker.Publisher) error
		calls       int
		reason      string
		cause       error
	}{
		{name: "metadata", stage: broker.StageMetadata, metadataErr: marker, cause: marker},
		{name: "encode", stage: broker.StageEncode, encodeErr: marker, cause: marker},
		{name: "native", stage: broker.StagePublish, nativeErr: marker, calls: 1, cause: marker},
		{name: "skipped success", stage: broker.StagePublish,
			invoke: func(context.Context, broker.Message, broker.Publisher) error { return nil }, reason: "missing_publication"},
		{name: "skipped error", stage: broker.StagePublish,
			invoke: func(context.Context, broker.Message, broker.Publisher) error { return marker }, cause: marker},
		{name: "swallowed native error", stage: broker.StagePublish, nativeErr: marker, calls: 1, cause: marker,
			invoke: func(ctx context.Context, msg broker.Message, next broker.Publisher) error {
				_ = next.Publish(ctx, msg)
				return nil
			}},
		{name: "post publication error", stage: broker.StagePublish, calls: 1, cause: marker,
			invoke: func(ctx context.Context, msg broker.Message, next broker.Publisher) error {
				if err := next.Publish(ctx, msg); err != nil {
					return err
				}
				return marker
			}},
		{name: "swallowed second call", stage: broker.StagePublish, calls: 1, reason: "repeated_publication",
			invoke: func(ctx context.Context, msg broker.Message, next broker.Publisher) error {
				_ = next.Publish(ctx, msg)
				_ = next.Publish(ctx, msg)
				return nil
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			publish := broker.NewTypedPublisherWithResult(broker.EncoderFunc(func(any) ([]byte, error) { return []byte("body"), test.encodeErr }),
				func(context.Context, broker.Message) (int, error) { calls++; return 42, test.nativeErr },
				func(context.Context, string) (broker.MessageMetadata, error) {
					return broker.MessageMetadata{}, test.metadataErr
				},
				func(next broker.Publisher) broker.Publisher {
					return broker.PublisherFunc(func(ctx context.Context, msg broker.Message) error {
						if test.invoke != nil {
							return test.invoke(ctx, msg, next)
						}
						return next.Publish(ctx, msg)
					})
				})
			result, err := publish(context.Background(), "value")
			var stage *broker.StageError
			if !errors.As(err, &stage) || stage.Stage != test.stage {
				t.Fatalf("stage=%v err=%v", stage, err)
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("original cause lost: %v", err)
			}
			var field *broker.FieldError
			hasField := errors.As(err, &field)
			if test.reason != "" {
				if !hasField || field.Field != "PublisherMiddleware" || field.Reason != test.reason {
					t.Fatalf("field=%v err=%v", field, err)
				}
			} else if hasField {
				t.Fatalf("unnecessary contract error: %v", field)
			}
			if calls != test.calls {
				t.Fatalf("native calls=%d want=%d", calls, test.calls)
			}
			want := 0
			if test.calls != 0 {
				want = 42
			}
			if result != want {
				t.Fatalf("native result=%d want=%d", result, want)
			}
		})
	}
}

func TestTypedPublisherWithResultOwnsMiddlewareConfiguration(t *testing.T) {
	middleware := []broker.PublisherMiddleware{func(next broker.Publisher) broker.Publisher { return next }}
	publish := broker.NewTypedPublisherWithResult(broker.EncoderFunc(json.Marshal),
		func(_ context.Context, message broker.Message) (string, error) { return message.ID, nil },
		func(_ context.Context, value string) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: value}, nil
		},
		middleware...)
	marker := errors.New("replacement middleware")
	middleware[0] = func(broker.Publisher) broker.Publisher {
		return broker.PublisherFunc(func(context.Context, broker.Message) error { return marker })
	}
	result, err := publish(context.Background(), "configured")
	if err != nil || result != "configured" {
		t.Fatalf("caller mutation changed configured publisher: result=%q err=%v", result, err)
	}
}
