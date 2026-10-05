package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/velmie/broker"
)

func TestTypedPublisherPreservesIdentityContextAndMetadata(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "publication")
	metadata := broker.MessageMetadata{ID: "command-1", Headers: []broker.Header{
		{Name: "Binary", Value: []byte{0xff, 0x00}},
		{Name: "X-Value", Value: []byte("one")}, {Name: "x-value", Value: []byte("two")},
	}}
	wantHeaders := []broker.Header{
		{Name: "Binary", Value: []byte{0xff, 0x00}},
		{Name: "X-Value", Value: []byte("one")}, {Name: "x-value", Value: []byte("two")},
	}
	var order []string
	pub := broker.PublisherFunc(func(received context.Context, message broker.Message) error {
		order = append(order, "publish")
		if received != ctx || message.ID != metadata.ID || string(message.Body) != `{"ID":"order-1"}` ||
			!reflect.DeepEqual(message.Headers, wantHeaders) {
			t.Fatalf("publication context/data changed: %+v", message)
		}
		return nil
	})
	encoder := broker.EncoderFunc(func(value any) ([]byte, error) {
		order = append(order, "encode")
		return json.Marshal(value)
	})
	publish := broker.NewTypedPublisher(encoder, pub, func(received context.Context, value command) (broker.MessageMetadata, error) {
		order = append(order, "metadata")
		if received != ctx || value.ID != "order-1" {
			t.Fatal("metadata input changed")
		}
		return metadata, nil
	})
	input := command{ID: "order-1"}
	for i := 0; i < 2; i++ {
		if err := publish(ctx, input); err != nil {
			t.Fatal(err)
		}
	}
	wantOrder := []string{"metadata", "encode", "publish", "metadata", "encode", "publish"}
	if !reflect.DeepEqual(order, wantOrder) || !reflect.DeepEqual(metadata.Headers, wantHeaders) || input.ID != "order-1" {
		t.Fatalf("order or caller data changed: order=%v, input=%+v, metadata=%+v", order, input, metadata)
	}
}

func TestTypedPublisherFailuresRetainStageAndCause(t *testing.T) {
	for _, failedStage := range []string{broker.StageMetadata, broker.StageEncode, broker.StagePublish} {
		t.Run(failedStage, func(t *testing.T) {
			cause := &publicationFailure{field: "order_id"}
			var order []string
			encoder := broker.EncoderFunc(func(any) ([]byte, error) {
				order = append(order, broker.StageEncode)
				if failedStage == broker.StageEncode {
					return nil, cause
				}
				return []byte(`{}`), nil
			})
			pub := broker.PublisherFunc(func(context.Context, broker.Message) error {
				order = append(order, broker.StagePublish)
				return cause
			})
			publish := broker.NewTypedPublisher(encoder, pub, func(context.Context, command) (broker.MessageMetadata, error) {
				order = append(order, broker.StageMetadata)
				if failedStage == broker.StageMetadata {
					return broker.MessageMetadata{}, cause
				}
				return broker.MessageMetadata{}, nil // A logical ID is optional for ordinary publication.
			})
			err := publish(context.Background(), command{})
			var stage *broker.StageError
			var original *publicationFailure
			if !errors.Is(err, cause) || !errors.As(err, &stage) || stage.Stage != failedStage ||
				!errors.As(err, &original) || original.field != "order_id" {
				t.Fatalf("lost diagnostic: %v", err)
			}
			want := []string{broker.StageMetadata}
			if failedStage != broker.StageMetadata {
				want = append(want, broker.StageEncode)
			}
			if failedStage == broker.StagePublish {
				want = append(want, broker.StagePublish)
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("work after failure: %v, want %v", order, want)
			}
		})
	}
}

func TestPublisherMiddlewareOrderAndOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	message := broker.Message{ID: "stable", Body: []byte("payload")}
	cause := errors.New("publication outcome unknown")
	var order []string
	wrap := func(name string) broker.PublisherMiddleware {
		return func(next broker.Publisher) broker.Publisher {
			return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
				order = append(order, name+" enter")
				err := next.Publish(ctx, message)
				order = append(order, name+" exit")
				return err
			})
		}
	}
	pub := &recordingPublisher{publish: func(received context.Context, got broker.Message) error {
		if received != ctx || !reflect.DeepEqual(got, message) {
			t.Fatal("wrapper lost context or data")
		}
		order = append(order, "publish")
		return cause
	}}
	if broker.WrapPublisher(pub) != pub {
		t.Fatal("empty wrapping changed publisher")
	}
	wrapped := broker.WrapPublisher(broker.WrapPublisher(pub, wrap("first"), wrap("second")), wrap("outer"))
	if err := wrapped.Publish(ctx, message); err != cause {
		t.Fatalf("wrapper changed error identity: %v", err)
	}
	want := []string{"outer enter", "first enter", "second enter", "publish", "second exit", "first exit", "outer exit"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
}

type recordingPublisher struct{ publish broker.PublisherFunc }

func (p *recordingPublisher) Publish(ctx context.Context, message broker.Message) error {
	return p.publish(ctx, message)
}

type publicationFailure struct{ field string }

func (e *publicationFailure) Error() string { return e.field + ": invalid reference" }
