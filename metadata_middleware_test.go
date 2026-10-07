package broker_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestStampInstanceIDReplacesAliasesWithoutMutatingInput(t *testing.T) {
	input := broker.Message{ID: "logical-1", Body: []byte("body"), Headers: []broker.Header{
		{Name: "Binary", Value: []byte{0xff, 0}},
		{Name: "instance-id", Value: []byte("old")},
		{Name: "Duplicate", Value: []byte("first")},
		{Name: "INSTANCE-ID", Value: []byte("other")},
		{Name: "Duplicate", Value: []byte("second")},
	}}
	original := []broker.Header{
		{Name: "Binary", Value: []byte{0xff, 0}},
		{Name: "instance-id", Value: []byte("old")},
		{Name: "Duplicate", Value: []byte("first")},
		{Name: "INSTANCE-ID", Value: []byte("other")},
		{Name: "Duplicate", Value: []byte("second")},
	}
	ctx := context.Background()
	cause := errors.New("transport unavailable")
	calls := 0
	publisher := broker.WrapPublisher(broker.PublisherFunc(func(received context.Context, message broker.Message) error {
		calls++
		if received != ctx || message.ID != input.ID || !reflect.DeepEqual(message.Body, input.Body) {
			t.Fatalf("publication context or data changed: %+v", message)
		}
		var retained []broker.Header
		stamps := 0
		for _, header := range message.Headers {
			if strings.EqualFold(header.Name, "Instance-Id") {
				stamps++
				if header.Name != "Instance-Id" || string(header.Value) != "worker-1" {
					t.Errorf("stamp: %+v", header)
				}
			} else {
				retained = append(retained, header)
			}
		}
		if stamps != 1 || !reflect.DeepEqual(retained, []broker.Header{original[0], original[2], original[4]}) {
			t.Fatalf("headers lost ordering, bytes, or replacement: %+v", message.Headers)
		}
		for i := range message.Headers {
			message.Headers[i].Value[0] = 'x'
		}
		return cause
	}), broker.StampInstanceID("worker-1"))
	publisher = broker.WrapPublisher(publisher, broker.StampInstanceID("worker-1"))
	if err := publisher.Publish(ctx, input); err != cause || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	if !reflect.DeepEqual(input.Headers, original) {
		t.Fatalf("caller headers mutated: %+v", input.Headers)
	}
}

func TestInstanceMiddlewareRejectsInvalidConfiguration(t *testing.T) {
	for _, id := range []string{"", "private-" + string([]byte{0xff})} {
		t.Run("id_"+id, func(t *testing.T) {
			publisher := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error {
				t.Fatal("invalid configuration reached publisher")
				return nil
			}), broker.StampInstanceID(id))
			assertMetadataRejection(t, publisher.Publish(context.Background(), broker.Message{}), "Instance-Id", id)
			handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Fatal("invalid configuration reached handler")
				return broker.Handled{}, nil
			}).WithMiddleware(broker.SkipOwnMessages(id))
			result, err := handler.Handle(context.Background(), replyDelivery{})
			if result != nil {
				t.Fatalf("invalid configuration returned %T", result)
			}
			assertMetadataRejection(t, err, "Instance-Id", id)
		})
	}
}

func TestSkipOwnMessagesPreservesDelegationAndRequirements(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers []broker.Header
		skip    bool
	}{
		{name: "absent"},
		{name: "empty", headers: []broker.Header{{Name: "Instance-Id"}}},
		{name: "own", headers: []broker.Header{{Name: "instance-ID", Value: []byte("worker-1")}}, skip: true},
		{name: "other", headers: []broker.Header{{Name: "Instance-Id", Value: []byte("worker-2")}}},
		{name: "value case differs", headers: []broker.Header{{Name: "Instance-Id", Value: []byte("WORKER-1")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			input := &replyDelivery{message: broker.Message{ID: "logical", Headers: test.headers}}
			cause := errors.New("business failed")
			wanted := broker.RetryAfter{Delay: time.Second, Cause: cause}
			calls := 0
			handler := broker.NewHandler(func(received context.Context, delivery broker.Delivery) (broker.Disposition, error) {
				calls++
				if received != ctx || delivery != input {
					t.Error("delegated context or delivery changed")
				}
				return wanted, cause
			}, broker.WithKeepAlive(time.Second)).WithMiddleware(broker.SkipOwnMessages("worker-1"))
			if !reflect.DeepEqual(handler.Requirements(), []broker.Requirement{broker.KeepAliveRequirement{Interval: time.Second}}) {
				t.Fatalf("requirements changed: %+v", handler.Requirements())
			}
			result, err := handler.Handle(ctx, input)
			if test.skip {
				if _, ok := result.(broker.Handled); !ok || err != nil || calls != 0 {
					t.Fatalf("own result=%T err=%v calls=%d", result, err, calls)
				}
			} else if !reflect.DeepEqual(result, wanted) || err != cause || calls != 1 {
				t.Fatalf("delegated result=%+v err=%v calls=%d", result, err, calls)
			}
		})
	}
}

func TestSkipOwnMessagesRejectsAmbiguousOrBinaryMetadata(t *testing.T) {
	for _, headers := range [][]broker.Header{
		{{Name: "Instance-Id", Value: []byte("private-value")}, {Name: "Instance-Id", Value: []byte("private-value")}},
		{{Name: "Instance-Id", Value: []byte("private-value")}, {Name: "instance-id"}},
		{{Name: "Instance-Id", Value: []byte("private-value\xff")}},
	} {
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Fatal("invalid metadata reached business")
			return broker.Handled{}, nil
		}).WithMiddleware(broker.SkipOwnMessages("worker-1"))
		result, err := handler.Handle(context.Background(), replyDelivery{message: broker.Message{Headers: headers}})
		if result != nil {
			t.Fatalf("invalid metadata returned %T", result)
		}
		assertMetadataRejection(t, err, "Instance-Id", "private-value")
	}
}

func TestValidateIDHeaderAllowsOptionalMatchingConvention(t *testing.T) {
	for _, message := range []broker.Message{
		{ID: "logical", Headers: []broker.Header{{Name: "Unrelated", Value: []byte{0xff}}}},
		{ID: "logical", Headers: []broker.Header{{Name: "x-logical-id", Value: []byte("logical")}}},
		{Headers: []broker.Header{{Name: "X-Logical-Id"}}},
	} {
		ctx := context.Background()
		cause := errors.New("publication failed")
		calls := 0
		publisher := broker.WrapPublisher(broker.PublisherFunc(func(received context.Context, got broker.Message) error {
			calls++
			if received != ctx || !reflect.DeepEqual(got, message) {
				t.Fatalf("publication changed: %+v", got)
			}
			return cause
		}), broker.ValidateIDHeader("X-Logical-Id"))
		if err := publisher.Publish(ctx, message); err != cause || calls != 1 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	}
}

func TestValidateIDHeaderRejectsInvalidConvention(t *testing.T) {
	for _, headers := range [][]broker.Header{
		{{Name: "X-Logical-Id", Value: []byte("private-value")}},
		{{Name: "X-Logical-Id"}},
		{{Name: "X-Logical-Id", Value: []byte("logical")}, {Name: "X-Logical-Id", Value: []byte("logical")}},
		{{Name: "X-Logical-Id", Value: []byte("logical")}, {Name: "x-logical-id", Value: []byte("logical")}},
		{{Name: "X-Logical-Id", Value: []byte("private-value\xff")}},
	} {
		publisher := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error {
			t.Fatal("invalid identity convention reached transport")
			return nil
		}), broker.ValidateIDHeader("X-Logical-Id"))
		err := publisher.Publish(context.Background(), broker.Message{ID: "logical", Headers: headers})
		assertMetadataRejection(t, err, "X-Logical-Id", "private-value")
	}
}

func TestValidateIDHeaderRejectsInvalidHeaderName(t *testing.T) {
	for _, name := range []string{"", "Bad Header", "Bad:Header", "Bad\r\nHeader", "Náme"} {
		publisher := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error {
			t.Fatal("invalid header configuration reached transport")
			return nil
		}), broker.ValidateIDHeader(name))
		assertMetadataRejection(t, publisher.Publish(context.Background(), broker.Message{}), "", name)
	}
}

func assertMetadataRejection(t *testing.T, err error, field, sensitive string) {
	t.Helper()
	var stage *broker.StageError
	if !errors.As(err, &stage) || stage.Stage != broker.StageMetadata || stage.Cause == nil {
		t.Fatalf("missing metadata stage/cause: %v", err)
	}
	if field != "" && !strings.Contains(err.Error(), field) {
		t.Errorf("missing affected field %q: %v", field, err)
	}
	if sensitive != "" && strings.Contains(err.Error(), sensitive) {
		t.Errorf("diagnostic exposed metadata: %v", err)
	}
}
