package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestReplyValidatesRoutesAndPublishesBeforeHandled(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	var order []string
	publisher := broker.PublisherFunc(func(received context.Context, message broker.Message) error {
		order = append(order, "publish")
		if received != ctx || message.ID != "response-1" || string(message.Body) != `{"ID":"accepted"}` ||
			len(message.Headers) != 1 || string(message.Headers[0].Value) != "request-1" {
			t.Errorf("reply context/data: %+v", message)
		}
		return nil
	})
	decoder := func(body []byte) (*replyCommand, error) {
		order = append(order, "decode")
		return broker.DecodeJSON[*replyCommand](body)
	}
	encoder := broker.EncoderFunc(func(value any) ([]byte, error) {
		order = append(order, "encode")
		return json.Marshal(value)
	})
	resolver := func(received context.Context, message broker.Message) (broker.ReplyTarget, error) {
		order = append(order, "resolve")
		if received != ctx || message.ID != "request-1" {
			t.Error("resolver lost context/request identity")
		}
		return broker.ReplyTarget{Publisher: publisher, Metadata: broker.MessageMetadata{
			ID: "response-1", Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte(message.ID)}},
		}}, nil
	}
	h := broker.NewReplyHandler(decoder, encoder, resolver, func(received context.Context, value *replyCommand) (command, error) {
		order = append(order, "handle")
		if received != ctx || value.ID != "order-1" || value.correlation != "trace-1" || value.bindings != 1 {
			t.Errorf("business input: %+v", value)
		}
		return command{ID: "accepted"}, nil
	}, broker.WithCorrelationIDBinding(), broker.WithKeepAlive(time.Second))
	input := replyDelivery{broker.Message{ID: "request-1", Body: []byte(`{"ID":"order-1"}`),
		Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("trace-1")}},
	}}
	result, err := h.Handle(ctx, input)
	if _, ok := result.(broker.Handled); !ok || err != nil {
		t.Fatalf("reply result: %T, %v", result, err)
	}
	want := []string{"decode", "resolve", "handle", "encode", "publish"}
	if !reflect.DeepEqual(order, want) || !reflect.DeepEqual(h.Requirements(), []broker.Requirement{broker.KeepAliveRequirement{
		Interval: time.Second,
	}}) {
		t.Fatalf("order=%v requirements=%v", order, h.Requirements())
	}
}

func TestReplyFailuresNeverRetryHelperWork(t *testing.T) {
	for _, failedStage := range []string{broker.StageDecode, broker.StageMetadata, broker.StageResolve,
		broker.StageHandle, broker.StageEncode, broker.StagePublish} {
		t.Run(failedStage, func(t *testing.T) {
			cause := &publicationFailure{field: "order_id"}
			var called []string
			classifierCalls := 0
			decoder := func([]byte) (command, error) {
				called = append(called, broker.StageDecode)
				if failedStage == broker.StageDecode {
					return command{}, cause
				}
				return command{ID: "order-1"}, nil
			}
			encoder := broker.EncoderFunc(func(any) ([]byte, error) {
				called = append(called, broker.StageEncode)
				if failedStage == broker.StageEncode {
					return nil, cause
				}
				return []byte(`{}`), nil
			})
			publisher := broker.PublisherFunc(func(context.Context, broker.Message) error {
				called = append(called, broker.StagePublish)
				return cause
			})
			resolver := func(context.Context, broker.Message) (broker.ReplyTarget, error) {
				called = append(called, broker.StageResolve)
				if failedStage == broker.StageResolve {
					return broker.ReplyTarget{}, cause
				}
				return broker.ReplyTarget{Publisher: publisher, Metadata: broker.MessageMetadata{ID: "reply-1"}}, nil
			}
			h := broker.NewReplyHandler(decoder, encoder, resolver, func(context.Context, command) (command, error) {
				called = append(called, broker.StageHandle)
				if failedStage == broker.StageHandle {
					// This nested publication belongs to the application's chosen boundary.
					return command{}, &broker.StageError{Stage: broker.StagePublish, Cause: cause}
				}
				return command{}, nil
			}, broker.WithCorrelationIDBinding(), broker.WithRedelivery(time.Second, func(error) bool {
				classifierCalls++
				return true
			}))
			input := replyDelivery{}
			if failedStage == broker.StageMetadata {
				input.message.Headers = []broker.Header{{Name: "Correlation-Id"}, {Name: "correlation-id"}}
			}
			result, err := h.Handle(context.Background(), input)
			assertReplyFailure(t, failedStage, cause, result, err, classifierCalls)
			want := []string{broker.StageDecode}
			if failedStage != broker.StageDecode && failedStage != broker.StageMetadata {
				for _, stage := range []string{broker.StageResolve, broker.StageHandle, broker.StageEncode, broker.StagePublish} {
					want = append(want, stage)
					if stage == failedStage {
						break
					}
				}
			}
			if !reflect.DeepEqual(called, want) {
				t.Fatalf("work after failure: %v, want %v", called, want)
			}
		})
	}
}

func TestReplyRejectsMissingDestinationOrIdentityBeforeBusiness(t *testing.T) {
	publisher := broker.PublisherFunc(func(context.Context, broker.Message) error {
		t.Error("invalid target reached transport")
		return nil
	})
	for _, target := range []broker.ReplyTarget{
		{}, {Publisher: publisher}, {Metadata: broker.MessageMetadata{ID: "reply-1"}},
	} {
		h := broker.NewReplyHandler(broker.DecodeJSON[command], broker.EncoderFunc(json.Marshal),
			func(context.Context, broker.Message) (broker.ReplyTarget, error) { return target, nil },
			func(context.Context, command) (command, error) {
				t.Error("invalid target reached business")
				return command{}, nil
			})
		result, err := h.Handle(context.Background(), delivery{`{}`})
		var stage *broker.StageError
		if result != nil || !errors.As(err, &stage) || stage.Stage != broker.StageResolve {
			t.Fatalf("invalid target accepted: %T, %v", result, err)
		}
	}
}

func TestReplyRejectsNilPointerOrMalformedInputBeforeResolution(t *testing.T) {
	for _, body := range []string{`null`, `{`} {
		h := broker.NewReplyHandler(broker.DecodeJSON[*command], broker.EncoderFunc(json.Marshal),
			func(context.Context, broker.Message) (broker.ReplyTarget, error) {
				t.Error("invalid decode reached resolver")
				return broker.ReplyTarget{}, nil
			}, func(context.Context, *command) (command, error) {
				t.Error("invalid decode reached business")
				return command{}, nil
			})
		result, err := h.Handle(context.Background(), delivery{body})
		var decode *broker.DecodeError
		if result != nil || !errors.As(err, &decode) {
			t.Fatalf("invalid input accepted: %T, %v", result, err)
		}
	}
}

type replyDelivery struct{ message broker.Message }

func (d replyDelivery) Message() broker.Message { return d.message }
func (replyDelivery) Source() string            { return "orders" }

type replyCommand struct {
	ID          string
	correlation string
	bindings    int
}

func (c *replyCommand) SetCorrelationID(id string) { c.correlation, c.bindings = id, c.bindings+1 }

func assertReplyFailure(t *testing.T, failedStage string, cause error, result broker.Disposition, err error, classifierCalls int) {
	t.Helper()
	if failedStage == broker.StageHandle {
		retry, ok := result.(broker.RetryAfter)
		if !ok || err != nil || classifierCalls != 1 || !errors.Is(retry.Cause, cause) {
			t.Fatalf("business retry: %T, %v, calls=%d", result, err, classifierCalls)
		}
		return
	}
	if result != nil || err == nil || classifierCalls != 0 {
		t.Fatalf("helper failure was handled/retried: %T, %v, calls=%d", result, err, classifierCalls)
	}
	if failedStage != broker.StageMetadata && !errors.Is(err, cause) {
		t.Fatalf("helper cause lost: %v", err)
	}
	if failedStage == broker.StageDecode {
		var decode *broker.DecodeError
		if !errors.As(err, &decode) {
			t.Fatalf("missing decode evidence: %v", err)
		}
		return
	}
	var stage *broker.StageError
	if !errors.As(err, &stage) || stage.Stage != failedStage {
		t.Fatalf("lost helper stage: %v", err)
	}
}
