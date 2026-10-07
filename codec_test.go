package broker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/velmie/broker"
)

func TestJSONValueAndPointerResults(t *testing.T) {
	var values []command
	valueHandler := broker.NewTypedHandler(broker.DecodeJSON[command], func(_ context.Context, v command) error {
		values = append(values, v)
		return nil
	})
	for _, body := range []string{`{"ID":"one"}`, `null`, `{}`} {
		if _, err := valueHandler.Handle(context.Background(), delivery{body}); err != nil {
			t.Fatal(err)
		}
	}
	if len(values) != 3 || values[0].ID != "one" || values[1].ID != "" || values[2].ID != "" {
		t.Fatal(values)
	}
	var pointers []*command
	pointerHandler := broker.NewTypedHandler(broker.DecodeJSON[*command], func(_ context.Context, v *command) error {
		pointers = append(pointers, v)
		return nil
	})
	for i := 0; i < 2; i++ {
		if _, err := pointerHandler.Handle(context.Background(), delivery{`{"ID":"one"}`}); err != nil {
			t.Fatal(err)
		}
	}
	if len(pointers) != 2 || pointers[0] == pointers[1] {
		t.Fatal("pointer DTO reused")
	}
	_, err := pointerHandler.Handle(context.Background(), delivery{`null`})
	var decode *broker.DecodeError
	if !errors.As(err, &decode) || len(pointers) != 2 {
		t.Fatalf("nil pointer admitted: %v", err)
	}
}

func TestDecoderFailureSkipsBindingAndBusiness(t *testing.T) {
	cause := errors.New("custom decode failure")
	for _, failed := range []bool{true, false} {
		calls := 0
		decoder := func([]byte) (*correlated, error) {
			if failed {
				return &correlated{calls: &calls}, cause
			}
			return nil, nil
		}
		h := broker.NewTypedHandler(decoder, func(context.Context, *correlated) error {
			t.Error("business invoked with failed or nil result")
			return nil
		}, broker.WithCorrelationIDBinding())
		_, err := h.Handle(context.Background(), headerDelivery{headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("trace-1")}}})
		var decode *broker.DecodeError
		if !errors.As(err, &decode) || (failed && !errors.Is(err, cause)) || calls != 0 {
			t.Fatalf("decode evidence lost or binding ran: %v, calls=%d", err, calls)
		}
	}
}

func TestCorrelationBindingUsesValueThenAddressOnce(t *testing.T) {
	t.Run("value_method", func(t *testing.T) {
		calls := 0
		h := broker.NewTypedHandler(func([]byte) (valueSetter, error) { return valueSetter{calls: &calls}, nil },
			func(_ context.Context, v valueSetter) error {
				if calls != 1 {
					t.Errorf("binding calls before business=%d", calls)
				}
				return nil
			}, broker.WithCorrelationIDBinding())
		input := headerDelivery{headers: []broker.Header{{Name: "correlation-id", Value: []byte("trace-1")}}}
		if _, err := h.Handle(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatal(calls)
		}
	})
	t.Run("value_address", func(t *testing.T) {
		calls := 0
		h := broker.NewTypedHandler(func([]byte) (correlated, error) { return correlated{calls: &calls}, nil },
			func(_ context.Context, v correlated) error {
				if calls != 1 || v.correlation != "trace-1" {
					t.Errorf("binding=%d, value=%q", calls, v.correlation)
				}
				return nil
			}, broker.WithCorrelationIDBinding())
		input := headerDelivery{headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("trace-1")}}}
		if _, err := h.Handle(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("pointer_method", func(t *testing.T) {
		calls := 0
		h := broker.NewTypedHandler(func([]byte) (*correlated, error) { return &correlated{calls: &calls}, nil },
			func(_ context.Context, v *correlated) error {
				if calls != 1 || v.correlation != "trace-1" {
					t.Errorf("binding=%d, value=%q", calls, v.correlation)
				}
				return nil
			}, broker.WithCorrelationIDBinding())
		input := headerDelivery{headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("trace-1")}}}
		if _, err := h.Handle(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBindingRejectsAmbiguousMetadataWithoutRetry(t *testing.T) {
	for _, headers := range [][]broker.Header{
		{{Name: "Correlation-Id", Value: []byte("one")}, {Name: "correlation-id", Value: []byte("two")}},
		{{Name: "Correlation-Id", Value: []byte{0xff}}},
	} {
		calls := 0
		h := broker.NewTypedHandler(func([]byte) (correlated, error) { return correlated{calls: &calls}, nil },
			func(context.Context, correlated) error { t.Error("invalid metadata reached business"); return nil },
			broker.WithCorrelationIDBinding(), broker.WithRedelivery(0, func(error) bool { t.Error("classified metadata failure"); return true }))
		_, err := h.Handle(context.Background(), headerDelivery{headers: headers})
		var stage *broker.StageError
		if !errors.As(err, &stage) || stage.Stage != broker.StageMetadata || calls != 0 {
			t.Fatalf("metadata error=%v binding=%d", err, calls)
		}
	}
}

type correlated struct {
	calls       *int
	correlation string
}

func (v *correlated) SetCorrelationID(id string) { *v.calls++; v.correlation = id }

type valueSetter struct{ calls *int }

func (v valueSetter) SetCorrelationID(string) { *v.calls++ }

type headerDelivery struct{ headers []broker.Header }

func (d headerDelivery) Message() broker.Message {
	return broker.Message{Body: []byte(`{}`), Headers: d.headers}
}
func (headerDelivery) Source() string { return "orders" }
