package otelbroker_test

import (
	"context"
	"reflect"
	"testing"

	"go.opentelemetry.io/otel/propagation"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
)

type observingPropagator struct {
	keys   *[]string
	values *[][]string
}

func (p observingPropagator) Fields() []string                                   { return []string{"traceparent"} }
func (p observingPropagator) Inject(context.Context, propagation.TextMapCarrier) {}
func (p observingPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	*p.keys = carrier.Keys()
	getter := carrier.(propagation.ValuesGetter)
	for _, key := range *p.keys {
		*p.values = append(*p.values, getter.Values(key))
	}
	return ctx
}

func TestProcessingCarrierPreservesRepeatedAndAliasedHeaders(t *testing.T) {
	var keys []string
	var values [][]string
	h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { return broker.Handled{}, nil }).WithMiddleware(
		otelbroker.TraceProcessing("nats", otelbroker.WithPropagator(observingPropagator{keys: &keys, values: &values})),
	)
	_, err := h.Handle(context.Background(), delivery{message: broker.Message{Headers: []broker.Header{
		{Name: "traceparent", Value: []byte("one")},
		{Name: "traceparent", Value: []byte("two")},
		{Name: "TraceParent", Value: []byte("three")},
		{Name: "b3", Value: []byte("four")},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"traceparent", "TraceParent", "b3"}) ||
		!reflect.DeepEqual(values, [][]string{{"one", "two"}, {"three"}, {"four"}}) {
		t.Fatalf("keys=%v values=%v", keys, values)
	}
}
