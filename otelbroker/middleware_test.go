package otelbroker_test

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -destination ./mock/propagator.go -package mock_otelbroker go.opentelemetry.io/otel/propagation TextMapPropagator

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/velmie/broker"
	mock_broker "github.com/velmie/broker/mock"
	"github.com/velmie/broker/otelbroker"
	mock_otelbroker "github.com/velmie/broker/otelbroker/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/mock/gomock"
	"sync"
	"testing"
)

// this is wrapper for the global otel propagator
var globalPropagator = new(globalPropagatorSync)

func init() {
	otel.SetTextMapPropagator(globalPropagator)
}

func TestPublisherMiddlewareCallNext(t *testing.T) {
	mw := otelbroker.PublisherMiddleware()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	publisher := mock_broker.NewMockPublisher(ctrl)

	msg := broker.NewMessage()
	publisher.EXPECT().Publish("test", msg).Return(nil)

	err := mw(publisher).Publish("test", msg)

	require.NoError(t, err)
}

func TestPublisherMiddlewareTraceContextInjection(t *testing.T) {
	mw := otelbroker.PublisherMiddleware()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	publisher := mock_broker.NewMockPublisher(ctrl)
	propagator := mock_otelbroker.NewMockTextMapPropagator(ctrl)

	const traceCtxKey = "test-trace-context-key"
	propagator.EXPECT().
		Inject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, carrier propagation.TextMapCarrier) {
			carrier.Set(traceCtxKey, "test")
		})

	globalPropagator.set(propagator)
	defer globalPropagator.release()

	msg := broker.NewMessage()

	publisher.EXPECT().Publish("test", msg).DoAndReturn(func(topic string, m *broker.Message) error {
		assert.NotEmpty(t, m.Header.Get(traceCtxKey), "trace context should be injected")
		return nil
	})

	err := mw(publisher).Publish("test", msg)

	require.NoError(t, err)

}

func TestPublisherMiddlewareTraceContextInjectionCustomPropagator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	publisher := mock_broker.NewMockPublisher(ctrl)
	propagator := mock_otelbroker.NewMockTextMapPropagator(ctrl)

	const traceCtxKey = "test-trace-context-key"
	propagator.EXPECT().
		Inject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, carrier propagation.TextMapCarrier) {
			carrier.Set(traceCtxKey, "test")
		})

	mw := otelbroker.PublisherMiddleware(otelbroker.WithPropagator(propagator))

	msg := broker.NewMessage()

	publisher.EXPECT().Publish("test", msg).DoAndReturn(func(topic string, m *broker.Message) error {
		assert.NotEmpty(t, m.Header.Get(traceCtxKey), "trace context should be injected")
		return nil
	})

	err := mw(publisher).Publish("test", msg)

	require.NoError(t, err)
}

func TestConsumerMiddlewareTraceContextExtraction(t *testing.T) {
	mw := otelbroker.ConsumerMiddleware()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	propagator := mock_otelbroker.NewMockTextMapPropagator(ctrl)

	const traceCtxKey = "test-trace-context-key"
	propagator.EXPECT().
		Extract(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
			return context.WithValue(ctx, traceCtxKey, "test")
		})

	globalPropagator.set(propagator)
	defer globalPropagator.release()

	msg := broker.NewMessage()
	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(msg)
	event.EXPECT().Topic().Return("test")

	handler := broker.Handler(func(e broker.Event) error {
		assert.NotEmpty(t, msg.Context().Value(traceCtxKey), "trace context should be extracted")
		return nil
	})

	err := mw(handler)(event)

	require.NoError(t, err)
}

func TestConsumerMiddlewareTraceContextExtractionCustomPropagator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	propagator := mock_otelbroker.NewMockTextMapPropagator(ctrl)

	const traceCtxKey = "test-trace-context-key"
	propagator.EXPECT().
		Extract(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
			return context.WithValue(ctx, traceCtxKey, "test")
		})

	mw := otelbroker.ConsumerMiddleware(otelbroker.WithPropagator(propagator))

	msg := broker.NewMessage()
	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(msg)
	event.EXPECT().Topic().Return("test")

	handler := broker.Handler(func(e broker.Event) error {
		assert.NotEmpty(t, msg.Context().Value(traceCtxKey), "trace context should be extracted")
		return nil
	})

	err := mw(handler)(event)

	require.NoError(t, err)
}

type globalPropagatorSync struct {
	m       sync.Mutex
	current propagation.TextMapPropagator
}

func (gs *globalPropagatorSync) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	if gs.current == nil {
		return
	}
	gs.current.Inject(ctx, carrier)
}

func (gs *globalPropagatorSync) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	if gs.current == nil {
		return ctx
	}
	return gs.current.Extract(ctx, carrier)
}

func (gs *globalPropagatorSync) Fields() []string {
	if gs.current == nil {
		return []string{}
	}
	return gs.current.Fields()
}

func (gs *globalPropagatorSync) set(p propagation.TextMapPropagator) {
	gs.m.Lock()
	gs.current = p
}

func (gs *globalPropagatorSync) release() {
	gs.current = nil
	gs.m.Unlock()
}
