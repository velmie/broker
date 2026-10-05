package otelbroker_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
)

type delivery struct{ message broker.Message }

func (d delivery) Message() broker.Message { return d.message }
func (delivery) Source() string            { return "orders" }

type contextKey struct{}

func TestPublishingOwnershipAndConcurrentSiblings(t *testing.T) {
	provider, recorder := recording(t)
	ctx, parent := provider.Tracer("test").Start(context.Background(), "parent")
	defer parent.End()
	original := broker.Message{ID: "logical-id", Body: []byte("body"), Headers: []broker.Header{
		{Name: "binary", Value: []byte{0xff, 0}}, {Name: "binary", Value: []byte("second")},
		{Name: "TraceParent", Value: []byte("old")},
	}}
	snapshot := broker.Message{ID: original.ID, Body: []byte("body"), Headers: []broker.Header{
		{Name: "binary", Value: []byte{0xff, 0}}, {Name: "binary", Value: []byte("second")}, {Name: "TraceParent", Value: []byte("old")},
	}}
	p := broker.WrapPublisher(broker.PublisherFunc(func(call context.Context, msg broker.Message) error {
		if msg.ID != original.ID || string(msg.Body) != "body" || len(msg.Headers) != 3 {
			t.Errorf("changed data: %#v", msg)
		}
		if !trace.SpanContextFromContext(call).IsValid() || msg.Headers[2].Name != "traceparent" {
			t.Error("missing injection")
		}
		msg.Headers[0].Value[0] = 'x'
		return nil
	}),
		otelbroker.TracePublishing("nats", "orders", otelbroker.WithPropagator(propagation.TraceContext{})))
	var group sync.WaitGroup
	for range 12 {
		group.Go(func() {
			if err := p.Publish(ctx, original); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if !reflect.DeepEqual(original, snapshot) {
		t.Fatalf("caller input mutated: %#v", original)
	}
	spans := recorder.Ended()
	if len(spans) != 12 {
		t.Fatalf("spans: %d", len(spans))
	}
	for _, span := range spans {
		if !span.Parent().Equal(parent.SpanContext()) || span.SpanKind() != trace.SpanKindProducer {
			t.Fatal("not sibling producer")
		}
	}
}

func TestProcessingPreservesContextRequirementsAndOutcomes(t *testing.T) {
	sentinel := errors.New("secret error value")
	for _, test := range []struct {
		name    string
		result  broker.Disposition
		err     error
		outcome string
		failed  bool
	}{
		{"handled", broker.Handled{}, nil, "handled", false},
		{"retry", broker.RetryAfter{Delay: time.Second, Cause: sentinel}, nil, "retry_requested", false},
		{"failed", nil, sentinel, "failed", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider, recorder := recording(t)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
			cancel()
			var observed error
			h := broker.NewHandler(func(call context.Context, _ broker.Delivery) (broker.Disposition, error) {
				if call.Err() != context.Canceled || call.Value(contextKey{}) != "value" {
					t.Error("context lost")
				}
				return test.result, test.err
			}, broker.WithKeepAlive(time.Second)).WithMiddleware(otelbroker.TraceProcessing("nats",
				otelbroker.WithTracer(provider.Tracer("test")),
				otelbroker.WithErrorRecorder(func(_ context.Context, _ trace.Span, err error) { observed = err })))
			result, err := h.Handle(ctx, delivery{})
			if !reflect.DeepEqual(result, test.result) || err != test.err || len(h.Requirements()) != 1 {
				t.Fatal("changed callback contract")
			}
			if (test.failed || test.name == "retry") && observed != sentinel {
				t.Fatal("cause lost")
			}
			span := recorder.Ended()[0]
			if attr(span, "broker.processing.outcome") != test.outcome || (span.Status().Code == codes.Error) != test.failed {
				t.Fatalf("wrong span outcome: %v", span.Status())
			}
		})
	}
}

func TestSafeErrorsAndPanics(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(map[bool]string{false: "propagated", true: "recovered"}[recovered], func(t *testing.T) {
			provider, recorder := recording(t)
			secret := errors.New("secret-panic-payload")
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) { panic(secret) })
			if recovered {
				h = h.WithMiddleware(broker.RecoverPanics())
			}
			h = h.WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("test"))))
			var escaped any
			func() {
				defer func() { escaped = recover() }()
				_, err := h.Handle(context.Background(), delivery{})
				if recovered && !errors.Is(err, secret) {
					t.Error("panic cause lost")
				}
			}()
			if !recovered && escaped != secret {
				t.Fatal("panic swallowed")
			}
			spans := recorder.Ended()
			if len(spans) != 1 || spans[0].Status().Code != codes.Error {
				t.Fatal("panic span not ended as failure")
			}
			for _, event := range spans[0].Events() {
				for _, value := range event.Attributes {
					if strings.Contains(value.Value.String(), secret.Error()) {
						t.Fatal("panic secret exported")
					}
				}
			}
			if strings.Contains(spans[0].Status().Description, secret.Error()) {
				t.Fatal("secret status")
			}
		})
	}
	provider, recorder := recording(t)
	secret := errors.New("secret-publish")
	p := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error { return secret }),
		otelbroker.TracePublishing("nats", "orders", otelbroker.WithTracer(provider.Tracer("test"))))
	if err := p.Publish(context.Background(), broker.Message{}); err != secret {
		t.Fatal("publish cause lost")
	}
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Status().Code != codes.Error || strings.Contains(spans[0].Status().Description, "secret") {
		t.Fatal("unsafe failed send")
	}
}

func TestTracingOptions(t *testing.T) {
	provider, recorder := recording(t)
	starts := []trace.SpanStartOption{trace.WithAttributes(attribute.String("custom", "kept"))}
	option := otelbroker.WithSpanStartOptions(starts...)
	starts[0] = trace.WithAttributes(attribute.String("custom", "mutated"))
	p := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error { return nil }),
		otelbroker.TracePublishing("nats", "orders",
			otelbroker.WithTracer(provider.Tracer("chosen")), option,
			otelbroker.WithSpanNameFormatter(func(operation, destination string, msg broker.Message) string {
				return operation + ":" + destination + ":" + msg.ID
			})))
	if err := p.Publish(context.Background(), broker.Message{ID: "id"}); err != nil {
		t.Fatal(err)
	}
	span := recorder.Ended()[0]
	if span.Name() != "send:orders:id" || attr(span, "custom") != "kept" || span.InstrumentationScope().Name != "chosen" {
		t.Fatal("options ignored")
	}
}

func TestCustomMiddlewareAddsSelectedMessageAttributes(t *testing.T) {
	provider, recorder := recording(t)
	annotate := func(ctx context.Context, message broker.Message) {
		// These synthetic identifiers are explicitly approved by this application.
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(semconv.MessagingMessageID(message.ID), semconv.MessagingMessageBodySize(len(message.Body)))
		for _, header := range message.Headers {
			if header.Name == "Correlation-Id" {
				span.SetAttributes(semconv.MessagingMessageConversationID(string(header.Value)))
			}
		}
	}
	message := broker.Message{ID: "order-created", Body: []byte("private-body"),
		Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("order-command")}}}
	cause := errors.New("publication failed")
	publisher := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error { return cause }),
		otelbroker.TracePublishing("nats", "orders", otelbroker.WithTracer(provider.Tracer("test"))),
		func(next broker.Publisher) broker.Publisher {
			return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
				annotate(ctx, message)
				return next.Publish(ctx, message)
			})
		})
	if err := publisher.Publish(context.Background(), message); err != cause {
		t.Fatal("custom publisher wrapper changed outcome")
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, nil
	}, broker.WithKeepAlive(time.Second)).WithMiddleware(
		otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("test"))),
		broker.HandlerMiddleware{Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
			return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
				annotate(ctx, delivery.Message())
				return next(ctx, delivery)
			}
		}})
	result, err := handler.Handle(context.Background(), delivery{message: message})
	if _, handled := result.(broker.Handled); !handled || err != nil || len(handler.Requirements()) != 1 {
		t.Fatalf("custom handler changed result=%T err=%v requirements=%v", result, err, handler.Requirements())
	}
	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans=%d", len(spans))
	}
	for _, span := range spans {
		if attr(span, "messaging.message.id") != message.ID || attr(span, "messaging.message.conversation_id") != "order-command" {
			t.Fatal("custom wrapper did not annotate the active operation span")
		}
		bodySize := int64(-1)
		for _, value := range span.Attributes() {
			if value.Key == semconv.MessagingMessageBodySizeKey {
				bodySize = value.Value.AsInt64()
			}
			if strings.Contains(value.Value.String(), "private-body") {
				t.Fatal("custom wrapper exported body")
			}
		}
		if bodySize != int64(len(message.Body)) {
			t.Fatalf("body size=%d", bodySize)
		}
	}
}

func recording(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return provider, recorder
}
func attr(span sdktrace.ReadOnlySpan, key string) string {
	for _, value := range span.Attributes() {
		if string(value.Key) == key {
			return value.Value.AsString()
		}
	}
	return ""
}

func TestGlobalProviderChosenAtInvocation(t *testing.T) {
	old := otel.GetTracerProvider()
	defer otel.SetTracerProvider(old)
	p := broker.WrapPublisher(broker.PublisherFunc(func(context.Context, broker.Message) error { return nil }),
		otelbroker.TracePublishing("nats", "orders"))
	provider, recorder := recording(t)
	otel.SetTracerProvider(provider)
	if err := p.Publish(context.Background(), broker.Message{}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.Ended()) != 1 {
		t.Fatal("global provider captured too early")
	}
	if recorder.Ended()[0].InstrumentationScope().SchemaURL != semconv.SchemaURL {
		t.Fatal("semantic convention schema missing")
	}
}

func TestCustomPropagationSelectsOnlyUniqueSafeFields(t *testing.T) {
	for _, test := range []struct {
		name    string
		headers []broker.Header
		want    string
	}{
		{"case-insensitive", []broker.Header{{Name: "x-TrAcE", Value: []byte("safe")}}, "safe"},
		{"duplicate", []broker.Header{{Name: "X-Trace", Value: []byte("safe")}, {Name: "x-trace", Value: []byte("safe")}}, ""},
		{"control", []broker.Header{{Name: "X-Trace", Value: []byte("unsafe\r\nvalue")}}, ""},
		{"binary", []broker.Header{{Name: "X-Trace", Value: []byte{0xff}}}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
				if ctx.Value(contextKey{}) != test.want {
					t.Errorf("extracted=%v want=%q", ctx.Value(contextKey{}), test.want)
				}
				return broker.Handled{}, nil
			}).WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithPropagator(selectedPropagator{})))
			if _, err := h.Handle(context.Background(), delivery{message: broker.Message{Headers: test.headers}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	p := broker.WrapPublisher(broker.PublisherFunc(func(_ context.Context, message broker.Message) error {
		if len(message.Headers) != 2 || message.Headers[0].Name != "unrelated" ||
			message.Headers[1].Name != "x-trace" || string(message.Headers[1].Value) != "injected" {
			t.Fatalf("unexpected headers: %#v", message.Headers)
		}
		return nil
	}), otelbroker.TracePublishing("nats", "orders", otelbroker.WithPropagator(selectedPropagator{})))
	err := p.Publish(context.Background(), broker.Message{Headers: []broker.Header{
		{Name: "X-Trace", Value: []byte("old")}, {Name: "x-trace", Value: []byte("duplicate")}, {Name: "unrelated", Value: []byte{0xff}},
	}})
	if err != nil {
		t.Fatal(err)
	}
}

type selectedPropagator struct{}

func (selectedPropagator) Fields() []string { return []string{"X-Trace"} }
func (selectedPropagator) Inject(_ context.Context, carrier propagation.TextMapCarrier) {
	carrier.Set("X-TRACE", "injected")
	carrier.Set("unowned", "ignored")
}
func (selectedPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return context.WithValue(ctx, contextKey{}, carrier.Get("X-TRACE"))
}
