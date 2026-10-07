package otelbroker_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
)

func TestProcessingRemoteParentOrAmbientLink(t *testing.T) {
	for _, ambient := range []bool{false, true} {
		name := "remote-parent"
		if ambient {
			name = "ambient-parent-remote-link"
		}
		t.Run(name, func(t *testing.T) {
			provider, recorder := recording(t)
			remote := remoteContext()
			carrier := propagation.MapCarrier{}
			propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), remote), carrier)
			ctx := context.Background()
			var parent trace.Span
			if ambient {
				ctx, parent = provider.Tracer("local").Start(ctx, "ambient")
				defer parent.End()
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				return broker.Handled{}, nil
			}).WithMiddleware(
				otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("process")),
					otelbroker.WithPropagator(propagation.TraceContext{})))
			input := delivery{message: broker.Message{Headers: []broker.Header{{Name: "TrAcEpArEnT", Value: []byte(carrier["traceparent"])}}}}
			if _, err := h.Handle(ctx, input); err != nil {
				t.Fatal(err)
			}
			span := recorder.Ended()[0]
			if ambient {
				if !span.Parent().Equal(parent.SpanContext()) || len(span.Links()) != 1 || !span.Links()[0].SpanContext.Equal(remote) {
					t.Fatal("ambient topology lost")
				}
			} else if !span.Parent().Equal(remote) || len(span.Links()) != 0 {
				t.Fatal("remote topology lost")
			}
		})
	}
}

func TestMalformedPropagationIsIgnored(t *testing.T) {
	for _, headers := range [][]broker.Header{
		{{Name: "traceparent", Value: []byte("bad")}},
		{{Name: "traceparent", Value: []byte{0xff}}},
		{{Name: "traceparent", Value: []byte("\nforged")}},
		{{Name: "traceparent", Value: []byte("00-11111111111111111111111111111111-2222222222222222-01")},
			{Name: "TRACEPARENT", Value: []byte("00-11111111111111111111111111111111-2222222222222222-01")}},
	} {
		provider, recorder := recording(t)
		calls := 0
		h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			calls++
			return broker.Handled{}, nil
		}).WithMiddleware(
			otelbroker.TraceProcessing("nats", otelbroker.WithTracer(provider.Tracer("test")),
				otelbroker.WithPropagator(propagation.TraceContext{})))
		if _, err := h.Handle(context.Background(), delivery{message: broker.Message{Headers: headers}}); err != nil {
			t.Fatal(err)
		}
		if calls != 1 || recorder.Ended()[0].Parent().IsValid() {
			t.Fatal("malformed telemetry became parent or failed processing")
		}
	}
}

func TestProviderSelectionPrecedesExtraction(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, sampled := range []bool{false, true} {
			global, globalRecords := recording(t)
			old := otel.GetTracerProvider()
			otel.SetTracerProvider(global)
			localRecords := tracetest.NewSpanRecorder()
			sampler := sdktrace.AlwaysSample()
			if !sampled {
				sampler = sdktrace.NeverSample()
			}
			local := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(localRecords))
			ctx, parent := local.Tracer("local").Start(context.Background(), "local-parent")
			override, overrideRecords := recording(t)
			opts := []otelbroker.Option{otelbroker.WithPropagator(propagation.TraceContext{})}
			if explicit {
				opts = append(opts, otelbroker.WithTracer(override.Tracer("explicit")))
			}
			carrier := propagation.MapCarrier{}
			propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), remoteContext()), carrier)
			var callbackContext trace.SpanContext
			h := broker.NewHandler(func(call context.Context, _ broker.Delivery) (broker.Disposition, error) {
				callbackContext = trace.SpanContextFromContext(call)
				return broker.Handled{}, nil
			}).WithMiddleware(otelbroker.TraceProcessing("nats", opts...))
			input := delivery{message: broker.Message{Headers: []broker.Header{{Name: "traceparent", Value: []byte(carrier["traceparent"])}}}}
			if _, err := h.Handle(ctx, input); err != nil {
				t.Fatal(err)
			}
			if callbackContext.TraceID() != parent.SpanContext().TraceID() {
				t.Fatal("ambient trace replaced")
			}
			if len(globalRecords.Ended()) != 0 {
				t.Fatal("global stole local provider")
			}
			if explicit && len(overrideRecords.Ended()) != 1 {
				t.Fatal("explicit provider ignored")
			}
			if !explicit && sampled && len(localRecords.Ended()) != 1 {
				t.Fatal("local provider ignored")
			}
			if !explicit && !sampled && callbackContext.IsSampled() {
				t.Fatal("nonrecording local provider ignored")
			}
			parent.End()
			if err := local.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			otel.SetTracerProvider(old)
		}
	}
}

func remoteContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true,
	})
}
