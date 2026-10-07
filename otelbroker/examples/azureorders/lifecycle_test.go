package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	adapter "github.com/velmie/broker/azuresb"
)

func TestDeliverySpansEndForEveryFailure(t *testing.T) {
	for _, event := range []adapter.Observation{
		{Operation: adapter.OperationProcess, Outcome: adapter.OutcomeFailed, Err: errors.New("callback")},
		{Operation: adapter.OperationDisposition, Outcome: adapter.OutcomeFailed, Err: errors.New("disposition")},
		{Operation: adapter.OperationComplete, Outcome: adapter.OutcomeSuppressed, Err: context.Canceled},
		{Operation: adapter.OperationRenew, Outcome: adapter.OutcomeFailed, Err: errors.New("renewal")},
	} {
		recorder := tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		observer := &deliveryObserver{tracer: provider.Tracer("test")}
		hooks := observer.hooks()
		ctx := hooks.Begin(context.Background(), adapter.DeliveryInfo{Source: sourceName})
		hooks.Record(ctx, event)
		observer.finish(event.Err)
		spans := spanMap(t, recorder)
		if spans["delivery"].Status().Code != codes.Error {
			t.Fatal("failed delivery status lost")
		}
		if event.Outcome == adapter.OutcomeSuppressed && spans["complete"].SpanKind() != trace.SpanKindInternal {
			t.Fatal("suppressed completion described as native request")
		}
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

type failingProcessor struct {
	flushErr, shutdownErr         error
	flushContext, shutdownContext context.Context
}

func (*failingProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (*failingProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (p *failingProcessor) ForceFlush(ctx context.Context) error {
	p.flushContext = ctx
	return p.flushErr
}
func (p *failingProcessor) Shutdown(ctx context.Context) error {
	p.shutdownContext = ctx
	return p.shutdownErr
}

func TestTelemetryCleanupPreservesIndependentCausesAndBudgets(t *testing.T) {
	processor := &failingProcessor{flushErr: errors.New("flush failed"), shutdownErr: errors.New("shutdown failed")}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	err := finishTelemetry(provider)
	if !errors.Is(err, processor.flushErr) || !errors.Is(err, processor.shutdownErr) {
		t.Fatal("independent cleanup cause lost")
	}
	if processor.flushContext == processor.shutdownContext {
		t.Fatal("cleanup budgets are shared")
	}
	for _, ctx := range []context.Context{processor.flushContext, processor.shutdownContext} {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > telemetryBudget {
			t.Fatal("unbounded telemetry cleanup")
		}
	}
}
