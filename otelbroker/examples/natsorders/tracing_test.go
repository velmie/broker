package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/otelbroker"
)

func TestRunTracesReplyAndConfirmedSettlementThenClosesProvider(t *testing.T) {
	processor := newLifecycleProcessor()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	if err := run(ctx, serverURL, provider); err != nil {
		t.Fatal(err)
	}
	spans := processor.Ended()
	processes := findOperation(spans, "process")
	sends := findOperation(spans, "send")
	settlements := findOperation(spans, "settle")
	if len(processes) != 1 || len(sends) != 2 || len(settlements) != 1 {
		t.Fatalf("span topology: process=%d send=%d settle=%d all=%v", len(processes), len(sends), len(settlements), spanNames(spans))
	}
	process, settle := processes[0], settlements[0]
	assertSiblings(t, spans, process, settle)
	if spanAttribute(process, "broker.processing.outcome") != "handled" ||
		spanAttribute(settle, "broker.transport.outcome") != natsjs.OutcomeConfirmed {
		t.Fatal("processing and settlement outcomes lost")
	}
	if len(process.Links()) != 1 {
		t.Fatalf("remote producer link count=%d", len(process.Links()))
	}
	var command, reply sdktrace.ReadOnlySpan
	for _, send := range sends {
		if send.SpanContext().SpanID() == process.Links()[0].SpanContext.SpanID() &&
			send.SpanContext().TraceID() == process.Links()[0].SpanContext.TraceID() {
			command = send
		}
		if send.Parent().SpanID() == process.SpanContext().SpanID() {
			reply = send
		}
	}
	if command == nil || reply == nil {
		t.Fatalf("command propagation or reply parent lost: %v", spanNames(spans))
	}
	if reply.EndTime().After(settle.StartTime()) {
		t.Fatal("settlement preceded reply publication")
	}
	if processor.flushes != 1 || processor.shutdowns != 1 || processor.cleanupContextErr != nil {
		t.Fatalf("provider lifecycle flush=%d shutdown=%d context=%v", processor.flushes, processor.shutdowns, processor.cleanupContextErr)
	}
	assertAllEnded(t, processor)
}

func TestRunJoinsProviderFlushAndShutdownFailures(t *testing.T) {
	processor := newLifecycleProcessor()
	processor.flushErr = errors.New("flush failure")
	processor.shutdownErr = errors.New("shutdown failure")
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	err := run(ctx, serverURL, provider)
	if !errors.Is(err, processor.flushErr) || !errors.Is(err, processor.shutdownErr) {
		t.Fatalf("cleanup failure lost: %v", err)
	}
	if processor.flushes != 1 || processor.shutdowns != 1 || processor.cleanupContextErr != nil {
		t.Fatalf("cleanup lifecycle flush=%d shutdown=%d context=%v", processor.flushes, processor.shutdowns, processor.cleanupContextErr)
	}
}

func TestRunCleanupHasBudgetAfterCallerCancellation(t *testing.T) {
	processor := newLifecycleProcessor()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = run(ctx, serverURL, provider)
	if processor.flushes != 1 || processor.shutdowns != 1 || processor.cleanupContextErr != nil {
		t.Fatalf("canceled cleanup flush=%d shutdown=%d context=%v", processor.flushes, processor.shutdowns, processor.cleanupContextErr)
	}
	for _, budget := range processor.cleanupBudgets {
		if budget <= 0 || budget > 2*time.Second {
			t.Fatalf("cleanup budget=%v", budget)
		}
	}
}

func TestAdmittedAckTimeoutKeepsProcessingSuccessSeparate(t *testing.T) {
	nc, config := provision(t, time.Second)
	config.OperationTimeout = 75 * time.Millisecond
	processor := newLifecycleProcessor()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	tracer := provider.Tracer("orders-test")
	observer := newObserver(tracer)
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	var once sync.Once
	var terminals []natsjs.Observation
	config.Observer = natsjs.Observer{Begin: observer.Begin, Record: func(ctx context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeStarted {
			once.Do(func() { pauseServer(t) })
		}
		observer.Record(ctx, event)
		if isTerminal(event.Operation) && event.Outcome != natsjs.OutcomeStarted {
			terminals = append(terminals, event)
			stop()
		}
	}}
	calls := 0
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		calls++
		return broker.Handled{}, nil
	}).WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithTracer(tracer)))
	publishOne(t, nc, &config)
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(ctx, handler); err == nil {
		t.Fatal("ACK timeout reported success")
	}
	if calls != 1 || len(terminals) != 1 || terminals[0].Operation != natsjs.OperationAck || terminals[0].Outcome != natsjs.OutcomeUnknown {
		t.Fatalf("calls=%d terminal attempts=%+v", calls, terminals)
	}
	spans := processor.Ended()
	process := exactlyOne(t, findOperation(spans, "process"))
	settle := exactlyOne(t, findOperation(spans, "settle"))
	assertSiblings(t, spans, process, settle)
	if spanAttribute(process, "broker.processing.outcome") != "handled" || spanAttribute(settle, "broker.transport.outcome") != "unknown" {
		t.Fatal("unknown settlement rewrote processing outcome")
	}
	assertAllEnded(t, processor)
}

func TestRenewalAndForcedCancellationShareDeliveryAndEndSpans(t *testing.T) {
	nc, config := provision(t, 200*time.Millisecond)
	config.Shutdown = broker.ShutdownPolicy{Mode: broker.ShutdownCancel}
	processor := newLifecycleProcessor()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	tracer := provider.Tracer("orders-test")
	observer := newObserver(tracer)
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	config.Observer = natsjs.Observer{Begin: observer.Begin, Record: func(ctx context.Context, event natsjs.Observation) {
		observer.Record(ctx, event)
		if event.Operation == natsjs.OperationRenew && event.Outcome != natsjs.OutcomeStarted {
			stop()
		}
	}}
	calls := 0
	handler := broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
		calls++
		<-ctx.Done()
		return broker.Handled{}, nil
	}, broker.WithKeepAlive(10*time.Millisecond)).WithMiddleware(otelbroker.TraceProcessing("nats", otelbroker.WithTracer(tracer)))
	publishOne(t, nc, &config)
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if calls != 1 {
		t.Fatalf("business calls=%d", calls)
	}
	spans := processor.Ended()
	process := exactlyOne(t, findOperation(spans, "process"))
	var suppressed []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if spanAttribute(span, "broker.transport.outcome") == "suppressed" {
			suppressed = append(suppressed, span)
		}
	}
	settle := exactlyOne(t, suppressed)
	assertSiblings(t, spans, process, settle)
	if spanAttribute(settle, "broker.transport.outcome") != "suppressed" {
		t.Fatal("suppressed settlement was not explicit")
	}
	renewals := findOperation(spans, "renew")
	if len(renewals) == 0 {
		t.Fatal("renewal span missing")
	}
	for _, renewal := range renewals {
		if renewal.Parent().SpanID() != process.Parent().SpanID() {
			t.Fatal("renewal delivery correlation lost")
		}
	}
	assertAllEnded(t, processor)
}

func TestObserverUsesReportedDurationAndDoesNotExportRawErrors(t *testing.T) {
	processor := newLifecycleProcessor()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	observer := newObserver(provider.Tracer("observer-test"))
	ctx := observer.Begin(context.Background(), natsjs.DeliveryInfo{Source: "orders"})
	observer.Record(ctx, natsjs.Observation{Operation: natsjs.OperationProcess, Outcome: natsjs.OutcomeSucceeded})
	observer.Record(ctx, natsjs.Observation{Operation: natsjs.OperationAck, Outcome: natsjs.OutcomeStarted})
	duration := 35 * time.Millisecond
	observer.Record(ctx, natsjs.Observation{Operation: natsjs.OperationAck, Outcome: natsjs.OutcomeUnknown,
		Duration: duration, Err: errors.New("secret-error-value")})
	spans := processor.Ended()
	if len(findOperation(spans, "process")) != 0 {
		t.Fatal("observer duplicated process instrumentation")
	}
	settle := exactlyOne(t, findOperation(spans, "settle"))
	if settle.EndTime().Sub(settle.StartTime()) != duration {
		t.Fatalf("duration=%v", settle.EndTime().Sub(settle.StartTime()))
	}
	for _, span := range spans {
		if strings.Contains(span.Status().Description, "secret-error-value") {
			t.Fatal("raw error in status")
		}
		assertNoSecretAttributes(t, span.Attributes())
		for _, event := range span.Events() {
			assertNoSecretAttributes(t, event.Attributes)
		}
	}
	assertAllEnded(t, processor)
}

func assertNoSecretAttributes(t *testing.T, attrs []attribute.KeyValue) {
	t.Helper()
	for _, attr := range attrs {
		if strings.Contains(attr.Value.String(), "secret-error-value") {
			t.Fatalf("raw error attribute: %s", attr.Key)
		}
	}
}

func spanAttribute(span sdktrace.ReadOnlySpan, name string) string {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == name {
			return attr.Value.AsString()
		}
	}
	return ""
}
func findOperation(spans []sdktrace.ReadOnlySpan, operation string) []sdktrace.ReadOnlySpan {
	var found []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if spanAttribute(span, "messaging.operation.type") == operation {
			found = append(found, span)
		}
	}
	return found
}
func exactlyOne(t *testing.T, spans []sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	t.Helper()
	if len(spans) != 1 {
		t.Fatalf("expected one span, got %v", spanNames(spans))
	}
	return spans[0]
}
func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	var names []string
	for _, span := range spans {
		names = append(names, span.Name())
	}
	return names
}
func assertSiblings(t *testing.T, spans []sdktrace.ReadOnlySpan, process, settle sdktrace.ReadOnlySpan) {
	t.Helper()
	if !process.Parent().IsValid() || process.Parent().SpanID() != settle.Parent().SpanID() {
		t.Fatal("process and settlement lack shared delivery parent")
	}
	for _, span := range spans {
		if span.SpanContext().SpanID() == process.Parent().SpanID() {
			return
		}
	}
	t.Fatal("delivery parent not ended")
}
func assertAllEnded(t *testing.T, processor *lifecycleProcessor) {
	t.Helper()
	if len(processor.Started()) != len(processor.Ended()) {
		t.Fatalf("started=%d ended=%d", len(processor.Started()), len(processor.Ended()))
	}
}

type lifecycleProcessor struct {
	*tracetest.SpanRecorder
	cleanupBudgets                           []time.Duration
	flushes, shutdowns                       int
	flushErr, shutdownErr, cleanupContextErr error
}

func newLifecycleProcessor() *lifecycleProcessor {
	return &lifecycleProcessor{SpanRecorder: tracetest.NewSpanRecorder()}
}
func (p *lifecycleProcessor) ForceFlush(ctx context.Context) error {
	p.flushes++
	p.recordCleanupContext(ctx)
	return p.flushErr
}
func (p *lifecycleProcessor) Shutdown(ctx context.Context) error {
	p.shutdowns++
	p.recordCleanupContext(ctx)
	return p.shutdownErr
}

func (p *lifecycleProcessor) recordCleanupContext(ctx context.Context) {
	p.cleanupContextErr = errors.Join(p.cleanupContextErr, ctx.Err())
	deadline, ok := ctx.Deadline()
	if !ok {
		p.cleanupBudgets = append(p.cleanupBudgets, -1)
		return
	}
	p.cleanupBudgets = append(p.cleanupBudgets, time.Until(deadline))
}
