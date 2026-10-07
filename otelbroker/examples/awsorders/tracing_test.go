package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker/sqs"
)

func TestEmulatorTracedRoutes(t *testing.T) {
	endpoint := emulatorEndpoint(t)
	queues, topics := newClients(endpoint)
	for _, kind := range []string{"sqs", "sns-raw", "sns-envelope"} {
		t.Run(kind, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer func() {
				if err := finishTelemetry(provider); err != nil {
					t.Error(err)
				}
			}()
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			if err := runRoute(context.Background(), queues, topics, kind, provider.Tracer("test"), logger); err != nil {
				t.Fatal(err)
			}
			assertRouteSpans(t, recorder, kind)
		})
	}
}

func TestObserverEndsFailedAndSuppressedDeliveries(t *testing.T) {
	for _, event := range []sqs.Observation{
		{Operation: sqs.OperationProcess, Outcome: sqs.OutcomeFailed, Err: errors.New("callback")},
		{Operation: sqs.OperationDisposition, Outcome: sqs.OutcomeFailed, Err: errors.New("disposition")},
		{Operation: sqs.OperationDelete, Outcome: sqs.OutcomeSuppressed, Err: context.Canceled},
	} {
		recorder := tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		observation := &deliveryObserver{tracer: provider.Tracer("test")}
		hooks := observation.hooks()
		ctx := hooks.Begin(context.Background(), sqs.DeliveryInfo{Source: sourceName})
		hooks.Record(ctx, event)
		observation.finish(event.Err)
		if len(recorder.Started()) != len(recorder.Ended()) {
			t.Fatal("failure abandoned delivery span")
		}
		for _, span := range recorder.Ended() {
			if span.Status().Code != codes.Error {
				t.Fatal("failure status lost")
			}
			if event.Outcome == sqs.OutcomeSuppressed && span.Name() == "delete" && span.SpanKind() != trace.SpanKindInternal {
				t.Fatal("suppressed delete described as native request")
			}
		}
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

type failedProcessor struct {
	flushErr, shutdownErr         error
	flushContext, shutdownContext context.Context
}

func (*failedProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (*failedProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (p *failedProcessor) ForceFlush(ctx context.Context) error {
	p.flushContext = ctx
	return p.flushErr
}
func (p *failedProcessor) Shutdown(ctx context.Context) error {
	p.shutdownContext = ctx
	return p.shutdownErr
}

func TestProviderCleanupPreservesBothFailuresWithSeparateBudgets(t *testing.T) {
	processor := &failedProcessor{flushErr: errors.New("flush"), shutdownErr: errors.New("shutdown")}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	err := finishTelemetry(provider)
	if !errors.Is(err, processor.flushErr) || !errors.Is(err, processor.shutdownErr) {
		t.Fatalf("cleanup branches lost: %v", err)
	}
	if processor.flushContext == processor.shutdownContext {
		t.Fatal("cleanup must use separate contexts")
	}
	for _, ctx := range []context.Context{processor.flushContext, processor.shutdownContext} {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > telemetryBudget {
			t.Fatal("cleanup is unbounded")
		}
	}
}

func stringAttribute(span sdktrace.ReadOnlySpan, key string) string {
	for _, value := range span.Attributes() {
		if value.Key == attribute.Key(key) {
			return value.Value.AsString()
		}
	}
	return ""
}

func assertRouteSpans(t *testing.T, recorder *tracetest.SpanRecorder, kind string) {
	t.Helper()
	spans := recorder.Ended()
	if len(recorder.Started()) != len(spans) {
		t.Fatal("abandoned spans")
	}
	byName := make(map[string]sdktrace.ReadOnlySpan)
	for _, span := range spans {
		byName[span.Name()] = span
	}
	for _, name := range []string{"orders send", "orders process", "delivery", "delete", "publish"} {
		if byName[name] == nil {
			t.Fatalf("missing span %s", name)
		}
	}
	send, process, delivery := byName["orders send"], byName["orders process"], byName["delivery"]
	deleted, published := byName["delete"], byName["publish"]

	if len(process.Links()) != 1 || process.Links()[0].SpanContext.SpanID() != send.SpanContext().SpanID() {
		t.Fatal("publication propagation link missing")
	}
	if process.Parent().SpanID() != delivery.SpanContext().SpanID() || deleted.Parent().SpanID() != delivery.SpanContext().SpanID() {
		t.Fatal("processing and native deletion must be delivery siblings")
	}
	if published.Parent().SpanID() != send.SpanContext().SpanID() {
		t.Fatal("native publication must be distinct child of send")
	}
	if process.EndTime().After(deleted.StartTime()) || deleted.EndTime().After(delivery.EndTime()) {
		t.Fatal("effect/delete/delivery lifecycle ordering")
	}
	if stringAttribute(deleted, "broker.transport.outcome") != sqs.OutcomeAccepted ||
		stringAttribute(published, "broker.transport.outcome") != sqs.OutcomeAccepted {
		t.Fatal("missing accepted native outcomes")
	}
	if kind != "sqs" && stringAttribute(published, "messaging.system") != "aws_sns" {
		t.Fatal("SNS acceptance conflated with SQS deletion")
	}
}

func assertFailureSpans(t *testing.T, recorder *tracetest.SpanRecorder, unknownDelete, callbackFailed bool) {
	t.Helper()
	for _, span := range recorder.Ended() {
		if span.Name() == "delete" && unknownDelete {
			if stringAttribute(span, "broker.transport.outcome") != sqs.OutcomeUnknown || span.Status().Code != codes.Error {
				t.Fatal("unknown native delete outcome lost")
			}
		}
		if span.Name() == "orders process" {
			expected := "handled"
			if callbackFailed {
				expected = "failed"
			}
			if stringAttribute(span, "broker.processing.outcome") != expected {
				t.Fatal("processing outcome conflated with native deletion")
			}
		}
	}
}

type lifecycleProcessor struct {
	*tracetest.SpanRecorder
	check func(context.Context)
	steps []string
}

func (p *lifecycleProcessor) ForceFlush(ctx context.Context) error {
	p.check(ctx)
	p.steps = append(p.steps, "flush")
	return nil
}
func (p *lifecycleProcessor) Shutdown(ctx context.Context) error {
	p.check(ctx)
	p.steps = append(p.steps, "shutdown")
	return nil
}

func TestEmulatorWholeRunJoinsAndCleansBeforeTelemetry(t *testing.T) {
	endpoint := emulatorEndpoint(t)
	queues, topics := newClients(endpoint)
	processor := &lifecycleProcessor{SpanRecorder: tracetest.NewSpanRecorder()}
	processor.check = func(ctx context.Context) {
		if len(processor.Started()) == 0 || len(processor.Started()) != len(processor.Ended()) {
			t.Error("telemetry cleanup preceded joined spans")
		}
		queueList, err := queues.ListQueues(ctx, &nativeSQS.ListQueuesInput{QueueNamePrefix: aws.String("broker-traced-orders-")})
		if err != nil {
			t.Error(err)
		} else if len(queueList.QueueUrls) != 0 {
			t.Error("telemetry cleanup preceded queue cleanup")
		}
		topicList, err := topics.ListTopics(ctx, &nativeSNS.ListTopicsInput{})
		if err != nil {
			t.Error(err)
		} else {
			for _, topic := range topicList.Topics {
				if strings.Contains(aws.ToString(topic.TopicArn), ":broker-traced-orders-") {
					t.Error("topic remained at telemetry cleanup")
				}
			}
		}
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	if err := run(context.Background(), endpoint, provider, logger); err != nil {
		t.Fatal(err)
	}
	if strings.Join(processor.steps, ",") != "flush,shutdown" {
		t.Fatalf("unexpected provider lifecycle: %v", processor.steps)
	}
}
