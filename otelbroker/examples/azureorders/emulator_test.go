package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEmulatorTracedRoutes(t *testing.T) {
	connection, queue, topic, subscription := emulatorSettings(t)
	for _, settings := range []options{
		{queue: queue, receiveMode: peekLockMode},
		{topic: topic, subscription: subscription, receiveMode: peekLockMode},
		{queue: queue, receiveMode: receiveDeleteMode},
	} {
		t.Run(settings.receiveMode+settings.queue, func(t *testing.T) {
			settings.id = fmt.Sprintf("trace-%d", time.Now().UnixNano())
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			if err := run(context.Background(), connection, &settings, provider, logger); err != nil {
				t.Fatal(err)
			}
			assertRouteSpans(t, recorder, settings.receiveMode)
			assertEmpty(t, connection, &settings)
		})
	}
}

func TestEmulatorReplacementAfterInjectedSDKConnectionFailure(t *testing.T) {
	connection, queue, _, _ := emulatorSettings(t)
	settings := &options{queue: queue, receiveMode: peekLockMode, id: fmt.Sprintf("recovery-%d", time.Now().UnixNano())}
	good := nativeClient(t, connection, nil)
	var dials, created, closed, effects atomic.Int32
	bad := nativeClient(t, connection, func(context.Context, azservicebus.NewWebSocketConnArgs) (net.Conn, error) {
		dials.Add(1)
		return nil, io.EOF
	})
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = finishTelemetry(provider) }()
	sender := nativeSender(t, good, queue)
	if err := publishOrder(context.Background(), sender, settings.id, provider.Tracer("test")); err != nil {
		t.Fatal(err)
	}
	factory := func() (adapter.Receiver, error) {
		if created.Load() != closed.Load() {
			t.Error("replacement before old receiver closure")
		}
		selected := good
		if created.Load() == 0 {
			selected = bad
		}
		receiver, err := newReceiver(selected, settings)
		if err != nil {
			return nil, err
		}
		created.Add(1)
		return &countedReceiver{Receiver: receiver, closed: &closed}, nil
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, cancel := context.WithTimeout(context.Background(), routeBudget)
	defer cancel()
	err := consume(ctx, factory, settings, logger, provider.Tracer("test"), func(_ context.Context, delivery broker.Delivery, _ order) error {
		effects.Add(1)
		before := delivery.(adapter.MetadataReader).Metadata()
		before.ApplicationProperties["id"] = "changed"
		if delivery.(adapter.MetadataReader).Metadata().ApplicationProperties["id"] != settings.id {
			t.Error("metadata was not detached")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Load() != 2 || closed.Load() != 2 || dials.Load() != 1 || effects.Load() != 1 {
		t.Fatal("unexpected recovery lifecycle")
	}
	if strings.Count(logs.String(), "consumer restart scheduled") != 1 {
		t.Fatal("missing bounded replacement record")
	}
	assertRouteSpans(t, recorder, peekLockMode)
	assertEmpty(t, connection, settings)
}

func TestEmulatorExpiredLockIsUnknownWithoutRecovery(t *testing.T) {
	connection, queue, _, _ := emulatorSettings(t)
	settings := &options{queue: queue, receiveMode: peekLockMode, id: fmt.Sprintf("expiry-%d", time.Now().UnixNano())}
	client := nativeClient(t, connection, nil)
	sender := nativeSender(t, client, queue)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = finishTelemetry(provider) }()
	if err := publishOrder(context.Background(), sender, settings.id, provider.Tracer("test")); err != nil {
		t.Fatal(err)
	}
	defer completePending(t, client, settings)
	var created, closed, effects atomic.Int32
	factory := func() (adapter.Receiver, error) {
		receiver, err := newReceiver(client, settings)
		if err != nil {
			return nil, err
		}
		created.Add(1)
		return &countedReceiver{Receiver: receiver, closed: &closed}, nil
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := consume(ctx, factory, settings, logger, provider.Tracer("test"),
		func(ctx context.Context, delivery broker.Delivery, _ order) error {
			effects.Add(1)
			deadline := delivery.(adapter.MetadataReader).Metadata().LockedUntil
			if deadline.IsZero() {
				return errors.New("missing native lock deadline")
			}
			wait := time.NewTimer(time.Until(deadline) + time.Second)
			defer wait.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait.C:
				return nil
			}
		})
	var native *azservicebus.Error
	if !errors.As(err, &native) || native.Code != azservicebus.CodeLockLost {
		t.Fatalf("expected native LockLost: %v", err)
	}
	if created.Load() != 1 || closed.Load() != 1 || effects.Load() != 1 || strings.Contains(logs.String(), "consumer restart scheduled") {
		t.Fatal("uncertain completion caused recovery or effect replay")
	}
	spans := spanMap(t, recorder)
	if stringAttribute(spans["complete"], "broker.transport.outcome") != adapter.OutcomeUnknown ||
		spans["complete"].Status().Code != codes.Error {
		t.Fatal("unknown complete outcome missing")
	}
	if stringAttribute(spans["orders process"], "broker.processing.outcome") != "handled" {
		t.Fatal("successful processing conflated with failed completion")
	}
	if spans["abandon"] != nil {
		t.Fatal("opposite terminal operation issued")
	}
}

func emulatorSettings(t *testing.T) (connection, queue, topic, subscription string) {
	t.Helper()
	connection = os.Getenv("AZURESB_TEST_CONNECTION_STRING")
	if connection == "" {
		t.Skip("explicit Service Bus emulator configuration required")
	}
	if !strings.Contains(strings.ToLower(connection), "usedevelopmentemulator=true") {
		t.Fatal("tests require explicit development emulator connection")
	}
	queue, topic, subscription = os.Getenv("AZURESB_TEST_QUEUE"), os.Getenv("AZURESB_TEST_TOPIC"), os.Getenv("AZURESB_TEST_SUBSCRIPTION")
	if queue == "" || topic == "" || subscription == "" {
		t.Fatal("dedicated queue, topic and subscription are required")
	}
	return connection, queue, topic, subscription
}
func nativeClient(t *testing.T, connection string,
	hook func(context.Context, azservicebus.NewWebSocketConnArgs) (net.Conn, error)) *azservicebus.Client {
	t.Helper()
	client, err := azservicebus.NewClientFromConnectionString(connection, &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: -1}, NewWebSocketConn: hook})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeNative("close_client", client.Close); err != nil {
			t.Error(err)
		}
	})
	return client
}
func nativeSender(t *testing.T, client *azservicebus.Client, entity string) *azservicebus.Sender {
	t.Helper()
	sender, err := client.NewSender(entity, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeNative("close_sender", sender.Close); err != nil {
			t.Error(err)
		}
	})
	return sender
}
func assertEmpty(t *testing.T, connection string, settings *options) {
	t.Helper()
	client := nativeClient(t, connection, nil)
	receiver, err := newReceiver(client, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := closeNative("close", receiver.Close); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if len(messages) != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue not empty: count=%d err=%v", len(messages), err)
	}
}
func completePending(t *testing.T, client *azservicebus.Client, settings *options) {
	t.Helper()
	receiver, err := newReceiver(client, settings)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if closeErr := closeNative("close", receiver.Close); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
	defer cancel()
	messages, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil || len(messages) != 1 {
		t.Errorf("cleanup pending message: count=%d err=%v", len(messages), err)
		return
	}
	if messages[0].MessageID != settings.id {
		t.Error("unexpected pending logical ID")
		return
	}
	if err = receiver.CompleteMessage(ctx, messages[0], nil); err != nil {
		t.Error(err)
	}
}
func spanMap(t *testing.T, recorder *tracetest.SpanRecorder) map[string]sdktrace.ReadOnlySpan {
	t.Helper()
	spans := recorder.Ended()
	if len(recorder.Started()) != len(spans) {
		t.Fatal("abandoned spans")
	}
	result := make(map[string]sdktrace.ReadOnlySpan)
	for _, span := range spans {
		result[span.Name()] = span
	}
	return result
}
func assertRouteSpans(t *testing.T, recorder *tracetest.SpanRecorder, mode string) {
	t.Helper()
	spans := spanMap(t, recorder)
	for _, name := range []string{"delivery", "orders send", "orders process", "publish", "close"} {
		if spans[name] == nil {
			t.Fatalf("missing span %s", name)
		}
	}
	process, delivery, send := spans["orders process"], spans["delivery"], spans["orders send"]
	if len(process.Links()) != 1 || process.Links()[0].SpanContext.SpanID() != send.SpanContext().SpanID() {
		t.Fatal("missing publication propagation")
	}
	if process.Parent().SpanID() != delivery.SpanContext().SpanID() {
		t.Fatal("missing native delivery parent")
	}
	if stringAttribute(spans["publish"], "broker.transport.outcome") != adapter.OutcomeAccepted {
		t.Fatal("publication not recorded separately")
	}
	if mode == receiveDeleteMode {
		if spans["complete"] != nil || spans["abandon"] != nil {
			t.Fatal("ReceiveAndDelete invented settlement")
		}
		if delivery.EndTime().Before(spans["close"].EndTime()) {
			t.Fatal("ReceiveAndDelete delivery ended before joined receiver cleanup")
		}
		return
	}
	complete := spans["complete"]
	if complete == nil {
		t.Fatal("missing completion span")
	}
	if complete.Parent().SpanID() != delivery.SpanContext().SpanID() || process.EndTime().After(complete.StartTime()) {
		t.Fatal("completion must follow process as native sibling")
	}
	if complete.SpanKind() != trace.SpanKindClient || stringAttribute(complete, "broker.transport.outcome") != adapter.OutcomeAccepted {
		t.Fatal("native completion acceptance missing")
	}
}
func stringAttribute(span sdktrace.ReadOnlySpan, key string) string {
	for _, attr := range span.Attributes() {
		if attr.Key == attribute.Key(key) {
			return attr.Value.AsString()
		}
	}
	return ""
}
