package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestRecoverPanicsRetainsCauseAndStackWithoutRetry(t *testing.T) {
	typed := &processingFailure{field: "account_id", entity: "order-7", secret: "secret-token"}
	for _, value := range []any{typed, "secret-panic", nil} {
		calls := 0
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			calls++
			panic(value)
		}, broker.WithKeepAlive(time.Second)).WithMiddleware(broker.RecoverPanics())
		result, err := handler.Handle(context.Background(), replyDelivery{})
		var recovered *broker.PanicError
		if result != nil || calls != 1 || !errors.As(err, &recovered) {
			t.Fatalf("panic result=%T calls=%d err=%v", result, calls, err)
		}
		if len(recovered.Stack) == 0 || !bytes.Contains(recovered.Stack, []byte("TestRecoverPanics")) {
			t.Fatalf("missing panic stack: %q", recovered.Stack)
		}
		if value != nil && recovered.Value != value {
			t.Fatalf("panic value lost: %T", recovered.Value)
		}
		if value == typed {
			var original *processingFailure
			if !errors.Is(err, typed) || !errors.As(err, &original) || original != typed {
				t.Fatal("panic error cause lost")
			}
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("panic diagnostics exposed value: %v", err)
		}
		assertProcessingRequirements(t, handler)
	}
}

func TestProcessingMiddlewarePreservesOrdinaryDelegation(t *testing.T) {
	for _, cause := range []error{nil, errors.New("business failure")} {
		ctx := context.Background()
		input := &replyDelivery{message: broker.Message{ID: "logical"}}
		wanted := broker.RetryAfter{Delay: time.Second, Cause: cause}
		calls := 0
		handler := broker.NewHandler(func(received context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			calls++
			if received != ctx || delivery != input {
				t.Error("context or delivery changed")
			}
			return wanted, cause
		}, broker.WithKeepAlive(time.Second)).WithMiddleware(broker.RecoverPanics())
		result, err := handler.Handle(ctx, input)
		if !reflect.DeepEqual(result, wanted) || err != cause || calls != 1 {
			t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
		}
		assertProcessingRequirements(t, handler)
	}
}

func TestLogProcessingPreservesAndRedactsOriginalFailure(t *testing.T) {
	cause := &processingFailure{field: "account_id", entity: "order-7", secret: "private-cause"}
	staged := &broker.StageError{Stage: broker.StagePublish, Cause: cause}
	var output bytes.Buffer
	capture := newProcessingCapture(&output)
	ctx := context.Background()
	// Synthetic URL credentials exercise removal at the diagnostic boundary.
	//nolint:gosec // Reserved example.test fixture, not a credential.
	input := &processingDelivery{
		message: broker.Message{Body: []byte("private-body"),
			Headers: []broker.Header{{Name: "Authorization", Value: []byte("private-header")}}},
		source: "nats://user:password@example.test/orders?token=query-secret#fragment-secret",
	}
	calls := 0
	handler := broker.NewHandler(func(received context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		calls++
		if received != ctx || delivery != input {
			t.Error("logging changed context or delivery")
		}
		return broker.Handled{}, staged
	}, broker.WithKeepAlive(time.Second)).WithMiddleware(broker.LogProcessing(slog.New(capture), broker.ProcessingLogConfig{}))
	result, err := handler.Handle(ctx, input)
	if _, ok := result.(broker.Handled); !ok || err != staged || calls != 1 {
		t.Fatalf("result=%T err=%v calls=%d", result, err, calls)
	}
	assertProcessingRequirements(t, handler)
	if len(capture.records) != 1 {
		t.Fatalf("records=%d", len(capture.records))
	}
	record := capture.records[0]
	attrs := processingAttrs(&record)
	if attrs["error"].Any() != staged || record.Level != slog.LevelError {
		t.Fatalf("original error or severity lost: %+v", attrs)
	}
	assertProcessingFields(t, attrs, "failed", broker.StagePublish)
	text := output.String()
	for _, forbidden := range []string{"private-cause", "private-body", "private-header",
		"password", "query-secret", "fragment-secret", "user:"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("serialized diagnostics expose %q: %s", forbidden, text)
		}
	}
	for _, wanted := range []string{"account_id", "order-7", "dependency rejected field", "example.test"} {
		if !strings.Contains(text, wanted) {
			t.Errorf("serialized diagnostics lost %q: %s", wanted, text)
		}
	}
}

func TestLogProcessingSuccessAndSeverityPolicy(t *testing.T) {
	cause := errors.New("failure")
	tests := []struct {
		name    string
		result  broker.Disposition
		err     error
		config  broker.ProcessingLogConfig
		count   int
		level   slog.Level
		outcome string
	}{
		{name: "silent success", result: broker.Handled{}},
		{name: "enabled success", result: broker.Handled{}, config: broker.ProcessingLogConfig{LogSuccess: true},
			count: 1, level: slog.LevelInfo, outcome: "handled"},
		{name: "decode rejection", err: &broker.DecodeError{Cause: cause},
			count: 1, level: slog.LevelWarn, outcome: "failed"},
		{name: "metadata rejection", err: &broker.StageError{Stage: broker.StageMetadata, Cause: cause},
			count: 1, level: slog.LevelWarn, outcome: "failed"},
		{name: "outer failure", err: &broker.StageError{Stage: broker.StageHandle, Cause: &broker.DecodeError{Cause: cause}},
			count: 1, level: slog.LevelError, outcome: "failed"},
		{name: "retry", result: broker.RetryAfter{Delay: time.Second, Cause: cause},
			count: 1, level: slog.LevelWarn, outcome: "retry_requested"},
		{name: "retry without cause", result: broker.RetryAfter{Delay: time.Second},
			count: 1, level: slog.LevelWarn, outcome: "retry_requested"},
		{name: "custom result", result: privateDisposition{secret: "private-result"}, config: broker.ProcessingLogConfig{LogSuccess: true},
			count: 1, level: slog.LevelInfo, outcome: "returned"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			capture := newProcessingCapture(&output)
			handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				return test.result, test.err
			}).WithMiddleware(broker.LogProcessing(slog.New(capture), test.config))
			result, err := handler.Handle(context.Background(), replyDelivery{})
			if !reflect.DeepEqual(result, test.result) || err != test.err || len(capture.records) != test.count {
				t.Fatalf("result=%+v err=%v records=%d", result, err, len(capture.records))
			}
			if test.count == 0 {
				return
			}
			record := capture.records[0]
			attrs := processingAttrs(&record)
			if record.Level != test.level || attrs["outcome"].String() != test.outcome {
				t.Fatalf("level=%v attrs=%+v", record.Level, attrs)
			}
			if retry, ok := test.result.(broker.RetryAfter); ok && retry.Cause != nil && attrs["cause"].Any() != retry.Cause {
				t.Fatal("retry cause lost")
			}
			if strings.Contains(output.String(), "private-result") || strings.Contains(strings.ToLower(output.String()), "ack") {
				t.Fatalf("unsafe result or settlement claim: %s", output.String())
			}
		})
	}
}

func TestLogProcessingCustomAttributesAndSafeSource(t *testing.T) {
	var output bytes.Buffer
	capture := newProcessingCapture(&output)
	ctx := context.Background()
	input := &processingDelivery{source: "orders\r\nforged\x00" + strings.Repeat("x", 4096)}
	calls := 0
	config := broker.ProcessingLogConfig{LogSuccess: true,
		Level: func(result broker.Disposition, err error) slog.Level {
			if _, ok := result.(broker.Handled); !ok || err != nil {
				t.Error("level callback lost result")
			}
			return slog.LevelWarn
		},
		Attributes: func(received context.Context, delivery broker.Delivery, result broker.Disposition, err error) []slog.Attr {
			calls++
			if received != ctx || delivery != input {
				t.Error("attribute callback lost context/delivery")
			}
			if _, ok := result.(broker.Handled); !ok || err != nil {
				t.Error("attribute callback lost outcome")
			}
			return []slog.Attr{slog.String("operation", "custom"), slog.String("entity", "order-7")}
		},
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, nil
	}).WithMiddleware(broker.LogProcessing(slog.New(capture), config))
	_, err := handler.Handle(ctx, input)
	if err != nil || calls != 1 || len(capture.records) != 1 {
		t.Fatalf("err=%v calls=%d records=%d", err, calls, len(capture.records))
	}
	record := capture.records[0]
	attrs := processingAttrs(&record)
	if record.Level != slog.LevelWarn || attrs["operation"].String() != "process" || attrs["attributes"].Kind() != slog.KindGroup {
		t.Fatalf("custom attributes replaced built-ins: %+v", attrs)
	}
	source := attrs["source"].String()
	if len(source) >= len(input.source) || strings.ContainsAny(source, "\r\n\x00") {
		t.Fatalf("unbounded or injectable source: %q", source)
	}
	var serialized map[string]any
	if err := json.Unmarshal(output.Bytes(), &serialized); err != nil {
		t.Fatal(err)
	}
	custom, ok := serialized["attributes"].(map[string]any)
	if !ok || custom["entity"] != "order-7" || custom["operation"] != "custom" {
		t.Fatalf("custom attributes lost: %s", output.String())
	}
}

func TestLogProcessingSelectsAttributesFromOutcome(t *testing.T) {
	cause := &processingFailure{field: "account_id", entity: "order-7", secret: "private-cause"}
	staged := &broker.StageError{Stage: broker.StageHandle, Cause: cause}
	for _, test := range []struct {
		name   string
		result broker.Disposition
		err    error
	}{
		{name: "success", result: broker.Handled{}},
		{name: "failure", result: broker.Handled{}, err: staged},
		{name: "retry", result: broker.RetryAfter{Delay: time.Second, Cause: staged}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			capture := newProcessingCapture(&output)
			ctx := context.Background()
			input := &processingDelivery{source: "orders", message: broker.Message{
				Body: []byte("private-body"), Headers: []broker.Header{{Name: "Authorization", Value: []byte("private-header")}},
			}}
			calls := 0
			config := broker.ProcessingLogConfig{LogSuccess: true,
				Attributes: func(received context.Context, delivery broker.Delivery, result broker.Disposition, err error) []slog.Attr {
					calls++
					if received != ctx || delivery != input || !reflect.DeepEqual(result, test.result) || err != test.err {
						t.Fatal("selector lost original context, delivery or outcome")
					}
					if retry, ok := result.(broker.RetryAfter); err == nil && ok {
						err = retry.Cause
					}
					var failure *processingFailure
					if !errors.As(err, &failure) {
						return nil
					}
					return []slog.Attr{slog.String("field", failure.field), slog.String("entity", failure.entity)}
				},
			}
			handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				return test.result, test.err
			}, broker.WithKeepAlive(time.Second)).WithMiddleware(broker.LogProcessing(slog.New(capture), config))
			result, err := handler.Handle(ctx, input)
			if !reflect.DeepEqual(result, test.result) || err != test.err || calls != 1 || len(capture.records) != 1 {
				t.Fatalf("result=%T err=%v selector calls=%d records=%d", result, err, calls, len(capture.records))
			}
			assertProcessingRequirements(t, handler)
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if test.name == "success" {
				if _, exists := record["attributes"]; exists {
					t.Fatalf("success contains failure-only attributes: %s", output.String())
				}
			} else {
				fields, ok := record["attributes"].(map[string]any)
				if !ok || fields["field"] != cause.field || fields["entity"] != cause.entity {
					t.Fatalf("safe failure details lost: %s", output.String())
				}
			}
			if strings.Contains(output.String(), "private-") {
				t.Fatalf("diagnostics exposed sensitive input: %s", output.String())
			}
		})
	}
}

func TestLogProcessingDisabledRecordsDoNotSelectAttributes(t *testing.T) {
	for _, success := range []bool{false, true} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelError + 1}))
		if success {
			logger = slog.New(slog.NewJSONHandler(&output, nil))
		}
		cause := errors.New("failure")
		handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			if success {
				return broker.Handled{}, nil
			}
			return nil, cause
		}).WithMiddleware(broker.LogProcessing(logger, broker.ProcessingLogConfig{
			Attributes: func(context.Context, broker.Delivery, broker.Disposition, error) []slog.Attr {
				t.Fatal("disabled record evaluated attribute selector")
				return nil
			},
		}))
		_, err := handler.Handle(context.Background(), replyDelivery{})
		if (!success && err != cause) || (success && err != nil) || output.Len() != 0 {
			t.Fatalf("success=%v err=%v output=%s", success, err, output.String())
		}
	}
}

func TestLogProcessingSelectorInspectsRecoveredPanic(t *testing.T) {
	cause := &processingFailure{field: "account_id", entity: "order-7", secret: "private-panic"}
	var output bytes.Buffer
	capture := newProcessingCapture(&output)
	calls := 0
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		panic(cause)
	}).WithMiddleware(broker.LogProcessing(slog.New(capture), broker.ProcessingLogConfig{
		Attributes: func(_ context.Context, _ broker.Delivery, result broker.Disposition, err error) []slog.Attr {
			calls++
			var recovered *broker.PanicError
			if result != nil || !errors.As(err, &recovered) || !errors.Is(err, cause) || len(recovered.Stack) == 0 {
				t.Fatal("selector lost recovered panic evidence")
			}
			return []slog.Attr{slog.String("entity", cause.entity)}
		},
	}), broker.RecoverPanics())
	result, err := handler.Handle(context.Background(), replyDelivery{})
	if result != nil || !errors.Is(err, cause) || calls != 1 || len(capture.records) != 1 {
		t.Fatalf("result=%T err=%v selector calls=%d records=%d", result, err, calls, len(capture.records))
	}
	for _, wanted := range []string{"panic", "account_id", "order-7", "dependency rejected field"} {
		if !strings.Contains(output.String(), wanted) {
			t.Errorf("serialized diagnostic lost %q: %s", wanted, output.String())
		}
	}
	if strings.Contains(output.String(), "private-panic") {
		t.Fatalf("panic value exposed: %s", output.String())
	}
}

type processingFailure struct{ field, entity, secret string }

func (e *processingFailure) Error() string {
	return "dependency rejected field " + e.field + ": " + e.secret
}

type processingDelivery struct {
	message broker.Message
	source  string
}

func (d *processingDelivery) Message() broker.Message { return d.message }
func (d *processingDelivery) Source() string          { return d.source }

type privateDisposition struct{ secret string }

func (privateDisposition) DeliveryDisposition() {}

type processingCapture struct {
	slog.Handler
	records []slog.Record
}

//nolint:gocritic // slog.Handler requires a value record.
func (h *processingCapture) Handle(ctx context.Context, record slog.Record) error {
	h.records = append(h.records, record.Clone())
	return h.Handler.Handle(ctx, record)
}

func newProcessingCapture(output *bytes.Buffer) *processingCapture {
	options := &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		err, ok := attr.Value.Any().(error)
		if !ok {
			return attr
		}
		var failure *processingFailure
		if errors.As(err, &failure) {
			return slog.Group(attr.Key, slog.String("message", "dependency rejected field"),
				slog.String("field", failure.field), slog.String("entity", failure.entity))
		}
		return slog.String(attr.Key, err.Error())
	}}
	return &processingCapture{Handler: slog.NewJSONHandler(output, options)}
}

func processingAttrs(record *slog.Record) map[string]slog.Value {
	attrs := make(map[string]slog.Value)
	record.Attrs(func(attr slog.Attr) bool { attrs[attr.Key] = attr.Value; return true })
	return attrs
}

func assertProcessingRequirements(t *testing.T, handler broker.Handler) {
	t.Helper()
	if !reflect.DeepEqual(handler.Requirements(), []broker.Requirement{broker.KeepAliveRequirement{Interval: time.Second}}) {
		t.Fatalf("requirements changed: %+v", handler.Requirements())
	}
}

func assertProcessingFields(t *testing.T, attrs map[string]slog.Value, outcome, stage string) {
	t.Helper()
	if attrs["operation"].String() != "process" || attrs["outcome"].String() != outcome ||
		attrs["stage"].String() != stage || attrs["duration"].Kind() != slog.KindDuration {
		t.Fatalf("missing processing context: %+v", attrs)
	}
}
