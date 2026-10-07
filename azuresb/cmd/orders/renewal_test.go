package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestRenewalOptionsDeclareCapabilityAndCancelBusinessWait(t *testing.T) {
	settings, err := parseOptions([]string{"-queue", "orders", "-work-duration", "1h", "-renew-every", "5s"})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	handler := orderHandler(&settings, slog.New(slog.NewJSONHandler(&output, nil)))
	found := false
	for _, requirement := range handler.Requirements() {
		if renewal, ok := requirement.(broker.KeepAliveRequirement); ok {
			found = renewal.Interval == 5*time.Second
		}
	}
	if !found {
		t.Fatal("missing requested renewal cadence")
	}
	message := exampleDelivery{body: []byte(`{"id":"order-example-001","amount":42}`)}
	if disposition, handleErr := handler.Handle(context.Background(), message); handleErr != nil {
		t.Fatal(handleErr)
	} else if _, ok := disposition.(broker.RetryAfter); !ok {
		t.Fatalf("first callback must retry: %v", disposition)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	disposition, err := handler.Handle(ctx, message)
	if !errors.Is(err, context.Canceled) || disposition != nil {
		t.Fatalf("canceled work returned %v, %v", disposition, err)
	}
	if output.Len() != 0 {
		t.Fatal("canceled work reported business effect")
	}
}

func TestDurationOptionsRejectInvalidValuesBeforeSDKConstruction(t *testing.T) {
	for _, test := range []struct {
		args  []string
		field string
	}{
		{[]string{"-work-duration", "-1s"}, "work-duration"},
		{[]string{"-renew-every", "-1s"}, "renew-every"},
		{[]string{"-work-duration", "2562047h47m16.854775807s"}, "work-duration"},
		{[]string{"-receive-mode", "receive-and-delete", "-renew-every", "1s"}, "renew-every"},
		{[]string{"-work-duration", "private-token"}, "work-duration"},
		{[]string{"-renew-every", "private-token"}, "renew-every"},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			_, err := parseOptions(append([]string{"-queue", "orders"}, test.args...))
			var field *adapter.FieldError
			if !errors.As(err, &field) || field.Field != test.field || field.Cause == nil || !errors.Is(err, field.Cause) {
				t.Fatalf("missing original field cause: %v", err)
			}
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			logger.Error("invalid options", slog.Any("error", err))
			if !strings.Contains(output.String(), test.field) || !strings.Contains(output.String(), "cause") ||
				strings.Contains(output.String(), "private-token") {
				t.Fatalf("field diagnostics not safely retained: %s", output.String())
			}
		})
	}
}

func TestOrderWorkDurationDelaysSuccessfulEffect(t *testing.T) {
	settings, err := parseOptions([]string{"-queue", "orders", "-receive-mode", "receive-and-delete", "-work-duration", "10ms"})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	handler := orderHandler(&settings, slog.New(slog.NewJSONHandler(&output, nil)))
	started := time.Now()
	disposition, err := handler.Handle(context.Background(), exampleDelivery{body: []byte(`{"id":"order-example-001","amount":42}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := disposition.(broker.Handled); !ok {
		t.Fatalf("unexpected disposition: %v", disposition)
	}
	if time.Since(started) < 10*time.Millisecond || !strings.Contains(output.String(), "Order effect completed") {
		t.Fatal("work completed before requested duration")
	}
}

func TestRenewalDiagnosticsPreserveOperationAndNativeCause(t *testing.T) {
	native := context.DeadlineExceeded
	failure := &adapter.OperationError{Source: sourceName, Operation: "renew", Cause: native}
	if !errors.Is(failure, native) {
		t.Fatal("lost native cause")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("renewal failed", slog.Any("error", failure))
	for _, expected := range []string{`"operation":"renew"`, `"source":"orders"`, `"cause"`, `"kind":"deadline_exceeded"`} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("missing %s: %s", expected, output.String())
		}
	}
}

func TestRenewalSettingsFailBeforeSDKConstruction(t *testing.T) {
	for _, settings := range []options{
		{queue: "orders", id: defaultOrderID, receiveMode: peekLockMode, workDuration: -time.Second},
		{queue: "orders", id: defaultOrderID, receiveMode: receiveAndDeleteMode, renewEvery: time.Second},
	} {
		var output bytes.Buffer
		err := run(context.Background(), "invalid-connection-string", &settings, slog.New(slog.NewJSONHandler(&output, nil)))
		var field *adapter.FieldError
		if !errors.As(err, &field) || (field.Field != workDurationFlag && field.Field != renewEveryFlag) {
			t.Fatalf("expected duration validation before SDK: %v", err)
		}
		if output.Len() != 0 {
			t.Fatal("invalid options caused side effects")
		}
	}
}
