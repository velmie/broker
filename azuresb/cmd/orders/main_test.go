package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/velmie/broker"

	adapter "github.com/velmie/broker/azuresb"
)

func TestInvalidArgumentsFailBeforeSDKConstruction(t *testing.T) {
	for _, args := range [][]string{
		nil, {"-queue", "orders", "-topic", "orders"},
		{"-topic", "orders"},
		{"-subscription", "orders"},
		{"-queue", "orders", "-receive-mode", "unknown"},
		{"-queue", "orders", "-id", ""},
		{"-queue", "orders", "extra"}} {
		if _, err := parseOptions(args); err == nil {
			t.Errorf("accepted invalid options: %v", args)
		}
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	err := run(context.Background(), "intentionally-invalid-connection-string", &options{}, logger)
	var field *adapter.FieldError
	if !errors.As(err, &field) || field.Field != "queue/topic/subscription" {
		t.Fatalf("expected config rejection before SDK construction: %v", err)
	}
	if output.Len() != 0 {
		t.Fatal("invalid configuration caused command side effects")
	}
}

func TestOrderHandlerModesKeepEffectAfterRetry(t *testing.T) {
	for _, mode := range []string{peekLockMode, receiveAndDeleteMode} {
		t.Run(mode, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			handler := orderHandler(&options{id: defaultOrderID, receiveMode: mode}, logger)
			message := exampleDelivery{body: []byte(`{"id":"order-example-001","amount":42}`)}
			disposition, err := handler.Handle(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			if mode == peekLockMode {
				retry, ok := disposition.(broker.RetryAfter)
				if !ok || retry.Delay != 0 || retry.Cause == nil {
					t.Fatalf("expected immediate retry: %#v", disposition)
				}
				if output.Len() != 0 {
					t.Fatal("effect ran before first retry")
				}
				if len(handler.Requirements()) != 2 {
					t.Fatal("missing delivery attempt/redelivery requirements")
				}
				disposition, err = handler.Handle(context.Background(), message)
				if err != nil {
					t.Fatal(err)
				}
			} else if len(handler.Requirements()) != 0 {
				t.Fatal("ReceiveAndDelete requested settlement capabilities")
			}
			if _, ok := disposition.(broker.Handled); !ok {
				t.Fatalf("expected effect completion: %#v", disposition)
			}
			if !strings.Contains(output.String(), "Order effect completed") {
				t.Fatal("effect outcome missing")
			}
		})
	}
}

type exampleDelivery struct{ body []byte }

func (d exampleDelivery) Message() broker.Message {
	return broker.Message{ID: defaultOrderID, Body: d.body}
}
func (exampleDelivery) Source() string          { return sourceName }
func (exampleDelivery) Attempt() broker.Attempt { return broker.Attempt{Known: true, Number: 1} }
