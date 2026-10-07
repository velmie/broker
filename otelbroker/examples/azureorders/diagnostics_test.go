package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestDiagnosticsPreserveNativeFieldAndCleanupBranches(t *testing.T) {
	native := &amqp.Error{Condition: amqp.ErrCondNotFound, Description: "private-connection-token",
		Info: map[string]any{"header": "private-value"}}
	failure := errors.Join(
		&adapter.OperationError{Source: sourceName, Operation: adapter.OperationReceive, Cause: native},
		&adapter.OperationError{Source: sourceName, Operation: "close_sender", Cause: &azservicebus.Error{Code: azservicebus.CodeConnectionLost}},
		&adapter.FieldError{Field: "ApplicationProperties.Command-Id", ValueType: "[]string", Cause: errors.New("private-body")},
		&broker.StageError{Stage: broker.StageDecode, Cause: &adapter.FieldError{Field: "private-header", Cause: errors.New("private-value")}},
	)
	if !errors.Is(failure, native) {
		t.Fatal("native cause identity lost")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", failure))
	if strings.Contains(output.String(), "private-") {
		t.Fatalf("sensitive diagnostics: %s", output.String())
	}
	var record struct {
		Error diagnostic `json:"error"`
	}
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	branches := record.Error.Causes
	if len(branches) != 4 || branches[0].Operation != "receive" || branches[0].Cause.Code != "amqp:not-found" {
		t.Fatal("native operation/cause missing")
	}
	if branches[1].Operation != "close_sender" || branches[1].Cause.Code != "connlost" {
		t.Fatal("cleanup association missing")
	}
	if branches[2].Field != "ApplicationProperties.Command-Id" || branches[2].ValueType != "[]string" {
		t.Fatal("property type and field missing")
	}
	if branches[3].Stage != broker.StageDecode || branches[3].Cause.Field != redacted {
		t.Fatal("decode branch lost or sensitive field exposed")
	}
}

func TestOptionsFailBeforeNativeEffects(t *testing.T) {
	for _, args := range [][]string{nil, {"-queue", "q", "-topic", "t"}, {"-topic", "t"},
		{"-queue", "q", "-receive-mode", "unknown"}, {"-queue", "q", "-id", ""}, {"-queue", "q", "extra"}} {
		if _, err := parseOptions(args); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	failure := errors.New("independent cleanup")
	if onlyCancellation(errors.Join(context.Canceled, failure)) {
		t.Fatal("independent failure discarded")
	}
}
