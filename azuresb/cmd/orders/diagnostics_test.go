package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestDiagnosticsPreserveBranchesAndExcludeSensitiveValues(t *testing.T) {
	const secret = "private-entity-token-body-header"
	primary := &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess, Description: secret, Info: map[string]any{"token": secret}}
	cause := errors.Join(
		&adapter.OperationError{Source: "orders", Operation: "receive", Cause: &broker.StageError{Stage: broker.StageDecode,
			Cause: &broker.DecodeError{Cause: &adapter.FieldError{Field: "DeliveryCount", Cause: primary}}}},
		&adapter.OperationError{Source: "orders", Operation: "close_sender", Cause: &azservicebus.Error{Code: azservicebus.CodeConnectionLost}},
		&net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(10, 20, 30, 40), Port: 5671}, Err: errors.New(secret)},
		&x509.HostnameError{Host: secret, Certificate: &x509.Certificate{}},
	)
	if !errors.Is(cause, primary) {
		t.Fatal("original cause lost before logging")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", cause))
	serialized := output.String()
	for _, want := range []string{
		"orders", "receive", "decode", "DeliveryCount", "amqp:unauthorized-access", "close_sender", "connlost", "dial", "hostname_mismatch",
	} {
		if !strings.Contains(serialized, want) {
			t.Errorf("missing diagnostic %q: %s", want, serialized)
		}
	}
	for _, forbidden := range []string{secret, "10.20.30.40", "5671"} {
		if strings.Contains(serialized, forbidden) {
			t.Errorf("sensitive data escaped redactor: %s", serialized)
		}
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	branches := record["error"].(map[string]any)["causes"].([]any)
	if branches[0].(map[string]any)["operation"] != "receive" || branches[1].(map[string]any)["operation"] != "close_sender" {
		t.Fatal("primary and cleanup association lost")
	}
}

func TestDiagnosticsDoNotTrustArbitraryCodesOrLabels(t *testing.T) {
	const secret = "validlookingsecret"
	input := errors.Join(&azservicebus.Error{Code: azservicebus.Code(secret)}, &amqp.Error{Condition: amqp.ErrCond(secret)},
		&adapter.OperationError{Source: secret, Operation: secret, Cause: &adapter.FieldError{Field: secret, Cause: errors.New(secret)}},
		&broker.StageError{Stage: secret, Cause: errors.New(secret)})
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", input))
	if strings.Contains(output.String(), secret) {
		t.Fatal("unapproved diagnostic value leaked")
	}
	if !strings.Contains(output.String(), "redacted") {
		t.Fatal("missing explicit unknown-cause marker")
	}
}

func TestCancellationNeverSuppressesForeignCause(t *testing.T) {
	failure := errors.New("cleanup failed")
	if onlyCancellation(errors.Join(context.Canceled, failure)) {
		t.Fatal("cleanup failure hidden")
	}
	if !onlyCancellation(&adapter.OperationError{Source: "orders", Operation: "receive", Cause: context.Canceled}) {
		t.Fatal("wrapped normal stop not recognized")
	}
	if onlyCancellation(context.DeadlineExceeded) {
		t.Fatal("deadline failure hidden")
	}
}

func TestPropertyDiagnosticPreservesUnsupportedType(t *testing.T) {
	const secret = "private-property-value"
	cause := errors.New(secret)
	failure := &adapter.FieldError{Field: "ApplicationProperties.Correlation-Id", ValueType: "map[string]string", Cause: cause}
	if !errors.Is(failure, cause) {
		t.Fatal("original property cause lost")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", failure))
	text := output.String()
	for _, expected := range []string{"ApplicationProperties.Correlation-Id", "map[string]string", "errors.errorString"} {
		if !strings.Contains(text, expected) {
			t.Errorf("missing %s: %s", expected, text)
		}
	}
	if strings.Contains(text, secret) {
		t.Fatal("native property value leaked")
	}
}
