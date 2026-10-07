package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	adapter "github.com/velmie/broker/sqs"
)

func TestDiagnosticRedactionRetainsFailureDetails(t *testing.T) {
	cause := &smithy.GenericAPIError{Code: "InvalidAttributeValue", Message: "token=SECRET body=PRIVATE URL=https://private.example"}
	original := &adapter.OperationError{Source: "orders",
		Operation: adapter.OperationMetadata,
		Cause: &adapter.FieldError{Field: "MessageAttributes.amount",
			Cause: cause}}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.With("bound_error", original).Error("operation failed", "error", original)
	logger.Error("metadata failed", "error", &adapter.FieldError{Field: "MessageAttributes.unit",
		Cause: errors.New("expected nonempty binary value")})
	text := output.String()
	for _, expected := range []string{"InvalidAttributeValue", "MessageAttributes.amount", "orders", "metadata", "cause_type", "bound_error",
		"expected nonempty binary value", "MessageAttributes.unit"} {
		if !strings.Contains(text, expected) {
			t.Errorf("missing diagnostic %s: %s", expected, text)
		}
	}
	for _, secret := range []string{"SECRET", "PRIVATE", "private.example", "token="} {
		if strings.Contains(text, secret) {
			t.Errorf("secret serialized: %s", text)
		}
	}
	if !errors.Is(original, cause) {
		t.Fatal("redactor mutated original error")
	}
}
func TestOnlyCancellationPreservesJoinedFailures(t *testing.T) {
	marker := errors.New("independent operation failure")
	if onlyCancellation(errors.Join(context.Canceled, marker)) {
		t.Fatal("independent cause discarded")
	}
	if !onlyCancellation(errors.Join(context.Canceled, context.Canceled)) {
		t.Fatal("expected cancellation rejected")
	}
}

func TestDiagnosticJoinedCausesKeepBranchIdentity(t *testing.T) {
	primary := &smithy.GenericAPIError{Code: "AccessDenied", Message: "token=PRIMARY_SECRET https://private.example"}
	cleanup := &smithy.GenericAPIError{Code: "QueueDoesNotExist", Message: "body=CLEANUP_SECRET"}
	original := errors.Join(
		&adapter.OperationError{Source: "orders", Operation: "GetQueueURL", Cause: primary},
		&adapter.OperationError{Source: "temporary-queue", Operation: "DeleteQueue", Cause: cleanup},
	)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.With("bound_error", original).Error("failed", "error", original)
	var record map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"bound_error", "error"} {
		var tree struct {
			Causes []struct {
				Operation, Source string
				Cause             struct{ Code string }
			}
		}
		if err := json.Unmarshal(record[key], &tree); err != nil {
			t.Fatal(err)
		}
		if len(tree.Causes) != 2 {
			t.Fatalf("%s lost error branches: %s", key, record[key])
		}
		if tree.Causes[0].Operation != "GetQueueURL" || tree.Causes[0].Source != "orders" || tree.Causes[0].Cause.Code != "AccessDenied" {
			t.Errorf("primary branch misassociated: %+v", tree.Causes[0])
		}
		if tree.Causes[1].Operation != "DeleteQueue" || tree.Causes[1].Source != "temporary-queue" ||
			tree.Causes[1].Cause.Code != "QueueDoesNotExist" {
			t.Errorf("cleanup branch misassociated: %+v", tree.Causes[1])
		}
	}
	for _, secret := range []string{"PRIMARY_SECRET", "CLEANUP_SECRET", "private.example"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("secret leaked: %s", output.String())
		}
	}
	if !errors.Is(original, primary) || !errors.Is(original, cleanup) {
		t.Fatal("original causes changed")
	}
}

func TestDiagnosticParseCauseRedaction(t *testing.T) {
	_, cause := strconv.ParseUint("SECRET-TOKEN\nhttps://private.example", 10, 64)
	original := &adapter.OperationError{Source: "orders",
		Operation: adapter.OperationMetadata,
		Cause: &adapter.FieldError{Field: "Attributes.ApproximateReceiveCount",
			Cause: cause}}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("metadata failed", "error", original)
	for _, expected := range []string{"ParseUint", "syntax", "Attributes.ApproximateReceiveCount", "orders", "metadata"} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("missing %s: %s", expected, output.String())
		}
	}
	for _, secret := range []string{"SECRET-TOKEN", "private.example"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("raw parse input leaked: %s", output.String())
		}
	}
}

type cyclicError struct{}

func (*cyclicError) Error() string   { return "SECRET cycle" }
func (e *cyclicError) Unwrap() error { return e }

func TestDiagnosticTraversalIsBounded(t *testing.T) {
	branches := make([]error, maxDiagnosticNodes*2)
	for i := range branches {
		branches[i] = errors.New("SECRET branch")
	}
	for _, original := range []error{&cyclicError{}, errors.Join(branches...)} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
		logger.Error("bounded diagnostic", "error", original)
		text := output.String()
		if !strings.Contains(text, `"truncated":true`) {
			t.Fatalf("missing truncation marker: %s", text)
		}
		if strings.Count(text, `"type"`) > maxDiagnosticNodes+1 {
			t.Fatalf("unbounded diagnostic: %s", text)
		}
		if strings.Contains(text, "SECRET") {
			t.Fatalf("raw error serialized: %s", text)
		}
	}
}
