package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

func TestConfiguredLoggerPreservesBranchesAndRedactsValues(t *testing.T) {
	primary := &smithy.GenericAPIError{Code: "AuthorizationError", Message: "secret-token https://private.example/path?credential=value"}
	cleanup := &smithy.GenericAPIError{Code: "AccessDenied", Message: "private-header-value"}
	decode := base64.CorruptInputError(2)
	err := errors.Join(&sns.OperationError{Source: "orders/publish", Operation: "publish", Cause: primary},
		&sqs.OperationError{Source: "orders/cleanup", Operation: "DeleteQueue", Cause: cleanup},
		&sns.FieldError{Field: "MessageAttributes.opaque.Value", Cause: decode})
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || !errors.Is(err, decode) {
		t.Fatal("original diagnostics lost causes")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.With(slog.Any("bound", err)).ErrorContext(context.Background(), "operation failed", slog.Any("error", err))
	text := output.String()
	var record struct {
		Bound diagnosticError `json:"bound"`
		Error diagnosticError `json:"error"`
	}
	if decodeErr := json.Unmarshal(output.Bytes(), &record); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	for _, tree := range []*diagnosticError{&record.Bound, &record.Error} {
		if len(tree.Causes) != 3 {
			t.Fatalf("lost joined branches: %+v", tree)
		}
		if tree.Causes[0].Source != "orders/publish" || tree.Causes[0].Operation != "publish" ||
			tree.Causes[0].Cause.Code != "AuthorizationError" {
			t.Fatalf("primary error associated with wrong context: %+v", tree.Causes[0])
		}
		if tree.Causes[1].Source != "orders/cleanup" || tree.Causes[1].Operation != "DeleteQueue" || tree.Causes[1].Cause.Code != "AccessDenied" {
			t.Fatalf("cleanup error associated with wrong context: %+v", tree.Causes[1])
		}
		if tree.Causes[2].Field != "MessageAttributes.opaque.Value" || tree.Causes[2].Cause.Type != "base64.CorruptInputError" {
			t.Fatalf("parser cause detached from field: %+v", tree.Causes[2])
		}
	}

	for _, want := range []string{"AuthorizationError", "AccessDenied", "orders/publish", "orders/cleanup", "DeleteQueue", "publish",
		"MessageAttributes.opaque.Value", "base64.CorruptInputError"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing diagnostic %q: %s", want, text)
		}
	}
	for _, secret := range []string{"secret-token", "private.example", "credential=value", "private-header-value"} {
		if strings.Contains(text, secret) {
			t.Errorf("leaked %q", secret)
		}
	}
}
func TestSuccessfulDeletionDoesNotHideSecondaryFailure(t *testing.T) {
	failure := errors.New("cleanup failed")
	if onlyCancellation(errors.Join(context.Canceled, failure)) {
		t.Fatal("secondary failure erased")
	}
	if !onlyCancellation(&sns.OperationError{Source: "orders", Operation: "receive", Cause: context.Canceled}) {
		t.Fatal("ordinary stop misclassified")
	}
}
