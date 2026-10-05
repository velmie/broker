package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/velmie/idempo"

	"github.com/velmie/broker/idempotency"
	"github.com/velmie/broker/natsjs/v3"
)

func TestDiagnosticsPreserveOperationFieldCodeAndCleanup(t *testing.T) {
	original := fmt.Errorf("private-key-token-body: %w", idempo.ErrInvalidKey)
	cause := errors.Join(&idempotency.Error{Operation: "key", Field: "Message.ID", Cause: original},
		failure("delete_stream", "", errors.New("private-stream-name")),
		&natsjs.OperationError{Source: "private-source", Operation: natsjs.OperationAck, Cause: errors.New("private-receipt")})
	if !errors.Is(cause, original) || !errors.Is(cause, idempo.ErrInvalidKey) {
		t.Fatal("original cause lost")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", cause))
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
	if len(branches) != 3 || branches[0].Operation != "key" || branches[0].Field != "Message.ID" || branches[0].Source != sourceName {
		t.Fatal("idempotency operation/source/field association lost")
	}
	if branches[0].Cause.Cause.Code != "invalid_key" {
		t.Fatal("native idempo code lost")
	}
	if branches[1].Operation != "delete_stream" || branches[2].Operation != "ack" {
		t.Fatal("independent cleanup/native branches lost")
	}
}
func TestCleanupFailureCannotBecomeSuccessfulStop(t *testing.T) {
	failure := errors.New("cleanup failed")
	if onlyCancellation(errors.Join(context.Canceled, failure)) {
		t.Fatal("cleanup failure erased")
	}
}
