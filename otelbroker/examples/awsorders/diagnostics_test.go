package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

func TestDiagnosticsKeepBranchAssociationWithoutSensitiveValues(t *testing.T) {
	native := &smithy.GenericAPIError{Code: "RequestThrottled", Message: "private-token"}
	failure := errors.Join(&sqs.OperationError{Source: sourceName, Operation: sqs.OperationReceive, Cause: native},
		&commandError{Operation: "DeleteQueue", Cause: errors.New("private-queue")},
		&sqs.OperationError{Source: "private-source", Operation: "private-operation", Cause: &sqs.FieldError{
			Field: "private-header", Cause: &broker.StageError{Stage: broker.StageDecode, Cause: errors.New("private-body")}}})
	if !errors.Is(failure, native) {
		t.Fatal("original cause lost")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.Error("failed", slog.Any("error", failure))
	if strings.Contains(output.String(), "private-") {
		t.Fatalf("sensitive data: %s", output.String())
	}
	var record struct {
		Error diagnostic `json:"error"`
	}
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	branches := record.Error.Causes
	if len(branches) != 3 || branches[0].Operation != "receive" || branches[0].Source != sourceName ||
		branches[0].Cause.Code != "RequestThrottled" {
		t.Fatal("receive/native cause association missing")
	}
	if branches[1].Operation != "DeleteQueue" || branches[1].Cause.Type != "*errors.errorString" {
		t.Fatal("cleanup branch missing")
	}
	if branches[2].Cause.Cause.Stage != broker.StageDecode {
		t.Fatal("stage association missing")
	}
}

func TestOnlyCancellationRetainsIndependentFailure(t *testing.T) {
	failure := errors.New("cleanup")
	if onlyCancellation(errors.Join(context.Canceled, failure)) {
		t.Fatal("independent failure was discarded")
	}
	if !onlyCancellation(&sqs.OperationError{Operation: sqs.OperationReceive, Cause: context.Canceled}) {
		t.Fatal("expected own stop rejected")
	}
}
