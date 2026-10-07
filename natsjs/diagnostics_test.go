package natsjs_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestErrorFieldsPreserveNativeCodesAndOriginalCauses(t *testing.T) {
	legacy := &nats.APIError{Code: 400, ErrorCode: 10060, Description: "token-secret"}
	native := &jetstream.APIError{Code: 404, ErrorCode: 10059, Description: "private-subject"}
	joined := errors.Join(
		&natsjs.OperationError{Source: "orders", Operation: "publish", Cause: legacy},
		&natsjs.OperationError{Source: "commands", Operation: "startup", Cause: native},
	)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		if err, ok := attr.Value.Any().(error); ok {
			return slog.Any(attr.Key, broker.DescribeError(err, natsjs.ErrorFields))
		}
		return attr
	}}))
	logger.Error("failed", slog.Any("error", joined))
	for _, want := range []string{`"errorcode":10060`, `"errorcode":10059`, `"operation":"publish"`, `"operation":"startup"`} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("missing %s in %s", want, output.String())
		}
	}
	for _, secret := range []string{"token-secret", "private-subject"} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("leaked %s", secret)
		}
	}
	if !errors.Is(joined, legacy) || !errors.Is(joined, native) {
		t.Fatal("original causes lost")
	}
}

func TestPublicationValidationRetainsFieldAndPublishContext(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	err = publisher.Publish(context.Background(), broker.Message{Headers: []broker.Header{{Name: "invalid name"}}})
	var field *broker.FieldError
	var stage *broker.StageError
	var operation *natsjs.OperationError
	if !errors.As(err, &field) || field.Field != "Headers.Name" || field.Reason != "invalid_header_name" {
		t.Fatalf("field context: %v", err)
	}
	if field.Error() != "Headers.Name: invalid NATS header name" || !errors.Is(err, field.Cause) {
		t.Fatalf("original validation cause: %v", err)
	}
	if !errors.As(err, &stage) || stage.Stage != broker.StagePublish ||
		!errors.As(err, &operation) || operation.Operation != "publish" || operation.Source != config.Subject {
		t.Fatalf("publish context: %v", err)
	}
}
