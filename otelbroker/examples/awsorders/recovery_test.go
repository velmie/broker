package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

func TestNativeRecoveryAndUnknownDelete(t *testing.T) {
	for _, test := range []struct {
		name                       string
		throttles                  int32
		deleteFails                bool
		callbackError              error
		receives, effects, deletes int32
	}{
		{"one throttle", 1, false, nil, 2, 1, 1}, {"persistent throttle", 100, false, nil, 3, 0, 0},
		{"unknown delete", 0, true, nil, 1, 1, 1}, {"callback failure", 0, false, errors.New("private-callback"), 1, 1, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			var receives, effects, deletes, active, maximum atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := active.Add(1)
				defer active.Add(-1)
				for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonSQS.ReceiveMessage":
					if receives.Add(1) <= test.throttles {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte(`{"__type":"RequestThrottled","message":"private-token"}`))
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Messages": []any{map[string]any{
						"MessageId": "native-id", "ReceiptHandle": "private-receipt",
						"Body":              `{"amount":42}`,
						"Attributes":        map[string]string{"ApproximateReceiveCount": "1"},
						"MessageAttributes": map[string]any{"id": map[string]string{"DataType": "String", "StringValue": "order-example-001"}}}}})
				case "AmazonSQS.DeleteMessage":
					deletes.Add(1)
					if test.deleteFails {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`{"__type":"InternalError","message":"private-token"}`))
						return
					}
					_, _ = w.Write([]byte(`{}`))
				default:
					t.Errorf("unexpected native operation %s", r.Header.Get("X-Amz-Target"))
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			queues, _ := newClients(server.URL)
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer func() { _ = provider.Shutdown(context.Background()) }()
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := consume(ctx, queues, server.URL+"/queue", "", logger, provider.Tracer("test"),
				func(_ context.Context, delivery broker.Delivery, _ order) error {
					effects.Add(1)
					assertDetachedMetadata(t, delivery)
					return test.callbackError
				})
			got := [4]int32{receives.Load(), effects.Load(), deletes.Load(), maximum.Load()}
			want := [4]int32{test.receives, test.effects, test.deletes, 1}
			if got != want {
				t.Fatalf("counts receive/effect/delete/active: %d/%d/%d/%d err=%v",
					receives.Load(), effects.Load(), deletes.Load(), maximum.Load(), err)
			}
			if (test.throttles > 1 || test.deleteFails || test.callbackError != nil) != (err != nil) {
				t.Fatalf("unexpected outcome %v", err)
			}
			if test.callbackError != nil && !errors.Is(err, test.callbackError) {
				t.Fatal("callback cause lost")
			}
			if err != nil {
				logger.Error("command failed", slog.Any("error", err))
			}
			assertFailureSpans(t, recorder, test.deleteFails, test.callbackError != nil)
			if len(recorder.Started()) != len(recorder.Ended()) {
				t.Fatal("abandoned spans")
			}
			if strings.Contains(logs.String(), "private-") {
				t.Fatalf("sensitive diagnostics: %s", logs.String())
			}
			if test.throttles == 1 && strings.Count(logs.String(), "consumer restart scheduled") != 1 {
				t.Fatal("replacement not observed")
			}
		})
	}
}

func TestRecoveryRejectsIndependentCauses(t *testing.T) {
	throttle := &smithy.GenericAPIError{Code: "RequestThrottled", Message: "private-token"}
	receive := &sqs.OperationError{Source: sourceName, Operation: sqs.OperationReceive, Cause: throttle}
	if !retryReceiveThrottle(receive) {
		t.Fatal("native receive throttle rejected")
	}
	for _, err := range []error{throttle, errors.Join(receive, context.Canceled), errors.Join(receive, errors.New("cleanup")),
		&sqs.OperationError{Source: sourceName, Operation: sqs.OperationDelete, Cause: throttle},
		&sqs.OperationError{Source: sourceName, Operation: sqs.OperationProcess, Cause: throttle},
		&sqs.OperationError{Source: sourceName, Operation: sqs.OperationReceive, Cause: errors.Join(throttle, errors.New("independent"))}} {
		if retryReceiveThrottle(err) {
			t.Fatalf("unsafe recovery: %T", err)
		}
	}
}

func assertDetachedMetadata(t *testing.T, delivery broker.Delivery) {
	t.Helper()
	metadata := delivery.(sqs.MetadataReader).Metadata()
	metadata.Attributes["ApproximateReceiveCount"] = "changed"
	if delivery.(sqs.MetadataReader).Metadata().Attributes["ApproximateReceiveCount"] != "1" {
		t.Error("metadata storage was shared")
	}
}
