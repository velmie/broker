package sqs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

const testWait = 5 * time.Second

type request map[string]any

func newWire(t *testing.T, handle func(context.Context, string, request) (int, any)) (client *awssqs.Client, queueURL string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := r.Body.Close(); err != nil {
				t.Error(err)
			}
		}()
		var input request
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			return
		}
		status, body := handle(r.Context(), strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS."), input)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return awssqs.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("synthetic",
			"synthetic",
			""),
		RetryMaxAttempts: 1},
		func(options *awssqs.Options) {
			options.BaseEndpoint = aws.String(server.URL)
			options.DisableMessageChecksumValidation = true
		}), server.URL + "/queue"
}
func native(body string, attributes map[string]any) any {
	return map[string]any{"Body": body,
		"MessageId":         "native-id",
		"ReceiptHandle":     "private-receipt",
		"Attributes":        map[string]string{"ApproximateReceiveCount": "2"},
		"MessageAttributes": attributes}
}
func attr(kind, value string) any { return map[string]any{"DataType": kind, "StringValue": value} }
func newConsumer(t *testing.T, client *awssqs.Client, url string, edit func(*adapter.ConsumerConfig)) *adapter.Consumer {
	t.Helper()
	config := adapter.ConsumerConfig{QueueURL: url, Source: "orders", WaitTimeSeconds: 1}
	if edit != nil {
		edit(&config)
	}
	consumer, err := adapter.NewConsumer(client, config)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(testWait):
		t.Fatal("timed out waiting for lifecycle event")
		var zero T
		return zero
	}
}

//nolint:gocyclo // One end-to-end wire scenario checks publication, callback, and settlement together.
func TestSDKPublishTypedCallbackDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sent request
	var mu sync.Mutex
	var receives atomic.Int32
	var deletes atomic.Int32
	client, url := newWire(t, func(_ context.Context, op string, input request) (int, any) {
		switch op {
		case "SendMessage":
			mu.Lock()
			sent = input
			mu.Unlock()
			return 200, map[string]string{"MessageId": "service-send"}
		case "ReceiveMessage":
			if receives.Add(1) > 1 {
				t.Error("unexpected second receive")
			}
			mu.Lock()
			defer mu.Unlock()
			return 200, map[string]any{"Messages": []any{native(sent["MessageBody"].(string), sent["MessageAttributes"].(map[string]any))}}
		case "DeleteMessage":
			if input["ReceiptHandle"] != "private-receipt" {
				t.Error("wrong receipt")
			}
			deletes.Add(1)
			cancel()
			return 200, request{}
		default:
			t.Errorf("unexpected operation %s", op)
			return 500, request{}
		}
	})
	publisher,
		err := adapter.NewPublisher(client,
		adapter.PublisherConfig{QueueURL: url,
			Source:         "orders",
			FIFO:           true,
			MessageGroupID: "group"})
	if err != nil {
		t.Fatal(err)
	}
	type order struct{ Amount int }
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal),
		publisher,
		func(context.Context,
			order) (broker.MessageMetadata,
			error) {
			return broker.MessageMetadata{ID: "logical-id",
					Headers: []broker.Header{{Name: "binary",
						Value: []byte{0,
							255}},
						{Name: "Foo",
							Value: []byte("one")},
						{Name: "foo",
							Value: []byte("two")}}},
				nil
		})
	if err = publish(ctx, order{Amount: 42}); err != nil {
		t.Fatal(err)
	}
	var retained broker.Message
	var metadata adapter.Metadata
	var observations []adapter.Observation
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) { observations = append(observations, o) }}
	})
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(_ context.Context,
			d broker.Delivery,
			o order) (broker.Disposition,
			error) {
			retained = d.Message()
			metadata = d.(adapter.MetadataReader).Metadata()
			if o.Amount != 42 || retained.ID != "logical-id" || metadata.MessageID != "native-id" ||
				d.(broker.AttemptReader).Attempt() != (broker.Attempt{Known: true,
					Number: 2}) {
				t.Errorf("delivery mismatch: %+v %+v", retained, metadata)
			}
			metadata.MessageAttributes["binary"].BinaryValue[0] = 99
			if d.(adapter.MetadataReader).Metadata().MessageAttributes["binary"].BinaryValue[0] != 0 {
				t.Error("native metadata aliases")
			}
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}))
	if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if deletes.Load() != 1 {
		t.Fatal("delete missing")
	}
	if !bytes.Equal(retained.Body, []byte(`{"Amount":42}`)) {
		t.Fatalf("retained body %s", retained.Body)
	}
	if sent["MessageGroupId"] != "group" || sent["MessageDeduplicationId"] != "logical-id" {
		t.Fatal("missing FIFO mapping")
	}
	if len(observations) != 2 || observations[1].Operation != adapter.OperationDelete || observations[1].Outcome != adapter.OutcomeAccepted {
		t.Fatalf("observations %+v", observations)
	}
}

func TestSDKValidationBeforeEffects(t *testing.T) {
	var calls atomic.Int32
	client, url := newWire(t, func(context.Context, string, request) (int, any) { calls.Add(1); return 200, request{} })
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []broker.Message{
		{Body: []byte{0}}, {Body: []byte{255}}, {}, {Body: []byte("ok"), Headers: []broker.Header{{Name: "a", Value: nil}}},
		{Body: []byte("ok"), Headers: []broker.Header{{Name: "a", Value: []byte("x")}, {Name: "a", Value: []byte("y")}}},
		{ID: "logical", Body: []byte("ok"), Headers: []broker.Header{{Name: "ID", Value: []byte("logical")}}},
		{ID: "logical", Body: []byte("ok"), Headers: []broker.Header{{Name: "id", Value: []byte("other")}}},
		{Body: []byte("ok"), Headers: []broker.Header{{Name: "AWS.a", Value: []byte("x")}}},
		{Body: bytes.Repeat([]byte("x"), 1048577)},
	}
	for i, input := range cases {
		if err = publisher.Publish(context.Background(), input); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	many := broker.Message{Body: []byte("ok"), ID: "id"}
	for i := 0; i < 10; i++ {
		many.Headers = append(many.Headers, broker.Header{Name: fmt.Sprintf("h%d", i), Value: []byte("x")})
	}
	if err = publisher.Publish(context.Background(), many); err == nil {
		t.Error("attribute budget ignored")
	}
	for _, requirement := range []broker.Requirement{broker.KeepAliveRequirement{}, nil} {
		consumer := newConsumer(t, client, url, nil)
		if err = consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirement))); err == nil {
			t.Error("requirement accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("validation performed remote effects")
	}
}

func TestSDKConcurrentPublishReadOnlyAndStandardGroup(t *testing.T) {
	var calls atomic.Int32
	client, url := newWire(t, func(_ context.Context, op string, input request) (int, any) {
		if op != "SendMessage" || input["MessageGroupId"] != "tenant" || input["MessageDeduplicationId"] != nil {
			t.Error("standard queue wire fields")
		}
		calls.Add(1)
		return 200, request{}
	})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders", MessageGroupID: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	message := broker.Message{ID: "id", Body: []byte("body"), Headers: []broker.Header{{Name: "binary", Value: []byte{0, 255}}}}
	before := fmt.Sprintf("%#v", message)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if publishErr := publisher.Publish(context.Background(), message); publishErr != nil {
				t.Error(publishErr)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 16 || fmt.Sprintf("%#v", message) != before {
		t.Fatal("concurrent publish lost messages or mutated input")
	}
}

func TestSDKMalformedReceiveAndResultSuppressDelete(t *testing.T) {
	marker := errors.New("business field Amount rejected")
	cases := []struct {
		name    string
		message any
		handler broker.Handler
		cause   error
	}{
		{"decode",
			native("{",
				nil),
			broker.NewTypedHandler(broker.DecodeJSON[map[string]any],
				func(context.Context,
					map[string]any) error {
					t.Error("decoded malformed body")
					return nil
				}),
			nil},
		{"callback",
			native("{}",
				nil),
			broker.NewHandler(func(context.Context,
				broker.Delivery) (broker.Disposition,
				error) {
				return broker.Handled{},
					marker
			}),
			marker},
		{"unknown result", native("{}", nil), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			return broker.RetryAfter{Delay: time.Second}, nil
		}), nil},
		{"id alias",
			native("{}",
				map[string]any{"ID": attr("String",
					"x")}),
			broker.NewHandler(func(context.Context,
				broker.Delivery) (broker.Disposition,
				error) {
				t.Error("malformed metadata admitted")
				return broker.Handled{}, nil
			}), nil},
		{"binary shape",
			native("{}",
				map[string]any{"x": attr("Binary",
					"secret")}),
			broker.NewHandler(func(context.Context,
				broker.Delivery) (broker.Disposition,
				error) {
				t.Error("malformed metadata admitted")
				return broker.Handled{}, nil
			}), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deletes atomic.Int32
			var observations []adapter.Observation
			client, url := newWire(t, func(_ context.Context, op string, _ request) (int, any) {
				if op == "DeleteMessage" {
					deletes.Add(1)
				}
				return 200, map[string]any{"Messages": []any{tc.message}}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) { observations = append(observations, o) }}
			})
			err := consumer.Run(context.Background(), tc.handler)
			if err == nil || deletes.Load() != 0 || len(observations) != 1 {
				t.Fatalf("err=%v deletes=%d observation=%v", err, deletes.Load(), observations)
			}
			if tc.cause != nil && (!errors.Is(err, tc.cause) || !errors.Is(observations[0].Err, tc.cause)) {
				t.Fatal("original cause erased")
			}
		})
	}
}

func TestSDKNativeStringNumberBinaryAndNoIDFallback(t *testing.T) {
	stop := errors.New("stop")
	attributes := map[string]any{"text": attr("String.custom",
		"text"),
		"number": attr("Number.decimal",
			"12.34"),
		"binary": map[string]any{"DataType": "Binary.image",
			"BinaryValue": "AP8="}}
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 200, map[string]any{"Messages": []any{native("body", attributes)}}
	})
	consumer := newConsumer(t, client, url, nil)
	err := consumer.Run(context.Background(), broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
		m := d.Message()
		if m.ID != "" {
			t.Error("native id used as logical fallback")
		}
		expected := []broker.Header{{Name: "binary",
			Value: []byte{0,
				255}},
			{Name: "number",
				Value: []byte("12.34")},
			{Name: "text",
				Value: []byte("text")}}
		if !reflect.DeepEqual(m.Headers, expected) {
			t.Errorf("headers=%#v", m.Headers)
		}
		return nil, stop
	}))
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
}
