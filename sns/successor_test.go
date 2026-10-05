package sns_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	native "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sns"
)

func TestNativePublishMappingAndReadOnly(t *testing.T) {
	var got url.Values
	client := wireClient(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		got = r.PostForm
		publishOK(w)
	})
	topic := "arn:aws-cn:sns:cn-north-1:123456789012:orders.fifo"
	pub,
		err := adapter.NewPublisher(client,
		adapter.PublisherConfig{TopicARN: topic,
			Source:         "orders",
			FIFO:           true,
			MessageGroupID: "customers"})
	if err != nil {
		t.Fatal(err)
	}
	msg := broker.Message{ID: "logical",
		Body: []byte(`{"amount":42}`),
		Headers: []broker.Header{{Name: "binary",
			Value: []byte{0,
				255}},
			{Name: "traceparent",
				Value: []byte("trace")}}}
	before := broker.Message{ID: msg.ID,
		Body: bytes.Clone(msg.Body),
		Headers: []broker.Header{{Name: "binary",
			Value: []byte{0,
				255}},
			{Name: "traceparent",
				Value: []byte("trace")}}}
	if err = pub.Publish(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(msg, before) {
		t.Fatal("publisher mutated caller data")
	}
	if got.Get("TopicArn") != topic ||
		got.Get("Message") != string(msg.Body) ||
		got.Get("MessageGroupId") != "customers" ||
		got.Get("MessageDeduplicationId") != "logical" {
		t.Fatalf("native mapping: %v", got)
	}
	attrs := map[string][2]string{}
	for i := 1; i <= 3; i++ {
		prefix := fmt.Sprintf("MessageAttributes.entry.%d.", i)
		v := got.Get(prefix + "Value.StringValue")
		if v == "" {
			v = got.Get(prefix + "Value.BinaryValue")
		}
		attrs[got.Get(prefix+"Name")] = [2]string{got.Get(prefix + "Value.DataType"), v}
	}
	if attrs["id"] != [2]string{"String",
		"logical"} ||
		attrs["binary"] != [2]string{"Binary",
			base64.StdEncoding.EncodeToString([]byte{0,
				255})} ||
		attrs["traceparent"] != [2]string{"String",
			"trace"} {
		t.Fatalf("attributes: %v", attrs)
	}
}

func TestNativeValidationAndRouteBudget(t *testing.T) {
	var calls atomic.Int32
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); publishOK(w) })
	cfg := adapter.PublisherConfig{TopicARN: "arn:aws:sns:us-east-1:123456789012:orders", Source: "orders", RawSQSDelivery: true}
	pub, err := adapter.NewPublisher(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cases := []broker.Message{{},
		{Body: []byte{255}},
		{Body: []byte("ok"),
			Headers: []broker.Header{{Name: "x",
				Value: []byte("a")},
				{Name: "x",
					Value: []byte("b")}}},
		{ID: "a",
			Body: []byte("ok"),
			Headers: []broker.Header{{Name: "ID",
				Value: []byte("a")}}},
		{ID: "a",
			Body: []byte("ok"),
			Headers: []broker.Header{{Name: "id",
				Value: []byte("b")}}}}
	for i, msg := range cases {
		if err = pub.Publish(context.Background(), msg); err == nil {
			t.Errorf("invalid case %d accepted", i)
		}
	}
	many := broker.Message{ID: "logical", Body: []byte("ok")}
	for i := 0; i < 10; i++ {
		many.Headers = append(many.Headers, broker.Header{Name: fmt.Sprintf("h%d", i), Value: []byte("v")})
	}
	if err = pub.Publish(context.Background(), many); err == nil {
		t.Error("raw final attribute limit ignored")
	}
	if calls.Load() != 0 {
		t.Fatal("validation made native calls")
	}
	cfg.RawSQSDelivery = false
	pub, err = adapter.NewPublisher(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = pub.Publish(context.Background(), many); err != nil {
		t.Fatal("non-raw budget", err)
	}
	if err = pub.Publish(context.Background(), broker.Message{Body: bytes.Repeat([]byte("x"), 300000)}); err != nil {
		t.Fatal("adapter imposed obsolete global 256KiB maximum", err)
	}
}

func TestNativeCancellationAndBudget(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client := wireClient(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	pub,
		err := adapter.NewPublisher(client,
		adapter.PublisherConfig{TopicARN: "arn:aws:sns:us-east-1:123456789012:orders",
			Source:           "orders",
			OperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = pub.Publish(context.Background(), broker.Message{Body: []byte("ok")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost operation deadline: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = pub.Publish(ctx, broker.Message{Body: []byte("ok")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost caller cancellation: %v", err)
	}
}

func TestExplicitNotificationDecoding(t *testing.T) {
	const topic = "arn:aws:sns:us-east-1:123456789012:orders"
	body := []byte(`{"Type":"Notification","TopicArn":"` + topic + `","MessageId":"provider-id",
"Message":"payload",
"MessageAttributes":{"id":{"Type":"String",
"Value":"logical"},
"traceparent":{"Type":"String",
"Value":"trace"},
"binary":{"Type":"Binary",
"Value":"AP8="},
"number":{"Type":"Number",
"Value":"42"},
"array":{"Type":"String.Array",
"Value":"[1,2]"}}}`)
	msg, err := adapter.DecodeNotification(body, topic)
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != "logical" || string(msg.Body) != "payload" {
		t.Fatalf("identity/body: %+v", msg)
	}
	attrs := map[string][]byte{}
	for _, h := range msg.Headers {
		attrs[h.Name] = h.Value
	}
	if !bytes.Equal(attrs["binary"],
		[]byte{0,
			255}) ||
		string(attrs["traceparent"]) != "trace" ||
		string(attrs["number"]) != "42" ||
		string(attrs["array"]) != "[1,2]" {
		t.Fatalf("nested attrs: %v", attrs)
	}
	for i := range body {
		body[i] = 'x'
	}
	if string(msg.Body) != "payload" {
		t.Fatal("decoder aliases input")
	}
	plain,
		err := adapter.DecodeNotification([]byte(`{"Type":"Notification","TopicArn":"`+topic+`",
"MessageId":"provider",
"Message":"body"}`),
		topic)
	if err != nil || plain.ID != "" {
		t.Fatalf("native identity fallback: %+v %v", plain, err)
	}
}

func TestNotificationRejectsAmbiguityAndPreservesCause(t *testing.T) {
	const topic = "topic"
	cases := []string{
		`{"Type":"SubscriptionConfirmation","TopicArn":"topic","Message":"x"}`,
		`{"Type":"Notification","TopicArn":"other","Message":"x"}`,
		`{"Type":"Notification","TopicArn":"topic","Message":"x","Message":"y"}`,
		`{"Type":"Notification","TopicArn":"topic","Message":"x","message":"y"}`,
		`{"Type":"Notification","TopicArn":"topic","Message":"x","MessageAttributes":{"id":{"Type":"String",
"Value":"a"},
"id":{"Type":"String",
"Value":"b"}}}`,

		`{"Type":"Notification","TopicArn":"topic","Message":"x","MessageAttributes":{"ID":{"Type":"String","Value":"a"}}}`,
		`{"Type":"Notification","TopicArn":"topic","Message":"x","MessageAttributes":{"id":{"Type":"Binary","Value":"YQ=="}}}`,
		`{"Type":"Notification","TopicArn":"topic","Message":"x","MessageAttributes":{"a":{"Type":"String","Value":"x","value":"y"}}}`,
	}
	for i, input := range cases {
		if _, err := adapter.DecodeNotification([]byte(input), topic); err == nil {
			t.Errorf("ambiguous case %d accepted", i)
		}
	}
	_,
		err := adapter.DecodeNotification([]byte(`{"Type":"Notification","TopicArn":"topic",
"Message":"x",
"MessageAttributes":{"binary":{"Type":"Binary",
"Value":"!"}}}`),
		topic)
	var cause base64.CorruptInputError
	if !errors.As(err, &cause) {
		t.Fatalf("base64 cause lost: %v", err)
	}
	var field *adapter.FieldError
	if !errors.As(err, &field) || field.Field == "" {
		t.Fatalf("affected field lost: %v", err)
	}
}

func wireClient(t *testing.T, handler http.HandlerFunc) *native.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return native.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("synthetic",
			"synthetic",
			""),
		RetryMaxAttempts: 1},
		func(o *native.Options) { o.BaseEndpoint = aws.String(server.URL) })
}
func publishOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/xml")
	_,
		_ = fmt.Fprint(w,
		`<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
<PublishResult>
<MessageId>native</MessageId>
</PublishResult>
<ResponseMetadata>
<RequestId>request</RequestId>
</ResponseMetadata>
</PublishResponse>`)
}

func TestNativeFailureObservationAndPanic(t *testing.T) {
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusForbidden)
		_,
			_ = fmt.Fprint(w,
			`<ErrorResponse><Error><Type>Sender</Type><Code>AuthorizationError</Code>
<Message>private-token-value</Message>
</Error>
<RequestId>request</RequestId>
</ErrorResponse>`)
	})
	var observed adapter.Observation
	observer := &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) { observed = o }}
	cfg := adapter.PublisherConfig{TopicARN: "arn:aws:sns:us-east-1:123456789012:orders", Source: "orders", Observer: observer}
	pub, err := adapter.NewPublisher(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	observer.Record = nil
	err = pub.Publish(context.Background(), broker.Message{Body: []byte("payload")})
	var api smithy.APIError
	var operation *adapter.OperationError
	if !errors.As(err,
		&api) ||
		api.ErrorCode() != "AuthorizationError" ||
		!errors.As(err,
			&operation) ||
		operation.Source != "orders" ||
		operation.Operation != adapter.OperationPublish {
		t.Fatalf("native cause/context lost: %v", err)
	}
	if observed.Err == nil || observed.Source != "orders" || observed.Outcome != adapter.OutcomeUnknown || !errors.As(observed.Err, &api) {
		t.Fatalf("observation lost original cause: %+v", observed)
	}
	marker := errors.New("observer failure")
	cfg.Observer = &adapter.Observer{Record: func(context.Context, adapter.Observation) { panic(marker) }}
	pub, err = adapter.NewPublisher(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = pub.Publish(context.Background(), broker.Message{Body: []byte("payload")})
	if !errors.Is(err, marker) || !errors.As(err, &api) {
		t.Fatalf("observer replaced primary cause: %v", err)
	}
}

func TestNotificationLimitsAndSyntaxCause(t *testing.T) {
	_, err := adapter.DecodeNotification([]byte(`{"Type":`), "topic")
	var syntax *json.SyntaxError
	if !errors.As(err, &syntax) {
		t.Fatalf("JSON syntax cause lost: %v", err)
	}
	for _, body := range [][]byte{bytes.Repeat([]byte(" "),
		1024*1024+1),
		[]byte(`null`),
		[]byte(`[]`),
		[]byte(`{"Type":"Notification",
"TopicArn":"topic",
"Message":null}`),
		[]byte(`{"Type":"Notification",
"TopicArn":"topic",
"Message":"x"} {}`)} {
		if _, err = adapter.DecodeNotification(body, "topic"); err == nil {
			t.Error("invalid envelope accepted")
		}
	}
}

func TestNativeStandardGroupAndConcurrentReadOnly(t *testing.T) {
	const topic = "arn:aws-us-gov:sns:us-gov-west-1:123456789012:orders"
	var calls atomic.Int32
	client := wireClient(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.PostForm.Get("TopicArn") != topic || r.PostForm.Get("MessageGroupId") != "fair-group" ||
			r.PostForm.Get("MessageDeduplicationId") != "" {
			t.Errorf("standard native routing changed: %v", r.PostForm)
		}
		calls.Add(1)
		publishOK(w)
	})
	pub, err := adapter.NewPublisher(client, adapter.PublisherConfig{TopicARN: topic, Source: "orders", MessageGroupID: "fair-group"})
	if err != nil {
		t.Fatal(err)
	}
	msg := broker.Message{Body: []byte("body"), ID: "id", Headers: []broker.Header{{Name: "opaque", Value: []byte{0xff, 0}}}}
	results := make(chan error, 4)
	for i := 0; i < cap(results); i++ {
		go func() { results <- pub.Publish(context.Background(), msg) }()
	}
	for i := 0; i < cap(results); i++ {
		if err = <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 4 || string(msg.Body) != "body" || !bytes.Equal(msg.Headers[0].Value, []byte{0xff, 0}) {
		t.Fatal("concurrent publication mutated or lost input")
	}
}
