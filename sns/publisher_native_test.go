package sns_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sns"
)

func TestPublisherNativeBuildRequestControl(t *testing.T) {
	client := wireClient(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.PostForm.Get("Subject") != "Order accepted" || r.PostForm.Get("Message") != "payload" {
			t.Errorf("native request changed: %v", r.PostForm)
		}
		publishOK(w)
	})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:orders", Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	input, err := publisher.BuildRequest(broker.Message{ID: "logical", Body: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	input.Subject = aws.String("Order accepted")
	output, err := client.Publish(context.Background(), input)
	if err != nil || output == nil || aws.ToString(output.MessageId) != "native" {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestPublisherNativeResult(t *testing.T) {
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) { publishOK(w) })
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:orders", Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{Body: []byte("payload")})
	if err != nil || output == nil || aws.ToString(output.MessageId) != "native" {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestPublisherNativeRequestDetachedFIFOAndRawBudget(t *testing.T) {
	var requests, observations atomic.Int32
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); publishOK(w) })
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:orders.fifo", Source: "orders", FIFO: true, MessageGroupID: "group", RawSQSDelivery: true,
		Observer: &adapter.Observer{Record: func(context.Context, adapter.Observation) { observations.Add(1) }},
	})
	if err != nil {
		t.Fatal(err)
	}
	message := broker.Message{ID: "logical", Body: []byte("payload"), Headers: []broker.Header{{Name: "Binary", Value: []byte{0, 255}}}}
	input, err := publisher.BuildRequest(message)
	if err != nil {
		t.Fatal(err)
	}
	message.Body[0] = 'X'
	message.Headers[0].Value[0] = 1
	if aws.ToString(input.Message) != "payload" || input.MessageAttributes["Binary"].BinaryValue[0] != 0 {
		t.Fatal("request aliases message")
	}
	input.MessageAttributes["Binary"].BinaryValue[1] = 2
	if message.Headers[0].Value[1] != 255 {
		t.Fatal("message aliases native request")
	}
	if aws.ToString(input.MessageGroupId) != "group" || aws.ToString(input.MessageDeduplicationId) != "logical" {
		t.Fatalf("FIFO mapping changed: %+v", input)
	}
	if _, err = publisher.BuildRequest(broker.Message{Body: []byte("payload")}); err == nil {
		t.Fatal("missing FIFO identity accepted")
	}
	message.Headers = nil
	for i := 0; i < 10; i++ {
		message.Headers = append(message.Headers, broker.Header{Name: fmt.Sprintf("Attr%d", i), Value: []byte("value")})
	}
	_, err = publisher.BuildRequest(message)
	var field *adapter.FieldError
	if !errors.As(err, &field) {
		t.Fatalf("raw subscription budget not enforced: %v", err)
	}
	if requests.Load() != 0 || observations.Load() != 0 {
		t.Fatal("BuildRequest performed I/O or observation")
	}
}

func TestPublisherNativeObserverFailureRetainsAcceptedOutput(t *testing.T) {
	marker := errors.New("observer unavailable")
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprint(w, `<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
<PublishResult><MessageId>accepted</MessageId><SequenceNumber>42</SequenceNumber></PublishResult>
<ResponseMetadata><RequestId>request</RequestId></ResponseMetadata></PublishResponse>`)
	})
	var observed adapter.Observation
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:orders.fifo", Source: "orders", FIFO: true, MessageGroupID: "group",
		Observer: &adapter.Observer{Record: func(_ context.Context, event adapter.Observation) { observed = event; panic(marker) }},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{ID: "logical", Body: []byte("payload")})
	if output == nil || aws.ToString(output.MessageId) != "accepted" || aws.ToString(output.SequenceNumber) != "42" ||
		!errors.Is(err, marker) {
		t.Fatalf("output=%v err=%v", output, err)
	}
	if observed.Operation != adapter.OperationPublish || observed.Outcome != adapter.OutcomeAccepted || observed.Err != nil {
		t.Fatalf("accepted outcome changed: %+v", observed)
	}
}

func TestPublisherNativeFailurePreservesOperationAndCause(t *testing.T) {
	client := wireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
<Error><Type>Sender</Type><Code>InvalidParameter</Code><Message>rejected</Message></Error><RequestId>request</RequestId></ErrorResponse>`)
	})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:orders", Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{Body: []byte("payload")})
	var operation *adapter.OperationError
	var native smithy.APIError
	if output != nil || !errors.As(err, &operation) || !errors.As(err, &native) {
		t.Fatalf("output=%v err=%v", output, err)
	}
	if operation.Source != "orders" || operation.Operation != adapter.OperationPublish || native.ErrorCode() != "InvalidParameter" {
		t.Fatalf("native failure context changed: operation=%+v native=%v", operation, native)
	}
}
