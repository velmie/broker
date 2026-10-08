package sqs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestPublisherNativeBuildRequestAndOutput(t *testing.T) {
	client, url := newWire(t, func(_ context.Context, operation string, input request) (int, any) {
		if operation != "SendMessage" || input["DelaySeconds"] != float64(1) || input["MessageBody"] != "payload" {
			t.Errorf("native request changed: operation=%s input=%v", operation, input)
		}
		return 200, request{"MessageId": "native-id", "MD5OfMessageBody": "body-md5"}
	})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	input, err := publisher.BuildRequest(broker.Message{ID: "logical-id", Body: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	input.DelaySeconds = 1
	output, err := client.SendMessage(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(output.MessageId) != "native-id" || aws.ToString(output.MD5OfMessageBody) != "body-md5" {
		t.Fatalf("native output lost: %+v", output)
	}
}

func TestPublisherNativeResult(t *testing.T) {
	client, url := newWire(t, func(context.Context, string, request) (int, any) { return 200, request{"MessageId": "accepted"} })
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{Body: []byte("payload")})
	if err != nil || output == nil || aws.ToString(output.MessageId) != "accepted" {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestPublisherNativeRequestDetachedAndFIFO(t *testing.T) {
	var requests, observations atomic.Int32
	client, url := newWire(t, func(context.Context, string, request) (int, any) { requests.Add(1); return 200, request{} })
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders",
		FIFO: true, MessageGroupID: "group",
		Observer: &adapter.Observer{Record: func(context.Context, adapter.Observation) { observations.Add(1) }}})
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
	if aws.ToString(input.MessageBody) != "payload" || input.MessageAttributes["Binary"].BinaryValue[0] != 0 {
		t.Fatal("request aliases message")
	}
	input.MessageAttributes["Binary"].BinaryValue[1] = 2
	if message.Headers[0].Value[1] != 255 {
		t.Fatal("message aliases native request")
	}
	if aws.ToString(input.MessageGroupId) != "group" || aws.ToString(input.MessageDeduplicationId) != "logical" ||
		aws.ToString(input.QueueUrl) != url {
		t.Fatalf("FIFO binding changed: %+v", input)
	}
	if _, err = publisher.BuildRequest(broker.Message{Body: []byte("payload")}); err == nil {
		t.Fatal("missing FIFO identity accepted")
	}
	if requests.Load() != 0 || observations.Load() != 0 {
		t.Fatal("BuildRequest performed I/O or observation")
	}
}

func TestPublisherNativeObserverFailureRetainsAcceptedOutput(t *testing.T) {
	marker := errors.New("observer unavailable")
	client, url := newWire(t, func(context.Context, string, request) (int, any) { return 200, request{"MessageId": "accepted"} })
	var observed adapter.Observation
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders",
		Observer: &adapter.Observer{Record: func(_ context.Context, event adapter.Observation) { observed = event; panic(marker) }}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{Body: []byte("payload")})
	if output == nil || aws.ToString(output.MessageId) != "accepted" || !errors.Is(err, marker) {
		t.Fatalf("output=%v err=%v", output, err)
	}
	if observed.Operation != adapter.OperationPublish || observed.Outcome != adapter.OutcomeAccepted || observed.Err != nil {
		t.Fatalf("accepted outcome changed: %+v", observed)
	}
}

func TestPublisherNativeResultThroughSQS(t *testing.T) {
	client := roundTripClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queue := roundTripQueue(t, ctx, client, "native-publication", false)
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: queue, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(ctx, broker.Message{ID: "logical-id", Body: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(output.MessageId) == "" || aws.ToString(output.MD5OfMessageBody) != "321c3cf486ed509164edec1e1981fec8" {
		t.Fatalf("native result missing ID or payload checksum: %+v", output)
	}
	received, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queue), WaitTimeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(received.Messages) != 1 || aws.ToString(received.Messages[0].MessageId) != aws.ToString(output.MessageId) {
		t.Fatalf("native output does not identify stored message: %+v", received)
	}
}

func TestPublisherNativeFailurePreservesOperationAndCause(t *testing.T) {
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 400, request{"__type": "QueueDoesNotExist", "message": "queue removed"}
	})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: url, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := publisher.PublishWithResult(context.Background(), broker.Message{Body: []byte("payload")})
	var operation *adapter.OperationError
	var native smithy.APIError
	if output != nil || !errors.As(err, &operation) || !errors.As(err, &native) {
		t.Fatalf("output=%v err=%v", output, err)
	}
	if operation.Source != "orders" || operation.Operation != adapter.OperationPublish || native.ErrorCode() != "QueueDoesNotExist" {
		t.Fatalf("native failure context changed: operation=%+v native=%v", operation, native)
	}
}
