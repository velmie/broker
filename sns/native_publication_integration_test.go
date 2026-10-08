package sns_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sns"
)

func TestNativePublicationResultMatchesForwardedNotification(t *testing.T) {
	snsClient, sqsClient := forwardingClients(t, "SNS_TEST_IMAGE", "motoserver/moto:5.2.3")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic, queue := forwardingRoute(t, ctx, snsClient, sqsClient, "native-publication", false, false)
	publisher, err := adapter.NewPublisher(snsClient, adapter.PublisherConfig{TopicARN: topic, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	input := broker.Message{ID: "native-logical-id", Body: []byte(`{"ID":"native-order"}`)}
	request, err := publisher.BuildRequest(input)
	if err != nil {
		t.Fatal(err)
	}
	request.Subject = aws.String("Order received")
	result, err := snsClient.Publish(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	assertNativeNotification(t, ctx, sqsClient, queue, topic, aws.ToString(result.MessageId), "Order received", input)
	bound := broker.Message{ID: "bound-logical-id", Body: []byte(`{"ID":"bound-order"}`)}
	result, err = publisher.PublishWithResult(ctx, bound)
	if err != nil {
		t.Fatal(err)
	}
	assertNativeNotification(t, ctx, sqsClient, queue, topic, aws.ToString(result.MessageId), "", bound)
}

func assertNativeNotification(t *testing.T, ctx context.Context, client *awssqs.Client,
	queue, topic, messageID, subject string, expected broker.Message,
) {
	t.Helper()
	received, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queue), WaitTimeSeconds: 1})
	if err != nil || len(received.Messages) != 1 {
		t.Fatalf("forwarded native notification: %+v %v", received, err)
	}
	body := []byte(aws.ToString(received.Messages[0].Body))
	var envelope struct {
		MessageID string `json:"MessageId"`
		Subject   string
	}
	if err = json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if messageID == "" || envelope.MessageID != messageID || envelope.Subject != subject {
		t.Fatalf("native result/notification identity: id=%q envelope=%+v", messageID, envelope)
	}
	message, err := adapter.DecodeNotification(body, topic)
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != expected.ID || !bytes.Equal(message.Body, expected.Body) {
		t.Fatalf("logical message changed: %+v", message)
	}
	_, err = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(queue), ReceiptHandle: received.Messages[0].ReceiptHandle})
	if err != nil {
		t.Fatal(err)
	}
}
