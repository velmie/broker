package sqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestProcessorLeavesNativeVisibilityAndDeletionToCaller(t *testing.T) {
	client := roundTripClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := roundTripQueue(t, ctx, client, "native-processor", false)
	_, err := client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(queue), MessageBody: aws.String(`{"ID":"order"}`),
		MessageAttributes: map[string]types.MessageAttributeValue{"id": {DataType: aws.String("String"), StringValue: aws.String("logical-id")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queue), VisibilityTimeout: 30,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	}
	received, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(received.Messages) != 1 {
		t.Fatalf("native receive: %+v %v", received, err)
	}
	calls := 0
	processor, err := adapter.NewProcessor(broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{ ID string }],
		func(_ context.Context, delivery broker.Delivery, order struct{ ID string }) (broker.Disposition, error) {
			calls++
			if order.ID != "order" || delivery.Message().ID != "logical-id" || delivery.Source() != "orders" ||
				delivery.(broker.AttemptReader).Attempt() != (broker.Attempt{Number: 1, Known: true}) {
				t.Error("typed native receive lost metadata")
			}
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{})), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = processor.Process(ctx, &received.Messages[0]); err != nil {
		t.Fatal(err)
	}
	hidden, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(hidden.Messages) != 0 {
		t.Fatalf("processor unexpectedly changed visibility: %+v %v", hidden, err)
	}
	_, err = client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(queue), ReceiptHandle: received.Messages[0].ReceiptHandle, VisibilityTimeout: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(replay.Messages) != 1 {
		t.Fatalf("processor deleted native message: %+v %v", replay, err)
	}
	if replay.Messages[0].Attributes["ApproximateReceiveCount"] != "2" || calls != 1 {
		t.Fatal("native receive progress changed")
	}
	_, err = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(queue), ReceiptHandle: replay.Messages[0].ReceiptHandle})
	if err != nil {
		t.Fatal(err)
	}
	final, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(final.Messages) != 0 {
		t.Fatalf("native delete: %+v %v", final, err)
	}
}
