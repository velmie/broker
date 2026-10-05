package sqs_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

func TestOrdersRoundTripThroughSQS(t *testing.T) {
	client := roundTripClient(t)
	for _, profile := range []struct {
		name  string
		fifo  bool
		group string
	}{
		{name: "orders"},
		{name: "orders.fifo", fifo: true, group: "orders"},
	} {
		t.Run(profile.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			queue := roundTripQueue(t, ctx, client, profile.name, profile.fifo)
			publisher, err := sqs.NewPublisher(client, sqs.PublisherConfig{
				QueueURL: queue, Source: "orders", FIFO: profile.fifo, MessageGroupID: profile.group,
			})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"order_id":"order-1"}`)
			binary := []byte{0xff, 0x00, 0x01}
			message := broker.Message{ID: "order-1.create", Body: bytes.Clone(body), Headers: []broker.Header{
				{Name: "Correlation-Id", Value: []byte("order-1")},
				{Name: "traceparent", Value: []byte("00-11111111111111111111111111111111-2222222222222222-01")},
				{Name: "opaque", Value: bytes.Clone(binary)},
			}}
			if err = publisher.Publish(ctx, message); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(message.Body, body) || len(message.Headers) != 3 || !bytes.Equal(message.Headers[2].Value, binary) {
				t.Fatal("publication changed caller data")
			}
			message.Body[0] = '!'
			message.Headers[2].Value[0] = '!'
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			var steps []string
			var received broker.Message
			observer := &sqs.Observer{Record: func(_ context.Context, event sqs.Observation) {
				if event.Operation == sqs.OperationDelete {
					if event.Outcome != sqs.OutcomeAccepted || event.Err != nil || event.Source != "orders" {
						t.Errorf("delete observation: %+v", event)
					}
					steps = append(steps, "delete accepted")
				}
			}}
			consumer, err := sqs.NewConsumer(client, sqs.ConsumerConfig{
				QueueURL: queue, Source: "orders", WaitTimeSeconds: 1, Observer: observer,
			})
			if err != nil {
				t.Fatal(err)
			}
			type command struct {
				OrderID string `json:"order_id"`
			}
			handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[*command],
				func(_ context.Context, delivery broker.Delivery, value *command) (broker.Disposition, error) {
					steps = append(steps, "effect")
					received = delivery.Message()
					if value.OrderID != "order-1" || received.ID != "order-1.create" || delivery.Source() != "orders" {
						t.Errorf("typed command or logical identity lost: %+v, %+v", value, received)
					}
					assertRoundTripMetadata(t, delivery, binary)
					stop() // Graceful stop still completes this admitted delivery.
					return broker.Handled{}, nil
				}, broker.WithRequirements(broker.AttemptsRequirement{}))
			if err = consumer.Run(runCtx, handler); !errors.Is(err, context.Canceled) {
				t.Fatalf("joined Run: %v", err)
			}
			if !reflect.DeepEqual(steps, []string{"effect", "delete accepted"}) {
				t.Fatalf("processing/transport order: %v", steps)
			}
			if !bytes.Equal(received.Body, body) {
				t.Fatal("retained body changed after callback return")
			}
			assertRoundTripHeaders(t, received, binary, message.Headers[1].Value)
			assertRoundTripEmpty(t, ctx, client, queue)
			// The borrowed client remains available after Run has joined.
			if _, err = client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(queue)}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertRoundTripMetadata(t *testing.T, delivery broker.Delivery, binary []byte) {
	t.Helper()
	attempt := delivery.(broker.AttemptReader).Attempt()
	if !attempt.Known || attempt.Number != 1 {
		t.Errorf("first attempt: %+v", attempt)
	}
	reader := delivery.(sqs.MetadataReader)
	metadata := reader.Metadata()
	if metadata.MessageID == "" || metadata.MessageID == delivery.Message().ID {
		t.Error("native and logical identity were not kept separate")
	}
	if !bytes.Equal(metadata.MessageAttributes["opaque"].BinaryValue, binary) {
		t.Fatal("native binary metadata changed")
	}
	metadata.MessageAttributes["opaque"].BinaryValue[0] = '!'
	if !bytes.Equal(reader.Metadata().MessageAttributes["opaque"].BinaryValue, binary) {
		t.Error("native metadata shares caller-mutated storage")
	}
}

func assertRoundTripHeaders(t *testing.T, received broker.Message, binary, traceparent []byte) {
	t.Helper()
	headers := make(map[string][]byte, len(received.Headers))
	for _, header := range received.Headers {
		headers[header.Name] = header.Value
	}
	if string(headers["id"]) != received.ID || string(headers["Correlation-Id"]) != "order-1" ||
		!bytes.Equal(headers["opaque"], binary) || !bytes.Equal(headers["traceparent"], traceparent) {
		t.Fatal("logical identity or application attributes changed in transport")
	}
}

func assertRoundTripEmpty(t *testing.T, ctx context.Context, client *awssqs.Client, queue string) {
	t.Helper()
	attributes, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queue), AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"} {
		if attributes.Attributes[name] != "0" {
			t.Fatalf("emulator retains a message after delete: %v", attributes.Attributes)
		}
	}
}

func roundTripClient(t *testing.T) *awssqs.Client {
	t.Helper()
	image := os.Getenv("SQS_TEST_IMAGE")
	if image == "" {
		image = "softwaremill/elasticmq-native:1.7.1"
	}
	output, err := roundTripDocker("run", "--rm", "-d", "-p", "127.0.0.1::9324", image)
	if err != nil {
		t.Fatalf("start SQS test service: %v: %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if cleanupOutput, cleanupErr := roundTripDocker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("remove SQS test service: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	output, err = roundTripDocker("port", container, "9324/tcp")
	if err != nil {
		t.Fatalf("find SQS test service port: %v: %s", err, output)
	}
	endpoint := "http://" + strings.TrimSpace(string(output))
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(options *awssqs.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.RetryMaxAttempts = 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		if _, err = client.ListQueues(ctx, &awssqs.ListQueuesInput{}); err == nil {
			return client
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SQS test service did not become ready: %v", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func roundTripQueue(t *testing.T, ctx context.Context, client *awssqs.Client, name string, fifo bool) string {
	t.Helper()
	input := &awssqs.CreateQueueInput{QueueName: aws.String(name)}
	if fifo {
		input.Attributes = map[string]string{"FifoQueue": "true"}
	}
	queue, err := client.CreateQueue(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	return aws.ToString(queue.QueueUrl)
}

func roundTripDocker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// #nosec G702 -- Fixed Docker operations use test-owned IDs and the selected CI image, without a shell.
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}
