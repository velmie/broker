package sns_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	qtypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

func TestSNSForwardingToTypedSQSConsumer(t *testing.T) {
	snsClient, sqsClient := forwardingClients(t, "SNS_TEST_IMAGE", "motoserver/moto:5.2.3")
	for _, profile := range []struct {
		name string
		raw  bool
		fifo bool
	}{
		{name: "raw", raw: true},
		{name: "envelope"},
		{name: "raw-fifo", raw: true, fifo: true},
		{name: "envelope-fifo", fifo: true},
	} {
		t.Run(profile.name, func(t *testing.T) {
			binary := []byte{0xff, 0x00, 0x01}
			if profile.raw {
				binary = []byte("opaque-value")
			}
			assertForwardingProfile(t, snsClient, sqsClient, profile.name, profile.raw, profile.fifo, binary)
		})
	}
}

// Moto 5.2.3 stores raw SNS BinaryValue as text and fails while computing SQS
// MD5. The earlier release proves binary forwarding through its working FIFO
// route. The current release above proves standard-group and envelope routes.
func TestSNSRawBinaryForwarding(t *testing.T) {
	snsClient, sqsClient := forwardingClients(t, "SNS_BINARY_TEST_IMAGE", "motoserver/moto:5.1.14")
	assertForwardingProfile(t, snsClient, sqsClient, "binary-raw", true, true, []byte{0xff, 0x00, 0x01})
}

func assertForwardingProfile(t *testing.T, snsClient *awssns.Client, sqsClient *awssqs.Client,
	name string, raw, fifo bool, binary []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic, queue := forwardingRoute(t, ctx, snsClient, sqsClient, name, raw, fifo)
	publisher, err := adapter.NewPublisher(snsClient, adapter.PublisherConfig{
		TopicARN: topic, Source: "orders/published", FIFO: fifo,
		MessageGroupID: "orders", RawSQSDelivery: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"order_id":"forwarded-order-1"}`)
	trace := []byte("00-11111111111111111111111111111111-2222222222222222-01")
	message := broker.Message{ID: "forwarded-event-1", Body: bytes.Clone(body), Headers: []broker.Header{
		{Name: "Correlation-Id", Value: []byte("forwarded-order-1")},
		{Name: "traceparent", Value: bytes.Clone(trace)},
		{Name: "opaque", Value: bytes.Clone(binary)},
	}}
	if err = publisher.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	if len(message.Headers) != 3 || !bytes.Equal(message.Body, body) || !bytes.Equal(message.Headers[2].Value, binary) {
		t.Fatal("SNS publication changed caller data")
	}
	message.Body[0] = '!'
	message.Headers[2].Value[0] = '!'
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var steps []string
	var retained broker.Message
	consumer, err := sqs.NewConsumer(sqsClient, sqs.ConsumerConfig{
		QueueURL: queue, Source: "orders/received", WaitTimeSeconds: 1,
		Observer: &sqs.Observer{Record: func(_ context.Context, event sqs.Observation) {
			if event.Operation == sqs.OperationDelete {
				if event.Outcome != sqs.OutcomeAccepted || event.Err != nil {
					t.Errorf("forwarded delete: %+v", event)
				}
				steps = append(steps, "delete")
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	type command struct {
		OrderID string `json:"order_id"`
	}
	typed := broker.NewTypedDeliveryHandler(broker.DecodeJSON[command],
		func(_ context.Context, delivery broker.Delivery, value command) (broker.Disposition, error) {
			steps = append(steps, "effect")
			retained = delivery.Message()
			if value.OrderID != "forwarded-order-1" || retained.ID != "forwarded-event-1" {
				t.Errorf("forwarded command or logical identity changed: %+v id=%q", value, retained.ID)
			}
			assertForwardedMetadata(t, delivery, binary, trace)
			stop()
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}))
	handler := typed
	if !raw {
		handler = notificationHandler(t, typed, topic, body)
	}
	if err = consumer.Run(runCtx, handler); !onlyForwardingCancellation(err) {
		t.Fatalf("forwarded consumer: %v", err)
	}
	if strings.Join(steps, ",") != "effect,delete" || !bytes.Equal(retained.Body, body) {
		t.Fatalf("processing/delete order or retained body changed: %v", steps)
	}
	assertForwardingEmpty(t, ctx, sqsClient, queue)
}

type forwardedDelivery struct {
	original broker.Delivery
	message  broker.Message
}

func (d *forwardedDelivery) Message() broker.Message { return d.message }
func (d *forwardedDelivery) Source() string          { return d.original.Source() }
func (d *forwardedDelivery) Attempt() broker.Attempt {
	return d.original.(broker.AttemptReader).Attempt()
}
func (d *forwardedDelivery) Metadata() sqs.Metadata {
	return d.original.(sqs.MetadataReader).Metadata()
}

func assertForwardedMetadata(t *testing.T, delivery broker.Delivery, binary, trace []byte) {
	t.Helper()
	headers := make(map[string][]byte)
	for _, header := range delivery.Message().Headers {
		headers[header.Name] = header.Value
	}
	if string(headers["id"]) != "forwarded-event-1" || string(headers["Correlation-Id"]) != "forwarded-order-1" ||
		!bytes.Equal(headers["traceparent"], trace) || !bytes.Equal(headers["opaque"], binary) {
		t.Error("forwarding lost logical identity, correlation, trace or binary metadata")
	}
	attempt := delivery.(broker.AttemptReader).Attempt()
	if !attempt.Known || attempt.Number != 1 {
		t.Errorf("forwarded attempt: %+v", attempt)
	}
	reader := delivery.(sqs.MetadataReader)
	metadata := reader.Metadata()
	if metadata.MessageID == "" || metadata.MessageID == delivery.Message().ID {
		t.Error("native and logical identity were conflated")
	}
	if metadata.Attributes["MessageGroupId"] != "orders" {
		t.Error("native group was not forwarded")
	}
	metadata.Attributes["ApproximateReceiveCount"] = "modified"
	if reader.Metadata().Attributes["ApproximateReceiveCount"] != "1" {
		t.Error("native metadata is not detached")
	}
}

func notificationHandler(t *testing.T, typed broker.Handler, topic string, body []byte) broker.Handler {
	t.Helper()
	return broker.NewHandler(func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		outer := delivery.Message()
		if outer.ID != "" || bytes.Equal(outer.Body, body) {
			t.Error("SQS default path did not retain the undecoded envelope")
		}
		inner, err := adapter.DecodeNotification(outer.Body, topic)
		if err != nil {
			return nil, &broker.DecodeError{Cause: err}
		}
		return typed.Handle(ctx, &forwardedDelivery{original: delivery, message: inner})
	}, broker.WithRequirements(typed.Requirements()...))
}

func assertForwardingEmpty(t *testing.T, ctx context.Context, client *awssqs.Client, queue string) {
	t.Helper()
	attributes, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queue), AttributeNames: []qtypes.QueueAttributeName{
			qtypes.QueueAttributeNameApproximateNumberOfMessages, qtypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"} {
		if attributes.Attributes[name] != "0" {
			t.Fatalf("forwarded message still queued after accepted delete: %v", attributes.Attributes)
		}
	}
}

func forwardingRoute(t *testing.T, ctx context.Context, snsClient *awssns.Client, sqsClient *awssqs.Client,
	name string, raw, fifo bool) (topicARN, queueURL string) {
	t.Helper()
	topicAttributes, queueAttributes := map[string]string{}, map[string]string{}
	if fifo {
		name += ".fifo"
		topicAttributes["FifoTopic"], queueAttributes["FifoQueue"] = "true", "true"
	}
	topic, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String(name), Attributes: topicAttributes})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: queueAttributes})
	if err != nil {
		t.Fatal(err)
	}
	attributes, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl, AttributeNames: []qtypes.QueueAttributeName{qtypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	queueARN := attributes.Attributes["QueueArn"]
	policy, err := json.Marshal(map[string]any{
		"Version": "2012-10-17", "Statement": []any{map[string]any{
			"Effect": "Allow", "Principal": map[string]string{"Service": "sns.amazonaws.com"},
			"Action": "sqs:SendMessage", "Resource": queueARN,
			"Condition": map[string]any{"ArnEquals": map[string]string{"aws:SourceArn": aws.ToString(topic.TopicArn)}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sqsClient.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: queue.QueueUrl, Attributes: map[string]string{"Policy": string(policy)},
	}); err != nil {
		t.Fatal(err)
	}
	subscription, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(queueARN), ReturnSubscriptionArn: true,
		Attributes: map[string]string{"RawMessageDelivery": fmt.Sprint(raw)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, unsubscribeErr := snsClient.Unsubscribe(cleanupCtx, &awssns.UnsubscribeInput{SubscriptionArn: subscription.SubscriptionArn})
		_, topicErr := snsClient.DeleteTopic(cleanupCtx, &awssns.DeleteTopicInput{TopicArn: topic.TopicArn})
		_, queueErr := sqsClient.DeleteQueue(cleanupCtx, &awssqs.DeleteQueueInput{QueueUrl: queue.QueueUrl})
		if cleanupErr := errors.Join(unsubscribeErr, topicErr, queueErr); cleanupErr != nil {
			t.Errorf("remove forwarding route: %v", cleanupErr)
		}
	})
	return aws.ToString(topic.TopicArn), aws.ToString(queue.QueueUrl)
}

func forwardingClients(t *testing.T, imageVariable, defaultImage string) (snsClient *awssns.Client, sqsClient *awssqs.Client) {
	t.Helper()
	image := os.Getenv(imageVariable)
	if image == "" {
		image = defaultImage
	}
	output, err := forwardingDocker("run", "--rm", "-d", "-p", "127.0.0.1::5000", image)
	if err != nil {
		t.Fatalf("start SNS forwarding service: %v: %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if t.Failed() {
			logs, logErr := forwardingDocker("logs", "--tail", "100", container)
			t.Logf("forwarding service diagnostics: %v\n%s", logErr, logs)
		}
		if cleanupOutput, cleanupErr := forwardingDocker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("remove forwarding service: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	output, err = forwardingDocker("port", container, "5000/tcp")
	if err != nil {
		t.Fatalf("forwarding service endpoint: %v: %s", err, output)
	}
	endpoint := "http://" + strings.TrimSpace(string(output))
	config := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	snsClient = awssns.NewFromConfig(config, func(options *awssns.Options) {
		options.BaseEndpoint, options.RetryMaxAttempts = aws.String(endpoint), 1
	})
	sqsClient = awssqs.NewFromConfig(config, func(options *awssqs.Options) {
		options.BaseEndpoint, options.RetryMaxAttempts = aws.String(endpoint), 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if _, err = snsClient.ListTopics(ctx, &awssns.ListTopicsInput{}); err == nil {
			return snsClient, sqsClient
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SNS forwarding service readiness: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func forwardingDocker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// #nosec G702 -- Fixed Docker operations use test-owned IDs and the selected CI image without a shell.
	command := exec.CommandContext(ctx, "docker", args...)
	if args[0] == "logs" {
		return command.CombinedOutput()
	}
	output, err := command.Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = fmt.Errorf("Docker operation: %w: %s", err, exit.Stderr)
	}
	return output, err
}

func onlyForwardingCancellation(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyForwardingCancellation(cause) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyForwardingCancellation(cause)
	}
	return false
}
