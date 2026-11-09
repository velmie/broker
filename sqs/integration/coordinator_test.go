package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/sqs"
)

//nolint:gocyclo // This scenario provisions both services and verifies their coordinated delivery and stop.
func TestCoordinatorWithNATSAndSQS(t *testing.T) {
	sqsEndpoint := container(t, "SQS_TEST_IMAGE", "softwaremill/elasticmq-native:1.7.1", "9324")
	natsEndpoint := container(t, "NATS_TEST_IMAGE", "nats:2.15.0", "4222", "--jetstream")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := awssqs.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(options *awssqs.Options) {
		options.BaseEndpoint = aws.String("http://" + sqsEndpoint)
		options.RetryMaxAttempts = 1
	})
	var queue *awssqs.CreateQueueOutput
	var err error
	for ctx.Err() == nil {
		queue, err = client.CreateQueue(ctx, &awssqs.CreateQueueInput{
			QueueName: aws.String("coordinator"),
		})
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := sqs.NewPublisher(client, sqs.PublisherConfig{
		QueueURL: aws.ToString(queue.QueueUrl),
		Source:   "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(ctx, broker.Message{
		ID:   "one",
		Body: []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}
	var nc *nats.Conn
	for ctx.Err() == nil {
		nc, err = nats.Connect("nats://"+natsEndpoint, nats.NoReconnect(), nats.Timeout(time.Second))
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:       "worker",
		FilterSubject: "orders",
		AckPolicy:     nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish("orders", []byte("two")); err != nil {
		t.Fatal(err)
	}
	coordinator := broker.NewCoordinator()
	deliveries := make(chan string, 2)
	handler := func(name string) broker.Handler {
		return broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			deliveries <- name
			return broker.Handled{}, nil
		})
	}
	if err = coordinator.Add("sqs", func() (broker.Consumer, error) {
		return sqs.NewConsumer(client, sqs.ConsumerConfig{
			QueueURL:        aws.ToString(queue.QueueUrl),
			Source:          "orders",
			WaitTimeSeconds: 1,
		})
	}, handler("sqs")); err != nil {
		t.Fatal(err)
	}
	if err = coordinator.Add("nats", func() (broker.Consumer, error) {
		return natsjs.NewConsumer(nc, natsjs.ConsumerConfig{
			Stream:           "ORDERS",
			Consumer:         "worker",
			Subject:          "orders",
			FetchTimeout:     100 * time.Millisecond,
			OperationTimeout: time.Second,
		})
	}, handler("nats")); err != nil {
		t.Fatal(err)
	}
	parent, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- coordinator.RunService(parent) }()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-deliveries:
			seen[name] = true
		case err = <-done:
			t.Fatalf("coordinator returned early: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	stop()
	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("coordinator stop: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func container(t *testing.T, env, fallback, port string, args ...string) string {
	t.Helper()
	image := os.Getenv(env)
	if image == "" {
		image = fallback
	}
	arguments := append([]string{"run", "--rm", "-d", "-p", "127.0.0.1::" + port, image}, args...)
	output, err := docker(arguments...)
	if err != nil {
		t.Fatalf("start test service: %v: %s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if cleanupOutput, cleanupErr := docker("rm", "-f", id); cleanupErr != nil {
			t.Errorf("remove test service: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	output, err = docker("port", id, port+"/tcp")
	if err != nil {
		t.Fatalf("find service port: %v: %s", err, output)
	}
	return strings.TrimSpace(string(output))
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// #nosec G702 -- arguments are fixed test operations, images and test-owned container IDs.
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("docker: %w", err)
	}
	return output, nil
}
