package sqs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

//nolint:gocyclo // One fixture exercises the service lifecycle and its independent failure cases.
func TestSQSServiceLifecycle(t *testing.T) {
	client := roundTripClient(t)
	ctx, finish := context.WithTimeout(context.Background(), 30*time.Second)
	defer finish()
	for _, mode := range []string{"pre-canceled", "idle", "deadline", "custom"} {
		t.Run(mode, func(t *testing.T) {
			queue := roundTripQueue(t, ctx, client, "service-"+mode, false)
			parent, stop := context.WithCancelCause(ctx)
			defer stop(nil)
			switch mode {
			case "deadline":
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
				defer cancel()
			case "pre-canceled":
				stop(errors.New("pre-canceled"))
			default:
				timer := time.AfterFunc(200*time.Millisecond, func() { stop(errors.New("requested stop")) })
				defer timer.Stop()
			}
			consumer, err := sqs.NewConsumer(client, sqs.ConsumerConfig{
				QueueURL:        queue,
				Source:          "orders",
				WaitTimeSeconds: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("unexpected callback")
				return broker.Handled{}, nil
			}))
			if err != nil {
				t.Fatalf("stop returned failure: %v", err)
			}
		})
	}
	for _, mode := range []string{"graceful", "forced", "joined", "grace-expiry", "settlement", "renewal"} {
		t.Run(mode, func(t *testing.T) {
			queue := roundTripQueue(t, ctx, client, "service-"+mode, false)
			visibilityPublish(t, ctx, client, queue)
			parent, stop := context.WithCancel(ctx)
			defer stop()
			entered := make(chan struct{})
			release := make(chan struct{})
			observed := make(chan error, 1)
			config := sqs.ConsumerConfig{
				QueueURL:          queue,
				Source:            "orders",
				WaitTimeSeconds:   1,
				VisibilityTimeout: 2 * time.Second,
				Observer: &sqs.Observer{
					Record: func(_ context.Context, event sqs.Observation) {
						if event.Err != nil && (event.Operation == sqs.OperationDelete || event.Operation == sqs.OperationRenew) {
							observed <- event.Err
						}
					},
				},
			}
			if mode == "forced" || mode == "joined" {
				config.Shutdown.Mode = broker.ShutdownCancel
			}
			if mode == "grace-expiry" {
				config.Shutdown.GracePeriod = 30 * time.Millisecond
			}
			consumer, err := sqs.NewConsumer(client, config)
			if err != nil {
				t.Fatal(err)
			}
			var options []broker.HandlerOption
			if mode == "renewal" {
				options = append(options, broker.WithKeepAlive(100*time.Millisecond))
			}
			handler := broker.NewHandler(func(callbackCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
				close(entered)
				if mode == "graceful" || mode == "settlement" {
					<-release
					return broker.Handled{}, nil
				}
				<-callbackCtx.Done()
				if mode == "joined" {
					return nil, errors.Join(callbackCtx.Err(), errServiceFailure)
				}
				return nil, callbackCtx.Err()
			}, options...)
			result := make(chan error, 1)
			go func() { result <- consumer.RunService(parent, handler) }()
			await(t, entered)
			if mode == "settlement" || mode == "renewal" {
				if _, err = client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{
					QueueUrl: aws.String(queue),
				}); err != nil {
					t.Fatal(err)
				}
			}
			stop()
			if mode == "graceful" || mode == "settlement" {
				close(release)
			}
			err = await(t, result)
			switch mode {
			case "graceful", "forced":
				if err != nil {
					t.Fatalf("stop failed: %v", err)
				}
			case "joined":
				if !errors.Is(err, errServiceFailure) || !errors.Is(err, context.Canceled) {
					t.Fatalf("callback causes lost: %v", err)
				}
			case "grace-expiry":
				if !errors.Is(err, sqs.ErrGracePeriodExceeded) {
					t.Fatalf("grace expiry lost: %v", err)
				}
			case "settlement", "renewal":
				original := await(t, observed)
				if !errors.Is(err, original) {
					t.Fatalf("SDK failure lost: result=%v observed=%v", err, original)
				}
			}
			if mode == "graceful" {
				assertRoundTripEmpty(t, ctx, client, queue)
			}
		})
	}
}
