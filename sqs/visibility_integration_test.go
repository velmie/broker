package sqs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sqs"
)

func TestSQSNativeVisibility(t *testing.T) {
	client := roundTripClient(t)
	t.Run("immediate redelivery", func(t *testing.T) { assertNativeRedelivery(t, client, "retry-now", 0) })
	t.Run("delayed redelivery", func(t *testing.T) { assertNativeRedelivery(t, client, "retry-later", time.Second) })
	t.Run("renewal protects graceful drain", func(t *testing.T) { assertNativeRenewal(t, client) })
}

func assertNativeRedelivery(t *testing.T, client *awssqs.Client, name string, delay time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queue := roundTripQueue(t, ctx, client, name, false)
	visibilityPublish(t, ctx, client, queue)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var called uint64
	var firstID string
	var firstAt time.Time
	var retryAccepted atomic.Bool
	consumer, err := sqs.NewConsumer(client, sqs.ConsumerConfig{
		QueueURL: queue, Source: "orders", VisibilityTimeout: 5 * time.Second, WaitTimeSeconds: 1,
		Observer: &sqs.Observer{Record: func(_ context.Context, event sqs.Observation) {
			if event.Operation == "retry" && event.Outcome == sqs.OutcomeAccepted {
				retryAccepted.Store(true)
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		called++
		attempt := delivery.(broker.AttemptReader).Attempt()
		if !attempt.Known || attempt.Number != called || delivery.Message().ID != "order-retry-1" {
			t.Errorf("redelivery identity/attempt: id=%q attempt=%+v call=%d", delivery.Message().ID, attempt, called)
		}
		nativeID := delivery.(sqs.MetadataReader).Metadata().MessageID
		if called == 1 {
			firstID, firstAt = nativeID, time.Now()
			return broker.RetryAfter{Delay: delay}, nil
		}
		if called != 2 || nativeID != firstID || !retryAccepted.Load() {
			t.Errorf("expected the same native message after visibility acceptance: calls=%d nativeID=%q", called, nativeID)
		}
		if delay > 0 && time.Since(firstAt) < delay-100*time.Millisecond {
			t.Error("native redelivery ignored requested visibility delay")
		}
		stop()
		return broker.Handled{}, nil
	}, broker.WithRequirements(broker.RedeliveryRequirement{}, broker.AttemptsRequirement{}))
	if err = consumer.Run(runCtx, handler); !cancellationOnly(err) {
		t.Fatalf("native retry run failed: %v", err)
	}
	if called != 2 {
		t.Fatalf("callbacks=%d, expected native second delivery", called)
	}
	assertRoundTripEmpty(t, ctx, client, queue)
}

func assertNativeRenewal(t *testing.T, client *awssqs.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queue := roundTripQueue(t, ctx, client, "renew-active", false)
	visibilityPublish(t, ctx, client, queue)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var renewed atomic.Uint32
	consumer, err := sqs.NewConsumer(client, sqs.ConsumerConfig{
		QueueURL: queue, Source: "orders", VisibilityTimeout: time.Second, WaitTimeSeconds: 1,
		Observer: &sqs.Observer{Record: func(_ context.Context, event sqs.Observation) {
			if event.Operation == "renew" && event.Outcome == sqs.OutcomeAccepted {
				renewed.Add(1)
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := broker.NewHandler(func(callbackCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
		stop() // Graceful shutdown retains renewal until this admitted work finishes.
		timer := time.NewTimer(1300 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-callbackCtx.Done():
			return nil, context.Cause(callbackCtx)
		case <-timer.C:
		}
		// The original one-second visibility has elapsed. A second native receiver
		// cannot obtain this message while the admitted callback is being renewed.
		peekCtx, cancelPeek := context.WithTimeout(ctx, 2*time.Second)
		defer cancelPeek()
		response, receiveErr := client.ReceiveMessage(peekCtx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(queue), MaxNumberOfMessages: 1,
		})
		if receiveErr != nil {
			return nil, receiveErr
		}
		if len(response.Messages) != 0 || renewed.Load() == 0 {
			t.Errorf("active visibility not retained: second receiver=%d renewals=%d", len(response.Messages), renewed.Load())
		}
		return broker.Handled{}, nil
	}, broker.WithKeepAlive(100*time.Millisecond))
	if err = consumer.Run(runCtx, handler); !cancellationOnly(err) {
		t.Fatalf("native renewal run failed: %v", err)
	}
	assertRoundTripEmpty(t, ctx, client, queue)
}

func visibilityPublish(t *testing.T, ctx context.Context, client *awssqs.Client, queue string) {
	t.Helper()
	publisher, err := sqs.NewPublisher(client, sqs.PublisherConfig{QueueURL: queue, Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(ctx, broker.Message{ID: "order-retry-1", Body: []byte(`{"amount":42}`)}); err != nil {
		t.Fatal(err)
	}
}

func cancellationOnly(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationOnly(cause) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return cancellationOnly(cause)
	}
	return false
}
