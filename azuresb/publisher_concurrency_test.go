package azuresb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestPublisherConcurrentReadOnlyAndOriginalNativeFailure(t *testing.T) {
	marker := errors.New("native failure")
	var calls atomic.Int32
	sender := nativeSenderFunc(func(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
		calls.Add(1)
		m.Body[0] = 'x'
		return marker
	})
	publisher, err := NewPublisher(sender, PublisherConfig{Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	input := broker.Message{ID: "event", Body: []byte("body")}
	results := make(chan error, 4)
	for i := 0; i < cap(results); i++ {
		go func() { results <- publisher.Publish(context.Background(), input) }()
	}
	for i := 0; i < cap(results); i++ {
		err = <-results
		var operation *OperationError
		if !errors.Is(err, marker) || !errors.As(err, &operation) || operation.Source != "orders" {
			t.Fatalf("native cause/source: %v", err)
		}
	}
	if calls.Load() != 4 || string(input.Body) != "body" {
		t.Fatal("concurrent input mutated or sends lost")
	}
}

func TestPublisherCancellationAndFiniteOperationBudget(t *testing.T) {
	sender := nativeSenderFunc(func(ctx context.Context, _ *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
		<-ctx.Done()
		return ctx.Err()
	})
	publisher, err := NewPublisher(sender, PublisherConfig{Source: "orders", OperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(context.Background(),
		broker.Message{ID: "event",
			Body: []byte("body")}); !errors.Is(err,
		context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = publisher.Publish(ctx, broker.Message{ID: "event"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
}
