package azuresb

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
)

func TestPublisherObserverGetsOriginalFailureAndDoesNotOwnSender(t *testing.T) {
	marker := errors.New("sensitive native failure")
	var observed Observation
	calls := 0
	sender := nativeSenderFunc(func(context.Context, *azservicebus.Message, *azservicebus.SendMessageOptions) error {
		calls++
		return marker
	})
	observer := &Observer{Record: func(_ context.Context, event Observation) { observed = event }}
	publisher, err := NewPublisher(sender, PublisherConfig{Source: "orders", Observer: observer})
	if err != nil {
		t.Fatal(err)
	}
	observer.Record = nil
	err = publisher.Publish(context.Background(), broker.Message{ID: "event", Body: []byte("body")})
	if !errors.Is(err,
		marker) ||
		!errors.Is(observed.Err,
			marker) ||
		observed.Source != "orders" ||
		observed.Operation != OperationPublish ||
		observed.Outcome != OutcomeUnknown {
		t.Fatalf("original diagnostic cause lost: %v %+v", err, observed)
	}
	if err = sender.SendMessage(context.Background(), &azservicebus.Message{}, nil); !errors.Is(err, marker) || calls != 2 {
		t.Fatal("borrowed native sender unusable")
	}
}
