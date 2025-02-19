package azuresb_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/velmie/broker"

	"github.com/velmie/broker/azuresb"
)

type fakeReceiver struct {
	receiveFunc func(ctx context.Context) (*azuresb.Message, error)
	closeFunc   func() error
}

func (fr *fakeReceiver) Receive(ctx context.Context) (*azuresb.Message, error) {
	if fr.receiveFunc != nil {
		return fr.receiveFunc(ctx)
	}
	return nil, nil
}

func (fr *fakeReceiver) Close() error {
	if fr.closeFunc != nil {
		return fr.closeFunc()
	}
	return nil
}

type fakeReceiverFactory struct {
	receiver azuresb.Receiver
	err      error
}

func (f *fakeReceiverFactory) CreateReceiver(topic string) (azuresb.Receiver, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.receiver, nil
}

func TestMessage_CompleteAndAbandon(t *testing.T) {
	t.Run("Complete calls completeFunc", func(t *testing.T) {
		completeCalled := false
		msg := azuresb.NewMessage([]byte("test"), nil, "id",
			func() error {
				completeCalled = true
				return nil
			},
			nil,
		)
		if err := msg.Complete(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !completeCalled {
			t.Error("completeFunc was not called")
		}
	})

	t.Run("Abandon calls abandonFunc", func(t *testing.T) {
		abandonCalled := false
		msg := azuresb.NewMessage([]byte("test"), nil, "id",
			nil,
			func() error {
				abandonCalled = true
				return nil
			},
		)
		if err := msg.Abandon(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !abandonCalled {
			t.Error("abandonFunc was not called")
		}
	})

	t.Run("Complete with nil completeFunc returns nil", func(t *testing.T) {
		msg := azuresb.NewMessage([]byte("test"), nil, "id", nil, nil)
		if err := msg.Complete(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Abandon with nil abandonFunc returns nil", func(t *testing.T) {
		msg := azuresb.NewMessage([]byte("test"), nil, "id", nil, nil)
		if err := msg.Abandon(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestSubscriber_Subscribe_HandlerSuccess(t *testing.T) {
	var completeCalled bool

	fakeMsg := azuresb.NewMessage([]byte("payload"), map[string]string{"foo": "bar"}, "msg-123",
		func() error {
			completeCalled = true
			return nil
		},
		nil,
	)

	msgReturned := false
	fr := &fakeReceiver{
		receiveFunc: func(ctx context.Context) (*azuresb.Message, error) {
			if !msgReturned {
				msgReturned = true
				return fakeMsg, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		closeFunc: func() error {
			return nil
		},
	}

	factory := &fakeReceiverFactory{receiver: fr}
	subscriber := azuresb.NewSubscriber(factory)

	handlerCalled := make(chan struct{})
	handler := func(e broker.Event) error {
		if e.Topic() != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %s", e.Topic())
		}
		msg := e.Message()
		if string(msg.Body) != "payload" {
			t.Errorf("expected payload 'payload', got %s", msg.Body)
		}
		close(handlerCalled)
		return nil
	}

	sub, err := subscriber.Subscribe("test-topic", handler, func(o *broker.SubscribeOptions) {
		o.AutoAck = true
	})
	if err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	select {
	case <-handlerCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("handler was not called")
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if !completeCalled {
		t.Error("expected complete to be called (auto ack), but it was not")
	}
}

func TestSubscriber_Subscribe_HandlerError(t *testing.T) {
	var abandonCalled bool

	fakeMsg := azuresb.NewMessage([]byte("error-payload"), map[string]string{"key": "val"}, "msg-error",
		nil,
		func() error {
			abandonCalled = true
			return nil
		},
	)

	msgReturned := false
	fr := &fakeReceiver{
		receiveFunc: func(ctx context.Context) (*azuresb.Message, error) {
			if !msgReturned {
				msgReturned = true
				return fakeMsg, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		closeFunc: func() error {
			return nil
		},
	}
	factory := &fakeReceiverFactory{receiver: fr}
	subscriber := azuresb.NewSubscriber(factory)

	handlerCalled := make(chan struct{})
	handler := func(e broker.Event) error {
		close(handlerCalled)
		return errors.New("handler error")
	}

	sub, err := subscriber.Subscribe("test-topic", handler, func(o *broker.SubscribeOptions) {
		o.AutoAck = true
	})
	if err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	select {
	case <-handlerCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("handler was not called")
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if !abandonCalled {
		t.Error("expected abandon to be called due to handler error, but it was not")
	}
}

func TestSubscription_UnsubscribeAndMethods(t *testing.T) {
	fr := &fakeReceiver{
		receiveFunc: func(ctx context.Context) (*azuresb.Message, error) {
			return nil, context.Canceled
		},
		closeFunc: func() error {
			return nil
		},
	}
	factory := &fakeReceiverFactory{receiver: fr}
	subscriber := azuresb.NewSubscriber(factory)

	handler := func(e broker.Event) error { return nil }

	sub, err := subscriber.Subscribe("test-topic", handler)
	if err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if sub.Topic() != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", sub.Topic())
	}
	if sub.Handler() == nil {
		t.Error("expected non-nil handler")
	}
	if sub.Options() == nil {
		t.Error("expected non-nil subscribe options")
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}

	select {
	case <-sub.Done():
	case <-time.After(1 * time.Second):
		t.Error("Done channel was not closed after unsubscribe")
	}
}

func TestSubscription_MultipleUnsubscribe(t *testing.T) {
	fr := &fakeReceiver{
		receiveFunc: func(ctx context.Context) (*azuresb.Message, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		closeFunc: func() error {
			return nil
		},
	}
	factory := &fakeReceiverFactory{receiver: fr}
	subscriber := azuresb.NewSubscriber(factory)

	handler := func(e broker.Event) error { return nil }

	sub, err := subscriber.Subscribe("test-topic", handler)
	if err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("Unsubscribe returned error: %v", err)
			}
		}()
	}
	wg.Wait()
}
