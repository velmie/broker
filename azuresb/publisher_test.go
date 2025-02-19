package azuresb_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/require"
	"github.com/velmie/broker"

	"github.com/velmie/broker/azuresb"
)

type fakeASBSender struct {
	sendCalled bool
	sentMsg    *azservicebus.Message
	sendErr    error
	lastCtx    context.Context
}

func (f *fakeASBSender) SendMessage(ctx context.Context, msg *azservicebus.Message, opts *azservicebus.SendMessageOptions) error {
	f.sendCalled = true
	f.sentMsg = msg
	f.lastCtx = ctx
	return f.sendErr
}

type fakeSenderFactory struct {
	createSenderFunc func(topic string) (azuresb.ASBSender, error)
	createCount      int
	mu               sync.Mutex
}

func (f *fakeSenderFactory) CreateSender(topic string) (azuresb.ASBSender, error) {
	f.mu.Lock()
	f.createCount++
	f.mu.Unlock()
	return f.createSenderFunc(topic)
}

func anyMapToHeader(in map[string]any) broker.Header {
	out := make(broker.Header, len(in))
	for k, v := range in {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func TestPublisherPublishSuccess(t *testing.T) {
	fakeSender := &fakeASBSender{}
	factory := &fakeSenderFactory{
		createSenderFunc: func(topic string) (azuresb.ASBSender, error) {
			return fakeSender, nil
		},
	}

	pub := azuresb.NewPublisher(factory)
	topic := "test-topic"
	msgID := "msg-123"

	message := broker.NewMessage()
	message.Body = []byte("test-body")
	message.ID = msgID
	message.Header = broker.Header{}

	err := pub.Publish(topic, message)
	require.NoError(t, err)

	require.True(t, fakeSender.sendCalled, "expected SendMessage to be called")
	require.NotNil(t, fakeSender.sentMsg)

	require.Equal(t, []byte("test-body"), fakeSender.sentMsg.Body)
	require.NotNil(t, fakeSender.sentMsg.MessageID)
	require.Equal(t, msgID, *fakeSender.sentMsg.MessageID)

	expectedHeader := message.Header
	require.Equal(t, expectedHeader, anyMapToHeader(fakeSender.sentMsg.ApplicationProperties))
}

func TestPublisherPublishSenderError(t *testing.T) {
	fakeSender := &fakeASBSender{
		sendErr: errors.New("send failed"),
	}
	factory := &fakeSenderFactory{
		createSenderFunc: func(topic string) (azuresb.ASBSender, error) {
			return fakeSender, nil
		},
	}
	pub := azuresb.NewPublisher(factory)
	topic := "test-topic"

	message := broker.NewMessage()
	message.Body = []byte("body")
	message.ID = "id1"
	message.Header = broker.Header{}

	err := pub.Publish(topic, message)
	require.Error(t, err)
	require.Contains(t, err.Error(), "azuresb: cannot send message")
}

func TestPublisherCreateSenderError(t *testing.T) {
	factory := &fakeSenderFactory{
		createSenderFunc: func(topic string) (azuresb.ASBSender, error) {
			return nil, errors.New("create failed")
		},
	}
	pub := azuresb.NewPublisher(factory)
	topic := "test-topic"

	message := broker.NewMessage()
	message.Body = []byte("body")
	message.ID = "id1"
	message.Header = broker.Header{}

	err := pub.Publish(topic, message)
	require.Error(t, err)
	require.Contains(t, err.Error(), "azuresb: cannot create sender")
}

func TestPublisherCaching(t *testing.T) {
	callCount := 0
	fakeSender := &fakeASBSender{}
	factory := &fakeSenderFactory{
		createSenderFunc: func(topic string) (azuresb.ASBSender, error) {
			callCount++
			return fakeSender, nil
		},
	}

	pub := azuresb.NewPublisher(factory)
	topic := "test-topic"

	message1 := broker.NewMessage()
	message1.Body = []byte("body1")
	message1.ID = "id1"
	message1.Header = broker.Header{}
	err := pub.Publish(topic, message1)
	require.NoError(t, err)

	message2 := broker.NewMessage()
	message2.Body = []byte("body2")
	message2.ID = "id2"
	message2.Header = broker.Header{}
	err = pub.Publish(topic, message2)
	require.NoError(t, err)

	require.Equal(t, 1, callCount, "expected CreateSender to be called only once for the same topic")
}

func TestPublisherCustomContext(t *testing.T) {
	fakeSender := &fakeASBSender{}
	factory := &fakeSenderFactory{
		createSenderFunc: func(topic string) (azuresb.ASBSender, error) {
			return fakeSender, nil
		},
	}
	pub := azuresb.NewPublisher(factory)
	topic := "test-topic"

	customCtx := context.WithValue(context.Background(), "key", "value")
	message := broker.NewMessageWithContext(customCtx)
	message.Body = []byte("body")
	message.ID = "id"
	message.Header = broker.Header{}

	err := pub.Publish(topic, message)
	require.NoError(t, err)

	require.NotNil(t, fakeSender.lastCtx)
	require.Equal(t, "value", fakeSender.lastCtx.Value("key"))
}
