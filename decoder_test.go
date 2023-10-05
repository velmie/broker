package broker_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/velmie/broker"
	mock_broker "github.com/velmie/broker/mock"
)

func TestCreateDecodingHandler(t *testing.T) {
	type testGreetingMessage struct {
		Greeting string `json:"greeting"`
		Name     string `json:"name"`
	}

	consumer := func(ctx context.Context, message *testGreetingMessage) error {
		require.Equal(t, "Hello", message.Greeting)
		require.Equal(t, "Test", message.Name)
		return nil
	}

	ctrl := gomock.NewController(t)

	jsonDecoder := broker.DecoderFunc(json.Unmarshal)
	handler := broker.CreateDecodingHandler(jsonDecoder, consumer)

	message := &broker.Message{
		ID:   "123",
		Body: []byte(`{"greeting":"Hello","name":"Test"}`),
	}

	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(message).AnyTimes()
	event.EXPECT().Topic().Return("test_topic").AnyTimes()

	err := handler(event)
	require.NoError(t, err)
}
