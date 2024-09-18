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

// Test for Pointer Type with CorrelationID
func TestCreateHandler_PointerType_CorrelationID(t *testing.T) {
	consumer := func(ctx context.Context, message *testGreetingMessage) error {
		require.Equal(t, "Hello", message.Greeting)
		require.Equal(t, "Test", message.Name)
		require.Equal(t, "correlation-id-123", message.CorrelationID)
		return nil
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonDecoder := broker.DecoderFunc(json.Unmarshal)
	handler := broker.CreateHandler(jsonDecoder, consumer)

	message := &broker.Message{
		ID:   "123",
		Body: []byte(`{"greeting":"Hello","name":"Test"}`),
		Header: broker.Header{
			broker.HdrCorrelationID: "correlation-id-123",
		},
	}

	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(message).AnyTimes()
	event.EXPECT().Topic().Return("test_topic").AnyTimes()

	err := handler(event)
	require.NoError(t, err)
}

// Test for Non-Pointer Type with CorrelationID
func TestCreateHandler_NonPointerType_CorrelationID(t *testing.T) {
	consumer := func(ctx context.Context, message testGreetingMessage) error {
		require.Equal(t, "Hello", message.Greeting)
		require.Equal(t, "Test", message.Name)
		require.Equal(t, "correlation-id-123", message.CorrelationID)
		return nil
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonDecoder := broker.DecoderFunc(json.Unmarshal)
	handler := broker.CreateHandler(jsonDecoder, consumer)

	message := &broker.Message{
		ID:   "123",
		Body: []byte(`{"greeting":"Hello","name":"Test"}`),
		Header: broker.Header{
			broker.HdrCorrelationID: "correlation-id-123",
		},
	}

	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(message).AnyTimes()
	event.EXPECT().Topic().Return("test_topic").AnyTimes()

	err := handler(event)
	require.NoError(t, err)
}

// Test for Pointer Type without CorrelationID
func TestCreateHandler_PointerType_NoCorrelationID(t *testing.T) {
	consumer := func(ctx context.Context, message *testGreetingMessage) error {
		require.Equal(t, "", message.CorrelationID)
		return nil
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonDecoder := broker.DecoderFunc(json.Unmarshal)
	handler := broker.CreateHandler(jsonDecoder, consumer)

	message := &broker.Message{
		ID:     "123",
		Body:   []byte(`{"greeting":"Hello","name":"Test"}`),
		Header: broker.Header{},
	}

	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(message).AnyTimes()

	err := handler(event)
	require.NoError(t, err)
}

// Test for Non-Pointer Type without CorrelationID
func TestCreateHandler_NonPointerType_NoCorrelationID(t *testing.T) {
	consumer := func(ctx context.Context, message testGreetingMessage) error {
		require.Equal(t, "", message.CorrelationID)
		return nil
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jsonDecoder := broker.DecoderFunc(json.Unmarshal)
	handler := broker.CreateHandler(jsonDecoder, consumer)

	message := &broker.Message{
		ID:     "123",
		Body:   []byte(`{"greeting":"Hello","name":"Test"}`),
		Header: broker.Header{},
	}

	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(message).AnyTimes()

	err := handler(event)
	require.NoError(t, err)
}

type testGreetingMessage struct {
	Greeting      string `json:"greeting"`
	Name          string `json:"name"`
	CorrelationID string
}

func (m *testGreetingMessage) SetCorrelationID(id string) {
	m.CorrelationID = id
}
