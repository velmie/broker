package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/velmie/broker"
	mock_broker "github.com/velmie/broker/mock"
)

type requestMessage struct {
	Name string `json:"name"`
}

type responseMessage struct {
	Greeting string `json:"greeting"`
}

func TestReplyHandler(t *testing.T) {
	dec := broker.DecoderFunc(json.Unmarshal)
	enc := broker.EncoderFunc(json.Marshal)

	tests := []struct {
		name         string
		inputMessage *broker.Message
		publishFunc  func(topic string, msg *broker.Message) error
		consumer     func(ctx context.Context, msg *requestMessage) (*responseMessage, error)
		wantErr      bool
	}{
		{
			name: "success",
			inputMessage: &broker.Message{
				ID: "test-message-id",
				Header: broker.Header{
					broker.HdrReplyTo: "test-topic",
				},
				Body: []byte(`{"name":"Gopher"}`),
			},
			publishFunc: func(_ string, msg *broker.Message) error {
				require.Equal(t, string(msg.Body), `{"greeting":"Hello Gopher"}`)
				return nil
			},
			consumer: func(_ context.Context, msg *requestMessage) (*responseMessage, error) {
				return &responseMessage{Greeting: "Hello " + msg.Name}, nil
			},
			wantErr: false,
		},
		{
			name: "consumer_error",
			inputMessage: &broker.Message{
				ID: "test-message-id",
				Header: broker.Header{
					broker.HdrReplyTo: "test-topic",
				},
				Body: []byte(`{"name":"Gopher"}`),
			},
			publishFunc: nil,
			consumer: func(_ context.Context, msg *requestMessage) (*responseMessage, error) {
				return nil, errors.New("error in consumer")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			pub := mock_broker.NewMockPublisher(ctrl)
			if tt.publishFunc != nil {
				pub.EXPECT().
					Publish("test-topic", gomock.Any()).
					DoAndReturn(tt.publishFunc)
			}

			event := mock_broker.NewMockEvent(ctrl)
			event.EXPECT().Topic().Return("test").AnyTimes()
			event.EXPECT().Message().Return(tt.inputMessage).AnyTimes()

			h := broker.CreateReplyHandler(
				dec,
				enc,
				pub,
				tt.consumer,
			)

			err := h(event)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
