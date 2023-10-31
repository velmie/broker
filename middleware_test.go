package broker_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/velmie/broker"
	mock_broker "github.com/velmie/broker/mock"
)

func TestPanicRecoveryMiddleware(t *testing.T) {
	tests := []struct {
		name          string
		handler       broker.Handler
		expectPanic   bool
		expectErr     bool
		expectedError string
	}{
		{
			name: "no_panic",
			handler: func(e broker.Event) error {
				return nil
			},
			expectPanic: false,
			expectErr:   false,
		},
		{
			name: "panic_with_string",
			handler: func(e broker.Event) error {
				panic("test panic")
			},
			expectPanic:   true,
			expectErr:     true,
			expectedError: "panic recovered: test panic",
		},
		{
			name: "panic_with_error",
			handler: func(e broker.Event) error {
				panic(errors.New("panic error"))
			},
			expectPanic:   true,
			expectErr:     true,
			expectedError: "panic recovered: panic error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware := broker.PanicRecoveryMiddleware()
			wrappedHandler := middleware(tt.handler)

			err := wrappedHandler(nil) // nil - event is not used

			if tt.expectErr {
				require.Error(t, err)
				if tt.expectPanic {
					require.True(t, strings.Contains(err.Error(), tt.expectedError), "error must contain panic message")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLoggingMiddleware(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := mock_broker.NewMockLogger(ctrl)
	noErrorHandler := func(e broker.Event) error {
		return nil
	}
	handlerWithError := func(err error) func(e broker.Event) error {
		return func(e broker.Event) error {
			return err
		}
	}

	const (
		messageID = "test-message-id"
		topic     = "test-topic"
	)
	msg := &broker.Message{
		ID:     messageID,
		Header: broker.Header{"test": "value"},
		Body:   []byte("test-data"),
	}
	event := mock_broker.NewMockEvent(ctrl)
	event.EXPECT().Message().Return(msg).AnyTimes()
	event.EXPECT().Topic().Return(topic).AnyTimes()

	tests := []struct {
		name       string
		middleware broker.Middleware
		wantLog    func()
		handler    func(e broker.Event) error
	}{
		{
			name:       "success_no_options",
			middleware: broker.LoggingMiddleware(logger),
			wantLog: func() {
				logger.
					EXPECT().
					Info(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
					)
			},
		},
		{
			name:       "error_logged",
			middleware: broker.LoggingMiddleware(logger),
			wantLog: func() {
				logger.
					EXPECT().
					Error(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"error", "test error",
					)
			},
			handler: handlerWithError(errors.New("test error")),
		},
		{
			name: "body_logged_on_error",
			middleware: broker.LoggingMiddleware(
				logger,
				broker.WithLogBodyOnError(true),
				broker.WithLogError(false), // disable error logging
			),
			wantLog: func() {
				logger.
					EXPECT().
					Error(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"body", string(msg.Body),
					)
			},
			handler: handlerWithError(errors.New("test error")),
		},
		{
			name: "body_always_logged",
			middleware: broker.LoggingMiddleware(
				logger,
				broker.WithLogBody(true),
			),
			wantLog: func() {
				logger.
					EXPECT().
					Info(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"body", string(msg.Body),
					)
			},
		},
		{
			name: "header_logged",
			middleware: broker.LoggingMiddleware(
				logger,
				broker.WithLogHeader(true),
			),
			wantLog: func() {
				logger.
					EXPECT().
					Info(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"header", fmt.Sprintf("%+v", msg.Header),
					)
			},
		},
		{
			name: "header_logged_custom_func",
			middleware: broker.LoggingMiddleware(
				logger,
				broker.WithLogHeader(true),
				broker.WithLogHeaderFunc(func(e broker.Event) string {
					return "custom header formatting"
				}),
			),
			wantLog: func() {
				logger.
					EXPECT().
					Info(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"header", "custom header formatting",
					)
			},
		},
		{
			name: "body_logged_custom_func",
			middleware: broker.LoggingMiddleware(
				logger,
				broker.WithLogBody(true),
				broker.WithLogBodyFunc(func(e broker.Event) string {
					return "custom body formatting"
				}),
			),
			wantLog: func() {
				logger.
					EXPECT().
					Info(
						"event processed",
						"messageId", messageID,
						"topic", topic,
						"duration", gomock.Any(),
						"body", "custom body formatting",
					)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantLog()
			handler := tt.handler
			if handler == nil {
				handler = noErrorHandler
			}
			mwHandler := tt.middleware(handler)

			_ = mwHandler(event)
		})
	}
}
