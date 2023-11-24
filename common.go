package broker

import (
	"context"
)

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source common.go -destination ./mock/common.go

// Handler is used to process messages via a subscription of a topic.
// The handler is passed a publication interface which contains the
// message and optional Ack method to acknowledge receipt of the message.
type Handler func(Event) error

// Middleware defines a function type that takes a Handler and returns a modified Handler.
// It is used to intercept and optionally modify the behavior of the Handler function.
// This can include pre-processing or post-processing steps, logging, error handling,
// authentication checks, or any other form of message or request manipulation.
//
// Middleware functions can be chained together to create a pipeline of handlers
// that process an Event before it reaches the final Handler. Each Middleware
// function in the chain is responsible for calling the next Middleware or the
// final Handler, allowing for flexible and customizable processing.
//
// Example:
//
//	func MyMiddleware(next Handler) Handler {
//	    return func(e Event) error {
//	        // Pre-processing logic here
//	        err := next(e)
//	        // Post-processing logic here
//	        return err
//	    }
//	}
type Middleware func(Handler) Handler

type Message struct {
	// ID must uniquely identify the message
	ID string
	// Header includes additional service data
	Header Header
	// Body is message payload
	Body []byte

	ctx context.Context
}

// NewMessage initializes message
func NewMessage() *Message {
	return NewMessageWithContext(context.Background())
}

// NewMessageWithContext initializes message with context
func NewMessageWithContext(ctx context.Context) *Message {
	return &Message{
		Header: make(Header),
		ctx:    ctx,
	}
}

func (m *Message) Context() context.Context {
	if m.ctx == nil {
		m.ctx = context.Background()
	}
	return m.ctx
}

func (m *Message) SetContext(ctx context.Context) *Message {
	if ctx == nil {
		panic("nil context")
	}
	m.ctx = ctx
	return m
}

// Event is given to a subscription handler for processing
type Event interface {
	Topic() string
	Message() *Message
	Ack() error
}

// ErrorHandler is used in order to handle errors
type ErrorHandler func(err error, sub Subscription)

// Logger abstracts the logging functionality
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

func SetIDHeader(message *Message) {
	if message.Header == nil {
		message.Header = make(map[string]string)
	}
	if _, ok := message.Header["id"]; !ok && message.ID != "" {
		message.Header["id"] = message.ID
	}
}
