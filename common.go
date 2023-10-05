package broker

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source common.go -destination ./mock/common.go

// Handler is used to process messages via a subscription of a topic.
// The handler is passed a publication interface which contains the
// message and optional Ack method to acknowledge receipt of the message.
type Handler func(Event) error

type Message struct {
	// ID must uniquely identify the message
	ID string
	// Header includes additional service data
	Header map[string]string
	// Body is message payload
	Body []byte
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
