package broker

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source publisher.go -destination ./mock/publisher.go

// Publisher defines an interface for publishing messages to a specific topic.
// Implementations of this interface should handle the logic for sending messages
// to the designated topic.
type Publisher interface {
	// Publish sends a message to the specified topic.
	Publish(topic string, message *Message) error
}

// PublisherMiddleware defines a function type used for creating middleware for a Publisher.
// Middleware can be used to add additional functionality like logging, metrics, or error handling
// to the publish process.
type PublisherMiddleware func(next Publisher) Publisher

// PublisherFunc is an adapter to allow the use of ordinary functions as Publishers.
type PublisherFunc func(topic string, message *Message) error

// Publish calls f(topic, message), effectively invoking the function wrapped by the PublisherFunc type.
func (f PublisherFunc) Publish(topic string, message *Message) error {
	return f(topic, message)
}
