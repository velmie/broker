package broker

// Publisher allows publishing to a specific topic
type Publisher interface {
	Publish(topic string, message *Message) error
}
