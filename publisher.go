package broker

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source publisher.go -destination ./mock/publisher.go

// Publisher allows publishing to a specific topic
type Publisher interface {
	Publish(topic string, message *Message) error
}
