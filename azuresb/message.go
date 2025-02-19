package azuresb

// Message represents a message received from Azure Service Bus.
type Message struct {
	Body         []byte
	Header       map[string]string
	ID           string
	completeFunc func() error
	abandonFunc  func() error
}

// NewMessage creates a new Message
func NewMessage(body []byte, header map[string]string, id string, completeFunc, abandonFunc func() error) *Message {
	return &Message{
		Body:         body,
		Header:       header,
		ID:           id,
		completeFunc: completeFunc,
		abandonFunc:  abandonFunc,
	}
}

// Complete completes the message (acknowledges successful processing)
func (m *Message) Complete() error {
	if m.completeFunc != nil {
		return m.completeFunc()
	}
	return nil
}

// Abandon releases the message (indicates that processing failed)
func (m *Message) Abandon() error {
	if m.abandonFunc != nil {
		return m.abandonFunc()
	}
	return nil
}
