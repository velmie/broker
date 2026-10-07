package broker

// Delivery is valid during its callback. Message returns detached data that may
// be retained. Operational interfaces must not escape the callback lifetime.
type Delivery interface {
	Message() Message
	Source() string
}

// NewDelivery wraps an application-owned message and source for direct handling.
// It shares the message's body, headers and header values without copying them.
// Callers must not mutate that storage concurrently with handling. The delivery
// exposes no transport attempt information or adapter-specific metadata.
func NewDelivery(message Message, source string) Delivery {
	return &messageDelivery{message: message, source: source}
}

// Attempt is a one-based transport delivery count within an adapter-defined
// scope. Number must be ignored when Known is false.
type Attempt struct {
	Number uint64
	Known  bool
}

// AttemptReader exposes optional transport metadata, not business call counts.
type AttemptReader interface{ Attempt() Attempt }

type messageDelivery struct {
	message Message
	source  string
}

func (d *messageDelivery) Message() Message { return d.message }
func (d *messageDelivery) Source() string   { return d.source }
