// Package broker provides utilities for message brokering and header manipulation.
package broker

import "strconv"

const (
	// HdrReplyTo is the constant used to represent the header key for the reply topic.
	HdrReplyTo    = "Reply-To"
	HdrReplyMsgID = "Reply-Message-Id"
	HdrCreatedAt  = "Created-At"
	// HdrCorrelationID is the unique identifier used to track and correlate messages as they flow through a system
	HdrCorrelationID = "Correlation-Id"
	// HdrInstanceID is the constant used to represent the header key for the instance identifier.
	// This identifier is used to uniquely identify a specific instance of a service or application
	// that generates or handles the message, often useful in distributed systems for tracing and diagnostics.
	HdrInstanceID = "Instance-Id"
)

// Header represents a set of key-value pairs
type Header map[string]string

// Get retrieves the value associated with the provided key from the header.
// Returns an empty string if the key does not exist.
func (h Header) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

// Set assigns the provided value to the provided key in the header.
func (h Header) Set(key, value string) {
	h[key] = value
}

// SetReplyTo sets the reply topic in the header using a predefined key.
func (h Header) SetReplyTo(topic string) {
	h.Set(HdrReplyTo, topic)
}

// GetReplyTo retrieves the reply topic value from the header.
// Returns an empty string if the reply topic is not set.
func (h Header) GetReplyTo() string {
	return h.Get(HdrReplyTo)
}

// SetReplyMessageID sets the reply message id in the header using a predefined key.
func (h Header) SetReplyMessageID(id string) {
	h.Set(HdrReplyMsgID, id)
}

// GetReplyMessageID retrieves the reply message id value from the header.
// Returns an empty string if the reply message id is not set.
func (h Header) GetReplyMessageID() string {
	return h.Get(HdrReplyMsgID)
}

// SetCreatedAt sets the creation timestamp (unix time) in the header using a predefined key.
func (h Header) SetCreatedAt(timestamp int64) {
	h.Set(HdrCreatedAt, strconv.FormatInt(timestamp, 10))
}

// SetCorrelationID sets the correlation id in the header using a predefined key.
func (h Header) SetCorrelationID(id string) {
	h.Set(HdrCorrelationID, id)
}

// GetCorrelationID retrieves the correlation id value from the header.
// Returns an empty string if the correlation id is not set.
func (h Header) GetCorrelationID() string {
	return h.Get(HdrCorrelationID)
}

// GetCreatedAt retrieves the creation timestamp from the header.
// Returns an empty string if the creation timestamp is not set.
func (h Header) GetCreatedAt() int64 {
	v := h.Get(HdrCreatedAt)
	if v == "" {
		return 0
	}
	timestamp, _ := strconv.ParseInt(v, 10, 64)
	return timestamp
}

// SetInstanceID sets the instance identifier in the header using a predefined key.
// This identifier is used to mark the specific instance of a service or application that processed the message.
func (h Header) SetInstanceID(id string) {
	h.Set(HdrInstanceID, id)
}

// GetInstanceID retrieves the instance identifier value from the header.
// Returns an empty string if the instance identifier is not set.
func (h Header) GetInstanceID() string {
	return h.Get(HdrInstanceID)
}
