// Package broker provides utilities for message brokering and header manipulation.
package broker

import "strconv"

const (
	// HdrReplyTopic is the constant used to represent the header key for the reply topic.
	HdrReplyTopic = "reply-topic"
	HdrReplyMsgID = "reply-message-id"
	HdrCreatedAt  = "created-at"
)

// Header represents a set of key-value pairs
type Header map[string]string

// Get retrieves the value associated with the provided key from the header.
// Returns an empty string if the key does not exist.
func (h Header) Get(key string) string {
	return h[key]
}

// Set assigns the provided value to the provided key in the header.
func (h Header) Set(key, value string) {
	h[key] = value
}

// SetReplyTopic sets the reply topic in the header using a predefined key.
func (h Header) SetReplyTopic(topic string) {
	h.Set(HdrReplyTopic, topic)
}

// GetReplyTopic retrieves the reply topic value from the header.
// Returns an empty string if the reply topic is not set.
func (h Header) GetReplyTopic() string {
	return h.Get(HdrReplyTopic)
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
