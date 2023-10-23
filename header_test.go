package broker_test

import (
	"testing"
	"time"

	. "github.com/velmie/broker"
)

func TestHeaderGetSet(t *testing.T) {
	h := make(Header)

	key := "test-key"
	value := "test-value"

	h.Set(key, value)
	if got := h.Get(key); got != value {
		t.Errorf("Get() = %v; want %v", got, value)
	}
}

func TestHeaderNonexistentKey(t *testing.T) {
	h := Header{}
	if got := h.Get("nonexistent"); got != "" {
		t.Errorf("expected empty string for nonexistent key, got %v", got)
	}
}

func TestHeaderSetReplyTopic(t *testing.T) {
	h := Header{}

	topic := "test-topic"
	h.SetReplyTopic(topic)
	if got := h.GetReplyTopic(); got != topic {
		t.Errorf("GetReplyTopic() = %v; want %v", got, topic)
	}
}

func TestHeaderGetReplyTopicWithoutSetting(t *testing.T) {
	h := Header{}
	if got := h.GetReplyTopic(); got != "" {
		t.Errorf("expected empty string for GetReplyTopic() without setting, got %v", got)
	}
}

func TestHeaderOverwriteValue(t *testing.T) {
	h := Header{}

	key := "key"
	value1 := "value1"
	value2 := "value2"

	h.Set(key, value1)
	h.Set(key, value2)
	if got := h.Get(key); got != value2 {
		t.Errorf("after overwriting, Get() = %v; want %v", got, value2)
	}
}

func TestHeaderSetGetCreatedAt(t *testing.T) {
	h := Header{}

	timestamp := time.Now().Unix()
	h.SetCreatedAt(timestamp)

	if got := h.GetCreatedAt(); got != timestamp {
		t.Errorf("GetCreatedAt() = %v; want %v", got, timestamp)
	}
}

func TestHeaderGetCreatedAtWithoutSetting(t *testing.T) {
	h := Header{}
	if got := h.GetCreatedAt(); got != 0 {
		t.Errorf("expected 0 for GetCreatedAt() without setting, got %v", got)
	}
}

func TestHeaderInvalidCreatedAtValue(t *testing.T) {
	h := Header{}
	h.Set("created-at", "invalidTimestamp")

	if got := h.GetCreatedAt(); got != 0 {
		t.Errorf("expected 0 for GetCreatedAt() with invalid timestamp, got %v", got)
	}
}
