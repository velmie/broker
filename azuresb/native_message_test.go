package azuresb_test

import (
	"errors"
	"testing"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestEncodeMessageNativeOwnership(t *testing.T) {
	message := broker.Message{ID: "logical", Body: []byte{0, 255}, Headers: []broker.Header{{Name: "Trace", Value: []byte("original")}}}
	native, err := adapter.EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	message.Body[0] = 1
	message.Headers[0].Value[0] = 'X'
	message.ID = "changed"
	if native.Body[0] != 0 || native.ApplicationProperties["Trace"] != "original" || *native.MessageID != "logical" {
		t.Fatalf("native message aliases caller data: %+v", native)
	}
	native.Body[1] = 2
	native.ApplicationProperties["Trace"] = "native-change"
	*native.MessageID = "native-id"
	session, subject, contentType := "customer", "created", "application/json"
	native.SessionID = &session
	native.Subject = &subject
	native.ContentType = &contentType
	if message.Body[1] != 255 || string(message.Headers[0].Value) != "Xriginal" || message.ID != "changed" {
		t.Fatalf("native edits changed Broker message: %+v", message)
	}
	if native.ApplicationProperties["id"] != "logical" {
		t.Fatal("native ID edit unexpectedly rewrote logical property")
	}
}

func TestEncodeMessageNativeValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		message broker.Message
		field   string
	}{
		{name: "missing identity", message: broker.Message{}, field: "ID"},
		{name: "invalid identity", message: broker.Message{ID: string([]byte{255})}, field: "ID"},
		{name: "binary property",
			message: broker.Message{ID: "logical", Headers: []broker.Header{{Name: "Trace", Value: []byte{255}}}},
			field:   "ApplicationProperties.Trace"},
		{name: "reserved conflict",
			message: broker.Message{ID: "logical", Headers: []broker.Header{{Name: "id", Value: []byte("different")}}},
			field:   "ApplicationProperties.id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			native, err := adapter.EncodeMessage(test.message)
			var field *adapter.FieldError
			if native != nil || !errors.As(err, &field) || field.Field != test.field || errors.Unwrap(field) == nil {
				t.Fatalf("validation lost field or cause: message=%v err=%v", native, err)
			}
		})
	}
}
