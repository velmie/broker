package natsjs

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestDetachedDataDoesNotShareSDKStorage(t *testing.T) {
	native := &nats.Msg{
		Sub:     &nats.Subscription{},
		Reply:   "$JS.ACK.ORDERS.worker.1.2.3.1.0",
		Subject: "orders.created",
		Data:    []byte("payload"), Header: nats.Header{"X": []string{"first", "second"}},
	}
	delivery, err := detach(native, "ORDERS/worker")
	if err != nil {
		t.Fatal(err)
	}
	native.Data[0] = '!'
	native.Header["X"][0] = "changed"
	native.Subject = "another.subject"
	message := delivery.Message()
	if string(message.Body) != "payload" || string(message.Headers[0].Value) != "first" {
		t.Fatalf("SDK mutation reached application: %#v", message)
	}
	message.Body[1] = '!'
	if string(native.Data) != "!ayload" {
		t.Fatal("application mutation reached SDK data")
	}
	if delivery.Subject() != "orders.created" || delivery.Source() != "ORDERS/worker" {
		t.Fatal("SDK subject mutation changed detached subject or source identity")
	}
}

func TestAttemptMetadataNeverSynthesizesKnownCount(t *testing.T) {
	for _, number := range []uint64{0, 1, 7} {
		delivery := &delivery{metadata: nats.MsgMetadata{NumDelivered: number}}
		attempt := delivery.Attempt()
		if attempt.Number != number || attempt.Known != (number != 0) {
			t.Fatalf("count=%d attempt=%+v", number, attempt)
		}
	}
}
